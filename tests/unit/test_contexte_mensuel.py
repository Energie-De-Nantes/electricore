"""Tests unitaires du module de build `contexte_mensuel`.

Couvre `ContexteMensuel` + `charger()` — la composition pure des pipelines
de facturation pour un mois donné. Les loaders restent à la charge de l'appelant
(adapter ERP), conformément à la nouvelle topologie ; le module
de build ne déclenche jamais d'I/O.

Stratégie : on stub le helper privé `_composer` (la séquence des 4 pipelines)
pour isoler la valeur ajoutée de `charger()` (résolution du mois + emballage
en `ContexteMensuel`). Le chemin pipeline réel reste couvert par les tests
des pipelines individuels et le smoke test CTA d'intégration.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from electricore.core.builds.contexte_mensuel import ContexteMensuel, charger

TZ = ZoneInfo("Europe/Paris")


def _make_stub_composition(
    *, debuts_mois: list[datetime]
) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
    """Construit le tuple de 5 LazyFrames renvoyé par `_composer`.

    `debuts_mois` peuple `facturation_mensuelle.debut` — c'est l'unique colonne
    dont `charger()` se sert pour résoudre le mois par défaut. Depuis ADR-0019
    (#110), `pipeline_facturation` retourne un LazyFrame ; la matérialisation
    est portée par `charger()` au boundary du build.
    """
    facturation_lf = pl.LazyFrame({"debut": debuts_mois}).with_columns(
        pl.col("debut").dt.replace_time_zone("Europe/Paris")
    )
    return (
        pl.LazyFrame({"sentinel": [1]}),  # historique_enrichi
        pl.LazyFrame({"sentinel": [2]}),  # abonnements
        pl.LazyFrame({"sentinel": [3]}),  # energie
        pl.LazyFrame({"sentinel": [4]}),  # releves_utilises (journal #233)
        facturation_lf,
    )


@pytest.fixture
def stub_composer(monkeypatch):
    """Remplace `_composer` par un stub configurable.

    Retourne une fonction qui prend le tuple à renvoyer.
    """

    def _install(resultat):
        monkeypatch.setattr(
            "electricore.core.builds.contexte_mensuel._composer",
            lambda historique, releves, horizon: resultat,
        )

    return _install


class TestChargerMoisExplicite:
    """Quand `mois` est fourni, `charger()` doit le propager tel quel sans rejouer la résolution."""

    def test_propage_le_mois_fourni(self, stub_composer):
        stub_composer(_make_stub_composition(debuts_mois=[datetime(2024, 1, 1)]))

        ctx = charger(
            historique=pl.LazyFrame({}),
            releves=pl.LazyFrame({}),
            mois="2024-02-01",
        )

        assert isinstance(ctx, ContexteMensuel)
        assert ctx.mois == "2024-02-01"


class TestChargerMoisParDefaut:
    """Quand `mois=None`, `charger()` doit prendre le dernier `debut` tronqué au mois."""

    def test_resout_le_dernier_mois_depuis_facturation(self, stub_composer):
        # `debut` au milieu du mois pour vérifier la troncature
        stub_composer(
            _make_stub_composition(
                debuts_mois=[
                    datetime(2024, 1, 15),
                    datetime(2024, 3, 10),
                    datetime(2024, 2, 5),
                ]
            )
        )

        ctx = charger(historique=pl.LazyFrame({}), releves=pl.LazyFrame({}), mois=None)

        # Le dernier debut est mars → mois résolu = 2024-03-01
        assert ctx.mois == "2024-03-01"


class TestChargerFramesQuiPassent:
    """`ContexteMensuel` doit exposer les 4 frames produits par la composition des pipelines."""

    def test_expose_les_quatre_frames_du_resultat(self, stub_composer):
        # Sentinelles distinctes pour identifier chaque frame en sortie
        stub_composer(_make_stub_composition(debuts_mois=[datetime(2024, 1, 1)]))

        ctx = charger(historique=pl.LazyFrame({}), releves=pl.LazyFrame({}), mois="2024-01-01")

        # Les LazyFrames doivent porter les sentinelles définies dans le stub
        assert ctx.historique_enrichi.collect()["sentinel"].item() == 1
        assert ctx.abonnements.collect()["sentinel"].item() == 2
        assert ctx.energie.collect()["sentinel"].item() == 3
        # releves_utilises : le journal des relevés consommés (#233) est exposé.
        assert ctx.releves_utilises.collect()["sentinel"].item() == 4
        # facturation_mensuelle est une DataFrame (déjà collectée)
        assert isinstance(ctx.facturation_mensuelle, pl.DataFrame)
        assert "debut" in ctx.facturation_mensuelle.columns


class TestChargerHorizon:
    """`charger()` résout l'horizon au boundary (défaut) ou propage l'explicite (issue #179)."""

    def test_resout_l_horizon_par_defaut_quand_absent(self, monkeypatch):
        """Sans `horizon`, `charger()` résout le 1er du mois courant une seule fois et le passe à `_composer`."""
        import electricore.core.builds.contexte_mensuel as cm

        captures: dict = {}

        def _composer_capture(historique, releves, horizon):
            captures["horizon"] = horizon
            return _make_stub_composition(debuts_mois=[datetime(2024, 1, 1)])

        monkeypatch.setattr(cm, "_composer", _composer_capture)

        cm.charger(historique=pl.LazyFrame({}), releves=pl.LazyFrame({}), mois="2024-01-01")

        # Un horizon (datetime Paris, 1er du mois) a bien été résolu et propagé.
        assert isinstance(captures["horizon"], datetime)
        assert captures["horizon"].day == 1
        assert str(captures["horizon"].tzinfo) == "Europe/Paris"

    def test_propage_l_horizon_explicite(self, monkeypatch):
        """Un `horizon` explicite est transmis tel quel à `_composer` (déterminisme)."""
        import electricore.core.builds.contexte_mensuel as cm

        captures: dict = {}
        horizon = datetime(2024, 5, 1, tzinfo=ZoneInfo("Europe/Paris"))

        def _composer_capture(historique, releves, horizon):
            captures["horizon"] = horizon
            return _make_stub_composition(debuts_mois=[datetime(2024, 1, 1)])

        monkeypatch.setattr(cm, "_composer", _composer_capture)

        cm.charger(historique=pl.LazyFrame({}), releves=pl.LazyFrame({}), mois="2024-01-01", horizon=horizon)

        assert captures["horizon"] == horizon


class TestContexteDuMois:
    """`contexte_du_mois()` — l'entrée I/O (#145) : loaders DuckDB par défaut, puis `charger()`."""

    def test_cable_les_loaders_et_delegue_a_charger(self, monkeypatch):
        """c15 → historique, releves → relevés (sentinelles : l'inversion échoue)."""
        import electricore.core.builds.contexte_mensuel as cm

        hist_sentinel = pl.LazyFrame({"source": ["c15"]})
        rel_sentinel = pl.LazyFrame({"source": ["releves"]})

        class _Query:
            def __init__(self, lf):
                self._lf = lf

            def lazy(self):
                return self._lf

        monkeypatch.setattr(cm, "c15", lambda: _Query(hist_sentinel))
        monkeypatch.setattr(cm, "releves", lambda: _Query(rel_sentinel))

        captures: dict = {}
        composition = _make_stub_composition(debuts_mois=[datetime(2025, 3, 1)])

        def _composer_capture(historique, releves, horizon):
            captures["historique"] = historique
            captures["releves"] = releves
            captures["horizon"] = horizon
            return composition

        monkeypatch.setattr(cm, "_composer", _composer_capture)

        ctx = cm.contexte_du_mois(mois="2025-03-01")

        assert captures["historique"] is hist_sentinel
        assert captures["releves"] is rel_sentinel
        # L'horizon est résolu au boundary (1er du mois courant) et passé à `_composer`.
        assert captures["horizon"] is not None
        assert isinstance(ctx, ContexteMensuel)
        assert ctx.mois == "2025-03-01"
