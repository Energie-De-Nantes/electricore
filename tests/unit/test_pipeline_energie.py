"""Tests unitaires pour le pipeline énergie Polars (nouveau)."""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from electricore.core.pipelines.energie import (
    calculer_periodes_energie,
    # Tests d'expressions ajoutés localement dans chaque test
    expr_bornes_depuis_shift,
)

PARIS = ZoneInfo("Europe/Paris")


def _chronologie_un_contrat(natures: list, niveaux: list, dates: list | None = None) -> pl.LazyFrame:
    """Chronologie synthétique 1 contrat, index croissants, avec nature/niveau par relevé.

    `natures`/`niveaux` ont la même longueur (N relevés → N-1 périodes). Les bornes d'une
    période sont les relevés consécutifs ; chaque axe rollupe les deux bornes. `dates`
    optionnel (défaut : un relevé par mois) permet de placer plusieurs relevés dans un
    même mois (cas « bascule mid-mois »).
    """
    n = len(natures)
    cadrans = ("base", "hp", "hc", "hph", "hpb", "hch", "hcb")
    if dates is None:
        dates = [datetime(2024, 1 + i, 1, tzinfo=PARIS) for i in range(n)]
    return pl.LazyFrame(
        {
            "pdl": ["PDL001"] * n,
            "ref_situation_contractuelle": ["REF001"] * n,
            "date_releve": dates,
            "source": ["flux_C15"] + ["flux_R151"] * (n - 1),
            "nature_index": natures,
            "niveau_ouverture_services": niveaux,
            "releve_manquant": [None] + [False] * (n - 1),
            "ordre_index": [False] * n,
            **{f"index_{c}_kwh": [1000.0 * (i + 1) for i in range(n)] for c in cadrans},
        },
        schema_overrides={"date_releve": pl.Datetime(time_unit="us", time_zone="Europe/Paris")},
    )


def test_calculer_periodes_qualite_pire_gagne():
    """ADR-0033 : la qualité d'une période d'énergie est le rollup PIRE-GAGNE de la nature
    d'index de ses deux bornes — réel/corrigé → réelle ; estimé → estimée ; relevé
    manquant (nature null) → incalculable (incalculable > estimée > réelle)."""
    # 4 relevés (natures) → 3 périodes. niveaux indifférents ici (axe orthogonal).
    chrono = _chronologie_un_contrat(
        natures=["réel", "estimé", "corrigé", None],
        niveaux=["2", "2", "2", "2"],
    )

    result = calculer_periodes_energie(chrono).collect().sort("debut")

    # P1 (réel, estimé) → estimée ; P2 (estimé, corrigé) → estimée ; P3 (corrigé, null) → incalculable
    assert result["qualite"].to_list() == ["estimée", "estimée", "incalculable"]


def test_calculer_periodes_statut_communication():
    """ADR-0036 : une période d'énergie est COMMUNICANTE ssi ses DEUX bornes sont à niveau
    d'ouverture ≥ 1 ; une borne à niveau 0 (ou absente) la rend non-communicante. Axe
    orthogonal à la qualité (natures indifférentes ici)."""
    # 4 relevés (niveaux) → 3 périodes.
    chrono = _chronologie_un_contrat(
        natures=["réel", "réel", "réel", "réel"],
        niveaux=["2", "1", "0", "2"],
    )

    result = calculer_periodes_energie(chrono).collect().sort("debut")

    # P1 (2,1) → communicante ; P2 (1,0) → non ; P3 (0,2) → non
    assert result["statut_communication"].to_list() == [
        "communicante",
        "non_communicante",
        "non_communicante",
    ]


def test_pas_de_demi_mois_bascule_niveau_mid_mois():
    """ADR-0036 « pas de demi-mois » : une bascule de niveau EN COURS DE MOIS (une borne à
    niveau 0) rend le mois ENTIER non-communicant via le rollup plein-ou-rien — bout-en-bout
    calculer_periodes_energie → agreger_energies_mensuel. Un mois plein niveau ≥1 reste
    communicant (les bornes mensuelles tronquées n'introduisent pas de borne niveau-0)."""
    from electricore.core.pipelines.facturation import agreger_energies_mensuel

    # 3 relevés en mars → 2 sous-périodes dans le même mois.
    dates = [datetime(2024, 3, d, tzinfo=PARIS) for d in (1, 10, 20)]
    bascule = _chronologie_un_contrat(["réel"] * 3, ["2", "0", "0"], dates=dates)
    plein = _chronologie_un_contrat(["réel"] * 3, ["2", "2", "2"], dates=dates)

    def _meta(chrono):
        # turpe_variable_eur est ajouté par pipeline_energie en aval ; ici on shim à 0
        # (hors-sujet pour l'axe communication) pour conformer l'entrée de l'agrégat.
        periodes = calculer_periodes_energie(chrono).with_columns(pl.lit(0.0).alias("turpe_variable_eur"))
        return agreger_energies_mensuel(periodes).collect()

    assert _meta(bascule)["statut_communication"][0] == "non_communicante"
    assert _meta(plein)["statut_communication"][0] == "communicante"


def test_expr_bornes_depuis_shift():
    """Teste le calcul des bornes temporelles avec shift."""
    df = pl.LazyFrame(
        {
            "ref_situation_contractuelle": ["A", "A", "A", "B", "B"],
            "date_releve": [
                datetime(2024, 1, 1),
                datetime(2024, 2, 1),
                datetime(2024, 3, 1),
                datetime(2024, 1, 15),
                datetime(2024, 2, 15),
            ],
            "source": ["flux_R151", "flux_R151", "flux_R151", "flux_C15", "flux_R151"],
            "releve_manquant": [None, None, None, None, None],
        }
    )

    result = df.sort(["ref_situation_contractuelle", "date_releve"]).with_columns(expr_bornes_depuis_shift()).collect()

    # Vérifier que debut = date_releve shifted
    debuts_attendus = [None, datetime(2024, 1, 1), datetime(2024, 2, 1), None, datetime(2024, 1, 15)]
    assert result["debut"].to_list() == debuts_attendus

    # Vérifier les sources
    sources_avant_attendues = [None, "flux_R151", "flux_R151", None, "flux_C15"]
    assert result["source_avant"].to_list() == sources_avant_attendues


def test_propagation_flags_releve_manquant():
    """Test que les flags releve_manquant sont bien propagés dans les périodes.

    `calculer_periodes_energie` est décoré `@pa.check_types` et exige une sortie
    conforme à `PeriodeEnergie` (debut/fin tz-aware Europe/Paris).
    """
    from zoneinfo import ZoneInfo

    paris = ZoneInfo("Europe/Paris")
    releves = pl.LazyFrame(
        {
            "pdl": ["PDL001", "PDL001", "PDL001"],
            "ref_situation_contractuelle": ["REF001", "REF001", "REF001"],
            "date_releve": [
                datetime(2024, 1, 1, tzinfo=paris),
                datetime(2024, 2, 1, tzinfo=paris),
                datetime(2024, 3, 1, tzinfo=paris),
            ],
            "source": ["flux_C15", "flux_R151", "flux_R151"],
            "index_base_kwh": [1000.0, 2000.0, 3000.0],
            "index_hp_kwh": [500.0, 1000.0, 1500.0],
            "index_hc_kwh": [200.0, 400.0, 600.0],
            "index_hph_kwh": [100.0, 200.0, 300.0],
            "index_hpb_kwh": [150.0, 300.0, 450.0],
            "index_hch_kwh": [80.0, 160.0, 240.0],
            "index_hcb_kwh": [120.0, 240.0, 360.0],
            "releve_manquant": [None, False, True],  # C15: null, R151 trouvé: False, R151 manquant: True
            "ordre_index": [0, 0, 0],
        },
        schema_overrides={
            "date_releve": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
        },
    )

    result = calculer_periodes_energie(releves).collect()

    # Vérifier qu'on a 2 périodes (la première sera filtrée car pas de début)
    periodes = result.filter(pl.col("debut").is_not_null())
    assert len(periodes) == 2

    # Première période : début=C15 (null), fin=R151 (False)
    periode1 = periodes.filter(pl.col("fin") == datetime(2024, 2, 1, tzinfo=paris))
    assert len(periode1) == 1
    assert periode1["releve_manquant_debut"][0] is None  # C15 n'a pas de flag
    assert periode1["releve_manquant_fin"][0] is False  # R151 trouvé

    # Deuxième période : début=R151 (False), fin=R151 (True)
    periode2 = periodes.filter(pl.col("fin") == datetime(2024, 3, 1, tzinfo=paris))
    assert len(periode2) == 1
    assert periode2["releve_manquant_debut"][0] is False  # R151 trouvé
    assert periode2["releve_manquant_fin"][0] is True  # R151 manquant


def test_mois_annee_au_format_cle_calculable():
    """`mois_annee` est une clé calculable `YYYY-MM`, pas un libellé d'affichage (issue #115).

    Le libellé français reste porté par `debut_lisible` / `fin_lisible`.
    """
    from zoneinfo import ZoneInfo

    paris = ZoneInfo("Europe/Paris")
    releves = pl.LazyFrame(
        {
            "pdl": ["PDL001", "PDL001", "PDL001"],
            "ref_situation_contractuelle": ["REF001", "REF001", "REF001"],
            "date_releve": [
                datetime(2024, 1, 1, tzinfo=paris),
                datetime(2024, 2, 1, tzinfo=paris),
                datetime(2024, 3, 1, tzinfo=paris),
            ],
            "source": ["flux_C15", "flux_R151", "flux_R151"],
            "index_base_kwh": [1000.0, 2000.0, 3000.0],
            "index_hp_kwh": [500.0, 1000.0, 1500.0],
            "index_hc_kwh": [200.0, 400.0, 600.0],
            "index_hph_kwh": [100.0, 200.0, 300.0],
            "index_hpb_kwh": [150.0, 300.0, 450.0],
            "index_hch_kwh": [80.0, 160.0, 240.0],
            "index_hcb_kwh": [120.0, 240.0, 360.0],
            "releve_manquant": [None, False, False],
            "ordre_index": [0, 0, 0],
        },
        schema_overrides={
            "date_releve": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
        },
    )

    result = calculer_periodes_energie(releves).collect()

    periodes = result.filter(pl.col("debut").is_not_null()).sort("debut")
    assert periodes["mois_annee"].to_list() == ["2024-01", "2024-02"]


def test_expr_calculer_energie_cadran():
    """Teste le calcul d'énergie pour un cadran."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A", "A", "B", "B"],
            "index_base_kwh": [1000.0, 1050.0, 1100.0, 500.0, 530.0],
        }
    ).lazy()

    from electricore.core.pipelines.energie import expr_calculer_energie_cadran

    result = (
        df.sort(["ref_situation_contractuelle"])
        .with_columns(expr_calculer_energie_cadran("index_base_kwh").alias("energie_base_kwh"))
        .collect()
    )

    # Premier relevé de chaque contrat = None (pas de précédent)
    # Relevés suivants = différence avec précédent
    energies_attendues = [
        None,  # A: 1er relevé
        50.0,  # A: 1050 - 1000
        50.0,  # A: 1100 - 1050
        None,  # B: 1er relevé
        30.0,  # B: 530 - 500
    ]
    assert result["energie_base_kwh"].to_list() == energies_attendues


# Les tests de expr_date_formatee_fr / expr_nb_jours vivent dans
# test_expressions_periodes.py (formules partagées, issue #178).


if __name__ == "__main__":
    pytest.main([__file__])
