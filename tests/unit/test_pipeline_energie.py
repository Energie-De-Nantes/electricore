"""Tests unitaires pour le pipeline énergie Polars (nouveau)."""

from datetime import datetime

import polars as pl
import pytest

from electricore.core.pipelines.energie import (
    calculer_periodes_energie,
    # Tests d'expressions ajoutés localement dans chaque test
    expr_bornes_depuis_shift,
    interroger_releves,
    reconstituer_chronologie_releves,
)


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


class TestInterrogerRelevesPolars:
    """Tests pour interroger_releves."""

    def test_interrogation_tous_releves_trouves(self):
        """Test cas nominal : tous les relevés demandés sont trouvés."""
        requete = pl.LazyFrame(
            {
                "pdl": ["PDL001", "PDL002"],
                "date_releve": [datetime(2024, 1, 15), datetime(2024, 1, 16)],
                "ref_situation_contractuelle": ["REF001", "REF002"],
            }
        )

        releves = pl.LazyFrame(
            {
                "pdl": ["PDL001", "PDL002", "PDL003"],
                "date_releve": [datetime(2024, 1, 15), datetime(2024, 1, 16), datetime(2024, 1, 17)],
                "source": ["flux_R151", "flux_R151", "flux_R151"],
                "index_base_kwh": [1000.0, 2000.0, 3000.0],
                "ordre_index": [0, 0, 0],
                "id_calendrier_distributeur": ["DI000001", "DI000001", "DI000001"],
            }
        )

        result = interroger_releves(requete, releves).collect()

        assert len(result) == 2  # Même taille que la requête
        assert not any(result["releve_manquant"])  # Aucun relevé manquant
        assert result["index_base_kwh"].to_list() == [1000.0, 2000.0]

    def test_interrogation_avec_releves_manquants(self):
        """Test cas mixte : certains relevés trouvés, d'autres non."""
        requete = pl.LazyFrame(
            {"pdl": ["PDL001", "PDL002"], "date_releve": [datetime(2024, 1, 15), datetime(2024, 1, 16)]}
        )

        # Base de relevés (seulement PDL001)
        releves = pl.LazyFrame(
            {
                "pdl": ["PDL001"],
                "date_releve": [datetime(2024, 1, 15)],
                "source": ["flux_R151"],
                "index_base_kwh": [1000.0],
                "ordre_index": [0],
                "id_calendrier_distributeur": ["DI000001"],
            }
        )

        result = interroger_releves(requete, releves).collect()

        assert len(result) == 2  # Même taille que la requête

        # PDL002 doit avoir releve_manquant=True
        pdl002 = result.filter(pl.col("pdl") == "PDL002")
        assert len(pdl002) == 1
        assert pdl002["releve_manquant"][0] is True
        assert pdl002["index_base_kwh"][0] is None

        # PDL001 doit avoir releve_manquant=False
        pdl001 = result.filter(pl.col("pdl") == "PDL001")
        assert pdl001["releve_manquant"][0] is False


class TestReconstituerChronologieRelevesPolars:
    """Tests pour reconstituer_chronologie_releves."""

    def test_reconstitution_avec_evenements_et_facturation(self):
        """Test nominal : combinaison d'événements contractuels + facturation."""
        evenements = pl.LazyFrame(
            {
                "pdl": ["PDL001", "PDL001", "PDL002"],
                "ref_situation_contractuelle": ["REF001", "REF001", "REF002"],
                "formule_tarifaire_acheminement": ["TURPE 5", "TURPE 5", "TURPE 6"],
                "evenement_declencheur": ["MES", "FACTURATION", "FACTURATION"],
                "date_evenement": [datetime(2024, 1, 15), datetime(2024, 2, 1), datetime(2024, 2, 1)],
                # Données pour événement contractuel MES
                "avant_index_base_kwh": [1000.0, None, None],
                "apres_index_base_kwh": [1500.0, None, None],
                "avant_index_hp_kwh": [500.0, None, None],
                "apres_index_hp_kwh": [750.0, None, None],
                # Autres colonnes nulles
                **{
                    f"{pos}_index_{col}_kwh": [None] * 3
                    for pos in ["avant", "apres"]
                    for col in ["hc", "hch", "hph", "hcb", "hpb"]
                },
            }
        )

        # Ajouter manuellement les colonnes ID calendrier
        evenements = evenements.with_columns(
            [
                pl.Series("avant_id_calendrier_distributeur", [1, None, None], dtype=pl.Int64),
                pl.Series("apres_id_calendrier_distributeur", [1, None, None], dtype=pl.Int64),
            ]
        )

        # Base de relevés R151 (seulement PDL001)
        # Relevés canoniques (ADR-0029) : C15 (avant/après du MES) + périodique R151.
        releves = pl.LazyFrame(
            {
                "pdl": ["PDL001", "PDL001", "PDL001"],
                "date_releve": [datetime(2024, 1, 15), datetime(2024, 1, 15), datetime(2024, 2, 1)],
                "source": ["flux_C15", "flux_C15", "flux_R151"],
                "index_base_kwh": [1000.0, 1500.0, 2000.0],
                "index_hp_kwh": [500.0, 750.0, None],
                "ordre_index": [False, True, False],
                "ref_situation_contractuelle": ["REF001", "REF001", None],
                "id_calendrier_distributeur": ["DI000001", "DI000001", "DI000001"],
            }
        )

        result = reconstituer_chronologie_releves(evenements, releves).collect()

        # Vérifications
        assert len(result) >= 3  # Au moins : MES avant, MES après, FACTURATION PDL001

        # Vérifier qu'on a les relevés de l'événement MES
        mes_releves = result.filter(pl.col("source") == "flux_C15")
        assert len(mes_releves) == 2  # avant + après

        # Vérifier les flags releve_manquant
        facturation_pdl001 = result.filter((pl.col("pdl") == "PDL001") & (pl.col("source") == "flux_R151"))
        if len(facturation_pdl001) > 0:
            assert facturation_pdl001["releve_manquant"][0] is False

    def test_priorite_flux_c15_sur_r151(self):
        """Test que flux_C15 a priorité sur flux_R151 en cas de conflit."""
        evenements = pl.LazyFrame(
            {
                "pdl": ["PDL001", "PDL001"],
                "ref_situation_contractuelle": ["REF001", "REF001"],
                "formule_tarifaire_acheminement": ["TURPE 5", "TURPE 5"],
                "evenement_declencheur": ["MES", "FACTURATION"],
                "date_evenement": [datetime(2024, 1, 15), datetime(2024, 1, 15)],
                # Données MES minimales
                "avant_index_base_kwh": [1000.0, None],
                "apres_index_base_kwh": [1500.0, None],
                **{
                    f"{pos}_index_{col}_kwh": [None, None]
                    for pos in ["avant", "apres"]
                    for col in ["hp", "hc", "hch", "hph", "hcb", "hpb"]
                },
            }
        )

        # Ajouter manuellement les colonnes ID calendrier
        evenements = evenements.with_columns(
            [
                pl.Series("avant_id_calendrier_distributeur", [1, None], dtype=pl.Int64),
                pl.Series("apres_id_calendrier_distributeur", [1, None], dtype=pl.Int64),
            ]
        )

        # C15 (avant/après) + R151 au même jour : C15 doit gagner (ADR-0029).
        releves = pl.LazyFrame(
            {
                "pdl": ["PDL001", "PDL001", "PDL001"],
                "date_releve": [datetime(2024, 1, 15), datetime(2024, 1, 15), datetime(2024, 1, 15)],
                "source": ["flux_C15", "flux_C15", "flux_R151"],
                "index_base_kwh": [1000.0, 1500.0, 9999.0],  # 9999 = conflit R151 à écarter
                "ordre_index": [False, True, False],
                "ref_situation_contractuelle": ["REF001", "REF001", "REF001"],
                "id_calendrier_distributeur": ["DI000001", "DI000001", "DI000001"],
            }
        )

        result = reconstituer_chronologie_releves(evenements, releves).collect()

        # À cette date, seuls les relevés flux_C15 doivent rester (priorité)
        releves_date = result.filter(pl.col("date_releve") == datetime(2024, 1, 15))

        # Tous les relevés à cette date doivent être flux_C15
        sources = releves_date["source"].unique().to_list()
        assert sources == ["flux_C15"]

        # Les valeurs doivent être celles des événements, pas du R151
        assert 1000.0 in releves_date["index_base_kwh"].to_list()  # Relevé avant
        assert 1500.0 in releves_date["index_base_kwh"].to_list()  # Relevé après
        assert 9999.0 not in releves_date["index_base_kwh"].to_list()  # Pas le R151


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


def test_expr_arrondir_index_kwh():
    """Teste l'arrondi des index à l'entier inférieur."""
    df = pl.DataFrame(
        {
            "index_base_kwh": [1000.8, 1001.2, None],
            "index_hp_kwh": [500.9, None, 502.1],
            "index_hc_kwh": [None, 400.7, 401.3],
        }
    ).lazy()

    from electricore.core.pipelines.energie import expr_arrondir_index_kwh

    result = df.with_columns(expr_arrondir_index_kwh(["index_base_kwh", "index_hp_kwh", "index_hc_kwh"])).collect()

    # Vérifier l'arrondi à l'entier inférieur
    assert result["index_base_kwh"].to_list() == [1000.0, 1001.0, None]
    assert result["index_hp_kwh"].to_list() == [500.0, None, 502.0]
    assert result["index_hc_kwh"].to_list() == [None, 400.0, 401.0]


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
