"""Tests unitaires pour le pipeline énergie Polars (nouveau)."""

import polars as pl
import pytest
from datetime import datetime

from electricore.core.pipelines_polars.energie_polars import (
    expr_bornes_depuis_shift,
    extraire_releves_evenements_polars,
    interroger_releves_polars,
    reconstituer_chronologie_releves_polars,
    calculer_periodes_energie_polars,
    # Tests d'expressions ajoutés localement dans chaque test
)


def test_expr_bornes_depuis_shift():
    """Teste le calcul des bornes temporelles avec shift."""
    df = pl.LazyFrame({
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
    })

    result = (
        df
        .sort(["ref_situation_contractuelle", "date_releve"])
        .with_columns(expr_bornes_depuis_shift())
        .collect()
    )

    # Vérifier que debut = date_releve shifted
    debuts_attendus = [
        None, datetime(2024, 1, 1), datetime(2024, 2, 1),
        None, datetime(2024, 1, 15)
    ]
    assert result["debut"].to_list() == debuts_attendus

    # Vérifier les sources
    sources_avant_attendues = [
        None, "flux_R151", "flux_R151", None, "flux_C15"
    ]
    assert result["source_avant"].to_list() == sources_avant_attendues


class TestExtraireRelevesEvenementsPolars:
    """Tests pour extraire_releves_evenements_polars."""

    def test_extraction_releves_avant_apres(self):
        """Test nominal : extraction des relevés avant et après d'un événement."""
        historique = pl.LazyFrame({
            "pdl": ["PDL001"],
            "ref_situation_contractuelle": ["REF001"],
            "formule_tarifaire_acheminement": ["TURPE 5"],
            "date_evenement": [datetime(2024, 1, 15, 10, 0)],
            "avant_base": [1000.0],
            "apres_base": [1500.0],
            "avant_hp": [500.0],
            "apres_hp": [750.0],
            "avant_hc": [None],
            "apres_hc": [None],
            "avant_hch": [None],
            "apres_hch": [None],
            "avant_hph": [None],
            "apres_hph": [None],
            "avant_hcb": [None],
            "apres_hcb": [None],
            "avant_hpb": [None],
            "apres_hpb": [None],
            "avant_id_calendrier_distributeur": [1],
            "apres_id_calendrier_distributeur": [1]
        })

        result = extraire_releves_evenements_polars(historique).collect()

        assert len(result) == 2  # avant + après

        # Test relevé "avant" (ordre_index=0)
        avant = result.filter(pl.col("ordre_index") == 0)
        assert len(avant) == 1
        assert avant["pdl"][0] == "PDL001"
        assert avant["BASE"][0] == 1000.0
        assert avant["HP"][0] == 500.0
        assert avant["source"][0] == "flux_C15"
        assert avant["ordre_index"][0] == 0


class TestInterrogerRelevesPolars:
    """Tests pour interroger_releves_polars."""

    def test_interrogation_tous_releves_trouves(self):
        """Test cas nominal : tous les relevés demandés sont trouvés."""
        requete = pl.LazyFrame({
            "pdl": ["PDL001", "PDL002"],
            "date_releve": [datetime(2024, 1, 15), datetime(2024, 1, 16)],
            "ref_situation_contractuelle": ["REF001", "REF002"]
        })

        releves = pl.LazyFrame({
            "pdl": ["PDL001", "PDL002", "PDL003"],
            "date_releve": [datetime(2024, 1, 15), datetime(2024, 1, 16), datetime(2024, 1, 17)],
            "source": ["flux_R151", "flux_R151", "flux_R151"],
            "BASE": [1000.0, 2000.0, 3000.0],
        })

        result = interroger_releves_polars(requete, releves).collect()

        assert len(result) == 2  # Même taille que la requête
        assert not any(result["releve_manquant"])  # Aucun relevé manquant
        assert result["BASE"].to_list() == [1000.0, 2000.0]

    def test_interrogation_avec_releves_manquants(self):
        """Test cas mixte : certains relevés trouvés, d'autres non."""
        requete = pl.LazyFrame({
            "pdl": ["PDL001", "PDL002"],
            "date_releve": [datetime(2024, 1, 15), datetime(2024, 1, 16)]
        })

        # Base de relevés (seulement PDL001)
        releves = pl.LazyFrame({
            "pdl": ["PDL001"],
            "date_releve": [datetime(2024, 1, 15)],
            "source": ["flux_R151"],
            "BASE": [1000.0]
        })

        result = interroger_releves_polars(requete, releves).collect()

        assert len(result) == 2  # Même taille que la requête

        # PDL002 doit avoir releve_manquant=True
        pdl002 = result.filter(pl.col("pdl") == "PDL002")
        assert len(pdl002) == 1
        assert pdl002["releve_manquant"][0] is True
        assert pdl002["BASE"][0] is None

        # PDL001 doit avoir releve_manquant=False
        pdl001 = result.filter(pl.col("pdl") == "PDL001")
        assert pdl001["releve_manquant"][0] is False


class TestReconstituerChronologieRelevesPolars:
    """Tests pour reconstituer_chronologie_releves_polars."""

    def test_reconstitution_avec_evenements_et_facturation(self):
        """Test nominal : combinaison d'événements contractuels + facturation."""
        evenements = pl.LazyFrame({
            "pdl": ["PDL001", "PDL001", "PDL002"],
            "ref_situation_contractuelle": ["REF001", "REF001", "REF002"],
            "formule_tarifaire_acheminement": ["TURPE 5", "TURPE 5", "TURPE 6"],
            "evenement_declencheur": ["MES", "FACTURATION", "FACTURATION"],
            "date_evenement": [
                datetime(2024, 1, 15),
                datetime(2024, 2, 1),
                datetime(2024, 2, 1)
            ],
            # Données pour événement contractuel MES
            "avant_base": [1000.0, None, None],
            "apres_base": [1500.0, None, None],
            "avant_hp": [500.0, None, None],
            "apres_hp": [750.0, None, None],
            # Autres colonnes nulles
            **{f"{pos}_{col}": [None] * 3 for pos in ["avant", "apres"]
               for col in ["hc", "hch", "hph", "hcb", "hpb", "id_calendrier_distributeur"]
               if not (pos == "avant" and col == "id_calendrier_distributeur" and [1, None, None] == [None] * 3)}
        })

        # Ajouter manuellement les colonnes ID calendrier
        evenements = evenements.with_columns([
            pl.Series("avant_id_calendrier_distributeur", [1, None, None], dtype=pl.Int64),
            pl.Series("apres_id_calendrier_distributeur", [1, None, None], dtype=pl.Int64)
        ])

        # Base de relevés R151 (seulement PDL001)
        releves = pl.LazyFrame({
            "pdl": ["PDL001"],
            "date_releve": [datetime(2024, 2, 1)],
            "source": ["flux_R151"],
            "BASE": [2000.0],
        })

        result = reconstituer_chronologie_releves_polars(evenements, releves).collect()

        # Vérifications
        assert len(result) >= 3  # Au moins : MES avant, MES après, FACTURATION PDL001

        # Vérifier qu'on a les relevés de l'événement MES
        mes_releves = result.filter(pl.col("source") == "flux_C15")
        assert len(mes_releves) == 2  # avant + après

        # Vérifier les flags releve_manquant
        facturation_pdl001 = result.filter(
            (pl.col("pdl") == "PDL001") & (pl.col("source") == "flux_R151")
        )
        if len(facturation_pdl001) > 0:
            assert facturation_pdl001["releve_manquant"][0] is False

    def test_priorite_flux_c15_sur_r151(self):
        """Test que flux_C15 a priorité sur flux_R151 en cas de conflit."""
        evenements = pl.LazyFrame({
            "pdl": ["PDL001", "PDL001"],
            "ref_situation_contractuelle": ["REF001", "REF001"],
            "formule_tarifaire_acheminement": ["TURPE 5", "TURPE 5"],
            "evenement_declencheur": ["MES", "FACTURATION"],
            "date_evenement": [datetime(2024, 1, 15), datetime(2024, 1, 15)],
            # Données MES minimales
            "avant_base": [1000.0, None],
            "apres_base": [1500.0, None],
            **{f"{pos}_{col}": [None, None] for pos in ["avant", "apres"]
               for col in ["hp", "hc", "hch", "hph", "hcb", "hpb", "id_calendrier_distributeur"]}
        })

        # Ajouter manuellement les colonnes ID calendrier
        evenements = evenements.with_columns([
            pl.Series("avant_id_calendrier_distributeur", [1, None], dtype=pl.Int64),
            pl.Series("apres_id_calendrier_distributeur", [1, None], dtype=pl.Int64)
        ])

        # Relevé R151 à la même date
        releves = pl.LazyFrame({
            "pdl": ["PDL001"],
            "date_releve": [datetime(2024, 1, 15)],
            "source": ["flux_R151"],
            "BASE": [9999.0]  # Valeur différente pour détecter le conflit
        })

        result = reconstituer_chronologie_releves_polars(evenements, releves).collect()

        # À cette date, seuls les relevés flux_C15 doivent rester (priorité)
        releves_date = result.filter(pl.col("date_releve") == datetime(2024, 1, 15))

        # Tous les relevés à cette date doivent être flux_C15
        sources = releves_date["source"].unique().to_list()
        assert sources == ["flux_C15"]

        # Les valeurs doivent être celles des événements, pas du R151
        assert 1000.0 in releves_date["BASE"].to_list()  # Relevé avant
        assert 1500.0 in releves_date["BASE"].to_list()  # Relevé après
        assert 9999.0 not in releves_date["BASE"].to_list()  # Pas le R151


def test_propagation_flags_releve_manquant():
    """Test que les flags releve_manquant sont bien propagés dans les périodes."""
    releves = pl.LazyFrame({
        "pdl": ["PDL001", "PDL001", "PDL001"],
        "ref_situation_contractuelle": ["REF001", "REF001", "REF001"],
        "date_releve": [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1)],
        "source": ["flux_C15", "flux_R151", "flux_R151"],
        "BASE": [1000.0, 2000.0, 3000.0],
        "HP": [500.0, 1000.0, 1500.0],
        "HC": [200.0, 400.0, 600.0],
        "HPH": [100.0, 200.0, 300.0],
        "HPB": [150.0, 300.0, 450.0],
        "HCH": [80.0, 160.0, 240.0],
        "HCB": [120.0, 240.0, 360.0],
        "releve_manquant": [None, False, True],  # C15: null, R151 trouvé: False, R151 manquant: True
        "ordre_index": [0, 0, 0]
    })

    result = calculer_periodes_energie_polars(releves).collect()

    # Vérifier qu'on a 2 périodes (la première sera filtrée car pas de début)
    periodes = result.filter(pl.col("debut").is_not_null())
    assert len(periodes) == 2

    # Première période : début=C15 (null), fin=R151 (False)
    periode1 = periodes.filter(pl.col("fin") == datetime(2024, 2, 1))
    assert len(periode1) == 1
    assert periode1["releve_manquant_debut"][0] is None  # C15 n'a pas de flag
    assert periode1["releve_manquant_fin"][0] is False   # R151 trouvé

    # Deuxième période : début=R151 (False), fin=R151 (True)
    periode2 = periodes.filter(pl.col("fin") == datetime(2024, 3, 1))
    assert len(periode2) == 1
    assert periode2["releve_manquant_debut"][0] is False # R151 trouvé
    assert periode2["releve_manquant_fin"][0] is True   # R151 manquant


def test_expr_arrondir_index_kwh():
    """Teste l'arrondi des index à l'entier inférieur."""
    df = pl.DataFrame({
        "BASE": [1000.8, 1001.2, None],
        "HP": [500.9, None, 502.1],
        "HC": [None, 400.7, 401.3],
    }).lazy()

    from electricore.core.pipelines_polars.energie_polars import expr_arrondir_index_kwh

    result = df.with_columns(expr_arrondir_index_kwh(["BASE", "HP", "HC"])).collect()

    # Vérifier l'arrondi à l'entier inférieur
    assert result["BASE"].to_list() == [1000.0, 1001.0, None]
    assert result["HP"].to_list() == [500.0, None, 502.0]
    assert result["HC"].to_list() == [None, 400.0, 401.0]


def test_expr_calculer_energie_cadran():
    """Teste le calcul d'énergie pour un cadran."""
    df = pl.DataFrame({
        "ref_situation_contractuelle": ["A", "A", "A", "B", "B"],
        "BASE": [1000.0, 1050.0, 1100.0, 500.0, 530.0],
    }).lazy()

    from electricore.core.pipelines_polars.energie_polars import expr_calculer_energie_cadran

    result = (
        df
        .sort(["ref_situation_contractuelle"])
        .with_columns(
            expr_calculer_energie_cadran("BASE").alias("BASE_energie")
        )
        .collect()
    )

    # Premier relevé de chaque contrat = None (pas de précédent)
    # Relevés suivants = différence avec précédent
    energies_attendues = [
        None,   # A: 1er relevé
        50.0,   # A: 1050 - 1000
        50.0,   # A: 1100 - 1050
        None,   # B: 1er relevé
        30.0,   # B: 530 - 500
    ]
    assert result["BASE_energie"].to_list() == energies_attendues


def test_expr_date_formatee_fr():
    """Teste le formatage des dates en français."""
    df = pl.DataFrame({
        "ma_date": [datetime(2024, 3, 15), datetime(2024, 12, 25)],
    }).lazy()

    from electricore.core.pipelines_polars.energie_polars import expr_date_formatee_fr

    result = df.with_columns(
        expr_date_formatee_fr("ma_date", "complet").alias("date_fr")
    ).collect()

    # Vérifier que les dates contiennent les éléments français
    dates_fr = result["date_fr"].to_list()
    assert "15" in dates_fr[0] and "mars" in dates_fr[0] and "2024" in dates_fr[0]
    assert "25" in dates_fr[1] and "décembre" in dates_fr[1] and "2024" in dates_fr[1]


def test_expr_nb_jours():
    """Teste le calcul du nombre de jours."""
    df = pl.DataFrame({
        "debut": [datetime(2024, 1, 1), datetime(2024, 2, 1)],
        "fin": [datetime(2024, 1, 15), datetime(2024, 2, 29)],
    }).lazy()

    from electricore.core.pipelines_polars.energie_polars import expr_nb_jours

    result = df.with_columns(expr_nb_jours()).collect()

    # Vérifier le calcul des jours
    assert result["nb_jours"].to_list() == [14, 28]


if __name__ == "__main__":
    pytest.main([__file__])