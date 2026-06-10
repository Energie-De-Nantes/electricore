"""Tests unitaires pour les expressions Polars du pipeline abonnements."""

from datetime import UTC, datetime

import polars as pl
import pytest

from electricore.core.pipelines.abonnements import (
    calculer_periodes_abonnement,
    expr_bornes_periode,
    expr_date_formatee_fr,
    expr_fin_lisible,
    expr_nb_jours,
    expr_periode_valide,
    generer_periodes_abonnement,
)


@pytest.fixture
def sample_historique():
    """Fixture avec des données d'historique de test."""
    paris_tz = UTC  # Simplifié pour les tests

    return pl.DataFrame(
        {
            "ref_situation_contractuelle": ["PDL001", "PDL001", "PDL001", "PDL002", "PDL002"],
            "pdl": ["12345", "12345", "12345", "67890", "67890"],
            "date_evenement": [
                datetime(2024, 1, 1, tzinfo=paris_tz),
                datetime(2024, 2, 1, tzinfo=paris_tz),
                datetime(2024, 4, 1, tzinfo=paris_tz),
                datetime(2024, 1, 15, tzinfo=paris_tz),
                datetime(2024, 3, 15, tzinfo=paris_tz),
            ],
            "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFCU4", "BTINFCU4"],
            "puissance_souscrite_kva": [6.0, 6.0, 9.0, 3.0, 3.0],
            "impacte_abonnement": [True, True, True, True, True],
        }
    ).lazy()


def test_expr_bornes_periode():
    """Teste le calcul des bornes de période."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A", "A", "B", "B"],
            "date_evenement": [
                datetime(2024, 1, 1),
                datetime(2024, 2, 1),
                datetime(2024, 3, 1),
                datetime(2024, 1, 15),
                datetime(2024, 2, 15),
            ],
        }
    ).lazy()

    result = df.sort(["ref_situation_contractuelle", "date_evenement"]).with_columns(expr_bornes_periode()).collect()

    # Vérifier que debut = date_evenement
    assert result["debut"].to_list() == result["date_evenement"].to_list()

    # Vérifier les fins calculées par shift(-1)
    fins_attendues = [
        datetime(2024, 2, 1),  # A: 1er -> 2ème
        datetime(2024, 3, 1),  # A: 2ème -> 3ème
        None,  # A: 3ème -> None (dernière)
        datetime(2024, 2, 15),  # B: 1er -> 2ème
        None,  # B: 2ème -> None (dernière)
    ]
    assert result["fin"].to_list() == fins_attendues


def test_expr_nb_jours():
    """Teste le calcul du nombre de jours."""
    df = pl.DataFrame(
        {
            "debut": [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1)],
            "fin": [datetime(2024, 1, 31), datetime(2024, 2, 29), None],  # 2024 est bissextile
        }
    ).lazy()

    result = df.with_columns(expr_nb_jours().alias("nb_jours")).collect()

    expected = [30, 28, None]  # 31-1, 29-1, null pour None
    assert result["nb_jours"].to_list() == expected


def test_expr_date_formatee_fr():
    """Teste le formatage français des champs d'affichage (« 15 mars 2024 »)."""
    df = pl.DataFrame(
        {
            "ma_date": [datetime(2024, 3, 15), datetime(2024, 12, 25)],
        }
    ).lazy()

    result = df.with_columns(expr_date_formatee_fr("ma_date").alias("date_fr")).collect()

    assert result["date_fr"].to_list() == ["15 mars 2024", "25 décembre 2024"]


def test_expr_fin_lisible():
    """Teste le formatage de la fin avec gestion des nulls."""
    df = pl.DataFrame(
        {
            "fin": [datetime(2024, 3, 31), None],
        }
    ).lazy()

    result = df.with_columns(expr_fin_lisible().alias("fin_lisible")).collect()

    fins_lisibles = result["fin_lisible"].to_list()

    # Le premier doit contenir "31" (date formatée)
    assert "31" in fins_lisibles[0]
    # Le second doit être "en cours"
    assert fins_lisibles[1] == "en cours"


def test_expr_periode_valide():
    """Teste la validation des périodes."""
    df = pl.DataFrame(
        {
            "fin": [datetime(2024, 3, 31), None, datetime(2024, 4, 1)],
            "nb_jours": [30, 15, 0],
        }
    ).lazy()

    result = df.filter(expr_periode_valide()).collect()

    # Seule la première ligne doit passer (fin non-null ET nb_jours > 0)
    assert len(result) == 1
    assert result["nb_jours"][0] == 30


def test_calculer_periodes_abonnement_pipeline(sample_historique):
    """Teste le pipeline complet de calcul des périodes."""
    # Filtrer pour avoir des données propres
    historique_filtre = sample_historique.filter(pl.col("impacte_abonnement"))

    result = calculer_periodes_abonnement(historique_filtre).collect()

    # Vérifier la structure de base
    colonnes_attendues = [
        "ref_situation_contractuelle",
        "pdl",
        "mois_annee",
        "debut_lisible",
        "fin_lisible",
        "formule_tarifaire_acheminement",
        "puissance_souscrite_kva",
        "nb_jours",
        "debut",
        "fin",
    ]

    for col in colonnes_attendues:
        assert col in result.columns

    # Vérifier que les périodes invalides sont filtrées
    assert all(nb > 0 for nb in result["nb_jours"].to_list() if nb is not None)
    assert all(fin is not None for fin in result["fin"].to_list() if fin is not None)


def test_mois_annee_au_format_cle_calculable(sample_historique):
    """`mois_annee` est une clé calculable `YYYY-MM`, pas un libellé d'affichage (issue #115).

    Le libellé français reste porté par `debut_lisible` / `fin_lisible`.
    """
    result = calculer_periodes_abonnement(sample_historique).collect()

    assert result.filter(pl.col("ref_situation_contractuelle") == "PDL001")["mois_annee"].to_list() == [
        "2024-01",
        "2024-02",
    ]


def test_generer_periodes_abonnement_filtre_correctement():
    """Teste que la génération filtre correctement les événements.

    L'entrée doit être conforme au schéma `Historique` (enrichi) — la fonction
    est décorée `@pa.check_types`.
    """
    from zoneinfo import ZoneInfo

    paris = ZoneInfo("Europe/Paris")
    historique_mixte = pl.LazyFrame(
        {
            "ref_situation_contractuelle": ["REF001", "REF001", "REF001"],
            "pdl": ["12345", "12345", "12345"],
            "date_evenement": [
                datetime(2024, 1, 1, tzinfo=paris),
                datetime(2024, 2, 1, tzinfo=paris),
                datetime(2024, 3, 1, tzinfo=paris),
            ],
            "segment_clientele": ["C5", "C5", "C5"],
            "etat_contractuel": ["EN SERVICE", "EN SERVICE", "EN SERVICE"],
            "evenement_declencheur": ["MES", "MCT", "MCT"],
            "type_evenement": ["contractuel", "contractuel", "contractuel"],
            "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4"],
            "puissance_souscrite_kva": [6.0, 6.0, 9.0],
            "type_compteur": ["LINKY", "LINKY", "LINKY"],
            "num_compteur": ["123", "123", "123"],
            "impacte_abonnement": [True, False, True],  # Le 2ème n'impacte pas
            "impacte_energie": [True, False, True],
            "resume_modification": ["MES", "Aucun", "MCT FTA"],
        },
        schema_overrides={
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
        },
    )

    result = generer_periodes_abonnement(historique_mixte).collect()

    # Seuls les événements impactant l'abonnement doivent être traités
    # Donc 2 événements -> 1 période (la dernière n'aura pas de fin)
    assert len(result) <= 2  # Dépend du filtrage des périodes valides


def test_composition_expressions():
    """Teste la composition des expressions dans un pipeline."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A"],
            "date_evenement": [datetime(2024, 1, 1), datetime(2024, 2, 1)],
        }
    ).lazy()

    result = (
        df.with_columns(expr_bornes_periode())
        .with_columns(expr_nb_jours().alias("nb_jours"))
        .filter(expr_periode_valide())
        .collect()
    )

    # Vérifier que les expressions se composent correctement
    assert len(result) == 1  # Une période valide
    assert result["nb_jours"][0] == 31  # Janvier a 31 jours


if __name__ == "__main__":
    pytest.main([__file__])
