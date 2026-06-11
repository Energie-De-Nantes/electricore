"""Tests unitaires des formules partagées entre pipelines de périodes (issue #178).

Le module `core/pipelines/periodes.py` porte les *formules* transverses
(nb_jours, mois_annee, libellés français) — pas les découpages, qui restent
propres à chaque périodisation (ADR-0023).
"""

from datetime import datetime

import polars as pl

from electricore.core.pipelines.periodes import (
    expr_date_formatee_fr,
    expr_fin_lisible,
    expr_mois_annee,
    expr_nb_jours,
    exprs_meta_periode,
)


def test_expr_nb_jours():
    """La formule canonique : différence des dates civiles, null si fin absente."""
    df = pl.DataFrame(
        {
            "debut": [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1)],
            "fin": [datetime(2024, 1, 31), datetime(2024, 2, 29), None],  # 2024 est bissextile
        }
    ).lazy()

    result = df.with_columns(expr_nb_jours().alias("nb_jours")).collect()

    expected = [30, 28, None]  # 31-1, 29-1, null pour None
    assert result["nb_jours"].to_list() == expected


def test_expr_mois_annee():
    """Clé `YYYY-MM` depuis debut, triable chronologiquement (issue #115)."""
    df = pl.DataFrame(
        {
            "debut": [datetime(2024, 12, 1), datetime(2025, 4, 15)],
        }
    ).lazy()

    result = df.with_columns(expr_mois_annee()).collect()

    assert result["mois_annee"].to_list() == ["2024-12", "2025-04"]


def test_expr_date_formatee_fr():
    """Formatage français des champs d'affichage (« 15 mars 2024 »)."""
    df = pl.DataFrame(
        {
            "ma_date": [datetime(2024, 3, 15), datetime(2024, 12, 25)],
        }
    ).lazy()

    result = df.with_columns(expr_date_formatee_fr("ma_date").alias("date_fr")).collect()

    assert result["date_fr"].to_list() == ["15 mars 2024", "25 décembre 2024"]


def test_expr_fin_lisible():
    """Formatage de la fin avec gestion du cas « en cours »."""
    df = pl.DataFrame(
        {
            "fin": [datetime(2024, 3, 31), None],
        }
    ).lazy()

    result = df.with_columns(expr_fin_lisible().alias("fin_lisible")).collect()

    assert result["fin_lisible"].to_list() == ["31 mars 2024", "en cours"]


def test_exprs_meta_periode():
    """Le bundle d'assemblage : les 4 méta-colonnes depuis les seules bornes.

    C'est le contrat partagé par les périodisations abonnements et énergie —
    y compris le cas « période ouverte » (fin nulle → nb_jours null,
    fin_lisible « en cours »).
    """
    df = pl.DataFrame(
        {
            "debut": [datetime(2025, 3, 1), datetime(2025, 4, 15)],
            "fin": [datetime(2025, 4, 1), None],
        }
    ).lazy()

    result = df.with_columns(exprs_meta_periode()).collect()

    assert result.columns == ["debut", "fin", "nb_jours", "debut_lisible", "fin_lisible", "mois_annee"]
    assert result["nb_jours"].to_list() == [31, None]
    assert result["debut_lisible"].to_list() == ["01 mars 2025", "15 avril 2025"]
    assert result["fin_lisible"].to_list() == ["01 avril 2025", "en cours"]
    assert result["mois_annee"].to_list() == ["2025-03", "2025-04"]
