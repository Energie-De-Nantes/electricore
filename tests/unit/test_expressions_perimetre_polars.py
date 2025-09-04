"""Tests unitaires pour les expressions Polars du pipeline périmètre."""

import polars as pl
import pytest
from electricore.core.pipelines_polars.perimetre_polars import (
    expr_changement,
    expr_resume_changement
)


def test_expr_changement_detecte_changements():
    """Teste que expr_changement détecte correctement les changements."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["A", "A", "A", "B", "B"],
        "Puissance_Souscrite": [6.0, 6.0, 9.0, 3.0, 3.0],
    })
    
    result = df.select(
        expr_changement("Puissance_Souscrite").alias("changement")
    )
    
    # Première ligne de chaque groupe : pas de précédent = False
    # Deuxième ligne groupe A : 6.0 -> 6.0 = False
    # Troisième ligne groupe A : 6.0 -> 9.0 = True
    # Première ligne groupe B : pas de précédent = False
    # Deuxième ligne groupe B : 3.0 -> 3.0 = False
    assert result["changement"].to_list() == [False, False, True, False, False]


def test_expr_changement_avec_nulls():
    """Teste que expr_changement gère correctement les valeurs nulles."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["A", "A", "A", "A"],
        "Valeur": [None, 5.0, 5.0, None],
    })
    
    result = df.select(
        expr_changement("Valeur").alias("changement")
    )
    
    # Première ligne : pas de précédent = False
    # Deuxième ligne : None -> 5.0 = False (pas de valeur précédente valide)
    # Troisième ligne : 5.0 -> 5.0 = False (pas de changement)
    # Quatrième ligne : 5.0 -> None = False (pas de comparaison avec None)
    assert result["changement"].to_list() == [False, False, False, False]


def test_expr_resume_changement():
    """Teste que expr_resume_changement génère le bon texte."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["A", "A", "A"],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4"],
    })
    
    result = df.select(
        expr_resume_changement("Formule_Tarifaire_Acheminement", "FTA").alias("resume")
    )
    
    expected = [
        "",  # Première ligne : pas de changement
        "",  # BTINFCU4 -> BTINFCU4 : pas de changement
        "FTA: BTINFCU4 → BTINFMU4"  # BTINFCU4 -> BTINFMU4 : changement
    ]
    assert result["resume"].to_list() == expected


def test_expr_resume_changement_avec_nombres():
    """Teste le formatage des nombres dans le résumé."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["A", "A"],
        "Puissance_Souscrite": [6.0, 9.0],
    })
    
    result = df.select(
        expr_resume_changement("Puissance_Souscrite", "P").alias("resume")
    )
    
    assert result["resume"].to_list() == ["", "P: 6.0 → 9.0"]


def test_composition_expressions():
    """Teste que les expressions se composent correctement."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["A", "A", "A"],
        "Puissance_Souscrite": [6.0, 6.0, 9.0],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFMU4", "BTINFMU4"],
    })
    
    # Utiliser les deux expressions ensemble
    result = df.select([
        expr_changement("Puissance_Souscrite").alias("change_puissance"),
        expr_changement("Formule_Tarifaire_Acheminement").alias("change_fta"),
        expr_resume_changement("Puissance_Souscrite", "P").alias("resume_puissance"),
        expr_resume_changement("Formule_Tarifaire_Acheminement", "FTA").alias("resume_fta"),
    ])
    
    assert result["change_puissance"].to_list() == [False, False, True]
    assert result["change_fta"].to_list() == [False, True, False]
    assert result["resume_puissance"].to_list() == ["", "", "P: 6.0 → 9.0"]
    assert result["resume_fta"].to_list() == ["", "FTA: BTINFCU4 → BTINFMU4", ""]