"""Tests unitaires pour les expressions Polars du pipeline périmètre."""

import polars as pl
import pytest
from electricore.core.pipelines_polars.perimetre_polars import (
    expr_changement,
    expr_resume_changement,
    expr_impacte_abonnement,
    expr_evenement_structurant
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
        "Evenement_Declencheur": ["FACTURATION", "MCT", "MCT"],  # Événements non structurants
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


def test_expr_impacte_abonnement():
    """Teste que expr_impacte_abonnement détecte les changements de puissance ou FTA."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["A", "A", "A", "A"],
        "Puissance_Souscrite": [6.0, 6.0, 9.0, 9.0],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
        "Evenement_Declencheur": ["FACTURATION", "MCT", "MCT", "FACTURATION"],  # Événements non structurants
    })
    
    result = df.select(
        expr_impacte_abonnement().alias("impacte")
    )
    
    # Ligne 1: pas de précédent = False
    # Ligne 2: puissance identique mais FTA change = True
    # Ligne 3: FTA identique mais puissance change = True
    # Ligne 4: aucun changement = False
    assert result["impacte"].to_list() == [False, True, True, False]


def test_expr_impacte_abonnement_avec_nulls():
    """Teste la gestion des nulls dans expr_impacte_abonnement."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["A", "A", "A"],
        "Puissance_Souscrite": [None, 6.0, 6.0],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", None, "BTINFCU4"],
        "Evenement_Declencheur": ["FACTURATION", "MCT", "FACTURATION"],  # Événements non structurants
    })
    
    result = df.select(
        expr_impacte_abonnement().alias("impacte")
    )
    
    # Ligne 1: pas de précédent = False
    # Ligne 2: comparaison avec None = False (logique conservatrice)
    # Ligne 3: aucun changement = False
    assert result["impacte"].to_list() == [False, False, False]


def test_expr_impacte_abonnement_composition():
    """Teste que expr_impacte_abonnement compose bien les expressions individuelles."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["A", "A", "A", "A", "A"],
        "Puissance_Souscrite": [6.0, 9.0, 9.0, 6.0, 6.0],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFCU4"],
        "Evenement_Declencheur": ["FACTURATION", "MCT", "MCT", "MCT", "MCT"],  # Événements non structurants
    })
    
    result = df.select([
        expr_changement("Puissance_Souscrite").alias("change_puissance"),
        expr_changement("Formule_Tarifaire_Acheminement").alias("change_fta"),
        expr_impacte_abonnement().alias("impacte_abonnement"),
    ])
    
    expected_puissance = [False, True, False, True, False]
    expected_fta = [False, False, True, False, True]
    expected_impacte = [
        False,  # Ligne 1: aucun précédent
        True,   # Ligne 2: puissance change
        True,   # Ligne 3: FTA change
        True,   # Ligne 4: puissance change
        True,   # Ligne 5: FTA change
    ]
    
    assert result["change_puissance"].to_list() == expected_puissance
    assert result["change_fta"].to_list() == expected_fta
    assert result["impacte_abonnement"].to_list() == expected_impacte
    
    # Vérification de la logique OR : impacte = change_puissance OR change_fta
    for i in range(len(expected_impacte)):
        expected_or = expected_puissance[i] or expected_fta[i]
        assert result["impacte_abonnement"][i] == expected_or, f"Ligne {i}: OR logic failed"


def test_expr_evenement_structurant():
    """Teste la détection des événements structurants."""
    df = pl.DataFrame({
        "Evenement_Declencheur": ["MES", "MCT", "CFNE", "FACTURATION", "RES", "PMES", "CFNS", "AUTRE"],
    })
    
    result = df.select(
        expr_evenement_structurant().alias("structurant")
    )
    
    # MES, CFNE, RES, PMES, CFNS = True (événements structurants)
    # MCT, FACTURATION, AUTRE = False (événements normaux)
    assert result["structurant"].to_list() == [True, False, True, False, True, True, True, False]


def test_expr_impacte_abonnement_avec_evenements_structurants():
    """Teste que expr_impacte_abonnement intègre bien les événements structurants."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["A", "A", "A", "A", "A"],
        "Puissance_Souscrite": [6.0, 6.0, 6.0, 9.0, 9.0],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
        "Evenement_Declencheur": ["MES", "FACTURATION", "MCT", "MCT", "RES"],
    })
    
    result = df.select([
        expr_changement("Puissance_Souscrite").alias("change_puissance"),
        expr_changement("Formule_Tarifaire_Acheminement").alias("change_fta"),
        expr_evenement_structurant().alias("evenement_structurant"),
        expr_impacte_abonnement().alias("impacte_abonnement"),
    ])
    
    expected = [
        # MES: événement structurant (même sans changement)
        {"change_p": False, "change_f": False, "struct": True, "impact": True},
        # FACTURATION: aucun changement, pas structurant
        {"change_p": False, "change_f": False, "struct": False, "impact": False},
        # MCT avec changement FTA
        {"change_p": False, "change_f": True, "struct": False, "impact": True},
        # MCT avec changement puissance
        {"change_p": True, "change_f": False, "struct": False, "impact": True},
        # RES: événement structurant
        {"change_p": False, "change_f": False, "struct": True, "impact": True},
    ]
    
    for i, exp in enumerate(expected):
        assert result["change_puissance"][i] == exp["change_p"], f"Ligne {i}: change_puissance incorrect"
        assert result["change_fta"][i] == exp["change_f"], f"Ligne {i}: change_fta incorrect"
        assert result["evenement_structurant"][i] == exp["struct"], f"Ligne {i}: structurant incorrect"
        assert result["impacte_abonnement"][i] == exp["impact"], f"Ligne {i}: impact incorrect"