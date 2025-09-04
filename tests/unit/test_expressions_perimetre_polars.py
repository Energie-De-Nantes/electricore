"""Tests unitaires pour les expressions Polars du pipeline périmètre."""

import polars as pl
import pytest
from electricore.core.pipelines_polars.perimetre_polars import (
    expr_changement,
    expr_resume_changement,
    expr_impacte_abonnement,
    expr_evenement_structurant,
    expr_changement_avant_apres,
    expr_changement_index,
    expr_impacte_energie,
    expr_resume_modification,
    detecter_points_de_rupture
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


def test_expr_changement_avant_apres_detecte_changements():
    """Teste que expr_changement_avant_apres détecte correctement les changements."""
    df = pl.DataFrame({
        "Avant_Calendrier": ["CAL1", "CAL1", "CAL2", None, "CAL4"],
        "Apres_Calendrier": ["CAL1", "CAL3", "CAL2", "CAL5", None],
    })
    
    result = df.select(
        expr_changement_avant_apres("Avant_Calendrier", "Apres_Calendrier").alias("changement")
    )
    
    # CAL1 -> CAL1: pas de changement
    # CAL1 -> CAL3: changement
    # CAL2 -> CAL2: pas de changement  
    # None -> CAL5: pas de changement (logique conservatrice)
    # CAL4 -> None: pas de changement (logique conservatrice)
    assert result["changement"].to_list() == [False, True, False, False, False]


def test_expr_changement_avant_apres_avec_nulls():
    """Teste la gestion des nulls dans expr_changement_avant_apres."""
    df = pl.DataFrame({
        "Avant_Value": [None, None, 5.0, 5.0],
        "Apres_Value": [None, 3.0, None, 7.0],
    })
    
    result = df.select(
        expr_changement_avant_apres("Avant_Value", "Apres_Value").alias("changement")
    )
    
    # None -> None: pas de changement (pas de valeurs valides)
    # None -> 3.0: pas de changement (valeur avant nulle)
    # 5.0 -> None: pas de changement (valeur après nulle)
    # 5.0 -> 7.0: changement détecté
    assert result["changement"].to_list() == [False, False, False, True]


def test_expr_changement_avant_apres_avec_nombres():
    """Teste expr_changement_avant_apres avec des nombres."""
    df = pl.DataFrame({
        "Avant_Puissance": [6.0, 6.0, 9.0, 3.0],
        "Apres_Puissance": [6.0, 9.0, 9.0, 6.0],
    })
    
    result = df.select(
        expr_changement_avant_apres("Avant_Puissance", "Apres_Puissance").alias("changement")
    )
    
    # 6.0 -> 6.0: pas de changement
    # 6.0 -> 9.0: changement
    # 9.0 -> 9.0: pas de changement
    # 3.0 -> 6.0: changement
    assert result["changement"].to_list() == [False, True, False, True]


def test_expr_changement_index_detecte_changements():
    """Teste que expr_changement_index détecte les changements d'index."""
    df = pl.DataFrame({
        # Colonnes d'index avec changements variés
        "Avant_BASE": [100.0, 200.0, 300.0, 400.0],
        "Après_BASE": [100.0, 250.0, 300.0, 400.0],
        "Avant_HP": [50.0, 75.0, None, 120.0],
        "Après_HP": [50.0, 75.0, 90.0, None],
        "Avant_HC": [30.0, 40.0, 50.0, 60.0],
        "Après_HC": [35.0, 40.0, 50.0, 60.0],
        # Autres colonnes d'index (pas de changements pour simplifier)
        "Avant_HPH": [None, None, None, None],
        "Après_HPH": [None, None, None, None],
        "Avant_HCH": [None, None, None, None],
        "Après_HCH": [None, None, None, None],
        "Avant_HPB": [None, None, None, None],
        "Après_HPB": [None, None, None, None],
        "Avant_HCB": [None, None, None, None],
        "Après_HCB": [None, None, None, None],
    })
    
    result = df.select(
        expr_changement_index().alias("changement")
    )
    
    # Ligne 1: BASE identique, HP identique, HC change (35.0 vs 30.0) → True
    # Ligne 2: BASE change (250.0 vs 200.0), HP identique, HC identique → True  
    # Ligne 3: BASE identique, HP avec null (logique conservatrice), HC identique → False
    # Ligne 4: BASE identique, HP avec null (logique conservatrice), HC identique → False
    assert result["changement"].to_list() == [True, True, False, False]


def test_expr_changement_index_aucun_changement():
    """Teste expr_changement_index quand aucun index ne change."""
    df = pl.DataFrame({
        "Avant_BASE": [100.0, 200.0],
        "Après_BASE": [100.0, 200.0],
        "Avant_HP": [50.0, 75.0],
        "Après_HP": [50.0, 75.0],
        "Avant_HC": [30.0, 40.0],
        "Après_HC": [30.0, 40.0],
        # Autres colonnes identiques
        "Avant_HPH": [None, None],
        "Après_HPH": [None, None],
        "Avant_HCH": [None, None],
        "Après_HCH": [None, None],
        "Avant_HPB": [None, None],
        "Après_HPB": [None, None],
        "Avant_HCB": [None, None],
        "Après_HCB": [None, None],
    })
    
    result = df.select(
        expr_changement_index().alias("changement")
    )
    
    # Aucun changement détecté
    assert result["changement"].to_list() == [False, False]


def test_expr_changement_index_avec_nulls():
    """Teste la gestion des nulls dans expr_changement_index."""
    df = pl.DataFrame({
        "Avant_BASE": [None, 100.0, 200.0],
        "Après_BASE": [150.0, None, 200.0],
        "Avant_HP": [None, None, None],
        "Après_HP": [None, None, None],
        "Avant_HC": [None, None, None],
        "Après_HC": [None, None, None],
        # Autres colonnes toutes nulles
        "Avant_HPH": [None, None, None],
        "Après_HPH": [None, None, None],
        "Avant_HCH": [None, None, None],
        "Après_HCH": [None, None, None],
        "Avant_HPB": [None, None, None],
        "Après_HPB": [None, None, None],
        "Avant_HCB": [None, None, None],
        "Après_HCB": [None, None, None],
    })
    
    result = df.select(
        expr_changement_index().alias("changement")
    )
    
    # Ligne 1: None -> 150.0 (logique conservatrice: pas de changement)
    # Ligne 2: 100.0 -> None (logique conservatrice: pas de changement)  
    # Ligne 3: 200.0 -> 200.0 (pas de changement)
    assert result["changement"].to_list() == [False, False, False]


def test_expr_impacte_energie_composition_complete():
    """Teste que expr_impacte_energie compose bien toutes les détections."""
    df = pl.DataFrame({
        # Colonnes d'identification
        "Ref_Situation_Contractuelle": ["A", "A", "A", "A", "A", "A"],
        "Evenement_Declencheur": ["MES", "MCT", "MCT", "MCT", "RES", "FACTURATION"],
        
        # Changement calendrier (ligne 2)
        "Avant_Id_Calendrier_Distributeur": ["CAL1", "CAL1", "CAL2", "CAL3", "CAL4", "CAL5"],
        "Après_Id_Calendrier_Distributeur": ["CAL1", "CAL2", "CAL2", "CAL3", "CAL4", "CAL5"],
        
        # Changement FTA (ligne 3)
        "Avant_Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
        
        # Changement index (ligne 4)
        "Avant_BASE": [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
        "Après_BASE": [100.0, 200.0, 300.0, 450.0, 500.0, 600.0],
        "Avant_HP": [None, None, None, None, None, None],
        "Après_HP": [None, None, None, None, None, None],
        "Avant_HC": [None, None, None, None, None, None],
        "Après_HC": [None, None, None, None, None, None],
        "Avant_HPH": [None, None, None, None, None, None],
        "Après_HPH": [None, None, None, None, None, None],
        "Avant_HCH": [None, None, None, None, None, None],
        "Après_HCH": [None, None, None, None, None, None],
        "Avant_HPB": [None, None, None, None, None, None],
        "Après_HPB": [None, None, None, None, None, None],
        "Avant_HCB": [None, None, None, None, None, None],
        "Après_HCB": [None, None, None, None, None, None],
    })
    
    result = df.select([
        expr_changement_avant_apres("Avant_Id_Calendrier_Distributeur", "Après_Id_Calendrier_Distributeur").alias("change_cal"),
        expr_changement_index().alias("change_index"),
        expr_changement_avant_apres("Avant_Formule_Tarifaire_Acheminement", "Formule_Tarifaire_Acheminement").alias("change_fta"),
        expr_evenement_structurant().alias("event_struct"),
        expr_impacte_energie().alias("impacte_energie"),
    ])
    
    expected = [
        # MES: événement structurant → True (même sans autres changements)
        {"cal": False, "idx": False, "fta": False, "struct": True, "energie": True},
        # MCT + changement calendrier → True
        {"cal": True, "idx": False, "fta": False, "struct": False, "energie": True},
        # MCT + changement FTA → True
        {"cal": False, "idx": False, "fta": True, "struct": False, "energie": True},
        # MCT + changement index → True
        {"cal": False, "idx": True, "fta": False, "struct": False, "energie": True},
        # RES: événement structurant → True
        {"cal": False, "idx": False, "fta": False, "struct": True, "energie": True},
        # FACTURATION: aucun changement → False
        {"cal": False, "idx": False, "fta": False, "struct": False, "energie": False},
    ]
    
    for i, exp in enumerate(expected):
        assert result["change_cal"][i] == exp["cal"], f"Ligne {i}: change_cal incorrect"
        assert result["change_index"][i] == exp["idx"], f"Ligne {i}: change_index incorrect"  
        assert result["change_fta"][i] == exp["fta"], f"Ligne {i}: change_fta incorrect"
        assert result["event_struct"][i] == exp["struct"], f"Ligne {i}: event_struct incorrect"
        assert result["impacte_energie"][i] == exp["energie"], f"Ligne {i}: impacte_energie incorrect"


def test_expr_impacte_energie_avec_evenements_structurants():
    """Teste que expr_impacte_energie intègre bien les événements structurants."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["A", "A", "A"],
        "Evenement_Declencheur": ["CFNE", "MCT", "CFNS"],
        
        # Aucun changement dans les données
        "Avant_Id_Calendrier_Distributeur": ["CAL1", "CAL1", "CAL1"],
        "Après_Id_Calendrier_Distributeur": ["CAL1", "CAL1", "CAL1"],
        "Avant_Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFCU4"],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFCU4"],
        
        # Colonnes d'index sans changement
        "Avant_BASE": [100.0, 100.0, 100.0],
        "Après_BASE": [100.0, 100.0, 100.0],
        "Avant_HP": [None, None, None],
        "Après_HP": [None, None, None],
        "Avant_HC": [None, None, None],
        "Après_HC": [None, None, None],
        "Avant_HPH": [None, None, None],
        "Après_HPH": [None, None, None],
        "Avant_HCH": [None, None, None],
        "Après_HCH": [None, None, None],
        "Avant_HPB": [None, None, None],
        "Après_HPB": [None, None, None],
        "Avant_HCB": [None, None, None],
        "Après_HCB": [None, None, None],
    })
    
    result = df.select([
        expr_evenement_structurant().alias("event_struct"),
        expr_impacte_energie().alias("impacte_energie"),
    ])
    
    # CFNE et CFNS sont structurants → True même sans changement de données
    # MCT n'est pas structurant et sans changement de données → False
    assert result["event_struct"].to_list() == [True, False, True]
    assert result["impacte_energie"].to_list() == [True, False, True]


def test_expr_impacte_energie_avec_nulls():
    """Teste la gestion des nulls dans expr_impacte_energie."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["A", "A", "A"],
        "Evenement_Declencheur": ["MCT", "MCT", "MCT"],
        
        # Changements avec nulls (logique conservatrice)
        "Avant_Id_Calendrier_Distributeur": [None, "CAL1", None],
        "Après_Id_Calendrier_Distributeur": ["CAL2", None, None],
        "Avant_Formule_Tarifaire_Acheminement": [None, "BTINFCU4", "BTINFCU4"],
        "Formule_Tarifaire_Acheminement": ["BTINFMU4", None, "BTINFCU4"],
        
        # Index avec nulls
        "Avant_BASE": [None, 200.0, 300.0],
        "Après_BASE": [150.0, None, 300.0],
        "Avant_HP": [None, None, None],
        "Après_HP": [None, None, None],
        "Avant_HC": [None, None, None],
        "Après_HC": [None, None, None],
        "Avant_HPH": [None, None, None],
        "Après_HPH": [None, None, None],
        "Avant_HCH": [None, None, None],
        "Après_HCH": [None, None, None],
        "Avant_HPB": [None, None, None],
        "Après_HPB": [None, None, None],
        "Avant_HCB": [None, None, None],
        "Après_HCB": [None, None, None],
    })
    
    result = df.select([
        expr_impacte_energie().alias("impacte_energie"),
    ])
    
    # Ligne 1: Tous null → pas de changements détectés → False
    # Ligne 2: Tous null → pas de changements détectés → False  
    # Ligne 3: Aucun changement → False
    assert result["impacte_energie"].to_list() == [False, False, False]


def test_expr_impacte_energie_donnees_realistes():
    """Teste expr_impacte_energie sur un scénario réaliste d'historique contractuel."""
    df = pl.DataFrame({
        "Ref_Situation_Contractuelle": ["PDL1", "PDL1", "PDL1", "PDL1", "PDL1", "PDL1"],
        "Date_Evenement": ["2024-01-01", "2024-02-01", "2024-03-15", "2024-04-01", "2024-05-20", "2024-06-01"],
        "Evenement_Declencheur": ["MES", "FACTURATION", "MCT", "FACTURATION", "MCT", "RES"],
        
        # Changements calendrier (ligne 3: changement de calendrier distributeur)
        "Avant_Id_Calendrier_Distributeur": ["CAL_HP_HC", "CAL_HP_HC", "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO"],
        "Après_Id_Calendrier_Distributeur": ["CAL_HP_HC", "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO"],
        
        # Changements FTA (ligne 5: changement de formule tarifaire)
        "Avant_Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFMU4"],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4"],
        
        # Changements d'index (ligne 3: relevé spécial avec changement d'index)
        "Avant_BASE": [1000.0, 1200.0, 1500.0, 1800.0, 2100.0, 2400.0],
        "Après_BASE": [1000.0, 1200.0, 1550.0, 1800.0, 2100.0, 2400.0],  # Ajustement ligne 3
        "Avant_HP": [500.0, 600.0, 750.0, 900.0, 1050.0, 1200.0],
        "Après_HP": [500.0, 600.0, 750.0, 900.0, 1050.0, 1200.0],
        "Avant_HC": [300.0, 400.0, 500.0, 600.0, 700.0, 800.0],
        "Après_HC": [300.0, 400.0, 500.0, 600.0, 700.0, 800.0],
        "Avant_HPH": [None, None, None, None, None, None],
        "Après_HPH": [None, None, None, None, None, None],
        "Avant_HCH": [None, None, None, None, None, None],
        "Après_HCH": [None, None, None, None, None, None],
        "Avant_HPB": [None, None, None, None, None, None],
        "Après_HPB": [None, None, None, None, None, None],
        "Avant_HCB": [None, None, None, None, None, None],
        "Après_HCB": [None, None, None, None, None, None],
    })
    
    result = df.select([
        pl.col("Date_Evenement"),
        pl.col("Evenement_Declencheur"),
        expr_changement_avant_apres("Avant_Id_Calendrier_Distributeur", "Après_Id_Calendrier_Distributeur").alias("change_calendrier"),
        expr_changement_index().alias("change_index"),
        expr_changement_avant_apres("Avant_Formule_Tarifaire_Acheminement", "Formule_Tarifaire_Acheminement").alias("change_fta"),
        expr_evenement_structurant().alias("event_structurant"),
        expr_impacte_energie().alias("impacte_energie"),
    ])
    
    # Vérifications détaillées
    expected_impacts = [
        True,   # MES: événement structurant 
        False,  # FACTURATION: aucun changement
        True,   # MCT: changement calendrier ET changement index
        False,  # FACTURATION: aucun changement  
        True,   # MCT: changement FTA
        True,   # RES: événement structurant
    ]
    
    assert result["impacte_energie"].to_list() == expected_impacts
    
    # Vérifications des sous-composants
    assert result["event_structurant"].to_list() == [True, False, False, False, False, True]
    assert result["change_calendrier"].to_list() == [False, False, True, False, False, False]
    assert result["change_index"].to_list() == [False, False, True, False, False, False]  
    assert result["change_fta"].to_list() == [False, False, False, False, True, False]
    
    # Compter les impacts détectés
    impacts_detectes = sum(result["impacte_energie"].to_list())
    print(f"Impacts énergie détectés: {impacts_detectes}/6 événements")
    assert impacts_detectes == 4, "4 impacts énergie attendus sur 6 événements"


def test_detecter_points_de_rupture_pipeline_complet():
    """Teste la fonction detecter_points_de_rupture complète avec LazyFrame."""
    # Données d'historique contractuel réaliste
    historique_data = {
        "Ref_Situation_Contractuelle": ["PDL1", "PDL1", "PDL1", "PDL1", "PDL1"],
        "Date_Evenement": ["2024-01-01", "2024-02-01", "2024-03-15", "2024-04-01", "2024-05-20"],
        "Evenement_Declencheur": ["MES", "FACTURATION", "MCT", "FACTURATION", "RES"],
        "Puissance_Souscrite": [6.0, 6.0, 9.0, 9.0, 9.0],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
        
        # Colonnes Avant_/Après_ pour calendrier et index (déjà présentes dans les données)
        "Avant_Id_Calendrier_Distributeur": ["CAL1", "CAL1", "CAL1", "CAL2", "CAL2"],
        "Après_Id_Calendrier_Distributeur": ["CAL1", "CAL1", "CAL2", "CAL2", "CAL2"],
        "Avant_BASE": [1000.0, 1200.0, 1500.0, 1800.0, 2100.0],
        "Après_BASE": [1000.0, 1200.0, 1550.0, 1800.0, 2100.0],  # Changement ligne 3
        "Avant_HP": [None, None, None, None, None],
        "Après_HP": [None, None, None, None, None],
        "Avant_HC": [None, None, None, None, None],
        "Après_HC": [None, None, None, None, None],
        "Avant_HPH": [None, None, None, None, None],
        "Après_HPH": [None, None, None, None, None],
        "Avant_HCH": [None, None, None, None, None],
        "Après_HCH": [None, None, None, None, None],
        "Avant_HPB": [None, None, None, None, None],
        "Après_HPB": [None, None, None, None, None],
        "Avant_HCB": [None, None, None, None, None],
        "Après_HCB": [None, None, None, None, None],
    }
    
    # Créer LazyFrame et appliquer la fonction
    historique_lf = pl.LazyFrame(historique_data)
    resultat_lf = detecter_points_de_rupture(historique_lf)
    resultat = resultat_lf.collect()
    
    # Vérifier que les colonnes attendues sont présentes
    colonnes_attendues = ["impacte_abonnement", "impacte_energie", "resume_modification"]
    for col in colonnes_attendues:
        assert col in resultat.columns, f"Colonne manquante: {col}"
    
    # Vérifier les impacts détectés
    impacts_abonnement = resultat["impacte_abonnement"].to_list()
    impacts_energie = resultat["impacte_energie"].to_list()
    
    # Événements attendus :
    # MES: structurant → True/True
    # FACTURATION: aucun changement → False/False
    # MCT: changement puissance + calendrier + index → True/True  
    # FACTURATION: aucun changement → False/False
    # RES: structurant → True/True
    assert impacts_abonnement == [True, False, True, False, True]
    assert impacts_energie == [True, False, True, False, True]
    
    # Vérifier que les résumés sont générés
    resumes = resultat["resume_modification"].to_list()
    resumes_non_vides = [r for r in resumes if r != ""]
    assert len(resumes_non_vides) == 1, "1 résumé non-vide attendu (ligne avec changements multiples)"
    
    # Vérifier le contenu du résumé principal (ligne 3 avec tous les changements)
    resume_principal = resumes_non_vides[0]
    assert "P: 6.0 → 9.0" in resume_principal, "Changement puissance manquant dans résumé"
    assert "FTA: BTINFCU4 → BTINFMU4" in resume_principal, "Changement FTA manquant dans résumé"
    assert "Cal: CAL1 → CAL2" in resume_principal, "Changement calendrier manquant dans résumé"
    assert "rupture index" in resume_principal, "Mention rupture index manquante dans résumé"
    
    # Vérifier la présence des colonnes Avant_ créées par la fonction
    assert "Avant_Puissance_Souscrite" in resultat.columns
    assert "Avant_Formule_Tarifaire_Acheminement" in resultat.columns
    
    print(f"Pipeline complet testé avec succès:")
    print(f"- Impacts abonnement: {sum(impacts_abonnement)}/5")
    print(f"- Impacts énergie: {sum(impacts_energie)}/5")
    print(f"- Résumés générés: {len([r for r in resumes if r != ''])}/5")