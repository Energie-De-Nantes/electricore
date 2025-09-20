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
    detecter_points_de_rupture,
    # Nouvelles expressions pour facturation
    expr_evenement_entree,
    expr_evenement_sortie,
    expr_date_entree_periode,
    expr_date_sortie_periode,
    colonnes_evenement_facturation,
    generer_dates_facturation,
    expr_colonnes_a_propager,
    inserer_evenements_facturation
)


def test_expr_changement_detecte_changements():
    """Teste que expr_changement détecte correctement les changements."""
    df = pl.DataFrame({
        "ref_situation_contractuelle": ["A", "A", "A", "B", "B"],
        "puissance_souscrite": [6.0, 6.0, 9.0, 3.0, 3.0],
    })
    
    result = df.select(
        expr_changement("puissance_souscrite").alias("changement")
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
        "ref_situation_contractuelle": ["A", "A", "A", "A"],
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
        "ref_situation_contractuelle": ["A", "A", "A"],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4"],
    })
    
    result = df.select(
        expr_resume_changement("formule_tarifaire_acheminement", "FTA").alias("resume")
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
        "ref_situation_contractuelle": ["A", "A"],
        "puissance_souscrite": [6.0, 9.0],
    })
    
    result = df.select(
        expr_resume_changement("puissance_souscrite", "P").alias("resume")
    )
    
    assert result["resume"].to_list() == ["", "P: 6.0 → 9.0"]


def test_composition_expressions():
    """Teste que les expressions se composent correctement."""
    df = pl.DataFrame({
        "ref_situation_contractuelle": ["A", "A", "A"],
        "puissance_souscrite": [6.0, 6.0, 9.0],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFMU4", "BTINFMU4"],
        "evenement_declencheur": ["FACTURATION", "MCT", "MCT"],  # Événements non structurants
    })
    
    # Utiliser les deux expressions ensemble
    result = df.select([
        expr_changement("puissance_souscrite").alias("change_puissance"),
        expr_changement("formule_tarifaire_acheminement").alias("change_fta"),
        expr_resume_changement("puissance_souscrite", "P").alias("resume_puissance"),
        expr_resume_changement("formule_tarifaire_acheminement", "FTA").alias("resume_fta"),
    ])
    
    assert result["change_puissance"].to_list() == [False, False, True]
    assert result["change_fta"].to_list() == [False, True, False]
    assert result["resume_puissance"].to_list() == ["", "", "P: 6.0 → 9.0"]
    assert result["resume_fta"].to_list() == ["", "FTA: BTINFCU4 → BTINFMU4", ""]


def test_expr_impacte_abonnement():
    """Teste que expr_impacte_abonnement détecte les changements de puissance ou FTA."""
    df = pl.DataFrame({
        "ref_situation_contractuelle": ["A", "A", "A", "A"],
        "puissance_souscrite": [6.0, 6.0, 9.0, 9.0],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
        "evenement_declencheur": ["FACTURATION", "MCT", "MCT", "FACTURATION"],  # Événements non structurants
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
        "ref_situation_contractuelle": ["A", "A", "A"],
        "puissance_souscrite": [None, 6.0, 6.0],
        "formule_tarifaire_acheminement": ["BTINFCU4", None, "BTINFCU4"],
        "evenement_declencheur": ["FACTURATION", "MCT", "FACTURATION"],  # Événements non structurants
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
        "ref_situation_contractuelle": ["A", "A", "A", "A", "A"],
        "puissance_souscrite": [6.0, 9.0, 9.0, 6.0, 6.0],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFCU4"],
        "evenement_declencheur": ["FACTURATION", "MCT", "MCT", "MCT", "MCT"],  # Événements non structurants
    })
    
    result = df.select([
        expr_changement("puissance_souscrite").alias("change_puissance"),
        expr_changement("formule_tarifaire_acheminement").alias("change_fta"),
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
        "evenement_declencheur": ["MES", "MCT", "CFNE", "FACTURATION", "RES", "PMES", "CFNS", "AUTRE"],
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
        "ref_situation_contractuelle": ["A", "A", "A", "A", "A"],
        "puissance_souscrite": [6.0, 6.0, 6.0, 9.0, 9.0],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
        "evenement_declencheur": ["MES", "FACTURATION", "MCT", "MCT", "RES"],
    })
    
    result = df.select([
        expr_changement("puissance_souscrite").alias("change_puissance"),
        expr_changement("formule_tarifaire_acheminement").alias("change_fta"),
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
        "avant_Calendrier": ["CAL1", "CAL1", "CAL2", None, "CAL4"],
        "apres_Calendrier": ["CAL1", "CAL3", "CAL2", "CAL5", None],
    })
    
    result = df.select(
        expr_changement_avant_apres("avant_Calendrier", "apres_Calendrier").alias("changement")
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
        "avant_Value": [None, None, 5.0, 5.0],
        "apres_Value": [None, 3.0, None, 7.0],
    })
    
    result = df.select(
        expr_changement_avant_apres("avant_Value", "apres_Value").alias("changement")
    )
    
    # None -> None: pas de changement (pas de valeurs valides)
    # None -> 3.0: pas de changement (valeur avant nulle)
    # 5.0 -> None: pas de changement (valeur après nulle)
    # 5.0 -> 7.0: changement détecté
    assert result["changement"].to_list() == [False, False, False, True]


def test_expr_changement_avant_apres_avec_nombres():
    """Teste expr_changement_avant_apres avec des nombres."""
    df = pl.DataFrame({
        "avant_Puissance": [6.0, 6.0, 9.0, 3.0],
        "apres_Puissance": [6.0, 9.0, 9.0, 6.0],
    })
    
    result = df.select(
        expr_changement_avant_apres("avant_Puissance", "apres_Puissance").alias("changement")
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
        "avant_base": [100.0, 200.0, 300.0, 400.0],
        "apres_base": [100.0, 250.0, 300.0, 400.0],
        "avant_hp": [50.0, 75.0, None, 120.0],
        "apres_hp": [50.0, 75.0, 90.0, None],
        "avant_hc": [30.0, 40.0, 50.0, 60.0],
        "apres_hc": [35.0, 40.0, 50.0, 60.0],
        # Autres colonnes d'index (pas de changements pour simplifier)
        "avant_hph": [None, None, None, None],
        "apres_hph": [None, None, None, None],
        "avant_hch": [None, None, None, None],
        "apres_hch": [None, None, None, None],
        "avant_hpb": [None, None, None, None],
        "apres_hpb": [None, None, None, None],
        "avant_hcb": [None, None, None, None],
        "apres_hcb": [None, None, None, None],
    })
    
    result = df.select(
        expr_changement_index().alias("changement")
    )
    
    # Ligne 1: base identique, HP identique, HC change (35.0 vs 30.0) → True
    # Ligne 2: base change (250.0 vs 200.0), HP identique, HC identique → True  
    # Ligne 3: base identique, HP avec null (logique conservatrice), HC identique → False
    # Ligne 4: base identique, HP avec null (logique conservatrice), HC identique → False
    assert result["changement"].to_list() == [True, True, False, False]


def test_expr_changement_index_aucun_changement():
    """Teste expr_changement_index quand aucun index ne change."""
    df = pl.DataFrame({
        "avant_base": [100.0, 200.0],
        "apres_base": [100.0, 200.0],
        "avant_hp": [50.0, 75.0],
        "apres_hp": [50.0, 75.0],
        "avant_hc": [30.0, 40.0],
        "apres_hc": [30.0, 40.0],
        # Autres colonnes identiques
        "avant_hph": [None, None],
        "apres_hph": [None, None],
        "avant_hch": [None, None],
        "apres_hch": [None, None],
        "avant_hpb": [None, None],
        "apres_hpb": [None, None],
        "avant_hcb": [None, None],
        "apres_hcb": [None, None],
    })
    
    result = df.select(
        expr_changement_index().alias("changement")
    )
    
    # Aucun changement détecté
    assert result["changement"].to_list() == [False, False]


def test_expr_changement_index_avec_nulls():
    """Teste la gestion des nulls dans expr_changement_index."""
    df = pl.DataFrame({
        "avant_base": [None, 100.0, 200.0],
        "apres_base": [150.0, None, 200.0],
        "avant_hp": [None, None, None],
        "apres_hp": [None, None, None],
        "avant_hc": [None, None, None],
        "apres_hc": [None, None, None],
        # Autres colonnes toutes nulles
        "avant_hph": [None, None, None],
        "apres_hph": [None, None, None],
        "avant_hch": [None, None, None],
        "apres_hch": [None, None, None],
        "avant_hpb": [None, None, None],
        "apres_hpb": [None, None, None],
        "avant_hcb": [None, None, None],
        "apres_hcb": [None, None, None],
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
        "ref_situation_contractuelle": ["A", "A", "A", "A", "A", "A"],
        "evenement_declencheur": ["MES", "MCT", "MCT", "MCT", "RES", "FACTURATION"],
        
        # Changement calendrier (ligne 2)
        "avant_id_calendrier_distributeur": ["CAL1", "CAL1", "CAL2", "CAL3", "CAL4", "CAL5"],
        "apres_id_calendrier_distributeur": ["CAL1", "CAL2", "CAL2", "CAL3", "CAL4", "CAL5"],

        # Changement FTA (ligne 3)
        "avant_formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
        
        # Changement index (ligne 4)
        "avant_base": [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
        "apres_base": [100.0, 200.0, 300.0, 450.0, 500.0, 600.0],
        "avant_hp": [None, None, None, None, None, None],
        "apres_hp": [None, None, None, None, None, None],
        "avant_hc": [None, None, None, None, None, None],
        "apres_hc": [None, None, None, None, None, None],
        "avant_hph": [None, None, None, None, None, None],
        "apres_hph": [None, None, None, None, None, None],
        "avant_hch": [None, None, None, None, None, None],
        "apres_hch": [None, None, None, None, None, None],
        "avant_hpb": [None, None, None, None, None, None],
        "apres_hpb": [None, None, None, None, None, None],
        "avant_hcb": [None, None, None, None, None, None],
        "apres_hcb": [None, None, None, None, None, None],
    })
    
    result = df.select([
        expr_changement_avant_apres("avant_id_calendrier_distributeur", "apres_id_calendrier_distributeur").alias("change_cal"),
        expr_changement_index().alias("change_index"),
        expr_changement_avant_apres("avant_formule_tarifaire_acheminement", "formule_tarifaire_acheminement").alias("change_fta"),
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
        "ref_situation_contractuelle": ["A", "A", "A"],
        "evenement_declencheur": ["CFNE", "MCT", "CFNS"],
        
        # Aucun changement dans les données
        "avant_id_calendrier_distributeur": ["CAL1", "CAL1", "CAL1"],
        "apres_id_calendrier_distributeur": ["CAL1", "CAL1", "CAL1"],
        "avant_formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFCU4"],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFCU4"],
        
        # Colonnes d'index sans changement
        "avant_base": [100.0, 100.0, 100.0],
        "apres_base": [100.0, 100.0, 100.0],
        "avant_hp": [None, None, None],
        "apres_hp": [None, None, None],
        "avant_hc": [None, None, None],
        "apres_hc": [None, None, None],
        "avant_hph": [None, None, None],
        "apres_hph": [None, None, None],
        "avant_hch": [None, None, None],
        "apres_hch": [None, None, None],
        "avant_hpb": [None, None, None],
        "apres_hpb": [None, None, None],
        "avant_hcb": [None, None, None],
        "apres_hcb": [None, None, None],
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
        "ref_situation_contractuelle": ["A", "A", "A"],
        "evenement_declencheur": ["MCT", "MCT", "MCT"],
        
        # Changements avec nulls (logique conservatrice)
        "avant_id_calendrier_distributeur": [None, "CAL1", None],
        "apres_id_calendrier_distributeur": ["CAL2", None, None],
        "avant_formule_tarifaire_acheminement": [None, "BTINFCU4", "BTINFCU4"],
        "formule_tarifaire_acheminement": ["BTINFMU4", None, "BTINFCU4"],
        
        # Index avec nulls
        "avant_base": [None, 200.0, 300.0],
        "apres_base": [150.0, None, 300.0],
        "avant_hp": [None, None, None],
        "apres_hp": [None, None, None],
        "avant_hc": [None, None, None],
        "apres_hc": [None, None, None],
        "avant_hph": [None, None, None],
        "apres_hph": [None, None, None],
        "avant_hch": [None, None, None],
        "apres_hch": [None, None, None],
        "avant_hpb": [None, None, None],
        "apres_hpb": [None, None, None],
        "avant_hcb": [None, None, None],
        "apres_hcb": [None, None, None],
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
        "ref_situation_contractuelle": ["PDL1", "PDL1", "PDL1", "PDL1", "PDL1", "PDL1"],
        "date_evenement": ["2024-01-01", "2024-02-01", "2024-03-15", "2024-04-01", "2024-05-20", "2024-06-01"],
        "evenement_declencheur": ["MES", "FACTURATION", "MCT", "FACTURATION", "MCT", "RES"],
        
        # Changements calendrier (ligne 3: changement de calendrier distributeur)
        "avant_id_calendrier_distributeur": ["CAL_HP_HC", "CAL_HP_HC", "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO"],
        "apres_id_calendrier_distributeur": ["CAL_HP_HC", "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO"],
        
        # Changements FTA (ligne 5: changement de formule tarifaire)
        "avant_formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFMU4"],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4"],
        
        # Changements d'index (ligne 3: relevé spécial avec changement d'index)
        "avant_base": [1000.0, 1200.0, 1500.0, 1800.0, 2100.0, 2400.0],
        "apres_base": [1000.0, 1200.0, 1550.0, 1800.0, 2100.0, 2400.0],  # Ajustement ligne 3
        "avant_hp": [500.0, 600.0, 750.0, 900.0, 1050.0, 1200.0],
        "apres_hp": [500.0, 600.0, 750.0, 900.0, 1050.0, 1200.0],
        "avant_hc": [300.0, 400.0, 500.0, 600.0, 700.0, 800.0],
        "apres_hc": [300.0, 400.0, 500.0, 600.0, 700.0, 800.0],
        "avant_hph": [None, None, None, None, None, None],
        "apres_hph": [None, None, None, None, None, None],
        "avant_hch": [None, None, None, None, None, None],
        "apres_hch": [None, None, None, None, None, None],
        "avant_hpb": [None, None, None, None, None, None],
        "apres_hpb": [None, None, None, None, None, None],
        "avant_hcb": [None, None, None, None, None, None],
        "apres_hcb": [None, None, None, None, None, None],
    })
    
    result = df.select([
        pl.col("date_evenement"),
        pl.col("evenement_declencheur"),
        expr_changement_avant_apres("avant_id_calendrier_distributeur", "apres_id_calendrier_distributeur").alias("change_calendrier"),
        expr_changement_index().alias("change_index"),
        expr_changement_avant_apres("avant_formule_tarifaire_acheminement", "formule_tarifaire_acheminement").alias("change_fta"),
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
        "ref_situation_contractuelle": ["PDL1", "PDL1", "PDL1", "PDL1", "PDL1"],
        "date_evenement": ["2024-01-01", "2024-02-01", "2024-03-15", "2024-04-01", "2024-05-20"],
        "evenement_declencheur": ["MES", "FACTURATION", "MCT", "FACTURATION", "RES"],
        "puissance_souscrite": [6.0, 6.0, 9.0, 9.0, 9.0],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
        
        # Colonnes Avant_/Après_ pour calendrier et index (déjà présentes dans les données)
        "avant_id_calendrier_distributeur": ["CAL1", "CAL1", "CAL1", "CAL2", "CAL2"],
        "apres_id_calendrier_distributeur": ["CAL1", "CAL1", "CAL2", "CAL2", "CAL2"],
        "avant_base": [1000.0, 1200.0, 1500.0, 1800.0, 2100.0],
        "apres_base": [1000.0, 1200.0, 1550.0, 1800.0, 2100.0],  # Changement ligne 3
        "avant_hp": [None, None, None, None, None],
        "apres_hp": [None, None, None, None, None],
        "avant_hc": [None, None, None, None, None],
        "apres_hc": [None, None, None, None, None],
        "avant_hph": [None, None, None, None, None],
        "apres_hph": [None, None, None, None, None],
        "avant_hch": [None, None, None, None, None],
        "apres_hch": [None, None, None, None, None],
        "avant_hpb": [None, None, None, None, None],
        "apres_hpb": [None, None, None, None, None],
        "avant_hcb": [None, None, None, None, None],
        "apres_hcb": [None, None, None, None, None],
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
    assert "avant_puissance_souscrite" in resultat.columns
    assert "avant_formule_tarifaire_acheminement" in resultat.columns
    
    print(f"Pipeline complet testé avec succès:")
    print(f"- Impacts abonnement: {sum(impacts_abonnement)}/5")
    print(f"- Impacts énergie: {sum(impacts_energie)}/5")
    print(f"- Résumés générés: {len([r for r in resumes if r != ''])}/5")


# =============================================================================
# TESTS POUR LES EXPRESSIONS DE FACTURATION
# =============================================================================

def test_expr_evenement_entree():
    """Teste la détection des événements d'entrée dans le périmètre."""
    df = pl.DataFrame({
        "evenement_declencheur": ["CFNE", "MES", "PMES", "MCT", "FACTURATION", "RES", "CFNS", "AUTRE"],
    })
    
    result = df.select(
        expr_evenement_entree().alias("entree")
    )
    
    # CFNE, MES, PMES = True (événements d'entrée)
    # Autres = False
    assert result["entree"].to_list() == [True, True, True, False, False, False, False, False]


def test_expr_evenement_sortie():
    """Teste la détection des événements de sortie du périmètre."""
    df = pl.DataFrame({
        "evenement_declencheur": ["RES", "CFNS", "CFNE", "MES", "MCT", "FACTURATION", "AUTRE"],
    })
    
    result = df.select(
        expr_evenement_sortie().alias("sortie")
    )
    
    # RES, CFNS = True (événements de sortie)
    # Autres = False
    assert result["sortie"].to_list() == [True, True, False, False, False, False, False]


def test_colonnes_evenement_facturation():
    """Teste la génération des colonnes d'événement de facturation."""
    df = pl.DataFrame({"dummy": [1, 2, 3]})
    
    result = df.with_columns(**colonnes_evenement_facturation())
    
    # Vérifier que toutes les colonnes attendues sont présentes
    colonnes_attendues = {
        "evenement_declencheur": "FACTURATION",
        "type_evenement": "artificiel",
        "source": "synthese_mensuelle", 
        "resume_modification": "Facturation mensuelle",
        "impacte_abonnement": True,
        "impacte_energie": True
    }
    
    for col, valeur_attendue in colonnes_attendues.items():
        assert col in result.columns, f"Colonne manquante: {col}"
        valeurs = result[col].to_list()
        assert all(v == valeur_attendue for v in valeurs), f"Valeurs incorrectes pour {col}: {valeurs}"


def test_expr_date_entree_periode():
    """Teste le calcul de la date d'entrée dans le périmètre."""
    df = pl.DataFrame({
        "ref_situation_contractuelle": ["PDL1", "PDL1", "PDL1", "PDL2", "PDL2"],
        "date_evenement": [
            "2024-01-15", "2024-02-01", "2024-03-10",  # PDL1
            "2024-06-01", "2024-07-15"  # PDL2
        ],
        "evenement_declencheur": ["MES", "MCT", "FACTURATION", "CFNE", "RES"],
    }).with_columns(
        pl.col("date_evenement").str.strptime(pl.Datetime, "%Y-%m-%d").dt.replace_time_zone("Europe/Paris")
    )
    
    result = df.group_by("ref_situation_contractuelle").agg(
        expr_date_entree_periode().alias("date_entree")
    ).sort("ref_situation_contractuelle")
    
    # PDL1: MES le 2024-01-15 (événement d'entrée le plus ancien)
    # PDL2: CFNE le 2024-06-01 (seul événement d'entrée)
    dates_entree = result["date_entree"].dt.strftime("%Y-%m-%d").to_list()
    assert dates_entree == ["2024-01-15", "2024-06-01"]


def test_expr_date_sortie_periode_avec_sortie():
    """Teste le calcul de la date de sortie quand il y a un événement de sortie."""
    df = pl.DataFrame({
        "ref_situation_contractuelle": ["PDL1", "PDL1", "PDL1"],
        "date_evenement": ["2024-01-01", "2024-02-01", "2024-03-15"],
        "evenement_declencheur": ["MES", "MCT", "RES"],
    }).with_columns(
        pl.col("date_evenement").str.strptime(pl.Datetime, "%Y-%m-%d").dt.replace_time_zone("Europe/Paris")
    )
    
    result = df.group_by("ref_situation_contractuelle").agg(
        expr_date_sortie_periode().alias("date_sortie")
    )
    
    # RES le 2024-03-15 (événement de sortie)
    date_sortie = result["date_sortie"][0].strftime("%Y-%m-%d")
    assert date_sortie == "2024-03-15"


def test_expr_date_sortie_periode_sans_sortie():
    """Teste le calcul de la date de sortie quand il n'y a pas d'événement de sortie."""
    df = pl.DataFrame({
        "ref_situation_contractuelle": ["PDL1", "PDL1"],
        "date_evenement": ["2024-01-01", "2024-02-01"],
        "evenement_declencheur": ["MES", "MCT"],
    }).with_columns([
        pl.col("date_evenement").str.strptime(pl.Datetime, "%Y-%m-%d").dt.replace_time_zone("Europe/Paris")
    ])
    
    result = df.group_by("ref_situation_contractuelle").agg(
        expr_date_sortie_periode().alias("date_sortie")
    )
    
    # Pas d'événement de sortie → doit utiliser la date par défaut (début du mois courant)
    date_sortie = result["date_sortie"][0]
    
    # La date doit être un début de mois (jour = 1) avec timezone Europe/Paris  
    assert date_sortie.day == 1
    assert str(date_sortie.tzinfo) == "Europe/Paris"




def test_inserer_evenements_facturation_scenario_simple():
    """Teste la fonction complète avec un scénario simple d'un PDL."""
    historique = pl.LazyFrame({
        "ref_situation_contractuelle": ["PDL1", "PDL1", "PDL1"],
        "pdl": ["14500000123456", "14500000123456", "14500000123456"],
        "date_evenement": ["2024-01-15", "2024-02-10", "2024-04-20"],
        "evenement_declencheur": ["MES", "MCT", "RES"],
        "puissance_souscrite": [6.0, 9.0, 9.0],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFMU4", "BTINFMU4"],
        "type_compteur": ["Linky", "Linky", "Linky"],
        "num_compteur": ["123456789", "123456789", "123456789"],
    }).with_columns(
        pl.col("date_evenement").str.strptime(pl.Datetime, "%Y-%m-%d").dt.replace_time_zone("Europe/Paris")
    )
    
    resultat = inserer_evenements_facturation(historique).collect().sort(["ref_situation_contractuelle", "date_evenement"])
    
    # Vérifier le nombre total de lignes : 3 originales + 3 FACTURATION (fév, mars, avril)
    assert len(resultat) == 6, f"6 lignes attendues, reçu: {len(resultat)}"
    
    # Vérifier les événements de facturation ajoutés
    evenements_facturation = resultat.filter(pl.col("evenement_declencheur") == "FACTURATION")
    assert len(evenements_facturation) == 3, "3 événements FACTURATION attendus"
    
    # Vérifier les dates des événements de facturation
    dates_facturation = evenements_facturation["date_evenement"].dt.strftime("%Y-%m-%d").to_list()
    dates_attendues = ["2024-02-01", "2024-03-01", "2024-04-01"]
    assert dates_facturation == dates_attendues, f"Dates: {dates_facturation}, attendues: {dates_attendues}"
    
    # Vérifier la propagation des données contractuelles
    # L'événement FACTURATION de février doit avoir les données de MES (Puissance_Souscrite=6.0)
    evenement_fevrier = resultat.filter(
        (pl.col("date_evenement").dt.strftime("%Y-%m-%d") == "2024-02-01") &
        (pl.col("evenement_declencheur") == "FACTURATION")
    )
    assert len(evenement_fevrier) == 1
    assert evenement_fevrier["puissance_souscrite"][0] == 6.0, "Propagation incorrecte pour février"
    assert evenement_fevrier["formule_tarifaire_acheminement"][0] == "BTINFCU4"
    
    # L'événement FACTURATION d'avril doit avoir les données de MCT (Puissance_Souscrite=9.0)
    evenement_avril = resultat.filter(
        (pl.col("date_evenement").dt.strftime("%Y-%m-%d") == "2024-04-01") &
        (pl.col("evenement_declencheur") == "FACTURATION")
    )
    assert len(evenement_avril) == 1
    assert evenement_avril["puissance_souscrite"][0] == 9.0, "Propagation incorrecte pour avril"
    assert evenement_avril["formule_tarifaire_acheminement"][0] == "BTINFMU4"


def test_inserer_evenements_facturation_plusieurs_pdl():
    """Teste la fonction avec plusieurs PDL ayant des périodes différentes."""
    historique = pl.LazyFrame({
        "ref_situation_contractuelle": ["PDL1", "PDL1", "PDL2", "PDL2", "PDL2"],
        "pdl": ["145001", "145001", "145002", "145002", "145002"],
        "date_evenement": ["2024-01-01", "2024-03-15", "2024-02-10", "2024-02-20", "2024-05-01"],
        "evenement_declencheur": ["MES", "RES", "CFNE", "MCT", "CFNS"],
        "puissance_souscrite": [6.0, 6.0, 9.0, 12.0, 12.0],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
        "type_compteur": ["Linky", "Linky", "Linky", "Linky", "Linky"],
        "num_compteur": ["111", "111", "222", "222", "222"],
    }).with_columns(
        pl.col("date_evenement").str.strptime(pl.Datetime, "%Y-%m-%d").dt.replace_time_zone("Europe/Paris")
    )
    
    resultat = inserer_evenements_facturation(historique).collect().sort(["ref_situation_contractuelle", "date_evenement"])
    
    # PDL1 : jan (MES) -> mars (RES) → FACTURATION en février et mars
    # PDL2 : fév (CFNE) -> mai (CFNS) → FACTURATION en mars, avril, mai
    # Total : 5 originales + 5 FACTURATION = 10 lignes
    assert len(resultat) == 10, f"10 lignes attendues, reçu: {len(resultat)}"
    
    # Vérifier les événements de facturation par PDL
    facturation_pdl1 = resultat.filter(
        (pl.col("ref_situation_contractuelle") == "PDL1") &
        (pl.col("evenement_declencheur") == "FACTURATION")
    ).sort("date_evenement")
    
    facturation_pdl2 = resultat.filter(
        (pl.col("ref_situation_contractuelle") == "PDL2") &
        (pl.col("evenement_declencheur") == "FACTURATION")
    ).sort("date_evenement")
    
    # PDL1 : février et mars
    assert len(facturation_pdl1) == 2
    dates_pdl1 = facturation_pdl1["date_evenement"].dt.strftime("%Y-%m-%d").to_list()
    assert dates_pdl1 == ["2024-02-01", "2024-03-01"]
    
    # PDL2 : mars, avril, mai
    assert len(facturation_pdl2) == 3  
    dates_pdl2 = facturation_pdl2["date_evenement"].dt.strftime("%Y-%m-%d").to_list()
    assert dates_pdl2 == ["2024-03-01", "2024-04-01", "2024-05-01"]


def test_generer_dates_facturation_cas_nominal():
    """Teste la génération des dates de facturation pour un cas nominal."""
    import datetime as dt
    
    df = pl.LazyFrame({
        "ref_situation_contractuelle": ["PDL1", "PDL1", "PDL1"],
        "pdl": ["123", "123", "123"], 
        "date_evenement": [
            dt.datetime(2024, 1, 15, tzinfo=dt.timezone.utc).replace(tzinfo=None),
            dt.datetime(2024, 3, 10, tzinfo=dt.timezone.utc).replace(tzinfo=None),
            dt.datetime(2024, 4, 20, tzinfo=dt.timezone.utc).replace(tzinfo=None),
        ],
        "evenement_declencheur": ["MES", "MCT", "RES"],
    }).with_columns([
        pl.col("date_evenement").dt.replace_time_zone("Europe/Paris")
    ])
    
    resultat = generer_dates_facturation(df).collect().sort("date_evenement")
    
    # Doit générer février, mars, avril (strictement après 15/01 et avant/égal 20/04)
    assert len(resultat) == 3
    dates = resultat["date_evenement"].dt.strftime("%Y-%m-%d").to_list()
    assert dates == ["2024-02-01", "2024-03-01", "2024-04-01"]
    
    # Vérifier les colonnes génériques 
    assert all(resultat["evenement_declencheur"] == "FACTURATION")
    assert all(resultat["type_evenement"] == "artificiel")
    assert all(resultat["source"] == "synthese_mensuelle")


def test_generer_dates_facturation_pdl_non_resilie():
    """Teste la génération pour un PDL non résilié (utilise date par défaut)."""
    import datetime as dt
    
    df = pl.LazyFrame({
        "ref_situation_contractuelle": ["PDL1", "PDL1"],
        "pdl": ["123", "123"],
        "date_evenement": [
            dt.datetime(2024, 1, 15, tzinfo=dt.timezone.utc).replace(tzinfo=None),
            dt.datetime(2024, 2, 10, tzinfo=dt.timezone.utc).replace(tzinfo=None),
        ],
        "evenement_declencheur": ["MES", "MCT"],  # Pas de RES
    }).with_columns([
        pl.col("date_evenement").dt.replace_time_zone("Europe/Paris")
    ])
    
    resultat = generer_dates_facturation(df).collect()
    
    # Doit générer des événements jusqu'au début du mois courant inclus
    assert len(resultat) > 0
    assert all(resultat["evenement_declencheur"] == "FACTURATION")


def test_generer_dates_facturation_aucune_periode_valide():
    """Teste le cas où aucune période d'activité valide n'est trouvée."""
    import datetime as dt
    
    df = pl.LazyFrame({
        "ref_situation_contractuelle": ["PDL1"],
        "pdl": ["123"],
        "date_evenement": [
            dt.datetime(2024, 1, 15, tzinfo=dt.timezone.utc).replace(tzinfo=None),
        ],
        "evenement_declencheur": ["MCT"],  # Pas d'événement d'entrée
    }).with_columns([
        pl.col("date_evenement").dt.replace_time_zone("Europe/Paris")
    ])
    
    resultat = generer_dates_facturation(df).collect()
    
    # Aucun événement ne doit être généré
    assert len(resultat) == 0


def test_expr_colonnes_a_propager_forward_fill():
    """Teste la propagation des colonnes par forward fill."""
    df = pl.LazyFrame({
        "ref_situation_contractuelle": ["PDL1", "PDL1", "PDL1"],
        "date_evenement": ["2024-01-01", "2024-02-01", "2024-03-01"],
        # Toutes les colonnes obligatoires
        "puissance_souscrite": [6.0, None, None],
        "segment_clientele": ["PRO", None, None],
        "type_compteur": ["ELEC", None, None],
        "etat_contractuel": ["EN_SERVICE", None, None],
        "formule_tarifaire_acheminement": ["TURPE", None, None],
        "num_compteur": ["123456", None, None],
        # Colonnes optionnelles
        "categorie": ["C5", None, None],
        "ref_demandeur": ["REF001", None, None],
        "id_affaire": ["AFF001", None, None],
    }).with_columns([
        pl.col("date_evenement").str.strptime(pl.Datetime, "%Y-%m-%d").dt.replace_time_zone("Europe/Paris")
    ]).sort(["ref_situation_contractuelle", "date_evenement"])
    
    resultat = df.with_columns(expr_colonnes_a_propager()).collect()
    
    # Vérifier que les valeurs sont propagées pour les colonnes obligatoires
    assert resultat["puissance_souscrite"].to_list() == [6.0, 6.0, 6.0]
    assert resultat["segment_clientele"].to_list() == ["PRO", "PRO", "PRO"] 
    assert resultat["type_compteur"].to_list() == ["ELEC", "ELEC", "ELEC"]
    assert resultat["etat_contractuel"].to_list() == ["EN_SERVICE", "EN_SERVICE", "EN_SERVICE"]
    assert resultat["formule_tarifaire_acheminement"].to_list() == ["TURPE", "TURPE", "TURPE"]
    assert resultat["num_compteur"].to_list() == ["123456", "123456", "123456"]
    
    # Vérifier que les valeurs sont propagées pour les colonnes optionnelles
    assert resultat["categorie"].to_list() == ["C5", "C5", "C5"]
    assert resultat["ref_demandeur"].to_list() == ["REF001", "REF001", "REF001"]
    assert resultat["id_affaire"].to_list() == ["AFF001", "AFF001", "AFF001"]


def test_inserer_evenements_facturation_integration():
    """Teste l'intégration complète de la fonction principale."""
    import datetime as dt
    
    # Historique avec entrée, modification et sortie 
    df = pl.LazyFrame({
        "ref_situation_contractuelle": ["PDL1", "PDL1", "PDL1"],
        "pdl": ["123", "123", "123"],
        "date_evenement": [
            dt.datetime(2024, 1, 15, tzinfo=dt.timezone.utc).replace(tzinfo=None),
            dt.datetime(2024, 2, 10, tzinfo=dt.timezone.utc).replace(tzinfo=None),
            dt.datetime(2024, 4, 20, tzinfo=dt.timezone.utc).replace(tzinfo=None),
        ],
        "evenement_declencheur": ["MES", "MCT", "RES"],
        "puissance_souscrite": [6.0, 9.0, 9.0],
        "segment_clientele": ["PRO", "PRO", "PRO"],
        "type_compteur": ["ELEC", "ELEC", "ELEC"],
        "formule_tarifaire_acheminement": ["TURPE", "TURPE", "TURPE"],
        "num_compteur": ["123456", "123456", "123456"],
        "etat_contractuel": ["EN_SERVICE", "EN_SERVICE", "RESILIE"],
    }).with_columns([
        pl.col("date_evenement").dt.replace_time_zone("Europe/Paris")
    ])
    
    resultat = inserer_evenements_facturation(df).collect().sort("date_evenement")
    
    # Doit avoir les 3 événements originaux + 3 événements FACTURATION (fév, mars, avr)
    assert len(resultat) == 6
    
    # Vérifier les événements FACTURATION
    facturation = resultat.filter(pl.col("evenement_declencheur") == "FACTURATION")
    assert len(facturation) == 3
    
    dates_factu = facturation["date_evenement"].dt.strftime("%Y-%m-%d").to_list()
    assert dates_factu == ["2024-02-01", "2024-03-01", "2024-04-01"]
    
    # Vérifier la propagation des données : les événements FACTURATION 
    # doivent hériter de la puissance de l'événement précédent
    factu_fevrier = resultat.filter(
        (pl.col("date_evenement").dt.strftime("%Y-%m-%d") == "2024-02-01")
    ).item(0, "puissance_souscrite")  # Hérité de MES (6.0)
    assert factu_fevrier == 6.0
    
    factu_mars = resultat.filter(
        (pl.col("date_evenement").dt.strftime("%Y-%m-%d") == "2024-03-01")
    ).item(0, "puissance_souscrite")  # Hérité de MCT (9.0)
    assert factu_mars == 9.0