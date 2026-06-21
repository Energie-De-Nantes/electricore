"""Tests unitaires pour les expressions Polars du pipeline historique (branche abonnement).

Depuis ADR-0041 (#378), `pipeline_historique` consomme la spine et ne porte plus que la
**branche abonnement** : changement puissance/FTA, événement structurant, bornes FACTURATION.
Les expressions de la branche énergie (calendrier/index) et la génération des FACTURATION
ont quitté le cœur (portées par dbt — spine + *Chronologie des relevés*, #375/#377) ; leurs
tests ont été retirés ici.
"""

import polars as pl

from electricore.core.pipelines.historique import (
    detecter_points_de_rupture,
    expr_changement,
    expr_evenement_structurant,
    expr_impacte_abonnement,
    expr_resume_changement,
)


def test_expr_changement_detecte_changements():
    """Teste que expr_changement détecte correctement les changements."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A", "A", "B", "B"],
            "puissance_souscrite_kva": [6.0, 6.0, 9.0, 3.0, 3.0],
        }
    )

    result = df.select(expr_changement("puissance_souscrite_kva").alias("changement"))

    # Première ligne de chaque groupe : pas de précédent = False
    # Deuxième ligne groupe A : 6.0 -> 6.0 = False
    # Troisième ligne groupe A : 6.0 -> 9.0 = True
    # Première ligne groupe B : pas de précédent = False
    # Deuxième ligne groupe B : 3.0 -> 3.0 = False
    assert result["changement"].to_list() == [False, False, True, False, False]


def test_expr_changement_avec_nulls():
    """Teste que expr_changement gère correctement les valeurs nulles."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A", "A", "A"],
            "Valeur": [None, 5.0, 5.0, None],
        }
    )

    result = df.select(expr_changement("Valeur").alias("changement"))

    # Première ligne : pas de précédent = False
    # Deuxième ligne : None -> 5.0 = False (pas de valeur précédente valide)
    # Troisième ligne : 5.0 -> 5.0 = False (pas de changement)
    # Quatrième ligne : 5.0 -> None = False (pas de comparaison avec None)
    assert result["changement"].to_list() == [False, False, False, False]


def test_expr_resume_changement():
    """Teste que expr_resume_changement génère le bon texte."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A", "A"],
            "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4"],
        }
    )

    result = df.select(expr_resume_changement("formule_tarifaire_acheminement", "FTA").alias("resume"))

    expected = [
        "",  # Première ligne : pas de changement
        "",  # BTINFCU4 -> BTINFCU4 : pas de changement
        "FTA: BTINFCU4 → BTINFMU4",  # BTINFCU4 -> BTINFMU4 : changement
    ]
    assert result["resume"].to_list() == expected


def test_expr_resume_changement_avec_nombres():
    """Teste le formatage des nombres dans le résumé."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A"],
            "puissance_souscrite_kva": [6.0, 9.0],
        }
    )

    result = df.select(expr_resume_changement("puissance_souscrite_kva", "P").alias("resume"))

    assert result["resume"].to_list() == ["", "P: 6.0 → 9.0"]


def test_composition_expressions():
    """Teste que les expressions se composent correctement."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A", "A"],
            "puissance_souscrite_kva": [6.0, 6.0, 9.0],
            "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFMU4", "BTINFMU4"],
            "evenement_declencheur": ["FACTURATION", "MCT", "MCT"],  # Événements non structurants
        }
    )

    # Utiliser les deux expressions ensemble
    result = df.select(
        [
            expr_changement("puissance_souscrite_kva").alias("change_puissance"),
            expr_changement("formule_tarifaire_acheminement").alias("change_fta"),
            expr_resume_changement("puissance_souscrite_kva", "P").alias("resume_puissance"),
            expr_resume_changement("formule_tarifaire_acheminement", "FTA").alias("resume_fta"),
        ]
    )

    assert result["change_puissance"].to_list() == [False, False, True]
    assert result["change_fta"].to_list() == [False, True, False]
    assert result["resume_puissance"].to_list() == ["", "", "P: 6.0 → 9.0"]
    assert result["resume_fta"].to_list() == ["", "FTA: BTINFCU4 → BTINFMU4", ""]


def test_expr_impacte_abonnement():
    """`expr_impacte_abonnement` détecte (purement) les changements de puissance ou FTA.

    La force des bornes FACTURATION est portée par `detecter_points_de_rupture`, pas par
    cette expression : ici une ligne FACTURATION sans changement reste False."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A", "A", "A"],
            "puissance_souscrite_kva": [6.0, 6.0, 9.0, 9.0],
            "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
            "evenement_declencheur": ["FACTURATION", "MCT", "MCT", "FACTURATION"],  # Événements non structurants
        }
    )

    result = df.select(expr_impacte_abonnement().alias("impacte"))

    # Ligne 1: pas de précédent = False
    # Ligne 2: puissance identique mais FTA change = True
    # Ligne 3: FTA identique mais puissance change = True
    # Ligne 4: aucun changement = False
    assert result["impacte"].to_list() == [False, True, True, False]


def test_expr_impacte_abonnement_avec_nulls():
    """Teste la gestion des nulls dans expr_impacte_abonnement."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A", "A"],
            "puissance_souscrite_kva": [None, 6.0, 6.0],
            "formule_tarifaire_acheminement": ["BTINFCU4", None, "BTINFCU4"],
            "evenement_declencheur": ["FACTURATION", "MCT", "FACTURATION"],  # Événements non structurants
        }
    )

    result = df.select(expr_impacte_abonnement().alias("impacte"))

    # Ligne 1: pas de précédent = False
    # Ligne 2: comparaison avec None = False (logique conservatrice)
    # Ligne 3: aucun changement = False
    assert result["impacte"].to_list() == [False, False, False]


def test_expr_impacte_abonnement_composition():
    """Teste que expr_impacte_abonnement compose bien les expressions individuelles."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A", "A", "A", "A"],
            "puissance_souscrite_kva": [6.0, 9.0, 9.0, 6.0, 6.0],
            "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFCU4"],
            "evenement_declencheur": ["FACTURATION", "MCT", "MCT", "MCT", "MCT"],  # Événements non structurants
        }
    )

    result = df.select(
        [
            expr_changement("puissance_souscrite_kva").alias("change_puissance"),
            expr_changement("formule_tarifaire_acheminement").alias("change_fta"),
            expr_impacte_abonnement().alias("impacte_abonnement"),
        ]
    )

    expected_puissance = [False, True, False, True, False]
    expected_fta = [False, False, True, False, True]
    expected_impacte = [
        False,  # Ligne 1: aucun précédent
        True,  # Ligne 2: puissance change
        True,  # Ligne 3: FTA change
        True,  # Ligne 4: puissance change
        True,  # Ligne 5: FTA change
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
    df = pl.DataFrame(
        {
            "evenement_declencheur": ["MES", "MCT", "CFNE", "FACTURATION", "RES", "PMES", "CFNS", "AUTRE"],
        }
    )

    result = df.select(expr_evenement_structurant().alias("structurant"))

    # MES, CFNE, RES, PMES, CFNS = True (événements structurants)
    # MCT, FACTURATION, AUTRE = False (événements normaux)
    assert result["structurant"].to_list() == [True, False, True, False, True, True, True, False]


def test_expr_impacte_abonnement_avec_evenements_structurants():
    """Teste que expr_impacte_abonnement intègre bien les événements structurants."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["A", "A", "A", "A", "A"],
            "puissance_souscrite_kva": [6.0, 6.0, 6.0, 9.0, 9.0],
            "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
            "evenement_declencheur": ["MES", "FACTURATION", "MCT", "MCT", "RES"],
        }
    )

    result = df.select(
        [
            expr_changement("puissance_souscrite_kva").alias("change_puissance"),
            expr_changement("formule_tarifaire_acheminement").alias("change_fta"),
            expr_evenement_structurant().alias("evenement_structurant"),
            expr_impacte_abonnement().alias("impacte_abonnement"),
        ]
    )

    expected = [
        # MES: événement structurant (même sans changement)
        {"change_p": False, "change_f": False, "struct": True, "impact": True},
        # FACTURATION: aucun changement, pas structurant (force portée par detecter_points_de_rupture)
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


def test_detecter_points_de_rupture_pipeline_complet():
    """`detecter_points_de_rupture` sur une spine (épine + situation forward-fillée).

    Branche abonnement uniquement (ADR-0041, #378) : `impacte_abonnement` + `resume_modification`
    (puissance/FTA). Les bornes FACTURATION (forward-fillées, donc « sans changement ») sont
    forcées à `impacte_abonnement=True` — ce sont des bornes de période."""
    # Spine d'une RSC : MES → FACTURATION → MCT (puissance + FTA) → FACTURATION → RES
    spine_data = {
        "ref_situation_contractuelle": ["PDL1", "PDL1", "PDL1", "PDL1", "PDL1"],
        "date_evenement": ["2024-01-01", "2024-02-01", "2024-03-15", "2024-04-01", "2024-05-20"],
        "evenement_declencheur": ["MES", "FACTURATION", "MCT", "FACTURATION", "RES"],
        # Discriminant typé de la spine (épine) : FACTURATION ⇒ 'facturation', sinon 'evenement'.
        "type_fait": ["evenement", "facturation", "evenement", "facturation", "evenement"],
        "puissance_souscrite_kva": [6.0, 6.0, 9.0, 9.0, 9.0],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],
    }

    resultat = detecter_points_de_rupture(pl.LazyFrame(spine_data)).collect()

    # Colonnes de la branche abonnement (plus d'impacte_energie)
    for col in ("impacte_abonnement", "resume_modification"):
        assert col in resultat.columns, f"Colonne manquante: {col}"
    assert "impacte_energie" not in resultat.columns

    # MES: structurant → True ; FACTURATION: forcé → True ; MCT: changement → True ;
    # FACTURATION: forcé → True ; RES: structurant → True
    assert resultat["impacte_abonnement"].to_list() == [True, True, True, True, True]

    # Un seul résumé non-vide : la ligne MCT (puissance + FTA), sans calendrier/index
    resumes = resultat["resume_modification"].to_list()
    resumes_non_vides = [r for r in resumes if r != ""]
    assert len(resumes_non_vides) == 1
    resume_principal = resumes_non_vides[0]
    assert "P: 6.0 → 9.0" in resume_principal
    assert "FTA: BTINFCU4 → BTINFMU4" in resume_principal
    assert "Cal:" not in resume_principal  # branche énergie : plus ici
    assert "rupture index" not in resume_principal

    # Colonnes Avant_ créées par la fonction
    assert "avant_puissance_souscrite" in resultat.columns
    assert "avant_formule_tarifaire_acheminement" in resultat.columns
