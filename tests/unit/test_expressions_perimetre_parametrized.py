"""
Tests unitaires paramétrés pour les expressions Polars du pipeline périmètre.

Ce module démontre l'usage de @pytest.mark.parametrize pour réduire
la duplication de code et améliorer la couverture des edge cases.
"""

import polars as pl
import pytest
from electricore.core.pipelines.perimetre import (
    expr_changement,
    expr_resume_changement,
    expr_evenement_structurant,
    expr_impacte_abonnement,
    expr_impacte_energie,
)


# =========================================================================
# TESTS PARAMÉTRÉS - expr_changement
# =========================================================================


@pytest.mark.unit
@pytest.mark.parametrize(
    "valeurs,expected_changements,description",
    [
        # Cas nominal - détection changement simple
        (
            [6.0, 6.0, 9.0, 9.0, 3.0],
            [False, False, True, False, True],
            "changement_simple"
        ),
        # Cas avec nulls - pas de changement si null
        (
            [None, 6.0, 6.0, None, 3.0],
            [False, False, False, False, False],
            "avec_nulls"
        ),
        # Cas séquence constante - pas de changement
        (
            [6.0, 6.0, 6.0, 6.0],
            [False, False, False, False],
            "sequence_constante"
        ),
        # Cas changements multiples successifs
        (
            [3.0, 6.0, 9.0, 12.0],
            [False, True, True, True],
            "changements_successifs"
        ),
        # Cas valeur unique - pas de changement
        (
            [9.0],
            [False],
            "valeur_unique"
        ),
    ],
    ids=lambda x: x if isinstance(x, str) else ""
)
def test_expr_changement_cases(valeurs, expected_changements, description):
    """Teste expr_changement avec différents patterns de données."""
    df = pl.DataFrame({
        "ref_situation_contractuelle": ["A"] * len(valeurs),
        "valeur": valeurs,
    })

    result = df.select(expr_changement("valeur").alias("changement"))

    assert result["changement"].to_list() == expected_changements, \
        f"Échec pour le cas: {description}"


# =========================================================================
# TESTS PARAMÉTRÉS - expr_resume_changement
# =========================================================================


@pytest.mark.unit
@pytest.mark.parametrize(
    "valeurs,label,expected_resumes",
    [
        # Cas avec strings (FTA)
        (
            ["BTINFCU4", "BTINFCU4", "BTINFMU4"],
            "FTA",
            ["", "", "FTA: BTINFCU4 → BTINFMU4"]
        ),
        # Cas avec nombres (puissance)
        (
            [6.0, 9.0, 12.0],
            "P",
            ["", "P: 6.0 → 9.0", "P: 9.0 → 12.0"]
        ),
        # Cas sans changement
        (
            ["MÊME", "MÊME", "MÊME"],
            "TEST",
            ["", "", ""]
        ),
        # Cas avec nulls
        (
            [None, "VAL", None],
            "X",
            ["", "", ""]
        ),
    ],
)
def test_expr_resume_changement_formats(valeurs, label, expected_resumes):
    """Teste le formatage du résumé de changement."""
    df = pl.DataFrame({
        "ref_situation_contractuelle": ["A"] * len(valeurs),
        "colonne": valeurs,
    })

    result = df.select(
        expr_resume_changement("colonne", label).alias("resume")
    )

    assert result["resume"].to_list() == expected_resumes


# =========================================================================
# TESTS PARAMÉTRÉS - expr_evenement_structurant
# =========================================================================


@pytest.mark.unit
@pytest.mark.parametrize(
    "evenements,expected_structurants",
    [
        # Cas avec événements structurants
        (
            ["MES", "MCT", "RES", "AUTRE"],
            [True, False, True, False]
        ),
        # Cas avec PMES/CFNE/CFNS
        (
            ["PMES", "CFNE", "CFNS", "MCT"],
            [True, True, True, False]
        ),
        # Cas sans événements structurants
        (
            ["MCT", "MCT", "MCT"],
            [False, False, False]
        ),
        # Cas nulls (retourne null, pas False)
        (
            [None, "MES", None],
            [None, True, None]
        ),
    ],
)
def test_expr_evenement_structurant_detection(evenements, expected_structurants):
    """Teste la détection des événements structurants."""
    df = pl.DataFrame({
        "evenement_declencheur": evenements,
    })

    result = df.with_columns(
        expr_evenement_structurant().alias("structurant")
    )

    assert result["structurant"].to_list() == expected_structurants


# =========================================================================
# TESTS PARAMÉTRÉS - expr_impacte_abonnement
# =========================================================================


@pytest.mark.unit
@pytest.mark.parametrize(
    "evenements,changements_puissance,changements_fta,expected_impacte",
    [
        # Cas événement structurant uniquement
        (
            ["MES", "MCT", "RES"],
            [False, False, False],
            [False, False, False],
            [True, False, True]
        ),
        # Cas changement puissance uniquement
        (
            ["MCT", "MCT", "MCT"],
            [False, True, False],
            [False, False, False],
            [False, True, False]
        ),
        # Cas changement FTA uniquement
        (
            ["MCT", "MCT", "MCT"],
            [False, False, False],
            [False, True, False],
            [False, True, False]
        ),
        # Cas combinés
        (
            ["MES", "MCT", "MCT"],
            [False, True, False],
            [False, False, True],
            [True, True, True]
        ),
    ],
)
def test_expr_impacte_abonnement_logique(
    evenements,
    changements_puissance,
    changements_fta,
    expected_impacte
):
    """Teste la logique de détection d'impact sur abonnement."""
    # Créer des valeurs qui changent exactement selon les flags attendus
    puissances = [6.0]  # Première valeur
    ftas = ["BTINFCUST"]  # Première valeur

    for i in range(1, len(evenements)):
        # Changement de puissance si le flag est True
        if changements_puissance[i]:
            puissances.append(9.0 if puissances[-1] == 6.0 else 6.0)
        else:
            puissances.append(puissances[-1])

        # Changement de FTA si le flag est True
        if changements_fta[i]:
            ftas.append("BTINFCU4" if ftas[-1] == "BTINFCUST" else "BTINFCUST")
        else:
            ftas.append(ftas[-1])

    df = pl.DataFrame({
        "ref_situation_contractuelle": ["A"] * len(evenements),
        "evenement_declencheur": evenements,
        "puissance_souscrite": puissances,
        "formule_tarifaire_acheminement": ftas,
    })

    result = df.with_columns(
        expr_impacte_abonnement().alias("impacte_abonnement")
    )

    assert result["impacte_abonnement"].to_list() == expected_impacte


# =========================================================================
# TESTS PARAMÉTRÉS - expr_impacte_energie
# =========================================================================


@pytest.mark.unit
@pytest.mark.parametrize(
    "evenements,changements_calendrier,expected_impacte",
    [
        # Cas événement structurant (MES/RES)
        (
            ["MES", "MCT", "RES", "MCT"],
            [False, False, False, False],
            [True, False, True, False]
        ),
        # Cas changement calendrier
        (
            ["MCT", "MCT", "MCT"],
            [False, True, False],
            [False, True, False]
        ),
        # Cas combinés
        (
            ["MES", "MCT", "RES"],
            [False, True, False],
            [True, True, True]
        ),
        # Cas sans impact
        (
            ["MCT", "MCT"],
            [False, False],
            [False, False]
        ),
    ],
)
@pytest.mark.skip(reason="Needs before/after columns from pipeline")
def test_expr_impacte_energie_logique(
    evenements,
    changements_calendrier,
    expected_impacte
):
    """Teste la logique de détection d'impact sur énergie."""
    # Créer des valeurs de calendrier qui changent selon les flags
    calendriers = ["DI000001"]  # Première valeur (BASE)

    for i in range(1, len(evenements)):
        if changements_calendrier[i]:
            calendriers.append("DI000002" if calendriers[-1] == "DI000001" else "DI000001")
        else:
            calendriers.append(calendriers[-1])

    df = pl.DataFrame({
        "ref_situation_contractuelle": ["A"] * len(evenements),
        "evenement_declencheur": evenements,
        "id_calendrier_distributeur": calendriers,
    })

    result = df.with_columns(
        expr_impacte_energie().alias("impacte_energie")
    )

    assert result["impacte_energie"].to_list() == expected_impacte


# =========================================================================
# TESTS EDGE CASES
# =========================================================================


@pytest.mark.unit
@pytest.mark.parametrize(
    "taille_df",
    [0, 1, 2, 100, 1000],
    ids=["vide", "une_ligne", "deux_lignes", "cent_lignes", "mille_lignes"]
)
def test_expr_changement_tailles_variables(taille_df):
    """Teste expr_changement avec différentes tailles de DataFrame."""
    if taille_df == 0:
        df = pl.DataFrame({
            "ref_situation_contractuelle": [],
            "valeur": [],
        })
    else:
        df = pl.DataFrame({
            "ref_situation_contractuelle": ["A"] * taille_df,
            "valeur": list(range(taille_df)),
        })

    # Doit s'exécuter sans erreur
    result = df.select(expr_changement("valeur").alias("changement"))

    if taille_df == 0:
        assert len(result) == 0
    else:
        assert len(result) == taille_df
        # Premier élément toujours False
        if taille_df >= 1:
            assert result["changement"][0] is False


@pytest.mark.unit
@pytest.mark.parametrize(
    "nb_groupes",
    [1, 2, 5, 10],
    ids=["un_groupe", "deux_groupes", "cinq_groupes", "dix_groupes"]
)
def test_expr_changement_groupes_multiples(nb_groupes):
    """Teste expr_changement avec plusieurs groupes."""
    # Créer des données avec nb_groupes différents
    refs = [f"REF{i:03d}" for i in range(nb_groupes) for _ in range(3)]
    valeurs = [6.0, 6.0, 9.0] * nb_groupes

    df = pl.DataFrame({
        "ref_situation_contractuelle": refs,
        "valeur": valeurs,
    })

    result = df.select(expr_changement("valeur").alias("changement"))

    # Vérifier pattern: False, False, True pour chaque groupe
    expected = [False, False, True] * nb_groupes
    assert result["changement"].to_list() == expected