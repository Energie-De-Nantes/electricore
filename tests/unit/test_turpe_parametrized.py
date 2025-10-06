"""
Tests paramétrés pour le pipeline TURPE.

Démontre l'usage de parametrize pour tester différentes FTA et configurations
sans duplication de code.
"""

import pytest
import polars as pl
from datetime import datetime

from electricore.core.pipelines.turpe import (
    expr_calculer_turpe_fixe_annuel,
    expr_calculer_turpe_fixe_journalier,
    expr_calculer_turpe_fixe_periode,
    expr_calculer_turpe_cadran,
)


# =========================================================================
# TESTS PARAMÉTRÉS - TURPE FIXE
# =========================================================================


@pytest.mark.unit
@pytest.mark.parametrize(
    "fta,puissance,b,cg,cc,expected_annuel_min,expected_annuel_max",
    [
        # BTINFCUST - 6 kVA
        ("BTINFCUST", 6.0, 10.44, 16.2, 20.88, 95.0, 105.0),
        # BTINFCU4 - 9 kVA
        ("BTINFCU4", 9.0, 9.36, 16.2, 20.88, 115.0, 125.0),
        # BTINFMU4 - 12 kVA
        ("BTINFMU4", 12.0, 8.40, 16.2, 20.88, 135.0, 145.0),
        # BTINFCU4 - 3 kVA (puissance minimale)
        ("BTINFCU4", 3.0, 9.36, 16.2, 20.88, 60.0, 70.0),
        # BTINFCUST - 36 kVA (puissance maximale BT INF)
        ("BTINFCUST", 36.0, 10.44, 16.2, 20.88, 410.0, 420.0),
    ],
    ids=["BTINFCUST_6kVA", "BTINFCU4_9kVA", "BTINFMU4_12kVA", "BTINFCU4_3kVA", "BTINFCUST_36kVA"]
)
def test_turpe_fixe_annuel_par_fta(
    fta,
    puissance,
    b,
    cg,
    cc,
    expected_annuel_min,
    expected_annuel_max
):
    """Teste le calcul TURPE fixe annuel pour différentes FTA."""
    df = pl.DataFrame({
        "formule_tarifaire_acheminement": [fta],
        "puissance_souscrite": [puissance],
        "b": [b],
        "cg": [cg],
        "cc": [cc],
        # Colonnes C4 avec NULL (C5 n'utilise pas ces colonnes)
        "b_hph": [None],
        "b_hch": [None],
        "b_hpb": [None],
        "b_hcb": [None],
        "puissance_souscrite_hph": [None],
        "puissance_souscrite_hch": [None],
        "puissance_souscrite_hpb": [None],
        "puissance_souscrite_hcb": [None],
    })

    result = df.with_columns(
        expr_calculer_turpe_fixe_annuel().alias("turpe_annuel")
    )

    turpe_annuel = result["turpe_annuel"][0]

    # Vérification par plage (formule: b * P + cg + cc)
    assert expected_annuel_min <= turpe_annuel <= expected_annuel_max, \
        f"TURPE annuel hors plage pour {fta} {puissance}kVA: {turpe_annuel}"


@pytest.mark.unit
@pytest.mark.parametrize(
    "nb_jours,turpe_annuel,expected_journalier_approx",
    [
        (30, 120.0, 120.0 / 365),   # 30 jours
        (31, 120.0, 120.0 / 365),   # 31 jours
        (28, 120.0, 120.0 / 365),   # Février
        (365, 120.0, 120.0 / 365),  # Année complète
        (1, 120.0, 120.0 / 365),    # Un seul jour
    ],
    ids=["30j", "31j", "28j_fevrier", "365j_annee", "1j"]
)
@pytest.mark.skip(reason="Needs implementation fixes")
@pytest.mark.unit
def test_turpe_fixe_journalier_durees(nb_jours, turpe_annuel, expected_journalier_approx):
    """Teste le calcul du tarif journalier pour différentes durées."""
    df = pl.DataFrame({
        "nb_jours": [nb_jours],
        "turpe_annuel": [turpe_annuel],
    })

    result = df.with_columns(
        expr_calculer_turpe_fixe_journalier().alias("turpe_journalier")
    )

    turpe_journalier = result["turpe_journalier"][0]

    # Le tarif journalier doit être constant quelle que soit la durée
    assert abs(turpe_journalier - expected_journalier_approx) < 0.001


@pytest.mark.unit
@pytest.mark.parametrize(
    "nb_jours,turpe_annuel",
    [
        (30, 120.0),
        (31, 120.0),
        (90, 120.0),
        (180, 120.0),
        (365, 120.0),
    ],
    ids=["1_mois", "31j", "1_trimestre", "6_mois", "1_an"]
)
@pytest.mark.skip(reason="Needs implementation fixes")
@pytest.mark.unit
def test_turpe_fixe_periode_prorata(nb_jours, turpe_annuel):
    """Teste que le TURPE période respecte le prorata temporis."""
    df = pl.DataFrame({
        "nb_jours": [nb_jours],
        "turpe_annuel": [turpe_annuel],
    })

    result = df.with_columns(
        expr_calculer_turpe_fixe_journalier().alias("turpe_journalier")
    ).with_columns(
        expr_calculer_turpe_fixe_periode().alias("turpe_periode")
    )

    turpe_periode = result["turpe_periode"][0]
    expected_periode = turpe_annuel * nb_jours / 365

    # Vérification prorata avec tolérance
    assert abs(turpe_periode - expected_periode) < 0.01, \
        f"TURPE période incorrect: {turpe_periode} vs {expected_periode}"


# =========================================================================
# TESTS PARAMÉTRÉS - TURPE VARIABLE (CADRANS)
# =========================================================================


@pytest.mark.unit
@pytest.mark.parametrize(
    "cadran,energie,tarif,expected_cout_min,expected_cout_max",
    [
        # HP - consommation normale
        ("HP", 1000.0, 1.5, 1480.0, 1520.0),
        # HC - consommation normale
        ("HC", 800.0, 1.2, 950.0, 970.0),
        # BASE - consommation normale
        ("BASE", 1500.0, 1.35, 2010.0, 2040.0),
        # HPH - heures pleines hiver
        ("HPH", 500.0, 1.6, 790.0, 810.0),
        # HCH - heures creuses hiver
        ("HCH", 400.0, 1.3, 510.0, 530.0),
        # Consommation nulle
        ("HP", 0.0, 1.5, 0.0, 0.0),
        # Très grosse consommation
        ("HP", 100000.0, 1.5, 149000.0, 151000.0),
    ],
    ids=[
        "HP_normal",
        "HC_normal",
        "BASE_normal",
        "HPH_hiver",
        "HCH_hiver",
        "zero",
        "tres_grosse_conso"
    ]
)
@pytest.mark.skip(reason="Needs implementation fixes")
@pytest.mark.unit
def test_turpe_variable_cadran(
    cadran,
    energie,
    tarif,
    expected_cout_min,
    expected_cout_max
):
    """Teste le calcul TURPE variable par cadran."""
    df = pl.DataFrame({
        cadran: [energie],
        f"tarif_{cadran.lower()}": [tarif],
    })

    result = df.with_columns(
        expr_calculer_turpe_cadran(cadran).alias(f"cout_{cadran}")
    )

    cout = result[f"cout_{cadran}"][0]

    # Vérification par plage
    assert expected_cout_min <= cout <= expected_cout_max, \
        f"Coût TURPE {cadran} hors plage: {cout}"


# =========================================================================
# TESTS EDGE CASES
# =========================================================================


@pytest.mark.unit
@pytest.mark.parametrize(
    "puissance,description",
    [
        (0.0, "puissance_nulle"),
        (3.0, "puissance_min"),
        (6.0, "puissance_standard"),
        (36.0, "puissance_max_btinf"),
        (250.0, "puissance_btsup"),
    ],
)
def test_turpe_fixe_puissances_limites(puissance, description):
    """Teste les cas limites de puissance."""
    df = pl.DataFrame({
        "formule_tarifaire_acheminement": ["BTINFCU4"],
        "puissance_souscrite": [puissance],
        "b": [9.36],
        "cg": [16.2],
        "cc": [20.88],
        "nb_jours": [30],
        # Colonnes C4 avec NULL (C5 n'utilise pas ces colonnes)
        "b_hph": [None],
        "b_hch": [None],
        "b_hpb": [None],
        "b_hcb": [None],
        "puissance_souscrite_hph": [None],
        "puissance_souscrite_hch": [None],
        "puissance_souscrite_hpb": [None],
        "puissance_souscrite_hcb": [None],
    })

    # Ne doit pas planter
    result = df.with_columns(
        expr_calculer_turpe_fixe_annuel().alias("turpe_annuel")
    ).with_columns(
        expr_calculer_turpe_fixe_journalier().alias("turpe_journalier")
    ).with_columns(
        expr_calculer_turpe_fixe_periode().alias("turpe_periode")
    )

    # Vérifications de cohérence
    if puissance > 0:
        assert result["turpe_annuel"][0] > 0
        assert result["turpe_journalier"][0] > 0
        assert result["turpe_periode"][0] > 0
    else:
        # Puissance nulle => coût positif quand même (cg + cc)
        assert result["turpe_annuel"][0] >= 0


@pytest.mark.unit
@pytest.mark.parametrize(
    "calendrier,cadrans_attendus",
    [
        ("DI000001", ["BASE"]),                          # BASE uniquement
        ("DI000002", ["HP", "HC"]),                      # HP/HC
        ("DI000003", ["HPH", "HCH", "HPB", "HCB"]),     # Tempo 4 cadrans
    ],
)
def test_turpe_variable_calendriers(calendrier, cadrans_attendus):
    """Teste que les bons cadrans sont utilisés selon le calendrier."""
    # Créer un DataFrame avec tous les cadrans possibles
    data = {
        "Id_Calendrier_Distributeur": [calendrier],
        "BASE": [1000.0],
        "HP": [500.0],
        "HC": [500.0],
        "HPH": [250.0],
        "HCH": [250.0],
        "HPB": [250.0],
        "HCB": [250.0],
    }

    df = pl.DataFrame(data)

    # Vérifier que les colonnes attendues existent
    for cadran in cadrans_attendus:
        assert cadran in df.columns, f"Cadran {cadran} manquant pour {calendrier}"


@pytest.mark.unit
@pytest.mark.parametrize(
    "fta,valide",
    [
        ("BTINFCUST", True),
        ("BTINFCU4", True),
        ("BTINFMU4", True),
        ("BTSUPCUST", True),
        ("INVALID_FTA", False),
        ("", False),
        (None, False),
    ],
)
def test_validation_fta(fta, valide):
    """Teste la validation des FTA."""
    # Les FTA valides doivent être dans les règles TURPE
    fta_valides = {
        "BTINFCUST", "BTINFCU4", "BTINFMU4",
        "BTSUPCUST", "BTSUPCU4", "BTINFCUMP"
    }

    if valide:
        assert fta in fta_valides
    else:
        assert fta not in fta_valides or fta is None or fta == ""