"""Tests d'intégration de `POST /facturation/turpe-variable` (ADR-0030, #247).

Calculateur **sans état** : Odoo POST l'assiette (énergies par cadran + FTA + `debut`),
electricore renvoie le **montant** €. Le calcul tourne pour de vrai contre
`turpe_rules.csv` (déterministe, versionné) — **pas de mock** : l'enjeu est de vérifier
les montants face aux coefficients réels. Le seam d'auth est surchargé comme pour les
méta-périodes ; les FTA/coefficients sont ceux du barème en vigueur au 2025-08-01.
"""

import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key


@pytest.fixture
def client():
    """TestClient avec auth surchargée (le chemin transport + calcul reste réel)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def test_turpe_variable_base(client):
    """Tracer bullet : une ligne BASE → montant = energie × c_base / 100, enveloppe JSON.

    BTINFCUST au 2025-08-01 : c_base = 4.84 c€/kWh → 1000 × 4.84 / 100 = 48.4 €.
    """
    response = client.post(
        "/facturation/turpe-variable",
        json={
            "lignes": [
                {
                    "id": "abc",
                    "formule_tarifaire_acheminement": "BTINFCUST",
                    "debut": "2025-08-15",
                    "energie_base_kwh": 1000.0,
                }
            ]
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["contract_version"] == 1
    assert body["results"] == [{"id": "abc", "turpe_variable_eur": 48.4}]


def test_turpe_variable_hp_hc(client):
    """Granularité HP/HC : montant = (hp × c_hp + hc × c_hc) / 100.

    BTINFMUDT au 2025-08-01 : c_hp = 4.94, c_hc = 3.5 →
    (312.4 × 4.94 + 145.2 × 3.5) / 100 = 20.51 €.
    """
    response = client.post(
        "/facturation/turpe-variable",
        json={
            "lignes": [
                {
                    "id": "hphc",
                    "formule_tarifaire_acheminement": "BTINFMUDT",
                    "debut": "2025-08-15",
                    "energie_hp_kwh": 312.4,
                    "energie_hc_kwh": 145.2,
                }
            ]
        },
    )
    assert response.status_code == 200
    assert response.json()["results"] == [{"id": "hphc", "turpe_variable_eur": 20.51}]


def test_turpe_variable_4_cadrans_zeros_arbitrent(client):
    """4 cadrans envoyés sur une FTA C5 communicante (BTINFCU4) : les zéros arbitrent.

    On envoie **les 7** cadrans (base/hp/hc en leurres) ; comme BTINFCU4 n'a de
    coefficients non-nuls qu'à 4 cadrans, les leurres sont multipliés par 0 → pas de
    double comptage. BTINFCU4 au 2025-08-01 : c_hph=7.49, c_hpb=1.66, c_hch=3.97,
    c_hcb=1.16 → (100×7.49 + 200×1.66 + 50×3.97 + 80×1.16) / 100 = 13.72 €.
    """
    response = client.post(
        "/facturation/turpe-variable",
        json={
            "lignes": [
                {
                    "id": "q4",
                    "formule_tarifaire_acheminement": "BTINFCU4",
                    "debut": "2025-08-15",
                    "energie_base_kwh": 999.0,  # leurre : c_base = 0 sur cette FTA
                    "energie_hp_kwh": 999.0,  # leurre : c_hp = 0
                    "energie_hc_kwh": 999.0,  # leurre : c_hc = 0
                    "energie_hph_kwh": 100.0,
                    "energie_hpb_kwh": 200.0,
                    "energie_hch_kwh": 50.0,
                    "energie_hcb_kwh": 80.0,
                }
            ]
        },
    )
    assert response.status_code == 200
    assert response.json()["results"] == [{"id": "q4", "turpe_variable_eur": 13.72}]


def test_turpe_variable_lot_id_reemis(client):
    """Un lot renvoie un montant par `id`, l'`id` ré-émis tel quel (résultat indexé par id)."""
    response = client.post(
        "/facturation/turpe-variable",
        json={
            "lignes": [
                {
                    "id": "L-base",
                    "formule_tarifaire_acheminement": "BTINFCUST",
                    "debut": "2025-08-15",
                    "energie_base_kwh": 1000.0,
                },
                {
                    "id": "L-hphc",
                    "formule_tarifaire_acheminement": "BTINFMUDT",
                    "debut": "2025-08-15",
                    "energie_hp_kwh": 312.4,
                    "energie_hc_kwh": 145.2,
                },
                {
                    "id": "L-q4",
                    "formule_tarifaire_acheminement": "BTINFCU4",
                    "debut": "2025-08-15",
                    "energie_hph_kwh": 100.0,
                    "energie_hpb_kwh": 200.0,
                    "energie_hch_kwh": 50.0,
                    "energie_hcb_kwh": 80.0,
                },
            ]
        },
    )
    assert response.status_code == 200
    results = response.json()["results"]
    assert len(results) == 3
    par_id = {r["id"]: r["turpe_variable_eur"] for r in results}
    assert par_id == {"L-base": 48.4, "L-hphc": 20.51, "L-q4": 13.72}


def test_turpe_variable_refuse_sans_api_key():
    """Endpoint sécurisé : 401 sans clé (pas d'override d'auth ici)."""
    response = TestClient(app).post("/facturation/turpe-variable", json={"lignes": []})
    assert response.status_code == 401


def test_turpe_variable_succes_partiel(client):
    """Lot mixte : ligne valide + FTA inconnue + date sans règle → succès partiel (#251).

    Chaque `id` envoyé revient (autant de résultats que d'entrées), portant **soit**
    `turpe_variable_eur` **soit** un motif d'erreur — jamais de silent-drop. Les lignes
    valides du même lot conservent leur montant correct.
    """
    response = client.post(
        "/facturation/turpe-variable",
        json={
            "lignes": [
                {
                    "id": "ok",
                    "formule_tarifaire_acheminement": "BTINFCUST",
                    "debut": "2025-08-15",
                    "energie_base_kwh": 1000.0,
                },
                {
                    "id": "inconnue",
                    "formule_tarifaire_acheminement": "FTA_QUI_NEXISTE_PAS",
                    "debut": "2025-08-15",
                    "energie_base_kwh": 1000.0,
                },
                {
                    "id": "hors-date",
                    "formule_tarifaire_acheminement": "BTINFCUST",
                    "debut": "1990-01-01",
                    "energie_base_kwh": 1000.0,
                },
            ]
        },
    )
    assert response.status_code == 200
    results = response.json()["results"]
    assert len(results) == 3  # autant de résultats que d'entrées
    par_id = {r["id"]: r for r in results}

    # Ligne valide : montant correct, pas d'erreur
    assert par_id["ok"] == {"id": "ok", "turpe_variable_eur": 48.4}

    # FTA inconnue : motif explicite, pas un drop ni un 0 silencieux
    assert "turpe_variable_eur" not in par_id["inconnue"]
    assert "FTA_QUI_NEXISTE_PAS" in par_id["inconnue"]["error"]

    # Date sans règle temporelle : motif distinct mentionnant la date
    assert "turpe_variable_eur" not in par_id["hors-date"]
    assert "1990" in par_id["hors-date"]["error"]
