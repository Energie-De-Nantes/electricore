"""Tests d'intégration de `GET /facturation/meta-periodes` (ADR-0027/0043, #227/#407).

Endpoint de lecture des méta-périodes mensuelles : Odoo tire d'electricore. Depuis #407,
la réponse est un **flux JSONL** (`application/jsonl`, une ligne = un `PeriodeMeta`) — plus
d'enveloppe paginée. Les métadonnées (`X-Contract-Version`, `X-Mois`) sont en en-têtes, et
chaque ligne est **validée par construction d'un modèle** côté serveur. Le seam de test est
la fonction de service `meta_periodes` référencée dans le router — on court-circuite l'I/O
DuckDB, le chemin transport + sérialisation reste réel.
"""

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from electricore_client.models import PeriodeMeta
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key

PARIS = ZoneInfo("Europe/Paris")


@pytest.fixture
def meta_periodes_synthetiques() -> pl.DataFrame:
    """Mime la sortie projetée de `meta_periodes` (champs `PeriodeMeta` du contrat v3).

    Porte le bloc imbriqué `releves_utilises` (ADR-0038, #360) en colonne `pl.Object` :
    RSC-1 (réelle) a ses relevés bornants, RSC-2 (incalculable) a `[]`. `source_hash` est
    présent (requis par le contrat).
    """
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-1", "RSC-2"],
            "pdl": ["12345678901234", "12345678905678"],
            "mois_annee": ["2026-05", "2026-05"],
            "debut": [datetime(2026, 5, 1, tzinfo=PARIS), datetime(2026, 5, 1, tzinfo=PARIS)],
            "fin": [datetime(2026, 6, 1, tzinfo=PARIS), datetime(2026, 6, 1, tzinfo=PARIS)],
            "nb_jours": [31, 31],
            "puissance_moyenne_kva": [6.0, 9.0],
            "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFCUST"],
            "energie_base_kwh": [None, 420.0],
            "energie_hp_kwh": [312.4, None],
            "energie_hc_kwh": [145.2, None],
            "turpe_fixe_eur": [9.13, 12.0],
            "turpe_variable_eur": [18.4, 22.0],
            "cta_eur": [1.97, 2.59],
            "taux_accise_eur_mwh": [22.5, 22.5],
            "has_changement": [False, False],
            "qualite": ["réelle", "incalculable"],
            "statut_communication": ["communicante", "non_communicante"],
            "source_hash": ["deadbeefcafe0001", "deadbeefcafe0002"],
        }
    )
    releves_utilises = [
        [
            {
                "releve_id": "a1b2c3d4e5f60718",
                "date_releve": "2026-05-01T00:00:00+02:00",
                "nature_index": "réel",
                "origine_releve": "périodique",
                "index_hp_kwh": 1000,
                "index_hc_kwh": 500,
            },
            {
                "releve_id": "1122334455667788",
                "date_releve": "2026-05-12T00:00:00+02:00",
                "nature_index": "réel",
                "origine_releve": "événementiel",
                "evenement": "MCT",
                "index_hp_kwh": 1080,
                "index_hc_kwh": 540,
            },
        ],
        [],
    ]
    return df.with_columns(pl.Series("releves_utilises", releves_utilises, dtype=pl.Object))


def _lignes(response) -> list[dict]:
    """Décode le corps JSONL en liste de dicts (une ligne = un objet)."""
    return [json.loads(ligne) for ligne in response.text.splitlines() if ligne.strip()]


def test_meta_periodes_streame_du_jsonl(monkeypatch, meta_periodes_synthetiques):
    """Tracer bullet : GET sert les méta-périodes en JSONL (`application/jsonl`)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.meta_periodes.meta_periodes",
        lambda mois=None, rsc=None: ("2026-05-01", meta_periodes_synthetiques),
    )
    try:
        response = TestClient(app).get("/facturation/meta-periodes", params={"mois": "2026-05-01"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/jsonl")

    lignes = _lignes(response)
    assert len(lignes) == 2
    assert lignes[0]["ref_situation_contractuelle"] == "RSC-1"
    assert lignes[0]["energie_hp_kwh"] == 312.4
    assert lignes[0]["turpe_fixe_eur"] == 9.13
    assert lignes[0]["qualite"] == "réelle"
    assert lignes[0]["statut_communication"] == "communicante"
    assert lignes[0]["source_hash"] == "deadbeefcafe0001"


def test_meta_periodes_metadonnees_en_en_tete(monkeypatch, meta_periodes_synthetiques):
    """`contract_version` et `mois` résolu remontent dans les en-têtes (plus d'enveloppe)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.meta_periodes.meta_periodes",
        lambda mois=None, rsc=None: ("2026-05-01", meta_periodes_synthetiques),
    )
    try:
        response = TestClient(app).get("/facturation/meta-periodes")
    finally:
        app.dependency_overrides.clear()

    assert response.headers["X-Contract-Version"] == "3"
    assert response.headers["X-Mois"] == "2026-05-01"


def test_meta_periodes_lignes_valident_le_modele(monkeypatch, meta_periodes_synthetiques):
    """Chaque ligne est un `PeriodeMeta` valide (`model_validate` passe), relevés imbriqués
    compris : périodique sans événement, événementiel MCT, mois incalculable → trace vide."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.meta_periodes.meta_periodes",
        lambda mois=None, rsc=None: ("2026-05-01", meta_periodes_synthetiques),
    )
    try:
        response = TestClient(app).get("/facturation/meta-periodes")
    finally:
        app.dependency_overrides.clear()

    periodes = [PeriodeMeta.model_validate(ligne) for ligne in _lignes(response)]
    p0 = periodes[0]
    assert p0.releves_utilises[0].index_hp_kwh == 1000
    assert isinstance(p0.releves_utilises[0].index_hp_kwh, int)
    assert p0.releves_utilises[0].origine_releve == "périodique"
    assert p0.releves_utilises[0].evenement is None
    assert p0.releves_utilises[1].evenement == "MCT"
    # Mois incalculable → trace vide.
    assert periodes[1].releves_utilises == []


def test_meta_periodes_ligne_hors_contrat_500_atomique(monkeypatch, meta_periodes_synthetiques):
    """Une ligne hors-contrat (champ requis retiré) fait un **500 propre** *avant* tout octet —
    pas un 200 tronqué en cours de flux (validate-then-stream, #426). Le consommateur n'applique
    alors aucune ligne et re-rejoue le mois."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    # On retire un champ requis du contrat (`source_hash`) → la validation échoue en amont.
    df_casse = meta_periodes_synthetiques.drop("source_hash")
    monkeypatch.setattr(
        "electricore.api.routers.meta_periodes.meta_periodes",
        lambda mois=None, rsc=None: ("2026-05-01", df_casse),
    )
    try:
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/facturation/meta-periodes")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 500
    # Le flux n'a pas commencé : aucune ligne JSONL (pas de données de contrat) n'a fuité.
    assert not response.headers["content-type"].startswith("application/jsonl")
    assert "ref_situation_contractuelle" not in response.text


def test_meta_periodes_openapi_annonce_le_flux_jsonl():
    """Découvrabilité (#455) : OpenAPI documente la 200 en `application/jsonl` (NDJSON), pas
    en `application/json` implicite — sinon Swagger tente un parse JSON unique (« [object Blob] »).
    """
    reponse_200 = app.openapi()["paths"]["/facturation/meta-periodes"]["get"]["responses"]["200"]
    assert list(reponse_200["content"].keys()) == ["application/jsonl"]
    assert "ligne par ligne" in reponse_200["description"]


def test_meta_periodes_refuse_sans_api_key():
    """Endpoint sécurisé : 401 sans clé."""
    response = TestClient(app).get("/facturation/meta-periodes")
    assert response.status_code == 401


def test_meta_periodes_propage_mois_et_rsc(monkeypatch, meta_periodes_synthetiques):
    """`mois` et les `rsc=` répétés atteignent le service (rsc en liste)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels: list[tuple] = []

    def _capture(mois=None, rsc=None):
        appels.append((mois, rsc))
        return "2026-05-01", meta_periodes_synthetiques

    monkeypatch.setattr("electricore.api.routers.meta_periodes.meta_periodes", _capture)
    try:
        response = TestClient(app).get(
            "/facturation/meta-periodes",
            params={"mois": "2026-05-01", "rsc": ["RSC-1", "RSC-2"]},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels == [("2026-05-01", ["RSC-1", "RSC-2"])]
