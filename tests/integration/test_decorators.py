"""Tests du décorateur de routes binaires (XLSX / Arrow / ZIP).

Issue #18 — un décorateur unifié remplace le squelette répété autour de
chaque endpoint binaire (auth, garde Odoo, executor, exception → 503,
Response wrapping, Content-Disposition).
"""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from electricore.api.decorators import arrow_endpoint, xlsx_endpoint
from electricore.api.security import get_current_api_key


def _app_avec_auth_neutralisee() -> FastAPI:
    app = FastAPI()
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    return app


def test_xlsx_endpoint_emballe_handler_avec_media_type_et_filename():
    """Tracer bullet : le décorateur enregistre la route et renvoie une `Response` XLSX."""
    app = _app_avec_auth_neutralisee()

    @xlsx_endpoint(app, "/test/xlsx", filename="test.xlsx")
    async def handler() -> bytes:
        return b"PK\x03\x04contenu"

    response = TestClient(app).get("/test/xlsx")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert "test.xlsx" in response.headers["content-disposition"]
    assert response.content == b"PK\x03\x04contenu"


def test_xlsx_endpoint_substitue_placeholders_du_filename_depuis_les_args():
    """Le `filename` peut contenir `{nom_arg}` ; la valeur de l'arg est interpolée."""
    from fastapi import Query

    app = _app_avec_auth_neutralisee()

    @xlsx_endpoint(app, "/test/avec_arg.xlsx", filename="accise_{trimestre}.xlsx")
    async def handler(trimestre: str = Query(...)) -> bytes:
        return b"PK"

    response = TestClient(app).get("/test/avec_arg.xlsx", params={"trimestre": "2025-T1"})

    assert response.status_code == 200
    assert "accise_2025-T1.xlsx" in response.headers["content-disposition"]


def test_xlsx_endpoint_placeholder_none_vaut_chaine_vide():
    """Quand un arg optionnel vaut `None`, son placeholder résout en chaîne vide.

    Cas d'usage : `filename="accise{trimestre}.xlsx"` doit donner
    `accise.xlsx` quand l'utilisateur n'a pas filtré par trimestre.
    """
    from fastapi import Query

    app = _app_avec_auth_neutralisee()

    @xlsx_endpoint(app, "/test/optionnel.xlsx", filename="accise{trimestre}.xlsx")
    async def handler(trimestre: str | None = Query(default=None)) -> bytes:
        return b"PK"

    response = TestClient(app).get("/test/optionnel.xlsx")

    assert response.status_code == 200
    assert "accise.xlsx" in response.headers["content-disposition"]


def test_xlsx_endpoint_convertit_exception_metier_en_503():
    """Une exception levée par le handler devient un `HTTPException(503)`."""
    app = _app_avec_auth_neutralisee()

    @xlsx_endpoint(app, "/test/erreur.xlsx", filename="x.xlsx")
    async def handler() -> bytes:
        raise RuntimeError("Odoo timeout")

    response = TestClient(app, raise_server_exceptions=False).get("/test/erreur.xlsx")

    assert response.status_code == 503
    assert "Odoo timeout" in response.json()["detail"]


def test_xlsx_endpoint_garde_odoo_renvoie_501_si_non_configure(monkeypatch):
    """`requires_odoo=True` + Odoo non configuré → 501 sans appeler le handler."""
    from electricore.api import decorators

    # `is_odoo_configured` est une property — on patche la classe pour la forcer à False.
    monkeypatch.setattr(type(decorators.settings), "is_odoo_configured", property(lambda self: False))

    app = _app_avec_auth_neutralisee()
    appels: list[bool] = []

    @xlsx_endpoint(app, "/test/garde.xlsx", filename="x.xlsx", requires_odoo=True)
    async def handler() -> bytes:
        appels.append(True)
        return b"PK"

    response = TestClient(app).get("/test/garde.xlsx")

    assert response.status_code == 501
    assert appels == [], "le handler ne doit pas être appelé quand Odoo manque"


def test_xlsx_endpoint_supporte_handler_synchrone():
    """Un handler `def` (synchrone) est appelé dans un executor.

    L'API actuelle utilise `run_in_executor` partout pour ne pas bloquer
    la boucle. Le décorateur doit gérer les deux signatures.
    """
    app = _app_avec_auth_neutralisee()

    @xlsx_endpoint(app, "/test/sync.xlsx", filename="sync.xlsx")
    def handler_sync() -> bytes:  # noqa: ANN201 — def volontaire
        return b"PK\x03\x04sync"

    response = TestClient(app).get("/test/sync.xlsx")

    assert response.status_code == 200
    assert response.content == b"PK\x03\x04sync"


def test_xlsx_endpoint_accepte_un_apirouter_via_target_kwarg():
    """Le décorateur accepte un `APIRouter` (pas seulement `FastAPI`) via le kwarg `target` (issue #81).

    Prérequis du split de `api/main.py` en routers per-domaine : `xlsx_endpoint`
    doit pouvoir enregistrer ses routes sur un `APIRouter` aussi bien que sur
    une instance `FastAPI`.
    """
    from fastapi import APIRouter

    router = APIRouter()

    @xlsx_endpoint(target=router, path="/test/router.xlsx", filename="depuis_router.xlsx")
    async def handler() -> bytes:
        return b"PK\x03\x04depuis_router"

    app = _app_avec_auth_neutralisee()
    app.include_router(router)

    response = TestClient(app).get("/test/router.xlsx")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert "depuis_router.xlsx" in response.headers["content-disposition"]
    assert response.content == b"PK\x03\x04depuis_router"


def test_arrow_endpoint_emballe_handler_en_arrow_ipc_sans_content_disposition():
    """`@arrow_endpoint` jumelle de `@xlsx_endpoint` pour le streaming Arrow IPC.

    Pas de `Content-Disposition` : le flux Arrow est consommé en mémoire
    par le client (pas téléchargé).
    """
    app = _app_avec_auth_neutralisee()

    @arrow_endpoint(app, "/test/arrow")
    async def handler() -> bytes:
        return b"ARROW1\x00\x00fake"

    response = TestClient(app).get("/test/arrow")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"
    assert "content-disposition" not in response.headers
    assert response.content == b"ARROW1\x00\x00fake"


def test_arrow_endpoint_accepte_un_apirouter_via_target_kwarg():
    """`@arrow_endpoint` accepte aussi un `APIRouter` via `target=` (cf. #81)."""
    from fastapi import APIRouter

    router = APIRouter()

    @arrow_endpoint(target=router, path="/test/arrow_router")
    async def handler() -> bytes:
        return b"ARROW1\x00\x00from_router"

    app = _app_avec_auth_neutralisee()
    app.include_router(router)

    response = TestClient(app).get("/test/arrow_router")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"
    assert response.content == b"ARROW1\x00\x00from_router"
