"""Router de l'endpoint relevés canoniques (`/releves*`, ADR-0032).

Expose le *modèle de relevés canonique* (mart dbt `releves`, ADR-0029) — union
arbitrée C15/R64/R151 — hors du namespace `/flux/*` réservé aux flux Enedis
bruts. Adossé au loader `releves()` (et **non** au registre `FLUX_CONFIGS`),
consommé par les notebooks distants via `ElectricoreClient.releves()`.

**Ordre des routes important** — comme `/flux/*`, les routes `.xlsx`, `.arrow`
et `/info` (sous-ressource) DOIVENT précéder la route JSON `/releves`.
"""

from fastapi import APIRouter, Query

from electricore.api.decorators import arrow_endpoint

router = APIRouter(tags=["releves"])


def _load_releves_df(limit: int):
    """Charge le mart `releves` via le loader `releves()` (ADR-0029).

    Test seam : monkeypatch ce helper pour court-circuiter l'IO DuckDB dans les
    tests d'endpoint (le SQL est paramétré et la sécurité est testée séparément).
    """
    from electricore.core.loaders.duckdb import releves

    return releves().limit(limit).collect()


@arrow_endpoint(router, "/releves.arrow")
def get_releves_arrow(
    limit: int = Query(1_000_000, le=10_000_000, description="Nombre maximum de lignes (défaut 1 000 000)"),
) -> bytes:
    """Exporte le mart `releves` en flux Arrow IPC (max 10 000 000 lignes).

    Consommable côté client avec `pl.read_ipc_stream(BytesIO(content))` — typage et
    timezones `Europe/Paris` préservés. Cf. `ElectricoreClient.releves`.
    """
    from electricore.api.serializers import arrow_stream

    df = _load_releves_df(limit)
    return arrow_stream(df)
