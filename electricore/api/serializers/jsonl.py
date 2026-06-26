"""Sérialiseur JSONL partagé `jsonl_response` (validate-then-stream, ADR-0043, #426).

Possède le format de fil des endpoints JSONL (`application/jsonl`, une ligne = un modèle
de contrat sérialisé) **et** la garantie d'atomicité : toutes les lignes sont construites
et **validées en amont**, donc une ligne hors-contrat lève *avant* que le premier octet ne
parte. Le consommateur (`electricore-client` → Odoo, ADR-0027) reçoit alors un **500 propre,
zéro ligne appliquée** — et re-rejoue le mois — au lieu d'un 200 tronqué en cours de flux.

Réutilisé par `/facturation/meta-periodes` et `/facturation/chronologie` : chacun passe son
validateur de ligne (`PeriodeMeta.model_validate` / `valider_ligne_chronologie`) et ses
en-têtes. Le flag `omettre_les_nuls` (défaut **off**) couvre la sémantique chronologie
(clés nulles retirées avant résolution de l'union discriminée + `exclude_none` au dump).
"""

import datetime as dt
import logging
from collections.abc import Callable
from typing import Any

import polars as pl
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, TypeAdapter

logger = logging.getLogger(__name__)

MEDIA_TYPE_JSONL = "application/jsonl"


def reponses_openapi_jsonl(modele: Any, description: str) -> dict:
    """Fragment OpenAPI documentant la réponse 200 comme un **flux JSONL** (NDJSON) typé par ligne.

    OpenAPI 3.2.0 décrit les *sequential media types* (NDJSON…) via **`itemSchema`** : le schéma
    de **chaque ligne**, validé indépendamment (https://spec.openapis.org/oas/v3.2.0.html
    #sequential-media-types). On le dérive du modèle de ligne (`LigneChronologie` — union
    discriminée — ou `PeriodeMeta`) pour que Swagger UI rende la forme d'une ligne, au lieu du
    « [object Blob] » qu'il affiche quand il prend le NDJSON pour un `application/json` unique (#455).

    On **n'utilise pas** la génération native d'`itemSchema` de FastAPI (endpoints générateurs)
    car elle valide/streame ligne par ligne : ça sacrifierait le *validate-then-stream* atomique
    de `jsonl_response` (ADR-0043, #426/#427) dont dépend le consommateur. On émet donc l'`itemSchema`
    à la main, le flux restant produit par `jsonl_response`.

    Les `$defs` du schéma (modèles imbriqués, membres de l'union) pointent vers
    `#/components/schemas/*` et sont remontés au niveau document par `lever_defs_itemschema_jsonl`.
    """
    item_schema = TypeAdapter(modele).json_schema(ref_template="#/components/schemas/{model}")
    return {
        200: {
            "description": (
                f"{description} Flux **JSONL streamé** (`{MEDIA_TYPE_JSONL}`, NDJSON) : une ligne = "
                "un objet JSON (cf. `itemSchema`). À consommer ligne par ligne — ce n'est pas un "
                "document JSON unique."
            ),
            "content": {MEDIA_TYPE_JSONL: {"itemSchema": item_schema}},
        }
    }


def lever_defs_itemschema_jsonl(schema: dict) -> None:
    """Remonte les `$defs` des `itemSchema` JSONL vers `components/schemas` (post-génération OpenAPI).

    `reponses_openapi_jsonl` émet un `itemSchema` auto-suffisant dont les `$ref` ciblent déjà
    `#/components/schemas/*` ; ce passage déplace les définitions correspondantes (laissées en
    `$defs` local par pydantic) vers le bloc `components` du document, où les `$ref` se résolvent.
    Idempotent : un `itemSchema` déjà remonté n'a plus de `$defs`.
    """
    composants = schema.setdefault("components", {}).setdefault("schemas", {})
    for chemin in schema.get("paths", {}).values():
        for operation in chemin.values():
            if not isinstance(operation, dict):
                continue
            for reponse in operation.get("responses", {}).values():
                for media in reponse.get("content", {}).values():
                    item = media.get("itemSchema")
                    if isinstance(item, dict):
                        composants.update(item.pop("$defs", {}))


def _stringifier_dates(ligne: dict) -> dict:
    """Rend les `datetime`/`date` de la ligne en ISO8601 (le contrat porte des `str`)."""
    return {
        clef: (valeur.isoformat() if isinstance(valeur, (dt.datetime, dt.date)) else valeur)
        for clef, valeur in ligne.items()
    }


def jsonl_response(
    df: pl.DataFrame,
    *,
    valider: Callable[[dict], BaseModel],
    headers: dict[str, str],
    omettre_les_nuls: bool = False,
) -> StreamingResponse:
    """Construit, valide et streame un DataFrame en JSONL (`application/jsonl`).

    Validate-then-stream : toutes les lignes sont matérialisées en `list[bytes]` avant la
    réponse, donc une ligne hors-contrat lève (et l'appel échoue) *avant* tout octet émis.

    Args:
        df: Lignes à émettre (une ligne = une ligne JSONL). Parcouru via `iter_rows(named=True)`.
        valider: Validateur de ligne `dict -> BaseModel` (construit/valide la ligne).
        headers: En-têtes de réponse (métadonnées : `X-Contract-Version`, `X-Mois`/`X-Grain`…).
        omettre_les_nuls: Si vrai, retire les clés nulles *avant* validation et sérialise avec
            `exclude_none=True` (sémantique chronologie : registre/énergie nul jamais émis).
    """
    lignes: list[bytes] = []
    for row in df.iter_rows(named=True):
        brut = _stringifier_dates(row)
        if omettre_les_nuls:
            brut = {clef: valeur for clef, valeur in brut.items() if valeur is not None}
        try:
            modele = valider(brut)
        except Exception:
            # Le corps 500 générique de FastAPI ne fuit rien : on logge ici de quoi
            # diagnostiquer en prod *quelle* ligne était hors-contrat, puis on propage
            # (le mois entier échoue, zéro ligne appliquée côté consommateur).
            # `logger.exception` attache la trace (et l'erreur pydantic qui nomme le champ).
            logger.exception(
                "Ligne hors-contrat (ref_situation_contractuelle=%s)",
                row.get("ref_situation_contractuelle"),
            )
            raise
        lignes.append((modele.model_dump_json(exclude_none=omettre_les_nuls) + "\n").encode())

    return StreamingResponse(iter(lignes), media_type=MEDIA_TYPE_JSONL, headers=headers)
