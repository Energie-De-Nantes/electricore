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

import polars as pl
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)

MEDIA_TYPE_JSONL = "application/jsonl"


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
        except Exception as exc:
            # Le corps 500 générique de FastAPI ne fuit rien : on logge ici de quoi
            # diagnostiquer en prod *quelle* ligne était hors-contrat, puis on propage
            # (le mois entier échoue, zéro ligne appliquée côté consommateur).
            logger.error(
                "Ligne hors-contrat (ref_situation_contractuelle=%s) : %s",
                row.get("ref_situation_contractuelle"),
                exc,
            )
            raise
        lignes.append((modele.model_dump_json(exclude_none=omettre_les_nuls) + "\n").encode())

    return StreamingResponse(iter(lignes), media_type=MEDIA_TYPE_JSONL, headers=headers)
