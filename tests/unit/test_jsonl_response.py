"""Le sérialiseur partagé `jsonl_response` (validate-then-stream, #426).

Construit et **valide toutes les lignes JSONL en amont** : une ligne hors-contrat lève
*avant* que le premier octet ne parte (500 atomique, zéro ligne appliquée côté consommateur)
plutôt qu'en cassant un flux déjà committé en 200. Le format de fil (`application/x-ndjson`,
une ligne = un modèle sérialisé) et la stringification des dates sont possédés par le helper ;
les deux endpoints JSONL (méta-périodes, chronologie) lui passent leur validateur de ligne.
"""

import asyncio
import datetime as dt
import json
import logging
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from pydantic import BaseModel, ConfigDict, ValidationError

from electricore.api.serializers.jsonl import MEDIA_TYPE_JSONL, jsonl_response

PARIS = ZoneInfo("Europe/Paris")


class _Ligne(BaseModel):
    """Modèle de ligne minimal pour exercer le helper générique."""

    model_config = ConfigDict(extra="ignore")

    ref_situation_contractuelle: str
    debut: str
    valeur: int
    note: str | None = None
    # Champ requis (non-optionnel) *avec* défaut : un `None` explicite casse la
    # validation s'il n'est pas retiré au préalable — exerce `omettre_les_nuls`.
    categorie: str = "defaut"


def _corps(response) -> bytes:
    """Draine le corps streamé (StreamingResponse) en bytes."""

    async def _collect() -> bytes:
        chunks = [c async for c in response.body_iterator]
        return b"".join(c if isinstance(c, bytes) else c.encode() for c in chunks)

    return asyncio.run(_collect())


def _lignes(response) -> list[dict]:
    return [json.loads(ligne) for ligne in _corps(response).decode().splitlines() if ligne.strip()]


def test_happy_path_streame_des_lignes_jsonl_validees():
    """Tracer bullet : chaque ligne du DataFrame est validée et émise en JSONL."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-1", "RSC-2"],
            "debut": [dt.datetime(2026, 5, 1, tzinfo=PARIS), dt.datetime(2026, 5, 1, tzinfo=PARIS)],
            "valeur": [10, 20],
        }
    )

    response = jsonl_response(df, valider=_Ligne.model_validate, headers={"X-Contract-Version": "9"})

    assert response.media_type == MEDIA_TYPE_JSONL
    assert response.headers["X-Contract-Version"] == "9"

    lignes = _lignes(response)
    assert len(lignes) == 2
    assert lignes[0]["ref_situation_contractuelle"] == "RSC-1"
    assert lignes[0]["valeur"] == 10
    # Les dates sont stringifiées en ISO8601 (le contrat porte des `str`).
    assert lignes[0]["debut"] == "2026-05-01T00:00:00+02:00"


def test_ligne_hors_contrat_leve_avant_de_streamer():
    """Validate-then-stream : une ligne hors-contrat fait lever l'appel lui-même
    (avant tout octet), pas le générateur de flux déjà committé en 200."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-1", "RSC-BAD"],
            "debut": [dt.datetime(2026, 5, 1, tzinfo=PARIS), dt.datetime(2026, 5, 1, tzinfo=PARIS)],
            # 2e ligne : `valeur` non coercible en int → champ requis invalide.
            "valeur": ["10", "pas-un-entier"],
        }
    )

    with pytest.raises(ValidationError):
        jsonl_response(df, valider=_Ligne.model_validate, headers={})


def test_omettre_les_nuls_retire_les_clefs_nulles_avant_validation_et_au_dump():
    """`omettre_les_nuls=True` retire les clés nulles *avant* la validation (le défaut
    s'applique) et sérialise avec `exclude_none` (sémantique chronologie)."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-1"],
            "debut": [dt.datetime(2026, 5, 1, tzinfo=PARIS)],
            "valeur": [10],
            "note": [None],
            "categorie": [None],  # None contre un champ `str` requis (avec défaut)
        }
    )

    # Sans le flag : `categorie=None` casse la validation (None vs str non-optionnel).
    with pytest.raises(ValidationError):
        jsonl_response(df, valider=_Ligne.model_validate, headers={})

    # Avec le flag : clés nulles retirées avant validation (défaut appliqué) ET au dump.
    response = jsonl_response(df, valider=_Ligne.model_validate, headers={}, omettre_les_nuls=True)
    lignes = _lignes(response)
    assert lignes[0]["categorie"] == "defaut"
    assert "note" not in lignes[0]  # null omis au dump (exclude_none)


def test_logge_la_ref_et_lerreur_sur_ligne_hors_contrat(caplog):
    """Sur échec, le serveur logge la `ref_situation_contractuelle` fautive + l'erreur
    pydantic (le corps 500 générique de FastAPI ne fuit rien — prod doit diagnostiquer)."""
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-CASSEE"],
            "debut": [dt.datetime(2026, 5, 1, tzinfo=PARIS)],
            "valeur": ["pas-un-entier"],
        }
    )

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValidationError):
            jsonl_response(df, valider=_Ligne.model_validate, headers={})

    assert "RSC-CASSEE" in caplog.text
    assert "valeur" in caplog.text  # l'erreur pydantic nomme le champ fautif
