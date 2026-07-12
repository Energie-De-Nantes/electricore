"""Modèles de contrat des sorties du périmètre (POST RPC, #632, ADR-0052).

`POST /perimetre/sorties` : l'appelant prête un lot de `ref_situation_contractuelle`,
electricore renvoie une ligne par RSC **sortie** du périmètre (événement C15 de code
`RES`/`CFNS`) — une RSC encore présente, ou inconnue, n'apparaît simplement pas dans la
réponse (pas d'erreur : c'est le cas nominal). `date_sortie` est un **jour civil** `DATE`
demi-ouvert (ADR-0042/0052) : la RSC est absente du périmètre dès ce jour-là — la
conversion en « dernier jour servi » appartient au consommateur.

Single-sourcés ici (ADR-0043) : le router FastAPI les importe, ne les redéfinit pas.
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field

# Version du contrat (cf. sorties_service.CONTRAT_VERSION). Source unique.
CONTRAT_VERSION_SORTIES = 1


class SortiesRequest(BaseModel):
    """Lot de RSC à interroger en une passe (le cas mono est `n = 1`)."""

    model_config = ConfigDict(extra="ignore")

    rsc: list[str] = Field(..., description="Situations contractuelles (ref_situation_contractuelle) à interroger")


class LigneSortie(BaseModel):
    """Une RSC sortie du périmètre : son PDL, le code déclencheur et la date (jour civil)."""

    model_config = ConfigDict(extra="ignore")

    ref_situation_contractuelle: str
    pdl: str
    evenement_declencheur: str = Field(..., description="Code de sortie C15 : RES ou CFNS")
    date_sortie: date = Field(
        ..., description="Jour civil demi-ouvert (ADR-0042/0052) : absente du périmètre dès ce jour"
    )
