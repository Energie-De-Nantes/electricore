"""Client de la facturiste electricore (httpx + pydantic, sans polars).

`ElectricoreClient` assemble le substrat de transport (`_BaseClient`) et les
méthodes d'endpoint. Ce module reste **polars-free** : le client Arrow
historique vit dans le sous-module `arrow` (extra `[arrow]`), jamais importé
ici au top-level.
"""

from __future__ import annotations

from .models.chronologie import (
    CONTRAT_VERSION_CHRONOLOGIE,
    LigneEvenement,
    LignePeriodeEnergie,
    LigneReleve,
    valider_ligne_chronologie,
)
from .models.meta_periodes import (
    CONTRAT_VERSION_META_PERIODES,
    PeriodeMeta,
)
from .streaming import JsonlStream
from .transport import _BaseClient

# Type de ligne résolu par l'union discriminée de la chronologie.
LigneFrise = LigneEvenement | LigneReleve | LignePeriodeEnergie


class ElectricoreClient(_BaseClient):
    """Client synchrone vers une instance de l'API facturiste electricore.

    Construit avec une `url` de base et une `api_key` (en-tête `X-API-Key`).
    Les méthodes de lecture (méta-périodes, chronologie) streament du JSONL ;
    `turpe_variable` est un POST RPC. Le client Arrow (DataFrames polars) est
    fourni séparément via l'extra `[arrow]` (`electricore_client.arrow`).
    """

    def meta_periodes(
        self,
        *,
        mois: str | None = None,
        rsc: list[str] | None = None,
        page_size: int | None = None,
    ) -> JsonlStream[PeriodeMeta]:
        """Flux JSONL typé des méta-périodes mensuelles (contrat v3, ADR-0027/0038).

        Le serveur **streame toutes les lignes** (pas de pagination) ; les
        métadonnées (`contract_version`, `mois` résolu) sont dans les en-têtes
        de réponse, lisibles via le flux retourné. La garde de version est
        appliquée à l'ouverture (`__enter__`).

        Args:
            mois: mois cible `YYYY-MM-DD` (premier du mois) ; `None` → dernier
                mois disponible côté serveur.
            rsc: filtre optionnel sur une liste de `ref_situation_contractuelle`.
            page_size: indication optionnelle de taille de lot serveur (hint).

        Returns:
            Un `JsonlStream[PeriodeMeta]`, à consommer en context-manager :

            ```python
            with client.meta_periodes(mois="2026-05-01") as stream:
                stream.contract_version
                for periode in stream:
                    ...
            ```
        """
        params: dict[str, object] = {}
        if mois is not None:
            params["mois"] = mois
        if rsc:
            params["rsc"] = rsc
        if page_size is not None:
            params["page_size"] = page_size

        return JsonlStream(
            client=self._http,
            method="GET",
            url=f"{self.url}/facturation/meta-periodes",
            params=params or None,
            headers=self._headers,
            validateur=PeriodeMeta.model_validate,
            version_attendue=CONTRAT_VERSION_META_PERIODES,
            verifier_version=lambda attendue, servie: self._verifier_version(attendue=attendue, servie=servie),
            raise_for_status=self._raise_for_status,
        )

    def chronologie(
        self,
        *,
        pdl: str | None = None,
        rsc: str | None = None,
        page_size: int | None = None,
    ) -> JsonlStream[LigneFrise]:
        """Flux JSONL de la frise d'un point (`pdl`) **ou** d'un contrat (`rsc`) — contrat v1.

        Chaque ligne se résout en son sous-type via l'union discriminée
        `LigneChronologie` (`LigneEvenement | LigneReleve | LignePeriodeEnergie`).
        Faits + verdicts, **sans montant tarifaire** (différenciateur vs
        méta-périodes). Pas de pagination ; `contract_version`/`grain` en en-têtes.

        Le grain est validé **côté client** (`pdl` XOR `rsc`) — un `ValueError`
        est levé *avant* toute requête (miroir du 422 serveur).

        Args:
            pdl: grain point — toute l'histoire du PDL (RSC successives + charnières).
            rsc: grain contrat — une tenure bornée entrée→sortie.
            page_size: indication optionnelle de taille de lot serveur (hint).

        Raises:
            ValueError: si ni `pdl` ni `rsc`, ou si les deux sont fournis (XOR).
        """
        if (pdl is None) == (rsc is None):
            raise ValueError("Fournir exactement un grain : `pdl` (point) XOR `rsc` (contrat).")

        params: dict[str, object] = {}
        if pdl is not None:
            params["pdl"] = pdl
        if rsc is not None:
            params["rsc"] = rsc
        if page_size is not None:
            params["page_size"] = page_size

        return JsonlStream(
            client=self._http,
            method="GET",
            url=f"{self.url}/facturation/chronologie",
            params=params or None,
            headers=self._headers,
            validateur=valider_ligne_chronologie,
            version_attendue=CONTRAT_VERSION_CHRONOLOGIE,
            verifier_version=lambda attendue, servie: self._verifier_version(attendue=attendue, servie=servie),
            raise_for_status=self._raise_for_status,
        )
