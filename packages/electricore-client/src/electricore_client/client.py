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
from .models.prestations import (
    CONTRAT_VERSION_PRESTATIONS,
    PrestationF15,
)
from .models.rsc import (
    CONTRAT_VERSION_RSC,
    ResolutionRscRequest,
    ResultatResolutionRsc,
)
from .models.turpe_variable import (
    CONTRAT_VERSION_TURPE_VARIABLE,
    LigneTurpeVariable,
    ResultatTurpeVariable,
    TurpeVariableRequest,
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

    def prestations(
        self,
        *,
        rsc: list[str] | None = None,
    ) -> JsonlStream[PrestationF15]:
        """Flux JSONL typé des prestations F15 à refacturer (contrat v1, pull-tout).

        Le serveur streame **toutes** les lignes `unite='UNITE'` du F15, sans
        fenêtre temporelle ni pagination — le volume est faible (~une par PDL)
        et la dédup vit chez le consommateur, par `reference`
        (souscriptions_odoo#37, ADR 0009 côté addon).

        Args:
            rsc: filtre optionnel sur une liste de `ref_situation_contractuelle`.

        Returns:
            Un `JsonlStream[PrestationF15]`, à consommer en context-manager :

            ```python
            with client.prestations() as stream:
                for presta in stream:
                    ...
            ```
        """
        params: dict[str, object] = {}
        if rsc:
            params["rsc"] = rsc

        return JsonlStream(
            client=self._http,
            method="GET",
            url=f"{self.url}/facturation/prestations",
            params=params or None,
            headers=self._headers,
            validateur=PrestationF15.model_validate,
            version_attendue=CONTRAT_VERSION_PRESTATIONS,
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

    def turpe_variable(
        self,
        lignes: list[LigneTurpeVariable | dict],
    ) -> list[ResultatTurpeVariable]:
        """Valorise un lot d'assiettes TURPE variable — **POST RPC** (pas un stream).

        Petit calcul stateless, batch POST : un round-trip pour tout le lot. Chaque
        résultat porte `turpe_variable_eur` **ou** un `error`, **apparié à l'entrée
        par l'`id` opaque** ré-émis (jamais positionnel — un serveur libre de
        réordonner reste correct). La garde de version s'applique (en-tête).

        Args:
            lignes: lot de `LigneTurpeVariable` (ou de dicts validés en
                `LigneTurpeVariable`) — `{id, formule_tarifaire_acheminement, debut,
                energie_*_kwh}` (7 cadrans nullables).

        Returns:
            Liste de `ResultatTurpeVariable` (autant que d'entrées). Pour un
            appariement explicite : `{r.id: r for r in resultats}`.
        """
        requete = TurpeVariableRequest(
            lignes=[
                ligne if isinstance(ligne, LigneTurpeVariable) else LigneTurpeVariable.model_validate(ligne)
                for ligne in lignes
            ]
        )
        response = self._http.post(
            f"{self.url}/facturation/turpe-variable",
            content=requete.model_dump_json(),
            headers={**self._headers, "Content-Type": "application/json"},
        )
        self._raise_for_status(response)

        version_servie = response.headers.get("X-Contract-Version")
        if version_servie is not None:
            self._verifier_version(attendue=CONTRAT_VERSION_TURPE_VARIABLE, servie=int(version_servie))

        corps = response.json()
        return [ResultatTurpeVariable.model_validate(r) for r in corps["results"]]

    def resoudre_rsc(self, ids: list[str]) -> list[ResultatResolutionRsc]:
        """Résout un lot d'`id_Affaire` en `ref_situation_contractuelle` — **POST RPC**.

        Petit recoupement stateless (X12 ⨝ C15), batch POST : un round-trip pour tout le
        lot. Chaque résultat porte `ref_situation_contractuelle` **ou** un `error`,
        **apparié à l'entrée par l'`id_affaire` opaque** ré-émis (jamais positionnel — un
        serveur libre de réordonner reste correct). La garde de version s'applique (en-tête).

        Args:
            ids: lot d'`id_Affaire` (Id_Affaire Enedis) à résoudre.

        Returns:
            Liste de `ResultatResolutionRsc` (autant que d'entrées). Pour un appariement
            explicite : `{r.id_affaire: r for r in resultats}`.
        """
        requete = ResolutionRscRequest(ids=list(ids))
        response = self._http.post(
            f"{self.url}/facturation/rsc",
            content=requete.model_dump_json(),
            headers={**self._headers, "Content-Type": "application/json"},
        )
        self._raise_for_status(response)

        version_servie = response.headers.get("X-Contract-Version")
        if version_servie is not None:
            self._verifier_version(attendue=CONTRAT_VERSION_RSC, servie=int(version_servie))

        corps = response.json()
        return [ResultatResolutionRsc.model_validate(r) for r in corps["results"]]
