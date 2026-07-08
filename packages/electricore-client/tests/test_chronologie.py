"""Tests du client `chronologie` (flux JSONL, union discriminée, sans serveur).

Fixtures façonnées sur `chronologie_service` (contrat v1) : les **trois** sous-types
de ligne (`evenement`, `releve`, `periode_energie`) sont exercés. MockTransport sert
le JSONL + en-têtes ; on vérifie la résolution de l'union, la validation pdl XOR rsc
côté client (avant toute requête), et `.collect()`.

Note (déférée) : la confirmation int-vs-float du *vrai* échantillon relève d'une
vérification API locale (pas de serveur en CI). index_*_kwh entiers / energie_*_kwh
flottants sont fixés par ADR-0034.
"""

from __future__ import annotations

import json

import httpx
import pydantic
import pytest
from electricore_client import ElectricoreClient
from electricore_client.models import LigneEvenement, LignePeriodeEnergie, LigneReleve

# Frise représentative : un événement (MES), un relevé (périodique), une période d'énergie.
_LIGNES = [
    {
        "type_ligne": "evenement",
        "date": "2024-01-05T00:00:00+01:00",
        "pdl": "PDL_X",
        "ref_situation_contractuelle": "REF_1",
        "source": "flux_C15",
        "type_fait": "evenement",
        "evenement_declencheur": "MES",
        "puissance_souscrite_kva": 6.0,
        "formule_tarifaire_acheminement": "BTINFCUST",
        "niveau_ouverture_services": "2",
        "impacte_abonnement": True,
    },
    {
        "type_ligne": "releve",
        "date": "2024-01-05T00:00:00+01:00",
        "pdl": "PDL_X",
        "ref_situation_contractuelle": "REF_1",
        "source": "flux_R151",
        "releve_id": "a1b2c3d4e5f60718",
        "nature_index": "réel",
        "origine_releve": "périodique",
        "ordre_index": 0,
        "index_hp_kwh": 1000,
        "index_hc_kwh": 500,
    },
    {
        "type_ligne": "periode_energie",
        "date": "2024-01-05T00:00:00+01:00",
        "pdl": "PDL_X",
        "ref_situation_contractuelle": "REF_1",
        "debut": "2024-01-05T00:00:00+01:00",
        "fin": "2024-02-01T00:00:00+01:00",
        "nb_jours": 27,
        "qualite": "réelle",
        "statut_communication": "communicante",
        "energie_base_kwh": 50.0,
    },
]


def _jsonl(lignes: list[dict]) -> bytes:
    return ("\n".join(json.dumps(ligne) for ligne in lignes) + "\n").encode()


def _handler(*, lignes=_LIGNES, version="1", grain="point"):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=_jsonl(lignes),
            headers={"Content-Type": "application/x-ndjson", "X-Contract-Version": version, "X-Grain": grain},
        )

    return handler


def _client(handler) -> ElectricoreClient:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return ElectricoreClient(url="http://testserver", api_key="key", http_client=http)


def test_chronologie_resout_les_trois_sous_types():
    """L'union discriminée résout chaque ligne au bon sous-type (evenement/releve/periode)."""
    client = _client(_handler())
    with client.chronologie(pdl="PDL_X") as stream:
        lignes = stream.collect()

    assert isinstance(lignes[0], LigneEvenement)
    assert isinstance(lignes[1], LigneReleve)
    assert isinstance(lignes[2], LignePeriodeEnergie)
    # Typage : index entier, énergie flottante (ADR-0034).
    assert lignes[1].index_hp_kwh == 1000
    assert isinstance(lignes[1].index_hp_kwh, int)
    assert lignes[2].energie_base_kwh == 50.0
    assert isinstance(lignes[2].energie_base_kwh, float)
    # Verdicts portés sans montant tarifaire.
    assert lignes[2].qualite == "réelle"
    assert lignes[2].statut_communication == "communicante"


def test_chronologie_par_rsc():
    """Le grain contrat (`rsc`) part en query string."""
    captures: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captures.append(request.url)
        return httpx.Response(200, content=_jsonl(_LIGNES), headers={"X-Contract-Version": "1", "X-Grain": "contrat"})

    client = _client(handler)
    with client.chronologie(rsc="REF_1") as stream:
        assert stream.grain == "contrat"
        stream.collect()
    assert captures[0].params.get("rsc") == "REF_1"
    assert "pdl" not in captures[0].params


def test_chronologie_exige_un_grain_avant_toute_requete():
    """Ni pdl ni rsc → ValueError côté client, AUCUNE requête émise."""
    appels: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        appels.append(request)
        return httpx.Response(200, content=b"", headers={"X-Contract-Version": "1"})

    client = _client(handler)
    with pytest.raises(ValueError, match="pdl.*rsc|grain"):
        client.chronologie()
    assert appels == []


def test_chronologie_refuse_les_deux_grains():
    """pdl ET rsc → ValueError côté client (XOR), AUCUNE requête émise."""
    appels: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        appels.append(request)
        return httpx.Response(200, content=b"", headers={"X-Contract-Version": "1"})

    client = _client(handler)
    with pytest.raises(ValueError):
        client.chronologie(pdl="PDL_X", rsc="REF_1")
    assert appels == []


def test_chronologie_rejette_qualite_desaccentuee():
    """`qualite='reelle'` (désaccentué) échoue au parse pydantic (#589)."""
    ligne = {**_LIGNES[2], "qualite": "reelle"}
    with pytest.raises(pydantic.ValidationError):
        LignePeriodeEnergie.model_validate(ligne)


def test_chronologie_pas_de_montant_tarifaire():
    """Différenciateur : aucun champ tarifaire dans les lignes de période d'énergie."""
    client = _client(_handler())
    with client.chronologie(pdl="PDL_X") as stream:
        periode = next(line for line in stream if isinstance(line, LignePeriodeEnergie))
    dump = periode.model_dump()
    assert not any(k for k in dump if "turpe" in k or k == "cta_eur" or "accise" in k)
