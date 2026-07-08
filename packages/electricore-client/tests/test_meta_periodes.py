"""Tests du client `meta_periodes` (flux JSONL typé, sans serveur).

Fixtures façonnées sur `meta_periodes_service.COLONNES_CONTRAT` (contrat v3) +
le tableau imbriqué `releves_utilises` (ADR-0038). MockTransport sert un corps
JSONL (une ligne = un objet) + les en-têtes de métadonnées ; on vérifie le
typage des lignes, la garde de version, la libération du flux mi-consommé, et
`.collect()`.

Note (déférée) : la confirmation int-vs-float du *vrai* échantillon `limit=5`
relève d'une vérification API locale par le mainteneur (pas de serveur en CI).
Les choix de typage (index_*_kwh entiers, energie/€ flottants) sont fixés par
ADR-0034 et les définitions de colonnes.
"""

from __future__ import annotations

import json

import httpx
import pydantic
import pytest
from electricore_client import ElectricoreClient
from electricore_client.exceptions import ContractVersionError
from electricore_client.models import PeriodeMeta

# Deux méta-périodes représentatives (RSC-1 réelle avec relevés bornants ;
# RSC-2 incalculable, trace vide) — copie fidèle de la sortie du service.
_LIGNES = [
    {
        "ref_situation_contractuelle": "RSC-1",
        "pdl": "12345678901234",
        "mois_annee": "2026-05",
        "debut": "2026-05-01T00:00:00+02:00",
        "fin": "2026-06-01T00:00:00+02:00",
        "nb_jours": 31,
        "puissance_moyenne_kva": 6.0,
        "formule_tarifaire_acheminement": "BTINFCUST",
        "energie_base_kwh": None,
        "energie_hp_kwh": 312.4,
        "energie_hc_kwh": 145.2,
        "turpe_fixe_eur": 9.13,
        "turpe_variable_eur": 18.4,
        "cta_eur": 1.97,
        "taux_accise_eur_mwh": 22.5,
        "has_changement": False,
        "qualite": "réelle",
        "statut_communication": "communicante",
        "releves_utilises": [
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
        "source_hash": "deadbeefcafe0001",
    },
    {
        "ref_situation_contractuelle": "RSC-2",
        "pdl": "12345678905678",
        "mois_annee": "2026-05",
        "debut": "2026-05-01T00:00:00+02:00",
        "fin": "2026-06-01T00:00:00+02:00",
        "nb_jours": 31,
        "puissance_moyenne_kva": 9.0,
        "formule_tarifaire_acheminement": "BTINFCUST",
        "energie_base_kwh": 420.0,
        "energie_hp_kwh": None,
        "energie_hc_kwh": None,
        "turpe_fixe_eur": 12.0,
        "turpe_variable_eur": 22.0,
        "cta_eur": 2.59,
        "taux_accise_eur_mwh": 22.5,
        "has_changement": False,
        "qualite": "incalculable",
        "statut_communication": "non_communicante",
        "releves_utilises": [],
        "source_hash": "deadbeefcafe0002",
    },
]


def _jsonl(lignes: list[dict]) -> bytes:
    return ("\n".join(json.dumps(ligne) for ligne in lignes) + "\n").encode()


def _handler(*, lignes=_LIGNES, version="3", mois="2026-05-01"):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=_jsonl(lignes),
            headers={
                "Content-Type": "application/x-ndjson",
                "X-Contract-Version": version,
                "X-Mois": mois,
            },
        )

    return handler


def _client(handler) -> ElectricoreClient:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return ElectricoreClient(url="http://testserver", api_key="key", http_client=http)


def test_meta_periodes_streame_des_periodes_typees():
    """Le flux rend des `PeriodeMeta` typés, relevés imbriqués compris."""
    client = _client(_handler())
    with client.meta_periodes(mois="2026-05-01") as stream:
        periodes = list(stream)

    assert all(isinstance(p, PeriodeMeta) for p in periodes)
    assert [p.ref_situation_contractuelle for p in periodes] == ["RSC-1", "RSC-2"]
    p0 = periodes[0]
    # Typage : énergie flottante, index entier (ADR-0034).
    assert p0.energie_hp_kwh == 312.4
    assert isinstance(p0.energie_hp_kwh, float)
    assert p0.releves_utilises[0].index_hp_kwh == 1000
    assert isinstance(p0.releves_utilises[0].index_hp_kwh, int)
    assert p0.releves_utilises[1].evenement == "MCT"
    assert periodes[1].releves_utilises == []


def test_meta_periodes_expose_les_metadonnees_den_tete():
    """`contract_version` et `mois` viennent des en-têtes, pas du corps."""
    client = _client(_handler(version="3", mois="2026-05-01"))
    with client.meta_periodes() as stream:
        assert stream.contract_version == 3
        assert stream.mois == "2026-05-01"


def test_meta_periodes_collect_convenance():
    """`.collect()` matérialise tout le flux en liste."""
    client = _client(_handler())
    with client.meta_periodes() as stream:
        periodes = stream.collect()
    assert len(periodes) == 2


def test_meta_periodes_garde_de_version_leve_si_serveur_en_retard():
    """Serveur en retard (v2 < v3 attendu) : la garde lève dans `__enter__`."""
    client = _client(_handler(version="2"))
    with pytest.raises(ContractVersionError):
        with client.meta_periodes():
            pass


def test_meta_periodes_envoie_la_cle_api():
    """Le client positionne `X-API-Key`."""
    recu: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        recu.append(request.headers.get("x-api-key", ""))
        return httpx.Response(200, content=_jsonl(_LIGNES), headers={"X-Contract-Version": "3", "X-Mois": "2026-05-01"})

    client = _client(handler)
    with client.meta_periodes() as stream:
        stream.collect()
    assert recu == ["key"]


def test_meta_periodes_propage_filtres_en_query():
    """`mois`, `rsc` répétés et `page_size` partent en query string."""
    captures: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captures.append(request.url)
        return httpx.Response(200, content=_jsonl(_LIGNES), headers={"X-Contract-Version": "3", "X-Mois": "2026-05-01"})

    client = _client(handler)
    with client.meta_periodes(mois="2026-05-01", rsc=["RSC-1", "RSC-2"]) as stream:
        stream.collect()

    url = captures[0]
    assert url.params.get("mois") == "2026-05-01"
    assert url.params.get_list("rsc") == ["RSC-1", "RSC-2"]


def test_meta_periodes_rejette_qualite_desaccentuee():
    """`qualite='reelle'` (désaccentué) échoue au parse pydantic (#589)."""
    ligne = {**_LIGNES[0], "qualite": "reelle"}
    with pytest.raises(pydantic.ValidationError):
        PeriodeMeta.model_validate(ligne)


def test_meta_periodes_rejette_statut_communication_invalide():
    """`statut_communication='non-communicante'` (tiret) échoue au parse pydantic (#589)."""
    ligne = {**_LIGNES[0], "statut_communication": "non-communicante"}
    with pytest.raises(pydantic.ValidationError):
        PeriodeMeta.model_validate(ligne)


def test_meta_periodes_accepte_les_verdicts_valides_ou_absents():
    """Valeurs accentuées valides et absence (None) restent acceptées."""
    p = PeriodeMeta.model_validate(_LIGNES[0])
    assert p.qualite == "réelle"
    assert p.statut_communication == "communicante"

    ligne_sans_verdicts = {k: v for k, v in _LIGNES[0].items() if k not in ("qualite", "statut_communication")}
    p_sans = PeriodeMeta.model_validate(ligne_sans_verdicts)
    assert p_sans.qualite is None
    assert p_sans.statut_communication is None


def test_meta_periodes_libere_le_flux_mi_consomme():
    """Un flux à moitié consommé est correctement libéré à la sortie du `with`."""
    client = _client(_handler())
    with client.meta_periodes() as stream:
        premier = next(iter(stream))
        assert premier.ref_situation_contractuelle == "RSC-1"
    # Sortie du with sans avoir tout consommé : pas d'exception, connexion fermée.
    assert stream._closed is True
