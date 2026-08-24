"""Tests du notebook exports : erreurs de config/clé affichées, pas de traceback (#719 revue).

`app.run(defs=...)` exécute le notebook marimo en process et permet d'injecter
une valeur pour la clé sans passer par une vraie interaction UI — même patron
que `test_accueil.py`.

Les onglets Relevés/Flux bruts sont passés à `mo.ui.tabs(..., lazy=True)` sous
forme de fonctions (pas de valeurs déjà calculées) : sous `app.run()` (mode
script, sans kernel), `mo.lazy` ne les invoque jamais — comportement voulu
(zéro fetch tant que l'onglet n'est pas ouvert dans un vrai navigateur), mais
qui empêche de tester leur rendu via `sorties[-1].text`. Le dernier cell les
`return` donc aussi comme définitions (`defs["onglet_releves"]` /
`defs["onglet_flux_bruts"]`) pour les appeler directement ici.
"""

from __future__ import annotations

from datetime import date

import pytest
from electricore_kiosque import exports, helpers


class _FakeCle:
    """Stand-in pour `mo.ui.text` : seule sa `.value` compte pour le notebook."""

    def __init__(self, value: str) -> None:
        self.value = value


class _FakeWidget:
    """Stand-in générique pour un widget `mo.ui.*` — seule sa `.value` compte."""

    def __init__(self, value: object) -> None:
        self.value = value


def _rendu(cle: str) -> str:
    sorties, _ = exports.app.run(defs={"cle": _FakeCle(cle)})
    return sorties[-1].text


def _defs(monkeypatch: pytest.MonkeyPatch, cle: str = "une-cle") -> dict:
    """Définitions du notebook (dont `onglet_releves`/`onglet_flux_bruts`), clé/config posées.

    `recuperer_meta_periodes` (onglet Facturation, hors sujet ici) est stubbée
    pour ne pas dépendre de son comportement propre.
    """
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.test.invalide")
    monkeypatch.setattr(helpers, "recuperer_meta_periodes", lambda cle, **kw: [])
    _, defs = exports.app.run(defs={"cle": _FakeCle(cle)})
    return defs


def test_exports_api_url_manquante_affiche_un_message_actionnable(monkeypatch: pytest.MonkeyPatch) -> None:
    """`config.ApiUrlManquante` (KIOSQUE__API_URL absente) est rattrapée, pas un traceback brut."""
    monkeypatch.delenv("KIOSQUE__API_URL", raising=False)

    assert "KIOSQUE__API_URL manquante" in _rendu("une-cle")


def test_exports_cle_refusee_affiche_toujours_un_message_actionnable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-régression : `CleApiRefusee` reste rattrapée après l'élargissement du except."""
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.test.invalide")

    def _lever(cle: str, **kwargs: object) -> list[dict]:
        raise helpers.CleApiRefusee()

    monkeypatch.setattr(helpers, "recuperer_meta_periodes", _lever)

    assert "Clé API refusée" in _rendu("une-cle")


def test_exports_service_indisponible_affiche_un_message_actionnable(monkeypatch: pytest.MonkeyPatch) -> None:
    """`ServiceIndisponible` (#722 : ingestion en cours, API injoignable, version) est
    rattrapée par l'onglet Facturation, pas de traceback marimo brute."""
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.test.invalide")

    def _lever(cle: str, **kwargs: object) -> list[dict]:
        raise helpers.ServiceIndisponible("Ingestion en cours côté serveur — réessaie dans quelques minutes.")

    monkeypatch.setattr(helpers, "recuperer_meta_periodes", _lever)

    assert "Ingestion en cours" in _rendu("une-cle")


def test_exports_sans_cle_ne_declenche_aucun_appel_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pas de clé → zéro fetch : le garde `mo.stop` doit vivre dans le cell qui fetch.

    `mo.stop` ne coupe que les *descendants* par flux de données : un garde isolé
    dans son propre cell (qui ne définit rien) ne couperait rien et laisserait
    partir une requête avec une clé vide.
    """
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.test.invalide")
    appels: list[str] = []

    def _fake(cle: str, **kwargs: object) -> list[dict]:
        appels.append(cle)
        return []

    monkeypatch.setattr(helpers, "recuperer_meta_periodes", _fake)

    assert "En attente" in _rendu("")
    assert appels == []


# -- onglet Relevés (#720) -----------------------------------------------------


def test_onglet_releves_affiche_le_tableau_avec_la_fenetre_par_defaut(monkeypatch: pytest.MonkeyPatch) -> None:
    captures: dict = {}

    def _fake(cle: str, **kwargs: object):
        captures.update(kwargs)
        return [{"pdl": "PDL456", "date_releve": "2026-07-15"}], False

    monkeypatch.setattr(helpers, "recuperer_releves", _fake)
    defs = _defs(monkeypatch)

    rendu = defs["onglet_releves"]().text

    assert "PDL456" in rendu
    debut_defaut, fin_defaut = helpers.fenetre_dernier_mois()
    assert captures["debut"] == debut_defaut
    assert captures["fin"] == fin_defaut


def test_onglet_releves_affiche_le_bandeau_vue_tronquee(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(helpers, "recuperer_releves", lambda cle, **kw: ([{"pdl": "P1"}], True))
    defs = _defs(monkeypatch)

    assert "tronqu" in defs["onglet_releves"]().text.lower()


def test_onglet_releves_cle_refusee_affiche_un_message_actionnable(monkeypatch: pytest.MonkeyPatch) -> None:
    def _lever(cle: str, **kwargs: object):
        raise helpers.CleApiRefusee()

    monkeypatch.setattr(helpers, "recuperer_releves", _lever)
    defs = _defs(monkeypatch)

    assert "Clé API refusée" in defs["onglet_releves"]().text


def test_onglet_releves_service_indisponible_affiche_un_message_actionnable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#722 : API injoignable/ingestion en cours rattrapée, pas de traceback."""

    def _lever(cle: str, **kwargs: object):
        raise helpers.ServiceIndisponible("L'API est injoignable — vérifie l'URL ou contacte ton admin.")

    monkeypatch.setattr(helpers, "recuperer_releves", _lever)
    defs = _defs(monkeypatch)

    assert "injoignable" in defs["onglet_releves"]().text


# -- onglet Flux bruts (#720) ---------------------------------------------------


def test_onglet_flux_bruts_affiche_le_tableau_et_l_avertissement_conventions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helpers, "recuperer_flux", lambda cle, table, **kw: ([{"pdl": "PDL456"}], False))
    defs = _defs(monkeypatch)

    rendu = defs["onglet_flux_bruts"]().text

    assert "PDL456" in rendu
    assert "conventions Enedis" in rendu


def test_onglet_flux_bruts_affiche_le_bandeau_vue_tronquee(monkeypatch: pytest.MonkeyPatch) -> None:
    """Même plafond `LIMITE_LIGNES` que Relevés — le bandeau doit suivre (#721)."""
    monkeypatch.setattr(helpers, "recuperer_flux", lambda cle, table, **kw: ([{"pdl": "P1"}], True))
    defs = _defs(monkeypatch)

    assert "tronqu" in defs["onglet_flux_bruts"]().text.lower()


def test_onglet_flux_bruts_table_absente_affiche_un_message_propre(monkeypatch: pytest.MonkeyPatch) -> None:
    def _lever(cle: str, table: str, **kwargs: object):
        raise helpers.TableFluxAbsente(table)

    monkeypatch.setattr(helpers, "recuperer_flux", _lever)
    defs = _defs(monkeypatch)

    assert "absente de cette box" in defs["onglet_flux_bruts"]().text


def test_onglet_flux_bruts_service_indisponible_affiche_un_message_actionnable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#722 : ingestion en cours/API injoignable rattrapée, pas de traceback."""

    def _lever(cle: str, table: str, **kwargs: object):
        raise helpers.ServiceIndisponible("Ingestion en cours côté serveur — réessaie dans quelques minutes.")

    monkeypatch.setattr(helpers, "recuperer_flux", _lever)
    defs = _defs(monkeypatch)

    assert "Ingestion en cours" in defs["onglet_flux_bruts"]().text


# -- réactivité des filtres (#721, bug d'inertie) --------------------------------
#
# `mo.lazy` n'appelle la fonction d'un onglet qu'UNE fois par ouverture, et ne
# retient pas l'objet retourné (weakref côté `UIElementRegistry`). Des widgets
# créés À L'INTÉRIEUR de `onglet_releves`/`onglet_flux_bruts` seraient donc
# inertes : les tests ci-dessus, qui appellent `defs["onglet_releves"]()`
# directement, ne l'auraient JAMAIS détecté puisqu'ils recréent des widgets par
# défaut à chaque appel — exactement le bug. La preuve qui aurait attrapé le
# bug : les widgets doivent vivre dans leurs PROPRES cellules (`pdl_releves`,
# `debut`, `fin`, `table_flux`, `pdl_flux`), overridables via `app.run(defs=…)`
# — si `onglet_releves`/`onglet_flux_bruts` les recréaient en interne, cet
# override n'aurait aucun effet sur ce que `helpers.recuperer_*` reçoit.


def test_onglet_releves_recoit_les_valeurs_des_widgets_de_filtre_injectes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.test.invalide")
    monkeypatch.setattr(helpers, "recuperer_meta_periodes", lambda cle, **kw: [])
    captures: dict = {}

    def _fake(cle: str, **kwargs: object):
        captures.update(kwargs)
        return [], False

    monkeypatch.setattr(helpers, "recuperer_releves", _fake)

    _, defs = exports.app.run(
        defs={
            "cle": _FakeCle("une-cle"),
            "pdl_releves": _FakeWidget("PDL999"),
            "debut": _FakeWidget(date(2020, 1, 1)),
            "fin": _FakeWidget(date(2020, 1, 31)),
        }
    )
    defs["onglet_releves"]()

    assert captures["prm"] == "PDL999"
    assert captures["debut"] == "2020-01-01"
    assert captures["fin"] == "2020-01-31"


def test_onglet_flux_bruts_recoit_la_table_du_dropdown_injecte(monkeypatch: pytest.MonkeyPatch) -> None:
    """Changer la valeur injectée du dropdown change la table demandée à l'API."""
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.test.invalide")
    monkeypatch.setattr(helpers, "recuperer_meta_periodes", lambda cle, **kw: [])
    captures: dict = {}

    def _fake(cle: str, table: str, **kwargs: object):
        captures["table"] = table
        return [], False

    monkeypatch.setattr(helpers, "recuperer_flux", _fake)

    _, defs = exports.app.run(
        defs={
            "cle": _FakeCle("une-cle"),
            "table_flux": _FakeWidget("r151"),
            "pdl_flux": _FakeWidget(""),
        }
    )
    defs["onglet_flux_bruts"]()

    assert captures["table"] == "r151"
