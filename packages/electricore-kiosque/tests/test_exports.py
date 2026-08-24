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

import pytest
from electricore_kiosque import exports, helpers


class _FakeCle:
    """Stand-in pour `mo.ui.text` : seule sa `.value` compte pour le notebook."""

    def __init__(self, value: str) -> None:
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


# -- onglet Flux bruts (#720) ---------------------------------------------------


def test_onglet_flux_bruts_affiche_le_tableau_et_l_avertissement_conventions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helpers, "recuperer_flux", lambda cle, table, **kw: [{"pdl": "PDL456"}])
    defs = _defs(monkeypatch)

    rendu = defs["onglet_flux_bruts"]().text

    assert "PDL456" in rendu
    assert "conventions Enedis" in rendu


def test_onglet_flux_bruts_table_absente_affiche_un_message_propre(monkeypatch: pytest.MonkeyPatch) -> None:
    def _lever(cle: str, table: str, **kwargs: object):
        raise helpers.TableFluxAbsente(table)

    monkeypatch.setattr(helpers, "recuperer_flux", _lever)
    defs = _defs(monkeypatch)

    assert "absente de cette box" in defs["onglet_flux_bruts"]().text
