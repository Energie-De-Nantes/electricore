"""Tests du service de résolution RSC (id_Affaire → ref_situation_contractuelle).

Résolution **exacte** par recoupement X12 ⨝ C15 : l'`Id_Affaire` que porte l'événement
déclencheur C15 est le même identifiant que l'affaire X12 (cf. `core/CONTEXT.md`). On lit
donc le `ref_situation_contractuelle` directement sur l'événement C15 dont `id_affaire`
matche — **pas** d'heuristique temporelle (le notebook `injection_rsc` n'asof-joint par PDL
que parce qu'un `sale.order` n'a pas d'Id_Affaire). `flux_affaires` (X12) sert au
**recoupement** : il distingue une affaire *connue mais sans RSC* d'une affaire *inconnue*.
"""

from __future__ import annotations

import polars as pl

from electricore.api.services.rsc_service import resoudre_rsc


def _c15(rows: list[tuple[str | None, str | None]]) -> pl.DataFrame:
    """Événements C15 minimaux (id_affaire natif, non forward-fillé) : (id_affaire, rsc)."""
    return pl.DataFrame(
        rows,
        schema={"id_affaire": pl.Utf8, "ref_situation_contractuelle": pl.Utf8},
        orient="row",
    )


def _affaires(ids: list[str]) -> pl.DataFrame:
    """Affaires X12 (flux_affaires) recoupées : la colonne porteuse est `affaire_id`."""
    return pl.DataFrame({"affaire_id": pl.Series(list(ids), dtype=pl.Utf8)})


def test_resout_id_affaire_unique_vers_rsc():
    """Tracer : un id_Affaire porté par un seul événement C15 → sa RSC."""
    resultats = resoudre_rsc(
        ["38233180"],
        c15=_c15([("38233180", "248912973")]),
        affaires=_affaires(["38233180"]),
    )
    assert resultats == [{"id_affaire": "38233180", "ref_situation_contractuelle": "248912973"}]


def test_affaire_connue_x12_sans_rsc_c15_est_une_erreur_distincte():
    """Affaire présente en X12 mais sans événement contractuel C15 → erreur « sans
    situation » (précurseur en cours / non contractuelle), pas « inconnue »."""
    resultats = resoudre_rsc(
        ["AME001"],
        c15=_c15([]),  # aucun événement C15 ne porte cet id_affaire
        affaires=_affaires(["AME001"]),  # mais X12 connaît l'affaire
    )
    assert len(resultats) == 1
    assert resultats[0]["id_affaire"] == "AME001"
    assert "ref_situation_contractuelle" not in resultats[0]
    assert "situation" in resultats[0]["error"].lower()


def test_affaire_inconnue_des_deux_flux_est_une_erreur_distincte():
    """Id absent de X12 ET de C15 → erreur « inconnue », distincte de « sans situation »."""
    resultats = resoudre_rsc(["ZZZ999"], c15=_c15([]), affaires=_affaires([]))
    assert len(resultats) == 1
    assert resultats[0]["id_affaire"] == "ZZZ999"
    assert "ref_situation_contractuelle" not in resultats[0]
    erreur = resultats[0]["error"].lower()
    assert "inconnue" in erreur
    assert "ZZZ999" in resultats[0]["error"]
    assert "situation contractuelle" not in erreur  # ≠ du motif « connue sans situation »


def test_resolution_ambigue_plusieurs_rsc_est_une_erreur():
    """Un id_Affaire porté par des événements C15 de RSC distinctes → erreur ambiguë qui
    nomme les situations (jamais un choix silencieux)."""
    resultats = resoudre_rsc(
        ["38233180"],
        c15=_c15([("38233180", "248912973"), ("38233180", "248912974")]),
        affaires=_affaires(["38233180"]),
    )
    assert len(resultats) == 1
    assert "ref_situation_contractuelle" not in resultats[0]
    erreur = resultats[0]["error"]
    assert "ambig" in erreur.lower()
    assert "248912973" in erreur and "248912974" in erreur


def test_rsc_dupliquee_sur_plusieurs_evenements_reste_resolue():
    """Plusieurs événements C15 portant la **même** RSC pour l'affaire → résolution
    unique (la déduplication ne déclenche pas une fausse ambiguïté)."""
    resultats = resoudre_rsc(
        ["38233180"],
        c15=_c15([("38233180", "248912973"), ("38233180", "248912973")]),
        affaires=_affaires(["38233180"]),
    )
    assert resultats == [{"id_affaire": "38233180", "ref_situation_contractuelle": "248912973"}]


def test_lot_un_resultat_par_entree_dans_l_ordre():
    """Lot mixte : autant de résultats que d'entrées, dans l'ordre, appariables par id —
    jamais de silent-drop (succès partiel à la turpe-variable)."""
    resultats = resoudre_rsc(
        ["38233180", "AME001", "ZZZ999"],
        c15=_c15([("38233180", "248912973")]),
        affaires=_affaires(["38233180", "AME001"]),
    )
    assert [r["id_affaire"] for r in resultats] == ["38233180", "AME001", "ZZZ999"]
    par_id = {r["id_affaire"]: r for r in resultats}
    assert par_id["38233180"] == {"id_affaire": "38233180", "ref_situation_contractuelle": "248912973"}
    assert "situation" in par_id["AME001"]["error"].lower()
    assert "inconnue" in par_id["ZZZ999"]["error"].lower()


def test_lot_vide_renvoie_liste_vide():
    """Lot vide → aucune résolution (le router court-circuite avant tout accès DuckDB)."""
    assert resoudre_rsc([], c15=_c15([]), affaires=_affaires([])) == []
