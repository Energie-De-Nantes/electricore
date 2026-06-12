"""
Tests du millésime des taux régulés (issue #185, ADR-0024).

Le millésime est dérivé, jamais déclaré : dernière ligne entrée en vigueur
d'un fichier de taux + sa référence réglementaire.
"""

import datetime as dt

import polars as pl

from electricore.core.millesimes import Millesime, deriver_millesime, millesimes


def _historique(starts: list[dt.datetime], taux: list[float], references: list[str]) -> pl.LazyFrame:
    return pl.LazyFrame({"start": starts, "taux_test": taux, "reference": references}).with_columns(
        pl.col("start").dt.replace_time_zone("Europe/Paris")
    )


class TestDeriverMillesime:
    def test_derniere_ligne_entree_en_vigueur(self):
        """Le millésime est la ligne au start le plus récent, avec sa référence."""
        historique = _historique(
            [dt.datetime(2024, 1, 1), dt.datetime(2025, 2, 1)],
            [21.0, 33.7],
            ["LF 2024", "LF 2025"],
        )

        m = deriver_millesime(historique, taxe="Accise", taux_col="taux_test", unite="€/MWh")

        assert m == Millesime(
            taxe="Accise", date_vigueur=dt.date(2025, 2, 1), reference="LF 2025", valeur=33.7, unite="€/MWh"
        )

    def test_ordre_du_fichier_indifferent(self):
        """La dérivation se fonde sur start, pas sur l'ordre des lignes."""
        historique = _historique(
            [dt.datetime(2026, 2, 1), dt.datetime(2021, 8, 1)],
            [15.0, 21.93],
            ["Arrêté 2026", "Arrêté 2021"],
        )

        m = deriver_millesime(historique, taxe="CTA", taux_col="taux_test", unite="%")

        assert m.date_vigueur == dt.date(2026, 2, 1)
        assert m.reference == "Arrêté 2026"
        assert m.valeur == 15.0

    def test_grille_sans_taux_scalaire(self):
        """Pour une grille (TURPE), pas de valeur scalaire : valeur et unite restent None."""
        historique = _historique(
            [dt.datetime(2025, 8, 1), dt.datetime(2025, 8, 1)],
            [1.0, 2.0],
            ["Délib TURPE 7", "Délib TURPE 7"],
        )

        m = deriver_millesime(historique, taxe="TURPE")

        assert m == Millesime(taxe="TURPE", date_vigueur=dt.date(2025, 8, 1), reference="Délib TURPE 7")


class TestMillesimesReels:
    """Smoke : les trois fichiers de taux versionnés portent un millésime exploitable."""

    def test_trois_millesimes(self):
        result = millesimes()

        assert [m.taxe for m in result] == ["TURPE", "Accise", "CTA"]
        for m in result:
            assert m.reference, f"{m.taxe} : référence vide"
            assert "http" in m.reference, f"{m.taxe} : pas de lien vers le document public"
            assert m.date_vigueur >= dt.date(2025, 1, 1), f"{m.taxe} : millésime suspect ({m.date_vigueur})"

    def test_accise_et_cta_portent_un_taux(self):
        turpe, accise, cta = millesimes()

        assert turpe.valeur is None  # grille, pas de scalaire
        assert accise.unite == "€/MWh" and accise.valeur > 0
        assert cta.unite == "%" and cta.valeur > 0
