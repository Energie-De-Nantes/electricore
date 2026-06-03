"""Tests unitaires de `rapprocher_facturation_mensuelle`.

Le rapprochement facturation mensuelle joint les lignes de facture Odoo
(déjà tagguées avec `x_ref_situation_contractuelle`) à la facturation Enedis
du mois ciblé, et calcule `quantite_enedis` selon la catégorie de produit.
"""

from datetime import datetime

import polars as pl
import pytest

from electricore.core.pipelines.facturation import rapprocher_facturation_mensuelle


def _ligne_odoo(
    *,
    rsc: str = "RSC001",
    categorie: str = "HP",
    pdl: str = "12345678901234",
    invoice_line_ids: int = 101,
    quantity: float = 0.0,
    memo_puissance: str = "",
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "x_ref_situation_contractuelle": [rsc],
            "invoice_line_ids": [invoice_line_ids],
            "x_pdl": [pdl],
            "x_lisse": [False],
            "name_account_move": ["INV/2025/0001"],
            "name_product_category": [categorie],
            "name_product_product": [f"Énergie {categorie}"],
            "quantity": [quantity],
        }
    )


def _fact_mensuelle(
    *,
    rsc: str = "RSC001",
    mois: datetime = datetime(2025, 1, 1),
    energie_hp_kwh: float = 0.0,
    energie_hc_kwh: float = 0.0,
    energie_base_kwh: float = 0.0,
    nb_jours: int = 31,
    memo_puissance: str = "",
) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "ref_situation_contractuelle": [rsc],
            "debut": [mois],
            "energie_hp_kwh": [energie_hp_kwh],
            "energie_hc_kwh": [energie_hc_kwh],
            "energie_base_kwh": [energie_base_kwh],
            "nb_jours": [nb_jours],
            "memo_puissance": [memo_puissance],
        }
    ).with_columns(pl.col("debut").dt.replace_time_zone("Europe/Paris"))


class TestMappingCategories:
    """quantite_enedis dépend de name_product_category."""

    def test_hp_recupere_energie_hp_kwh(self):
        lignes = _ligne_odoo(categorie="HP")
        fact = _fact_mensuelle(energie_hp_kwh=123.45)

        resultat = rapprocher_facturation_mensuelle(lignes_odoo=lignes, fact_mensuelle=fact, mois="2025-01-01")

        assert resultat["quantite_enedis"].item() == 123.45

    @pytest.mark.parametrize(
        "categorie,kwargs_fact,attendu",
        [
            ("HC", {"energie_hc_kwh": 234.5}, 234.5),
            ("Base", {"energie_base_kwh": 345.6}, 345.6),
            ("Abonnements", {"nb_jours": 31}, 31),
        ],
    )
    def test_autres_categories_recuperent_colonne_correspondante(self, categorie, kwargs_fact, attendu):
        lignes = _ligne_odoo(categorie=categorie)
        fact = _fact_mensuelle(**kwargs_fact)

        resultat = rapprocher_facturation_mensuelle(lignes_odoo=lignes, fact_mensuelle=fact, mois="2025-01-01")

        assert resultat["quantite_enedis"].item() == attendu

    def test_categorie_inconnue_donne_quantite_enedis_null(self):
        """Une catégorie hors mapping connu ne renseigne pas quantite_enedis."""
        lignes = _ligne_odoo(categorie="CategorieDouteuse")
        fact = _fact_mensuelle(energie_hp_kwh=999.0, energie_hc_kwh=999.0)

        resultat = rapprocher_facturation_mensuelle(lignes_odoo=lignes, fact_mensuelle=fact, mois="2025-01-01")

        assert resultat["quantite_enedis"].item() is None


class TestFiltreMois:
    """Sélection du mois de facturation à rapprocher."""

    def test_mois_none_selectionne_dernier_mois_disponible(self):
        """Sans mois explicite, on retient le dernier mois présent dans fact_mensuelle."""
        lignes = _ligne_odoo(categorie="HP")
        fact = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["RSC001", "RSC001"],
                "debut": [datetime(2025, 1, 1), datetime(2025, 2, 1)],
                "energie_hp_kwh": [100.0, 200.0],
                "energie_hc_kwh": [0.0, 0.0],
                "energie_base_kwh": [0.0, 0.0],
                "nb_jours": [31, 28],
                "memo_puissance": ["", ""],
            }
        ).with_columns(pl.col("debut").dt.replace_time_zone("Europe/Paris"))

        resultat = rapprocher_facturation_mensuelle(lignes_odoo=lignes, fact_mensuelle=fact, mois=None)

        assert resultat["quantite_enedis"].item() == 200.0


class TestColonnesSortie:
    """Le DataFrame `lignes_facture_rapprochees` a un schéma figé."""

    COLONNES_ATTENDUES = frozenset(
        [
            "invoice_line_ids",
            "x_pdl",
            "x_lisse",
            "name_account_move",
            "name_product_category",
            "name_product_product",
            "quantity",
            "quantite_enedis",
            "memo_puissance",
        ]
    )

    def test_sortie_ne_contient_que_les_colonnes_attendues(self):
        lignes = _ligne_odoo(categorie="HP")
        fact = _fact_mensuelle(energie_hp_kwh=10.0)

        resultat = rapprocher_facturation_mensuelle(lignes_odoo=lignes, fact_mensuelle=fact, mois="2025-01-01")

        assert frozenset(resultat.columns) == self.COLONNES_ATTENDUES


class TestLeftJoin:
    """Les lignes Odoo sans match RSC dans fact_mensuelle restent dans le résultat."""

    def test_ligne_sans_match_rsc_est_conservee_avec_quantite_null(self):
        lignes_avec_match = _ligne_odoo(rsc="RSC001", categorie="HP", invoice_line_ids=101)
        lignes_sans_match = _ligne_odoo(rsc="RSC_ABSENT", categorie="HP", invoice_line_ids=102)
        lignes = pl.concat([lignes_avec_match, lignes_sans_match])
        fact = _fact_mensuelle(rsc="RSC001", energie_hp_kwh=123.45)

        resultat = rapprocher_facturation_mensuelle(lignes_odoo=lignes, fact_mensuelle=fact, mois="2025-01-01").sort(
            "invoice_line_ids"
        )

        assert len(resultat) == 2
        assert resultat["invoice_line_ids"].to_list() == [101, 102]
        assert resultat["quantite_enedis"].to_list() == [123.45, None]
