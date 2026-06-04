"""Tests unitaires de `rapprocher_facturation_mensuelle`.

Le rapprochement facturation mensuelle joint les lignes de facture Odoo
(déjà tagguées avec `x_ref_situation_contractuelle`) à la facturation Enedis
du mois ciblé, et calcule `quantite_enedis` selon la catégorie de produit.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from electricore.core.models.lignes_facture_rapprochees import LignesFactureRapprochees
from electricore.core.pipelines.facturation import rapprocher_facturation_mensuelle


def _ligne_odoo(
    *,
    rsc: str = "RSC001",
    categorie: str = "HP",
    pdl: str = "12345678901234",
    invoice_line_ids: int = 101,
    quantity: float = 0.0,
    memo_puissance: str = "",
    a_facturer: bool = True,
    a_supprimer: bool = False,
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
            "a_facturer": [a_facturer],
            "a_supprimer": [a_supprimer],
        }
    )


def _fact_mensuelle(
    *,
    rsc: str = "RSC001",
    pdl: str = "12345678901234",
    mois: datetime = datetime(2025, 1, 1),
    fin: datetime = datetime(2025, 2, 1),
    energie_hp_kwh: float = 0.0,
    energie_hc_kwh: float = 0.0,
    energie_base_kwh: float = 0.0,
    nb_jours: int = 31,
    turpe_fixe_eur: float = 0.0,
    turpe_variable_eur: float = 0.0,
    data_complete: bool = True,
    memo_puissance: str = "",
) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "ref_situation_contractuelle": [rsc],
            "pdl": [pdl],
            "debut": [mois],
            "fin": [fin],
            "energie_hp_kwh": [energie_hp_kwh],
            "energie_hc_kwh": [energie_hc_kwh],
            "energie_base_kwh": [energie_base_kwh],
            "nb_jours": [nb_jours],
            "turpe_fixe_eur": [turpe_fixe_eur],
            "turpe_variable_eur": [turpe_variable_eur],
            "data_complete": [data_complete],
            "memo_puissance": [memo_puissance],
        }
    ).with_columns(
        pl.col("debut").dt.replace_time_zone("Europe/Paris"),
        pl.col("fin").dt.replace_time_zone("Europe/Paris"),
    )


def _historique(
    *,
    rsc: str = "RSC001",
    num_compteur: str = "12345678",
    type_compteur: str = "LINKY",
) -> pl.LazyFrame:
    """Petit historique : 1 ligne par RSC avec identifiants compteur."""
    return pl.LazyFrame(
        {
            "ref_situation_contractuelle": [rsc],
            "num_compteur": [num_compteur],
            "type_compteur": [type_compteur],
        }
    )


class TestMappingCategories:
    """quantite_enedis dépend de name_product_category."""

    def test_hp_recupere_energie_hp_kwh(self):
        lignes = _ligne_odoo(categorie="HP")
        fact = _fact_mensuelle(energie_hp_kwh=123.45)

        resultat = rapprocher_facturation_mensuelle(
            lignes_odoo=lignes, fact_mensuelle=fact, historique=_historique(), mois="2025-01-01"
        )

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

        resultat = rapprocher_facturation_mensuelle(
            lignes_odoo=lignes, fact_mensuelle=fact, historique=_historique(), mois="2025-01-01"
        )

        assert resultat["quantite_enedis"].item() == attendu

    def test_categorie_inconnue_donne_quantite_enedis_null(self):
        """Une catégorie hors mapping connu ne renseigne pas quantite_enedis."""
        lignes = _ligne_odoo(categorie="CategorieDouteuse")
        fact = _fact_mensuelle(energie_hp_kwh=999.0, energie_hc_kwh=999.0)

        resultat = rapprocher_facturation_mensuelle(
            lignes_odoo=lignes, fact_mensuelle=fact, historique=_historique(), mois="2025-01-01"
        )

        assert resultat["quantite_enedis"].item() is None


class TestFiltreMois:
    """Sélection du mois de facturation à rapprocher."""

    def test_mois_none_selectionne_dernier_mois_disponible(self):
        """Sans mois explicite, on retient le dernier mois présent dans fact_mensuelle."""
        lignes = _ligne_odoo(categorie="HP")
        fact = pl.concat(
            [
                _fact_mensuelle(mois=datetime(2025, 1, 1), fin=datetime(2025, 2, 1), energie_hp_kwh=100.0),
                _fact_mensuelle(mois=datetime(2025, 2, 1), fin=datetime(2025, 3, 1), energie_hp_kwh=200.0),
            ]
        )

        resultat = rapprocher_facturation_mensuelle(
            lignes_odoo=lignes, fact_mensuelle=fact, historique=_historique(), mois=None
        )

        assert resultat["quantite_enedis"].item() == 200.0


class TestColonnesSortie:
    """Le DataFrame `lignes_facture_rapprochees` a un schéma figé."""

    COLONNES_ATTENDUES = frozenset(
        [
            # Identifiants Odoo + quantité
            "invoice_line_ids",
            "x_pdl",
            "x_lisse",
            "name_account_move",
            "name_product_category",
            "name_product_product",
            "quantity",
            "quantite_enedis",
            "memo_puissance",
            # Méta-période Enedis
            "ref_situation_contractuelle",
            "pdl",
            "debut",
            "fin",
            "data_complete",
            "turpe_fixe_eur",
            "turpe_variable_eur",
            # Identifiants compteur
            "num_compteur",
            "type_compteur",
            # Flags d'état de facturation (ADR-0014)
            "a_facturer",
            "a_supprimer",
        ]
    )

    def test_sortie_contient_les_colonnes_attendues(self):
        lignes = _ligne_odoo(categorie="HP")
        fact = _fact_mensuelle(energie_hp_kwh=10.0)
        hist = _historique()

        resultat = rapprocher_facturation_mensuelle(
            lignes_odoo=lignes, fact_mensuelle=fact, historique=hist, mois="2025-01-01"
        )

        assert frozenset(resultat.columns) == self.COLONNES_ATTENDUES


class TestPropagationFlags:
    """`a_facturer` et `a_supprimer` venant de lignes_odoo doivent traverser le rapprochement."""

    def test_propage_a_facturer_et_a_supprimer(self):
        lignes = _ligne_odoo(categorie="HP").with_columns(
            [
                pl.lit(True).alias("a_facturer"),
                pl.lit(False).alias("a_supprimer"),
            ]
        )
        fact = _fact_mensuelle(energie_hp_kwh=10.0)

        resultat = rapprocher_facturation_mensuelle(
            lignes_odoo=lignes, fact_mensuelle=fact, historique=_historique(), mois="2025-01-01"
        )

        assert resultat["a_facturer"].item() is True
        assert resultat["a_supprimer"].item() is False


class TestJoinHistorique:
    """num_compteur et type_compteur sont projetés depuis l'Historique."""

    def test_compteur_provient_de_historique(self):
        lignes = _ligne_odoo(rsc="RSC001", categorie="HP")
        fact = _fact_mensuelle(rsc="RSC001", energie_hp_kwh=10.0)
        hist = _historique(rsc="RSC001", num_compteur="87654321", type_compteur="CBE")

        resultat = rapprocher_facturation_mensuelle(
            lignes_odoo=lignes, fact_mensuelle=fact, historique=hist, mois="2025-01-01"
        )

        assert resultat["num_compteur"].item() == "87654321"
        assert resultat["type_compteur"].item() == "CBE"

    def test_compteur_null_si_rsc_absent_de_historique(self):
        lignes = _ligne_odoo(rsc="RSC001", categorie="HP")
        fact = _fact_mensuelle(rsc="RSC001", energie_hp_kwh=10.0)
        hist = _historique(rsc="AUTRE_RSC", num_compteur="ZZZ", type_compteur="ZZZ")

        resultat = rapprocher_facturation_mensuelle(
            lignes_odoo=lignes, fact_mensuelle=fact, historique=hist, mois="2025-01-01"
        )

        assert resultat["num_compteur"].item() is None
        assert resultat["type_compteur"].item() is None


class TestSchemaLignesFactureRapprochees:
    """Le modèle Pandera transporte aussi les méta-données Enedis et compteur."""

    def test_modele_accepte_les_colonnes_etendues(self):
        """Le modèle accepte 9 colonnes Odoo + 9 méta Enedis/compteur + 2 flags."""
        df = pl.DataFrame(
            {
                # 9 colonnes existantes (Odoo + quantité Enedis)
                "invoice_line_ids": [101],
                "x_pdl": ["12345678901234"],
                "x_lisse": [False],
                "name_account_move": ["INV/2025/0001"],
                "name_product_category": ["HP"],
                "name_product_product": ["Énergie HP"],
                "quantity": [100.0],
                "quantite_enedis": [123.45],
                "memo_puissance": [""],
                # 9 colonnes méta Enedis + identifiants compteur
                "ref_situation_contractuelle": ["RSC001"],
                "pdl": ["12345678901234"],
                "data_complete": [True],
                "debut": [datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))],
                "fin": [datetime(2025, 1, 31, tzinfo=ZoneInfo("Europe/Paris"))],
                "turpe_fixe_eur": [12.34],
                "turpe_variable_eur": [56.78],
                "num_compteur": ["12345678"],
                "type_compteur": ["LINKY"],
                # 2 flags (cf. ADR-0014)
                "a_facturer": [True],
                "a_supprimer": [False],
            }
        )

        # Ne doit pas lever
        LignesFactureRapprochees.validate(df)

    def test_modele_tolere_nulls_sur_les_colonnes_etendues(self):
        """Les colonnes étendues sont nullable (ligne Odoo sans match Enedis)."""
        df = pl.DataFrame(
            {
                "invoice_line_ids": [102],
                "x_pdl": ["99999999999999"],
                "x_lisse": [False],
                "name_account_move": ["INV/2025/0002"],
                "name_product_category": ["HP"],
                "name_product_product": ["Énergie HP"],
                "quantity": [0.0],
                "quantite_enedis": [None],
                "memo_puissance": [None],
                "ref_situation_contractuelle": [None],
                "pdl": [None],
                "data_complete": [None],
                "debut": [None],
                "fin": [None],
                "turpe_fixe_eur": [None],
                "turpe_variable_eur": [None],
                "num_compteur": [None],
                "type_compteur": [None],
            },
            schema_overrides={
                "debut": pl.Datetime("us", "Europe/Paris"),
                "fin": pl.Datetime("us", "Europe/Paris"),
                "quantite_enedis": pl.Float64,
                "turpe_fixe_eur": pl.Float64,
                "turpe_variable_eur": pl.Float64,
                "data_complete": pl.Boolean,
            },
        )

        LignesFactureRapprochees.validate(df)


class TestLeftJoin:
    """Les lignes Odoo sans match RSC dans fact_mensuelle restent dans le résultat."""

    def test_ligne_sans_match_rsc_est_conservee_avec_quantite_null(self):
        lignes_avec_match = _ligne_odoo(rsc="RSC001", categorie="HP", invoice_line_ids=101)
        lignes_sans_match = _ligne_odoo(rsc="RSC_ABSENT", categorie="HP", invoice_line_ids=102)
        lignes = pl.concat([lignes_avec_match, lignes_sans_match])
        fact = _fact_mensuelle(rsc="RSC001", energie_hp_kwh=123.45)

        resultat = rapprocher_facturation_mensuelle(
            lignes_odoo=lignes, fact_mensuelle=fact, historique=_historique(), mois="2025-01-01"
        ).sort("invoice_line_ids")

        assert len(resultat) == 2
        assert resultat["invoice_line_ids"].to_list() == [101, 102]
        assert resultat["quantite_enedis"].to_list() == [123.45, None]
