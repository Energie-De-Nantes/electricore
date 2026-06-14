"""Tests unitaires de `rapprocher` (builds.contexte_mensuel).

Le rapprochement facturation mensuelle joint les *lignes de facture*
(shape agnostique `LignesFacture`) à la facturation Enedis du mois porté
par le `ContexteMensuel`, et calcule `quantite_enedis` selon
`categorie_produit`. Les flags ADR-0014 (`a_facturer`, `a_supprimer`)
sont **dérivés en core** depuis `est_brouillon` + `quantite`.

Voir `core/CONTEXT.md` (entrée *Rapprochement facturation mensuelle*).
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from electricore.core.builds.contexte_mensuel import ContexteMensuel, rapprocher
from electricore.core.models.lignes_facture_rapprochees import LignesFactureRapprochees


def _ligne(
    *,
    rsc: str = "RSC001",
    categorie: str = "HP",
    quantite: float = 0.0,
    est_brouillon: bool = False,
    invoice_line_ids: int = 101,
    pdl: str = "12345678901234",
) -> pl.DataFrame:
    """Construit une `LignesFacture`-conforme avec passe-plat ERP (Odoo-style)."""
    return pl.DataFrame(
        {
            # Clés métier renommées (les 4 colonnes du contrat minimal)
            "ref_situation_contractuelle": [rsc],
            "categorie_produit": [categorie],
            "quantite": [quantite],
            "est_brouillon": [est_brouillon],
            # ERP passe-plat
            "invoice_line_ids": [invoice_line_ids],
            "x_pdl": [pdl],
            "x_lisse": [False],
            "name_account_move": ["INV/2025/0001"],
            "name_product_product": [f"Énergie {categorie}"],
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
) -> pl.DataFrame:
    return pl.DataFrame(
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


def _ctx(
    *,
    fact_mensuelle: pl.DataFrame,
    historique: pl.LazyFrame,
    mois: str = "2025-01-01",
) -> ContexteMensuel:
    """Construit un `ContexteMensuel` minimal pour les tests de `rapprocher()`.

    Seuls `mois`, `facturation_mensuelle` et `historique_enrichi` sont consommés
    par `rapprocher()` ; `abonnements` et `energie` sont des placeholders.
    """
    return ContexteMensuel(
        mois=mois,
        historique_enrichi=historique,
        abonnements=pl.LazyFrame(),
        energie=pl.LazyFrame(),
        releves_utilises=pl.LazyFrame(),
        facturation_mensuelle=fact_mensuelle,
    )


class TestPassePlat:
    """Vraie passe-plat (issue #142) : sortie = colonnes d'entrée + colonnes calculées."""

    def test_colonne_erp_inedite_traverse_intacte(self):
        """Une colonne ERP que core ne connaît pas ressort telle quelle — aucun nom ERP en dur."""
        ligne = _ligne(categorie="HP").with_columns(pl.lit("survivant").alias("colonne_erp_inedite"))
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(energie_hp_kwh=10.0), historique=_historique())

        resultat = rapprocher(ctx, ligne)

        assert resultat["colonne_erp_inedite"].item() == "survivant"

    def test_ordre_contrat_calculees_puis_passe_plat(self):
        """Ordre déterministe : contrat, calculées, puis passe-plat en ordre d'entrée.

        Sans promesse pour les livrables — l'ordre facturiste vit dans
        `feuilles_rapport_*` (décision #142/#143).
        """
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(energie_hp_kwh=10.0), historique=_historique())

        resultat = rapprocher(ctx, _ligne(categorie="HP"))

        contrat = ["ref_situation_contractuelle", "categorie_produit", "quantite", "est_brouillon"]
        calculees = [
            "quantite_enedis",
            "memo_puissance",
            "pdl",
            "debut",
            "fin",
            "data_complete",
            "turpe_fixe_eur",
            "turpe_variable_eur",
            "num_compteur",
            "type_compteur",
            "a_facturer",
            "a_supprimer",
        ]
        passe_plat = ["invoice_line_ids", "x_pdl", "x_lisse", "name_account_move", "name_product_product"]
        assert resultat.columns == contrat + calculees + passe_plat

    def test_est_brouillon_conserve_a_cote_des_flags(self):
        """La colonne consommée pour dériver les flags ADR-0014 reste visible (auditable)."""
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(energie_hp_kwh=10.0), historique=_historique())

        resultat = rapprocher(ctx, _ligne(est_brouillon=True, quantite=42.0))

        assert resultat["est_brouillon"].item() is True
        assert resultat["a_facturer"].item() is True


class TestCollisionPassePlat:
    """Une colonne d'entrée homonyme d'une calculée échoue au seam, pas en silence."""

    def test_collision_avec_colonne_calculee_echoue_au_seam(self):
        ligne = _ligne(categorie="HP").with_columns(pl.lit("collision").alias("pdl"))
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(energie_hp_kwh=10.0), historique=_historique())

        with pytest.raises(ValueError, match="pdl"):
            rapprocher(ctx, ligne)

    def test_collision_avec_intermediaire_quantite_echoue_au_seam(self):
        """Les colonnes Enedis consommées par quantite_enedis sont aussi réservées."""
        ligne = _ligne(categorie="HP").with_columns(pl.lit(0.0).alias("energie_hp_kwh"))
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(energie_hp_kwh=10.0), historique=_historique())

        with pytest.raises(ValueError, match="energie_hp_kwh"):
            rapprocher(ctx, ligne)


class TestMappingCategories:
    """`quantite_enedis` dépend de `categorie_produit`."""

    def test_hp_recupere_energie_hp_kwh(self):
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(energie_hp_kwh=123.45), historique=_historique())

        resultat = rapprocher(ctx, _ligne(categorie="HP"))

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
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(**kwargs_fact), historique=_historique())

        resultat = rapprocher(ctx, _ligne(categorie=categorie))

        assert resultat["quantite_enedis"].item() == attendu


class TestSelectionMois:
    """`rapprocher` filtre `ctx.facturation_mensuelle` sur `ctx.mois`."""

    def test_mois_du_contexte_selectionne_la_bonne_ligne(self):
        fact = pl.concat(
            [
                _fact_mensuelle(mois=datetime(2025, 1, 1), fin=datetime(2025, 2, 1), energie_hp_kwh=100.0),
                _fact_mensuelle(mois=datetime(2025, 2, 1), fin=datetime(2025, 3, 1), energie_hp_kwh=200.0),
            ]
        )
        ctx = _ctx(fact_mensuelle=fact, historique=_historique(), mois="2025-02-01")

        resultat = rapprocher(ctx, _ligne(categorie="HP"))

        assert resultat["quantite_enedis"].item() == 200.0


class TestColonnesSortie:
    """Sortie = colonnes d'entrée + colonnes calculées (issue #142)."""

    COLONNES_CALCULEES = frozenset(
        [
            # Quantité Enedis + mémo
            "quantite_enedis",
            "memo_puissance",
            # Méta-période Enedis
            "pdl",
            "debut",
            "fin",
            "data_complete",
            "turpe_fixe_eur",
            "turpe_variable_eur",
            # Identifiants compteur
            "num_compteur",
            "type_compteur",
            # Flags ADR-0014 dérivés en core
            "a_facturer",
            "a_supprimer",
        ]
    )

    def test_sortie_egale_entree_plus_calculees(self):
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(energie_hp_kwh=10.0), historique=_historique())
        ligne = _ligne(categorie="HP")

        resultat = rapprocher(ctx, ligne)

        assert frozenset(resultat.columns) == frozenset(ligne.columns) | self.COLONNES_CALCULEES


class TestDerivationFlagsADR0014:
    """`a_facturer` et `a_supprimer` sont dérivés en core depuis `est_brouillon` + `quantite`."""

    def test_brouillon_et_qte_positive_donne_a_facturer(self):
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(energie_hp_kwh=10.0), historique=_historique())

        resultat = rapprocher(ctx, _ligne(est_brouillon=True, quantite=42.0))

        assert resultat["a_facturer"].item() is True
        assert resultat["a_supprimer"].item() is False

    def test_brouillon_et_qte_zero_donne_a_supprimer(self):
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(energie_hp_kwh=10.0), historique=_historique())

        resultat = rapprocher(ctx, _ligne(est_brouillon=True, quantite=0.0))

        assert resultat["a_facturer"].item() is False
        assert resultat["a_supprimer"].item() is True

    def test_non_brouillon_donne_les_deux_flags_a_false(self):
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(energie_hp_kwh=10.0), historique=_historique())

        # Même avec qte > 0 : si pas brouillon, rien à facturer (la ligne n'est pas une draft)
        resultat = rapprocher(ctx, _ligne(est_brouillon=False, quantite=42.0))

        assert resultat["a_facturer"].item() is False
        assert resultat["a_supprimer"].item() is False


class TestJoinHistorique:
    """`num_compteur` et `type_compteur` sont projetés depuis `ctx.historique_enrichi`."""

    def test_compteur_provient_de_historique(self):
        ctx = _ctx(
            fact_mensuelle=_fact_mensuelle(rsc="RSC001", energie_hp_kwh=10.0),
            historique=_historique(rsc="RSC001", num_compteur="87654321", type_compteur="CBE"),
        )

        resultat = rapprocher(ctx, _ligne(rsc="RSC001", categorie="HP"))

        assert resultat["num_compteur"].item() == "87654321"
        assert resultat["type_compteur"].item() == "CBE"

    def test_compteur_null_si_rsc_absent_de_historique(self):
        ctx = _ctx(
            fact_mensuelle=_fact_mensuelle(rsc="RSC001", energie_hp_kwh=10.0),
            historique=_historique(rsc="AUTRE_RSC", num_compteur="ZZZ", type_compteur="ZZZ"),
        )

        resultat = rapprocher(ctx, _ligne(rsc="RSC001", categorie="HP"))

        assert resultat["num_compteur"].item() is None
        assert resultat["type_compteur"].item() is None


class TestSchemaLignesFactureRapprochees:
    """Le modèle n'exige que contrat + calculées ; les colonnes ERP traversent sans être nommées."""

    def test_modele_accepte_les_colonnes_erp_en_passe_plat(self):
        df = pl.DataFrame(
            {
                # Identifiants ERP passe-plat (non nommés par le schéma, tolérés via strict=False)
                "invoice_line_ids": [101],
                "x_pdl": ["12345678901234"],
                "x_lisse": [False],
                "name_account_move": ["INV/2025/0001"],
                "name_product_product": ["Énergie HP"],
                # Contrat d'entrée conservé
                "categorie_produit": ["HP"],
                "quantite": [100.0],
                "est_brouillon": [True],
                # Quantité Enedis + mémo
                "quantite_enedis": [123.45],
                "memo_puissance": [""],
                # Méta-période Enedis
                "ref_situation_contractuelle": ["RSC001"],
                "pdl": ["12345678901234"],
                "data_complete": [True],
                "debut": [datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))],
                "fin": [datetime(2025, 1, 31, tzinfo=ZoneInfo("Europe/Paris"))],
                "turpe_fixe_eur": [12.34],
                "turpe_variable_eur": [56.78],
                # Identifiants compteur
                "num_compteur": ["12345678"],
                "type_compteur": ["LINKY"],
                # Flags ADR-0014
                "a_facturer": [True],
                "a_supprimer": [False],
            }
        )

        LignesFactureRapprochees.validate(df)


class TestLeftJoin:
    """Les lignes sans match RSC dans `facturation_mensuelle` restent dans le résultat."""

    def test_ligne_sans_match_rsc_est_conservee_avec_quantite_null(self):
        ligne_avec_match = _ligne(rsc="RSC001", categorie="HP", invoice_line_ids=101)
        ligne_sans_match = _ligne(rsc="RSC_ABSENT", categorie="HP", invoice_line_ids=102)
        lignes = pl.concat([ligne_avec_match, ligne_sans_match])
        ctx = _ctx(fact_mensuelle=_fact_mensuelle(rsc="RSC001", energie_hp_kwh=123.45), historique=_historique())

        resultat = rapprocher(ctx, lignes).sort("invoice_line_ids")

        assert len(resultat) == 2
        assert resultat["invoice_line_ids"].to_list() == [101, 102]
        assert resultat["quantite_enedis"].to_list() == [123.45, None]
