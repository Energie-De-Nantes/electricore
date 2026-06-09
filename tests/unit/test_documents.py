"""Tests unitaires de `documents()` (builds.contexte_mensuel).

`documents()` assemble le livrable XLSX multi-onglets de campagne mensuelle :
filtre F15 / C15 sur `ctx.mois`, applique `rapprocher()`, extrait
`Changements puissance`, retourne le dict 6-onglets + le suffixe `YYYY-MM`.

Reste pur : prend les LazyFrames `f15` et `c15` en arguments ; les loaders
sont à la charge de l'appelant (cf. topologie #87).
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from electricore.core.builds.contexte_mensuel import ContexteMensuel, documents

TZ = ZoneInfo("Europe/Paris")


def _ctx(mois: str = "2025-01-01") -> ContexteMensuel:
    """`ContexteMensuel` minimal cohérent avec le mois cible et un RSC connu."""
    fact = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC001"],
            "pdl": ["12345678901234"],
            "debut": [datetime(2025, 1, 1)],
            "fin": [datetime(2025, 2, 1)],
            "energie_hp_kwh": [123.45],
            "energie_hc_kwh": [0.0],
            "energie_base_kwh": [0.0],
            "nb_jours": [31],
            "turpe_fixe_eur": [12.34],
            "turpe_variable_eur": [56.78],
            "data_complete": [True],
            "memo_puissance": [""],
        }
    ).with_columns(
        pl.col("debut").dt.replace_time_zone("Europe/Paris"),
        pl.col("fin").dt.replace_time_zone("Europe/Paris"),
    )
    historique = pl.LazyFrame(
        {
            "ref_situation_contractuelle": ["RSC001"],
            "num_compteur": ["12345678"],
            "type_compteur": ["LINKY"],
        }
    )
    return ContexteMensuel(
        mois=mois,
        historique_enrichi=historique,
        abonnements=pl.LazyFrame(),
        energie=pl.LazyFrame(),
        facturation_mensuelle=fact,
    )


def _lignes() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC001"],
            "categorie_produit": ["HP"],
            "quantite": [100.0],
            "est_brouillon": [True],
            "invoice_line_ids": [101],
            "x_pdl": ["12345678901234"],
            "x_lisse": [False],
            "name_account_move": ["INV/2025/0001"],
            "name_product_product": ["Énergie HP"],
        }
    )


def _flux_vide_avec_schema() -> pl.LazyFrame:
    """LazyFrame F15/C15 vide mais avec le schéma minimal attendu par documents()."""
    return pl.LazyFrame(
        schema={
            "pdl": pl.Utf8,
            "date_facture": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "unite": pl.Utf8,
            "evenement_declencheur": pl.Utf8,
        }
    )


class TestDocumentsTracer:
    """`documents()` retourne un dict 6-clés + suffixe YYYY-MM."""

    def test_retourne_six_onglets_et_suffixe(self):
        docs, suffix = documents(
            _ctx(mois="2025-01-01"),
            _lignes(),
            f15=_flux_vide_avec_schema(),
            c15=_flux_vide_avec_schema(),
        )

        assert set(docs.keys()) == {
            "F15 complet",
            "F15 prestations",
            "C15 complet",
            "C15 sorties",
            "Réconciliation",
            "Changements puissance",
        }
        assert suffix == "2025-01"


class TestFiltreMensuel:
    """`documents()` filtre F15 et C15 sur le mois porté par `ctx`."""

    def _f15(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "pdl": ["A", "B", "C"],
                "date_facture": [
                    datetime(2025, 1, 15, tzinfo=TZ),  # dans le mois
                    datetime(2025, 2, 10, tzinfo=TZ),  # hors mois
                    datetime(2025, 1, 28, tzinfo=TZ),  # dans le mois
                ],
                "unite": ["UNITE", "kWh", "UNITE"],
                # Colonnes requises mais inutilisées par f15 (présentes côté C15 seulement)
                "date_evenement": [None, None, None],
                "evenement_declencheur": [None, None, None],
            },
            schema={
                "pdl": pl.Utf8,
                "date_facture": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
                "unite": pl.Utf8,
                "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
                "evenement_declencheur": pl.Utf8,
            },
        )

    def _c15(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "pdl": ["X", "Y", "Z"],
                "date_evenement": [
                    datetime(2025, 1, 5, tzinfo=TZ),  # dans le mois → RES
                    datetime(2025, 3, 1, tzinfo=TZ),  # hors mois
                    datetime(2025, 1, 22, tzinfo=TZ),  # dans le mois → MES
                ],
                "evenement_declencheur": ["RES", "MES", "MES"],
                "date_facture": [None, None, None],
                "unite": [None, None, None],
            },
            schema={
                "pdl": pl.Utf8,
                "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
                "evenement_declencheur": pl.Utf8,
                "date_facture": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
                "unite": pl.Utf8,
            },
        )

    def test_f15_complet_garde_uniquement_les_lignes_du_mois(self):
        docs, _ = documents(_ctx(mois="2025-01-01"), _lignes(), f15=self._f15(), c15=self._c15())

        assert docs["F15 complet"]["pdl"].sort().to_list() == ["A", "C"]

    def test_f15_prestations_filtre_unite_egal_UNITE(self):
        docs, _ = documents(_ctx(mois="2025-01-01"), _lignes(), f15=self._f15(), c15=self._c15())

        # 'A' et 'C' sont dans le mois ; tous deux UNITE → restent
        assert docs["F15 prestations"]["pdl"].sort().to_list() == ["A", "C"]

    def test_c15_complet_garde_uniquement_les_evenements_du_mois(self):
        docs, _ = documents(_ctx(mois="2025-01-01"), _lignes(), f15=self._f15(), c15=self._c15())

        assert docs["C15 complet"]["pdl"].sort().to_list() == ["X", "Z"]

    def test_c15_sorties_filtre_RES_CFNS(self):
        docs, _ = documents(_ctx(mois="2025-01-01"), _lignes(), f15=self._f15(), c15=self._c15())

        # 'X' RES + dans le mois → conservé ; 'Z' MES → exclu
        assert docs["C15 sorties"]["pdl"].to_list() == ["X"]


class TestChangementsPuissance:
    """`Changements puissance` = lignes de la réconciliation avec `memo_puissance != ""`."""

    def test_lignes_avec_memo_non_vide_apparaissent_dans_l_onglet(self):
        # On force `memo_puissance != ""` dans la facturation_mensuelle injectée au ctx
        ctx = _ctx(mois="2025-01-01")
        fact_avec_memo = ctx.facturation_mensuelle.with_columns(pl.lit("Hausse 6 → 9 kVA").alias("memo_puissance"))
        ctx_memo = ContexteMensuel(
            mois=ctx.mois,
            historique_enrichi=ctx.historique_enrichi,
            abonnements=ctx.abonnements,
            energie=ctx.energie,
            facturation_mensuelle=fact_avec_memo,
        )

        docs, _ = documents(ctx_memo, _lignes(), f15=_flux_vide_avec_schema(), c15=_flux_vide_avec_schema())

        assert len(docs["Changements puissance"]) == 1
        assert docs["Changements puissance"]["memo_puissance"].item() == "Hausse 6 → 9 kVA"

    def test_aucune_ligne_avec_memo_vide(self):
        # Le ctx par défaut a memo_puissance = ""
        docs, _ = documents(
            _ctx(mois="2025-01-01"), _lignes(), f15=_flux_vide_avec_schema(), c15=_flux_vide_avec_schema()
        )

        assert len(docs["Changements puissance"]) == 0
        # Réconciliation contient bien la ligne (l'onglet est un sous-ensemble)
        assert len(docs["Réconciliation"]) == 1
