"""Tests pour la règle d'attribution RSC de `notebooks/injection_rsc.py` (#580).

L'asof join (PDL, date_order) fige la RSC contemporaine de la souscription : pour un
PDL dont le contrat a été re-créé (RES→MES le même jour, CFN entrant…), l'asof re-choisit
indéfiniment la RSC résiliée. La règle cible attribue, pour chaque commande active
(`state = sale`, déjà filtré côté requête Odoo), la RSC du PDL dont le **dernier état
C15** n'est pas `RESILIE` — 0 ou plusieurs candidates → à traiter à part, jamais
d'écriture silencieuse.

Cette fonction est une reproduction fidèle du couple de cellules
`rsc_en_service_par_pdl` / `orders_attribution` de injection_rsc.py (cf. garde de
source statique en fin de fichier) — même approche que
`tests/integration/test_operator_notebooks_bundle.py`.
"""

from pathlib import Path

import polars as pl

_RACINE = Path(__file__).resolve().parents[2]
_SOURCE_INJECTION_RSC = (_RACINE / "notebooks" / "injection_rsc.py").read_text()


def _attribuer_rsc_en_service(orders: pl.DataFrame, contrats: pl.DataFrame) -> pl.DataFrame:
    """Reproduction fidèle de la règle d'attribution #580.

    `orders` : une ligne par commande active, colonnes `pdl` (+ tout identifiant).
    `contrats` : une ligne par (pdl, ref_situation_contractuelle), colonne
    `dernier_etat_contractuel`.
    """
    en_service = contrats.filter(pl.col("dernier_etat_contractuel") != "RESILIE")
    rsc_en_service_par_pdl = en_service.group_by("pdl").agg(
        pl.col("ref_situation_contractuelle").alias("rsc_en_service"),
        pl.len().alias("nb_rsc_en_service"),
    )
    return (
        orders.join(rsc_en_service_par_pdl, on="pdl", how="left")
        .with_columns(pl.col("nb_rsc_en_service").fill_null(0))
        .with_columns(
            pl.when(pl.col("nb_rsc_en_service") == 1)
            .then(pl.col("rsc_en_service").list.first())
            .otherwise(None)
            .alias("ref_situation_contractuelle")
        )
    )


class TestAttributionRscEnService:
    """Les 4 patterns de l'acceptance criteria #580."""

    def test_les_quatre_patterns(self):
        orders = pl.DataFrame(
            {"sale_order_id": [1, 2, 3, 4], "pdl": ["PDL_8ORDERS", "PDL_SORTI", "PDL_AMBIGU", "PDL_NORMAL"]}
        )

        contrats = pl.DataFrame(
            {
                "pdl": ["PDL_8ORDERS", "PDL_8ORDERS", "PDL_SORTI", "PDL_AMBIGU", "PDL_AMBIGU", "PDL_NORMAL"],
                "ref_situation_contractuelle": ["RSC_OLD", "RSC_NEW", "RSC_R1", "RSC_A1", "RSC_A2", "RSC_UNIQUE"],
                "dernier_etat_contractuel": [
                    "RESILIE",
                    "EN SERVICE",
                    "RESILIE",
                    "EN SERVICE",
                    "EN SERVICE",
                    "EN SERVICE",
                ],
            }
        )

        result = _attribuer_rsc_en_service(orders, contrats).sort("sale_order_id")

        # Pattern 1 (forme des 8 commandes réelles) : résiliée + en service sur le même
        # PDL → la RSC EN SERVICE est attribuée, pas celle contemporaine de l'asof.
        pdl_8orders = result.filter(pl.col("pdl") == "PDL_8ORDERS")
        assert pdl_8orders["ref_situation_contractuelle"][0] == "RSC_NEW"
        assert pdl_8orders["nb_rsc_en_service"][0] == 1

        # Pattern 2 : seule une RSC résiliée sur le PDL → aucune en service, à part.
        pdl_sorti = result.filter(pl.col("pdl") == "PDL_SORTI")
        assert pdl_sorti["ref_situation_contractuelle"][0] is None
        assert pdl_sorti["nb_rsc_en_service"][0] == 0

        # Pattern 3 : deux RSC en service sur le même PDL → ambigu, à part.
        pdl_ambigu = result.filter(pl.col("pdl") == "PDL_AMBIGU")
        assert pdl_ambigu["ref_situation_contractuelle"][0] is None
        assert pdl_ambigu["nb_rsc_en_service"][0] == 2

        # Pattern 4 : cas normal, une seule RSC en service → attribuée sans ambiguïté.
        pdl_normal = result.filter(pl.col("pdl") == "PDL_NORMAL")
        assert pdl_normal["ref_situation_contractuelle"][0] == "RSC_UNIQUE"
        assert pdl_normal["nb_rsc_en_service"][0] == 1

    def test_jamais_decriture_silencieuse_sur_pdl_ambigu_ou_sorti(self):
        """0 ou plusieurs candidates → ref_situation_contractuelle null (rien à écrire)."""
        orders = pl.DataFrame({"sale_order_id": [1, 2], "pdl": ["PDL_SORTI", "PDL_AMBIGU"]})
        contrats = pl.DataFrame(
            {
                "pdl": ["PDL_SORTI", "PDL_AMBIGU", "PDL_AMBIGU"],
                "ref_situation_contractuelle": ["RSC_R1", "RSC_A1", "RSC_A2"],
                "dernier_etat_contractuel": ["RESILIE", "EN SERVICE", "EN SERVICE"],
            }
        )
        result = _attribuer_rsc_en_service(orders, contrats)
        assert result["ref_situation_contractuelle"].null_count() == 2


class TestSourceInjectionRsc:
    """Garde anti-drift : le notebook implémente bien la règle #580 (pas l'asof seul)."""

    def test_regle_dattribution_presente(self):
        assert 'pl.col("dernier_etat_contractuel") != "RESILIE"' in _SOURCE_INJECTION_RSC
        assert "nb_rsc_en_service" in _SOURCE_INJECTION_RSC

    def test_injection_utilise_orders_attribution_pas_lasof_seul(self):
        """La cellule d'écriture Odoo doit dépendre de `orders_attribution` (règle #580),
        pas directement de `orders_avec_rsc` (asof brut) — sinon la RSC résiliée repart."""
        assert "def _(OdooWriter, orders_attribution, run_button, sim_mode):" in _SOURCE_INJECTION_RSC

    def test_asof_reste_present_en_diagnostic(self):
        """L'asof n'est pas supprimé : diagnostic/départage, cf. #580."""
        assert 'strategy="nearest"' in _SOURCE_INJECTION_RSC
        assert "diagnostic" in _SOURCE_INJECTION_RSC.lower()

    def test_jamais_ecriture_silencieuse_mentionne(self):
        assert "jamais d'écriture silencieuse" in _SOURCE_INJECTION_RSC
