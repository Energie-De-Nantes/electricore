"""Tests du build `rapport_facturation` (core/builds, issue #143).

Le livrable facturation est assemblé en core sur le shape agnostique
`LignesFacture` : les tests injectent des frames fixtures — zéro Odoo,
zéro monkeypatch (gain de test surface visé par la migration).
"""

from datetime import datetime

import polars as pl

from electricore.core.builds.contexte_mensuel import ContexteMensuel
from electricore.core.builds.rapport_facturation import (
    RapportFacturation,
    feuilles_rapport_facturation,
    rapport_facturation,
)


def _lignes(*, rscs: list[str], quantites: list[float], brouillons: list[bool]) -> pl.DataFrame:
    """`LignesFacture`-conformes avec passe-plat ERP (Odoo-style)."""
    n = len(rscs)
    return pl.DataFrame(
        {
            "ref_situation_contractuelle": rscs,
            "categorie_produit": ["HP"] * n,
            "quantite": quantites,
            "est_brouillon": brouillons,
            # ERP passe-plat
            "invoice_line_ids": list(range(101, 101 + n)),
            "x_pdl": [f"PDL-{r}" for r in rscs],
        }
    )


def _fact_mensuelle(*, rscs: list[str], memos: list[str] | None = None) -> pl.DataFrame:
    n = len(rscs)
    memos = memos if memos is not None else [""] * n
    return pl.DataFrame(
        {
            "ref_situation_contractuelle": rscs,
            "pdl": [f"PDL-{r}" for r in rscs],
            "debut": [datetime(2025, 3, 1)] * n,
            "fin": [datetime(2025, 4, 1)] * n,
            "energie_hp_kwh": [100.0] * n,
            "energie_hc_kwh": [0.0] * n,
            "energie_base_kwh": [0.0] * n,
            "nb_jours": [31] * n,
            "turpe_fixe_eur": [10.0] * n,
            "turpe_variable_eur": [5.0] * n,
            "qualite": ["réelle"] * n,
            "statut_communication": ["communicante"] * n,
            "memo_puissance": memos,
        }
    ).with_columns(
        pl.col("debut").dt.replace_time_zone("Europe/Paris"),
        pl.col("fin").dt.replace_time_zone("Europe/Paris"),
    )


def _ctx(fact: pl.DataFrame, mois: str = "2025-03-01") -> ContexteMensuel:
    historique = pl.LazyFrame(
        {
            "ref_situation_contractuelle": fact["ref_situation_contractuelle"].to_list(),
            "num_compteur": ["N"] * len(fact),
            "type_compteur": ["LINKY"] * len(fact),
        }
    )
    return ContexteMensuel(
        mois=mois,
        historique_enrichi=historique,
        abonnements=pl.LazyFrame(),
        energie=pl.LazyFrame(),
        releves_utilises=pl.LazyFrame(),
        facturation_mensuelle=fact,
    )


class TestRapportFacturation:
    def test_resume_porte_les_totaux_du_mois(self):
        """1 à facturer (brouillon, qte>0), 1 à supprimer (brouillon, qte=0), 1 hors draft."""
        rscs = ["R1", "R2", "R3"]
        ctx = _ctx(_fact_mensuelle(rscs=rscs))
        lignes = _lignes(rscs=rscs, quantites=[42.0, 0.0, 10.0], brouillons=[True, True, False])

        rapport = rapport_facturation(ctx, lignes)

        assert isinstance(rapport, RapportFacturation)
        assert rapport.resume["mois"].item() == "2025-03-01"
        assert rapport.resume["nb_pdl"].item() == 3
        assert rapport.resume["total_a_facturer"].item() == 1
        assert rapport.resume["total_a_supprimer"].item() == 1

    def test_changements_puissance_est_un_filtre_de_lignes(self):
        rscs = ["R1", "R2"]
        ctx = _ctx(_fact_mensuelle(rscs=rscs, memos=["", "Hausse 6 → 9 kVA"]))
        lignes = _lignes(rscs=rscs, quantites=[1.0, 1.0], brouillons=[True, True])

        rapport = rapport_facturation(ctx, lignes)

        assert len(rapport.changements_puissance) == 1
        assert rapport.changements_puissance["ref_situation_contractuelle"].item() == "R2"

    def test_lignes_preserve_la_passe_plat(self):
        """La sortie du build transporte les colonnes ERP de l'entrée (issue #142)."""
        rscs = ["R1"]
        ctx = _ctx(_fact_mensuelle(rscs=rscs))
        lignes = _lignes(rscs=rscs, quantites=[1.0], brouillons=[True])

        rapport = rapport_facturation(ctx, lignes)

        assert rapport.lignes["x_pdl"].item() == "PDL-R1"


class TestRapportFacturationResumeSchema:
    """Le schéma `RapportFacturationResume` (core/models) valide une frame une-ligne de totaux."""

    def test_resume_schema_valide_une_ligne_de_totaux(self):
        from electricore.core.models.rapport_facturation import RapportFacturationResume

        df = pl.DataFrame(
            {
                "mois": ["2025-03-01"],
                "nb_pdl": [42],
                "total_a_facturer": [156],
                "total_a_supprimer": [3],
            }
        )
        RapportFacturationResume.validate(df)


class TestFeuilles:
    def test_trois_onglets_standards(self):
        rscs = ["R1"]
        rapport = rapport_facturation(
            _ctx(_fact_mensuelle(rscs=rscs)), _lignes(rscs=rscs, quantites=[1.0], brouillons=[True])
        )

        feuilles = feuilles_rapport_facturation(rapport)

        assert set(feuilles.keys()) == {"Résumé", "Lignes", "Changements puissance"}

    def test_ordre_facturiste_passe_plat_puis_contrat_puis_calculees(self):
        """L'ordre du livrable vit ici (décision #142/#143) : identifiants ERP
        (passe-plat, ordre d'entrée) d'abord — ce que le facturiste reconnaît —
        puis contrat et colonnes calculées Enedis."""
        rscs = ["R1"]
        rapport = rapport_facturation(
            _ctx(_fact_mensuelle(rscs=rscs)), _lignes(rscs=rscs, quantites=[1.0], brouillons=[True])
        )

        feuilles = feuilles_rapport_facturation(rapport)

        colonnes = feuilles["Lignes"].columns
        assert colonnes[:2] == ["invoice_line_ids", "x_pdl"]  # passe-plat, ordre d'entrée
        assert colonnes[2:6] == ["ref_situation_contractuelle", "categorie_produit", "quantite", "est_brouillon"]
        assert colonnes[6] == "quantite_enedis"
        assert feuilles["Changements puissance"].columns == colonnes
