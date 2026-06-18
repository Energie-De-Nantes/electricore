"""Schéma agnostique `LignesFacture` consommé par `rapprocher()`.

Voir `electricore/core/CONTEXT.md` (entrée *Ligne de facture*).

**Contrat minimal** : 4 colonnes requises — celles sur lesquelles `rapprocher()`
branche pour produire le rapprochement Enedis. Toute autre colonne (`pdl`,
`est_lisse`, identifiants ERP `invoice_line_ids` / `name_account_move` /
`name_product_product`, etc.) traverse en passe-plat via `strict=False` et
finit dans la sortie `LignesFactureRapprochees`. C'est l'adaptateur ERP qui
décide quelles colonnes additionnelles fournir.
"""

import pandera.polars as pa
import polars as pl


class LignesFacture(pa.DataFrameModel):
    """Lignes de facture, contrat minimal agnostique ERP."""

    # Clé de jointure avec la facturation Enedis mensuelle
    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)

    # Détermine la colonne Enedis à projeter pour `quantite_enedis`.
    # L'enum est le sous-ensemble *facturable* (cf. CONTEXT.md, *Ligne de facture*),
    # pas le catalogue ERP : l'adaptateur/consommateur écarte les catégories hors
    # scope (`Prestation-Enedis`, racine Odoo « All »…) en amont (#335). Doit rester
    # aligné sur `_MAPPING_CATEGORIE_COLONNE` (contexte_mensuel.py).
    categorie_produit: pl.Utf8 = pa.Field(nullable=False, isin=["Base", "HP", "HC", "Abonnements"])

    # Utilisée pour la dérivation ADR-0014 (`> 0` vs `== 0`)
    quantite: pl.Float64 = pa.Field(nullable=False)

    # Utilisée pour la dérivation ADR-0014 (remplace `state == 'draft'` côté Odoo)
    est_brouillon: pl.Boolean = pa.Field(nullable=False)

    class Config:
        strict = False  # passe-plat : les adaptateurs ERP fournissent leurs propres colonnes
