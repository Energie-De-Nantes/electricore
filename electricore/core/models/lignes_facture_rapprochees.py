"""Modèle Pandera pour `lignes_facture_rapprochees`.

Résultat du *Rapprochement facturation mensuelle*. Voir
`electricore/core/CONTEXT.md`.

**Vraie passe-plat** (issue #142) : sortie = colonnes d'entrée + colonnes
calculées. Le schéma n'exige que le contrat `LignesFacture` et les colonnes
produites par `rapprocher()` — aucune colonne ERP n'est nommée ici, elles
traversent via `strict=False` (symétrique du contrat d'entrée).
"""

import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime


class LignesFactureRapprochees(pa.DataFrameModel):
    """Lignes de facture (shape agnostique) enrichies des données Enedis du mois."""

    # Contrat d'entrée `LignesFacture`, conservé en sortie (est_brouillon compris :
    # consommé pour dériver les flags ADR-0014, gardé visible à côté d'eux)
    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)
    # Sous-ensemble *facturable* (cf. LignesFacture) — doit rester aligné sur
    # `_MAPPING_CATEGORIE_COLONNE` (contexte_mensuel.py). Catégories hors scope
    # écartées en amont du rapprochement (#335).
    categorie_produit: pl.Utf8 = pa.Field(nullable=False, isin=["Base", "HP", "HC", "Abonnements"])
    quantite: pl.Float64 = pa.Field(nullable=False)
    est_brouillon: pl.Boolean = pa.Field(nullable=False)

    # Quantité Enedis + mémo
    quantite_enedis: pl.Float64 | None = pa.Field(nullable=True)
    memo_puissance: pl.Utf8 | None = pa.Field(nullable=True)

    # Méta-période Enedis du mois (nullable si pas de match RSC)
    pdl: pl.Utf8 | None = pa.Field(nullable=True)
    debut: DateTime | None = pa.Field(nullable=True, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    fin: DateTime | None = pa.Field(nullable=True, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    # Verdicts méta jumeaux (axes orthogonaux), remplaçant data_complete (ADR-0033/0036)
    qualite: pl.Utf8 | None = pa.Field(nullable=True, isin=["réelle", "estimée", "incalculable"])
    statut_communication: pl.Utf8 | None = pa.Field(nullable=True, isin=["communicante", "non_communicante"])
    turpe_fixe_eur: pl.Float64 | None = pa.Field(nullable=True)
    turpe_variable_eur: pl.Float64 | None = pa.Field(nullable=True)

    # Identifiants compteur (issus de l'Historique)
    num_compteur: pl.Utf8 | None = pa.Field(nullable=True)
    type_compteur: pl.Utf8 | None = pa.Field(nullable=True)

    # Flags d'état de facturation (cf. ADR-0014, mutuellement exclusifs sur sous-ensemble draft)
    a_facturer: pl.Boolean | None = pa.Field(nullable=True)
    a_supprimer: pl.Boolean | None = pa.Field(nullable=True)

    class Config:
        strict = False  # vraie passe-plat : les colonnes ERP traversent sans être nommées
        coerce = True
