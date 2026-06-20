"""Modèle Pandera de la *Chronologie des relevés* (issue #180, ADR-0023, ADR-0028).

La chronologie des relevés est la ligne de temps **énergie** d'un contrat : tous les
relevés d'index aux dates qui bornent ses périodes (événements C15 *avant*/*après* et
dates de facturation interrogées dans R151/R64), assemblés, dédoublonnés par source
prioritaire et ordonnés. C'est l'entrée pure de `calculer_periodes_energie`.

Ce schéma matérialise comme **contrat** les invariants jusqu'ici encodés
incidemment dans l'implémentation :

- **1 ligne par `(ref_situation_contractuelle, date_releve, ordre_index)`** — la clé
  métier de dédoublonnage (cf. ADR-0028, *Identité de relevé*).
- **`ref_situation_contractuelle` non-null** — l'attribution contractuelle est garantie
  en amont : relevés C15 à valeur native + relevés périodiques attribués par la requête
  FACTURATION (qui porte RSC/FTA/niveau du contrat depuis le substrat d'événements,
  ADR-0029/ADR-0039) ; la chronologie ne la recalcule pas, elle la présume attribuée.
- **`source` dans une énumération fermée** — la source gagnante d'un relevé est l'une des
  sources connues, choisie par la table de priorité explicite `flux_C15 > flux_R64 >
  flux_R151` (ADR-0028), jamais par hasard.
- **`ordre_index` booléen** (`False` = avant / relevé périodique, `True` = après C15) —
  type unique de bout en bout (fin de la divergence Boolean-modèle / Int32-code).
"""

import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime

from electricore.core.models.cadrans import CADRANS, col_index

# Énumération fermée des sources d'un relevé dans la chronologie.
SOURCES_CHRONOLOGIE: tuple[str, ...] = ("flux_C15", "flux_R64", "flux_R151", "flux_R15")


class ChronologieReleves(pa.DataFrameModel):
    """📌 Chronologie des relevés d'un contrat : ligne de temps énergie dédoublonnée.

    Contrat du mart dbt `chronologie_releves` (loader `chronologie()`, ADR-0041), entrée
    de `pipeline_energie` / `calculer_periodes_energie` et source de `releves_utilises`.
    Grain garanti : 1 ligne par `(ref_situation_contractuelle, date_releve, ordre_index)`.
    """

    # 🔹 Identifiants
    pdl: pl.Utf8 = pa.Field(nullable=False)
    # Attribution contractuelle garantie en amont (mart + requête FACTURATION, ADR-0029) —
    # invariant fort, présumé mart-shaped en entrée (plus de forward-fill local).
    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)
    formule_tarifaire_acheminement: pl.Utf8 | None = pa.Field(nullable=True)
    # Niveau d'ouverture aux services (statut de communication, ADR-0036). Natif sur les
    # relevés C15, attribué aux périodiques par la requête FACTURATION (substrat
    # d'événements, ADR-0039 — plus de recopie au mart) ; jumelle de nature_index. Nullable
    # (un relevé interrogé absent ou antérieur à tout C15 n'en porte pas). Le verdict
    # d'ouverture de période (#325) le rollupera sur les deux bornes.
    niveau_ouverture_services: pl.Utf8 | None = pa.Field(nullable=True)

    # 📆 Date du relevé (tz-aware Europe/Paris, comme RelevéIndex)
    date_releve: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})

    # Discriminant avant/après : False = avant ou relevé périodique, True = après C15.
    ordre_index: pl.Boolean = pa.Field(nullable=False)

    # Source gagnante (table de priorité explicite C15 > R64 > R151). Nullable : une borne
    # FACTURATION sans relevé apparié (releve_manquant) n'a pas de source — elle reste une
    # borne de période. Les valeurs non-nulles sont dans l'énumération fermée.
    source: pl.Utf8 | None = pa.Field(nullable=True, isin=SOURCES_CHRONOLOGIE)

    # 🆔 Identité (ADR-0028/0029, #244) portée jusqu'au journal (#233). Mintée au seam
    # dbt pour toutes les sources. Nullable : un relevé interrogé absent (releve_manquant)
    # n'en porte pas.
    releve_id: pl.Utf8 | None = pa.Field(nullable=True)
    # Nature canonique (réel/estimé/corrigé) ; nullable pour la même raison.
    nature_index: pl.Utf8 | None = pa.Field(nullable=True, isin=["réel", "estimé", "corrigé"])
    # Événement contractuel déclencheur (C15 `Nature_Evenement`) porté nativement par les
    # relevés C15 ; null pour un télérelevé périodique. Source du label d'*origine de relevé*
    # (périodique vs événementiel) dérivé à l'exposition (releves_utilises, ADR-0038).
    evenement_declencheur: pl.Utf8 | None = pa.Field(nullable=True)

    # 🏢 Référence calendrier distributeur (utile au contrôle des cadrans attendus)
    id_calendrier_distributeur: pl.Utf8 | None = pa.Field(nullable=True)

    # ⚡ Index de compteurs (kWh entiers, ADR-0034 ; Int64 hérité de RelevéIndex). Colonnes
    # `index_*_kwh` DÉRIVÉES de `cadrans.py` (source unique, ADR-0035 §1), comme RelevéIndex.
    vars().update({col_index(cadran): pa.Field(nullable=True) for cadran in CADRANS})
    __annotations__.update({col_index(cadran): pl.Int64 | None for cadran in CADRANS})

    # 🚩 Flag de complétude : relevé attendu mais absent à cette date (relevés interrogés).
    releve_manquant: pl.Boolean | None = pa.Field(nullable=True)

    @pa.dataframe_check
    def verifier_grain_unique(cls, data) -> bool:
        """Garantit 1 ligne par `(ref_situation_contractuelle, date_releve, ordre_index)`."""
        df = data.lazyframe
        cle = ["ref_situation_contractuelle", "date_releve", "ordre_index"]
        nb_total = df.select(pl.len()).collect().item()
        nb_uniques = df.select(cle).unique().select(pl.len()).collect().item()
        return nb_total == nb_uniques

    class Config:
        """Permet les colonnes supplémentaires (passe-plat des métadonnées source)."""

        strict = False
