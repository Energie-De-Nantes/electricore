"""Modèle Pandera de la *spine* de la Chronologie du contrat (ADR-0041, #375).

La spine est le **substrat relationnel** (Class-Table Inheritance) assemblé en dbt : une
ligne par fait d'une situation contractuelle (RSC) — événements C15 ∪ grille FACTURATION
(1ᵉʳ de chaque mois) — sur une **épine commune** `(pdl, ref_situation_contractuelle,
date_evenement, source, type_fait)`, augmentée des attributs de **situation** (FTA,
puissance, niveau d'ouverture, segment…) forward-fillés **en SQL** sur la timeline
d'événements complète (`last_value(<attr> IGNORE NULLS) OVER (PARTITION BY rsc ORDER BY
date_evenement)`).

Contrat (côté cœur) — ce que le loader `spine_contrat()` garantit :

- **`ref_situation_contractuelle` non-null** : la spine est partitionnée par RSC ; tout
  fait appartient à une situation contractuelle.
- **`source` dans une énumération fermée** : `flux_C15` (événement) ou `synthese_mensuelle`
  (FACTURATION). S'étendra (relevés #376, jalons d'affaire) au fil des relations.
- **`type_fait` dans une énumération fermée** : le sous-type de la spine.

Les attributs de situation sont **nullable** ici (le substrat tolère des têtes de
timeline sans valeur) ; leur non-nullité stricte est rétablie au boundary `Historique`
(part fixe) / aux branches qui les consomment.
"""

import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime

# Énumérations fermées de l'épine. S'étendront avec les relations (relevés, jalons).
SOURCES_SPINE: tuple[str, ...] = ("flux_C15", "synthese_mensuelle")
TYPES_FAIT_SPINE: tuple[str, ...] = ("evenement", "facturation")


class SpineContrat(pa.DataFrameModel):
    """📌 Spine de la Chronologie du contrat : une ligne par fait d'une RSC."""

    # 🔹 Épine commune
    date_evenement: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    pdl: pl.Utf8 = pa.Field(nullable=False)
    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)
    source: pl.Utf8 = pa.Field(nullable=False, isin=SOURCES_SPINE)
    type_fait: pl.Utf8 = pa.Field(nullable=False, isin=TYPES_FAIT_SPINE)

    # 🔸 Identité du fait événementiel (natif C15 ; 'FACTURATION'/'artificiel' pour la grille)
    evenement_declencheur: pl.Utf8 | None = pa.Field(nullable=True)
    type_evenement: pl.Utf8 | None = pa.Field(nullable=True)

    # 🏢 Attributs de SITUATION forward-fillés en SQL (nullable au niveau substrat)
    segment_clientele: pl.Utf8 | None = pa.Field(nullable=True)
    etat_contractuel: pl.Utf8 | None = pa.Field(nullable=True)
    puissance_souscrite_kva: pl.Float64 | None = pa.Field(nullable=True)
    formule_tarifaire_acheminement: pl.Utf8 | None = pa.Field(nullable=True)
    type_compteur: pl.Utf8 | None = pa.Field(nullable=True)
    num_compteur: pl.Utf8 | None = pa.Field(nullable=True)
    categorie: pl.Utf8 | None = pa.Field(nullable=True)
    ref_demandeur: pl.Utf8 | None = pa.Field(nullable=True)
    id_affaire: pl.Utf8 | None = pa.Field(nullable=True)
    niveau_ouverture_services: pl.Utf8 | None = pa.Field(nullable=True)
    date_changement_niveau_ouverture_services: pl.Date | None = pa.Field(nullable=True)

    class Config:
        """Tolère les colonnes supplémentaires (passe-plat des relations à venir)."""

        strict = False
