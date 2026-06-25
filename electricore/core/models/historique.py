"""Modèle Pandera pour `Historique`.

Valide la sortie *rétrécie* du pipeline `pipeline_historique` (ADR-0041, #378) :
la **spine** de la Chronologie du contrat (événements C15 ∪ grille FACTURATION mensuelle,
situation forward-fillée en dbt, #375) enrichie de la **détection de ruptures d'abonnement**
(`avant_*`, `impacte_abonnement`, `resume_modification`).

Depuis la descente d'assemblage d'ADR-0041, ce modèle ne porte plus la branche **énergie** :
plus de colonnes d'index/calendrier/relevé (`avant_*`/`apres_*`), plus d'`impacte_energie`.
L'énergie est assemblée en dbt (*Chronologie des relevés*, loader `chronologie_releves()`, #376/#377)
et ne passe plus par `pipeline_historique`.

Voir `electricore/core/CONTEXT.md` (entrée `Historique`) et ADR-0013/0041.
"""

import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime


class Historique(pa.DataFrameModel):
    """Séquence temporelle enrichie d'événements contractuels d'un PDL (branche abonnement)."""

    # Timestamp principal
    date_evenement: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})

    # Couple d'identifiants principaux
    pdl: pl.Utf8 = pa.Field(nullable=False)
    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)

    # Épine de la spine (Class-Table Inheritance, ADR-0041) — passe-plat
    source: pl.Utf8 | None = pa.Field(nullable=True)
    type_fait: pl.Utf8 | None = pa.Field(nullable=True)

    # Informations Contractuelles
    segment_clientele: pl.Utf8 = pa.Field(nullable=False)
    etat_contractuel: pl.Utf8 = pa.Field(nullable=False)
    evenement_declencheur: pl.Utf8 = pa.Field(nullable=False)
    type_evenement: pl.Utf8 = pa.Field(nullable=False)
    categorie: pl.Utf8 | None = pa.Field(nullable=True)

    # Informations pour calculs tarifs
    puissance_souscrite_kva: pl.Float64 = pa.Field(nullable=False)
    formule_tarifaire_acheminement: pl.Utf8 = pa.Field(nullable=False)

    # Informations Compteur
    type_compteur: pl.Utf8 = pa.Field(nullable=False)
    num_compteur: pl.Utf8 = pa.Field(nullable=False)

    # Statut de communication (épique #313) : niveau d'ouverture aux services Enedis
    # (xsd:string ∈ {0,1,2}, donc Utf8 — communicant ⇔ niveau ≥ 1) et sa date de bascule.
    # Déclaratif C15, forward-fillé par RSC dans la spine (en SQL, #375) ; nullable car
    # absent des flux antérieurs et des PDL hors C15.
    niveau_ouverture_services: pl.Utf8 | None = pa.Field(nullable=True)
    date_changement_niveau_ouverture_services: pl.Date | None = pa.Field(nullable=True)

    # Informations Demande (optionnelles)
    ref_demandeur: pl.Utf8 | None = pa.Field(nullable=True)
    id_affaire: pl.Utf8 | None = pa.Field(nullable=True)

    # Colonnes ajoutées par l'enrichissement (detecter_points_de_rupture)
    avant_puissance_souscrite: pl.Float64 | None = pa.Field(nullable=True)
    avant_formule_tarifaire_acheminement: pl.Utf8 | None = pa.Field(nullable=True)
    impacte_abonnement: pl.Boolean = pa.Field(nullable=False)
    resume_modification: pl.Utf8 = pa.Field(nullable=False)

    class Config:
        strict = False  # tolère colonnes supplémentaires (passe-plat des relations de la spine)
