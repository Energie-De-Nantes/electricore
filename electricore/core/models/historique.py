"""Modèle Pandera pour `Historique`.

Valide la sortie enrichie du pipeline `pipeline_historique` : événements
contractuels C15 + détection des points de rupture (impacts abonnement et
énergie, résumé des modifications) + événements FACTURATION artificiels insérés
au 1er de chaque mois.

Voir `electricore/core/CONTEXT.md` (entrée `Historique`) et ADR-0013.
"""

import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime


class Historique(pa.DataFrameModel):
    """Séquence temporelle enrichie d'événements contractuels d'un PDL."""

    # Timestamp principal
    date_evenement: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})

    # Couple d'identifiants principaux
    pdl: pl.Utf8 = pa.Field(nullable=False)
    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)

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
    # Déclaratif C15, reporté par RSC en forward-fill (cf. `expr_colonnes_a_propager`) ;
    # nullable car absent des flux antérieurs et des PDL hors C15.
    niveau_ouverture_services: pl.Utf8 | None = pa.Field(nullable=True)
    date_changement_niveau_ouverture_services: pl.Date | None = pa.Field(nullable=True)

    # Informations Demande (optionnelles)
    ref_demandeur: pl.Utf8 | None = pa.Field(nullable=True)
    id_affaire: pl.Utf8 | None = pa.Field(nullable=True)

    # Colonnes supplémentaires fréquemment présentes
    source: pl.Utf8 | None = pa.Field(nullable=True)
    marque: pl.Utf8 | None = pa.Field(nullable=True)
    unite: pl.Utf8 | None = pa.Field(nullable=True)
    precision: pl.Utf8 | None = pa.Field(nullable=True)
    num_depannage: pl.Utf8 | None = pa.Field(nullable=True)
    date_derniere_modification_fta: pl.Utf8 | None = pa.Field(nullable=True)

    # Colonnes de relevés "Avant"
    avant_date_releve: DateTime | None = pa.Field(
        nullable=True, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )
    avant_nature_index: pl.Utf8 | None = pa.Field(nullable=True)
    avant_id_calendrier_fournisseur: pl.Utf8 | None = pa.Field(nullable=True)
    avant_id_calendrier_distributeur: pl.Utf8 | None = pa.Field(nullable=True)
    # Index avant/après en kWh entiers (Int64) : flux_c15 émet du bigint, le loader ne
    # re-caste plus (ADR-0035) — il trust dbt.
    avant_index_hp_kwh: pl.Int64 | None = pa.Field(nullable=True)
    avant_index_hc_kwh: pl.Int64 | None = pa.Field(nullable=True)
    avant_index_hch_kwh: pl.Int64 | None = pa.Field(nullable=True)
    avant_index_hph_kwh: pl.Int64 | None = pa.Field(nullable=True)
    avant_index_hpb_kwh: pl.Int64 | None = pa.Field(nullable=True)
    avant_index_hcb_kwh: pl.Int64 | None = pa.Field(nullable=True)
    avant_index_base_kwh: pl.Int64 | None = pa.Field(nullable=True)

    # Colonnes de relevés "Après"
    apres_date_releve: DateTime | None = pa.Field(
        nullable=True, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )
    apres_nature_index: pl.Utf8 | None = pa.Field(nullable=True)
    apres_id_calendrier_fournisseur: pl.Utf8 | None = pa.Field(nullable=True)
    apres_id_calendrier_distributeur: pl.Utf8 | None = pa.Field(nullable=True)
    apres_index_hp_kwh: pl.Int64 | None = pa.Field(nullable=True)
    apres_index_hc_kwh: pl.Int64 | None = pa.Field(nullable=True)
    apres_index_hch_kwh: pl.Int64 | None = pa.Field(nullable=True)
    apres_index_hph_kwh: pl.Int64 | None = pa.Field(nullable=True)
    apres_index_hpb_kwh: pl.Int64 | None = pa.Field(nullable=True)
    apres_index_hcb_kwh: pl.Int64 | None = pa.Field(nullable=True)
    apres_index_base_kwh: pl.Int64 | None = pa.Field(nullable=True)

    # Colonnes ajoutées par l'enrichissement (detecter_points_de_rupture)
    avant_puissance_souscrite: pl.Float64 | None = pa.Field(nullable=True)
    avant_formule_tarifaire_acheminement: pl.Utf8 | None = pa.Field(nullable=True)
    impacte_abonnement: pl.Boolean = pa.Field(nullable=False)
    impacte_energie: pl.Boolean = pa.Field(nullable=False)
    resume_modification: pl.Utf8 = pa.Field(nullable=False)

    @pa.dataframe_check
    def verifier_coherence_dates(cls, data) -> pl.LazyFrame:
        """Les dates de relevés avant/après doivent être cohérentes avec date_evenement."""
        df_lazy = data.lazyframe

        condition_avant = (
            pl.when(pl.col("avant_date_releve").is_not_null())
            .then(pl.col("avant_date_releve") <= pl.col("date_evenement"))
            .otherwise(pl.lit(True))
        )

        condition_apres = (
            pl.when(pl.col("apres_date_releve").is_not_null())
            .then(pl.col("apres_date_releve") >= pl.col("date_evenement"))
            .otherwise(pl.lit(True))
        )

        return df_lazy.select((condition_avant & condition_apres).alias("dates_coherentes"))

    @pa.dataframe_check
    def verifier_presence_mesures_releves(cls, data) -> pl.LazyFrame:
        """Vérifie que les index présents correspondent au calendrier distributeur déclaré."""
        df_lazy = data.lazyframe

        cond_avant_d1 = (
            pl.when(pl.col("avant_id_calendrier_distributeur") == "DI000001")
            .then(pl.col("avant_index_base_kwh").is_not_null())
            .otherwise(pl.lit(True))
        )
        cond_avant_d2 = (
            pl.when(pl.col("avant_id_calendrier_distributeur") == "DI000002")
            .then(pl.col("avant_index_hp_kwh").is_not_null() & pl.col("avant_index_hc_kwh").is_not_null())
            .otherwise(pl.lit(True))
        )
        cond_avant_d3 = (
            pl.when(pl.col("avant_id_calendrier_distributeur") == "DI000003")
            .then(
                pl.col("avant_index_hph_kwh").is_not_null()
                & pl.col("avant_index_hch_kwh").is_not_null()
                & pl.col("avant_index_hpb_kwh").is_not_null()
                & pl.col("avant_index_hcb_kwh").is_not_null()
            )
            .otherwise(pl.lit(True))
        )
        cond_apres_d1 = (
            pl.when(pl.col("apres_id_calendrier_distributeur") == "DI000001")
            .then(pl.col("apres_index_base_kwh").is_not_null())
            .otherwise(pl.lit(True))
        )
        cond_apres_d2 = (
            pl.when(pl.col("apres_id_calendrier_distributeur") == "DI000002")
            .then(pl.col("apres_index_hp_kwh").is_not_null() & pl.col("apres_index_hc_kwh").is_not_null())
            .otherwise(pl.lit(True))
        )
        cond_apres_d3 = (
            pl.when(pl.col("apres_id_calendrier_distributeur") == "DI000003")
            .then(
                pl.col("apres_index_hph_kwh").is_not_null()
                & pl.col("apres_index_hch_kwh").is_not_null()
                & pl.col("apres_index_hpb_kwh").is_not_null()
                & pl.col("apres_index_hcb_kwh").is_not_null()
            )
            .otherwise(pl.lit(True))
        )

        mesures_valides = cond_avant_d1 & cond_avant_d2 & cond_avant_d3 & cond_apres_d1 & cond_apres_d2 & cond_apres_d3
        return df_lazy.select(mesures_valides.alias("mesures_releves_valides"))

    class Config:
        strict = False  # tolère colonnes supplémentaires (date_entree, dates_facturation, etc.)
