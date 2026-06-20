import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime

from electricore.core.models.cadrans import CADRANS, col_index


class RelevéIndex(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour les relevés d'index issus de différentes sources - Version Polars.

    Ce modèle permet de valider les relevés de compteurs avec leurs métadonnées
    en utilisant Polars pour des performances optimales.
    """

    # 📆 Date du relevé - Utilisation du type DateTime Polars avec timezone
    date_releve: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    ordre_index: pl.Boolean = pa.Field(default=False)

    # 🔹 Identifiant du Point de Livraison (PDL)
    pdl: pl.Utf8 = pa.Field(nullable=False)
    ref_situation_contractuelle: pl.Utf8 | None = pa.Field(nullable=True)
    formule_tarifaire_acheminement: pl.Utf8 | None = pa.Field(nullable=True)
    # Statut de communication (épique #313, ADR-0036) : niveau d'ouverture aux services
    # Enedis (xsd:string ∈ {0,1,2}, niveau PRM). Porté nativement par les relevés C15,
    # forward-fillé par PDL sur les périodiques au mart `releves` (#324). La *jumelle* de
    # nature_index : dbt porte l'attribution, le cœur en dérive le verdict (#325).
    niveau_ouverture_services: pl.Utf8 | None = pa.Field(nullable=True)
    # Événement contractuel déclencheur (C15 `Nature_Evenement`, ex. `MES`/`MCT`/`RES`).
    # Porté NATIVEMENT par les relevés C15 (comme RSC/FTA/niveau) ; NON forward-fillé — un
    # télérelevé périodique (R151/R64) n'est déclenché par aucun événement → null. Source du
    # label d'*origine de relevé* dérivé à l'exposition (périodique vs événementiel).
    evenement_declencheur: pl.Utf8 | None = pa.Field(nullable=True)

    # 🏢 Références Fournisseur & Distributeur
    id_calendrier_fournisseur: pl.Utf8 | None = pa.Field(nullable=True)
    id_calendrier_distributeur: pl.Utf8 | None = pa.Field(nullable=True)
    id_affaire: pl.Utf8 | None = pa.Field(nullable=True)

    # Source des données
    source: pl.Utf8 = pa.Field(nullable=False, isin=["flux_R151", "flux_R15", "flux_C15", "flux_R64", "FACTURATION"])

    # 📏 Unité : aucune. Tout est en kWh entiers — le grain facturable atomique, normalisé
    # Wh→kWh par floor au boundary dbt (ADR-0034). Les ex-champs `unite`/`precision`
    # (vestiges de l'ère Wh) sont retirés du contrat : le modèle de relevés canonique
    # `releves` ne les porte pas, et plus rien ne les lit. Un loader /flux peut encore
    # exposer un `unite` (colonne supplémentaire tolérée, Config.strict=False).

    # ⚡ Index de compteurs (valeurs cumulées en kWh **entiers**, ADR-0034). Les 7 colonnes
    # `index_*_kwh` sont DÉRIVÉES de `cadrans.py` (source unique, ADR-0035 §1) au lieu d'être
    # hand-listées : injection des annotations + `Field` depuis `CADRANS`. Type Int64 — celui
    # que dbt émet nativement (`contrat_releve()` : bigint) après le floor Wh→kWh au boundary
    # de linéarisation. Le loader ne re-caste plus (ADR-0035) ; la parité dbt↔Pandera est
    # prouvée par `test_releves_dbt_respecte_le_contrat_pandera`, la dérivation par
    # `test_releve_index_derive_ses_index_de_cadrans_py`.
    vars().update({col_index(cadran): pa.Field(nullable=True) for cadran in CADRANS})
    __annotations__.update({col_index(cadran): pl.Int64 | None for cadran in CADRANS})

    # 🔌 Métadonnées spécifiques R64 (optionnelles)
    type_releve: pl.Utf8 | None = pa.Field(nullable=True, isin=["AQ", "AM", "AC"])
    contexte_releve: pl.Utf8 | None = pa.Field(nullable=True, isin=["COL", "IND"])
    etape_metier: pl.Utf8 | None = pa.Field(nullable=True, isin=["BRUT", "CORR", "VALID"])
    grandeur_physique: pl.Utf8 | None = pa.Field(nullable=True, isin=["EA", "ER"])
    grandeur_metier: pl.Utf8 | None = pa.Field(nullable=True, isin=["CONS", "PROD"])

    @pa.dataframe_check
    def verifier_presence_mesures(cls, data) -> pl.LazyFrame:
        """
        Vérifie que les mesures attendues sont présentes selon l'Id_Calendrier_Distributeur.
        Utilise les expressions Polars natives pour la validation.
        """
        df_lazy = data.lazyframe

        # Créer des conditions pour chaque type de calendrier
        conditions = []

        # DI000001: index_base_kwh doit être non-null
        cond_d1 = (
            pl.when(pl.col("id_calendrier_distributeur") == "DI000001")
            .then(pl.col("index_base_kwh").is_not_null())
            .otherwise(pl.lit(True))
        )
        conditions.append(cond_d1)

        # DI000002: index_hp_kwh et index_hc_kwh doivent être non-null
        cond_d2 = (
            pl.when(pl.col("id_calendrier_distributeur") == "DI000002")
            .then(pl.col("index_hp_kwh").is_not_null() & pl.col("index_hc_kwh").is_not_null())
            .otherwise(pl.lit(True))
        )
        conditions.append(cond_d2)

        # DI000003: index_hph_kwh, index_hch_kwh, index_hpb_kwh, index_hcb_kwh doivent être non-null
        cond_d3 = (
            pl.when(pl.col("id_calendrier_distributeur") == "DI000003")
            .then(
                pl.col("index_hph_kwh").is_not_null()
                & pl.col("index_hch_kwh").is_not_null()
                & pl.col("index_hpb_kwh").is_not_null()
                & pl.col("index_hcb_kwh").is_not_null()
            )
            .otherwise(pl.lit(True))
        )
        conditions.append(cond_d3)

        # Combiner toutes les conditions
        combined_condition = conditions[0]
        for cond in conditions[1:]:
            combined_condition = combined_condition & cond

        return df_lazy.select(combined_condition.alias("mesures_valides"))

    class Config:
        """Configuration du modèle."""

        strict = False  # Permet les colonnes supplémentaires durant la migration


class RequêteRelevé(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour les requêtes d'interrogation des relevés d'index - Version Polars.

    Assure que les requêtes sont bien formatées avant d'interroger le DataFrame `RelevéIndex`.
    """

    # 📆 Date du relevé demandée
    date_releve: DateTime = pa.Field(nullable=False, dtype_kwargs={"time_unit": "ns", "time_zone": "Europe/Paris"})

    # 🔹 Identifiant du Point de Livraison (PDL)
    pdl: pl.Utf8 = pa.Field(nullable=False)
