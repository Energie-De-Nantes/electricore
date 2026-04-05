"""
Notebook de debug : Analyse d'un PDL avec anomalie de consommation.

PDL cible : 14290738060355
Problème : 961,974 kWh en septembre 2025 (impossible pour résidentiel)
Objectif : Tracer l'origine de la corruption des données
"""

import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    import os
    from electricore.core.loaders import releves_harmonises, c15
    from electricore.config.env import charger_env

    charger_env()
    PDL_CIBLE = "14290738060355"
    DB_PATH = os.getenv("DUCKDB_PATH", "electricore/etl/flux_enedis_pipeline.duckdb")
    return DB_PATH, PDL_CIBLE, c15, pl, releves_harmonises


@app.cell
def _(DB_PATH, PDL_CIBLE, pl, releves_harmonises):
    """Charger tous les relevés pour ce PDL"""

    relevés_pdl = (
        releves_harmonises(database_path=DB_PATH)
        .lazy()
        .filter(pl.col("pdl") == PDL_CIBLE)
        .sort("date_releve")
        .collect()
    )

    print(f"📊 Nombre de relevés trouvés : {len(relevés_pdl)}")
    print(f"📅 Période : {relevés_pdl['date_releve'].min()} → {relevés_pdl['date_releve'].max()}")
    print(f"🔄 Flux d'origine : {relevés_pdl['flux_origine'].unique().to_list()}")

    relevés_pdl
    return (relevés_pdl,)


@app.cell
def _(pl, relevés_pdl):
    """Analyser les index sur la période problématique (sept 2025)"""

    index_sept_2025 = relevés_pdl.filter(
        (pl.col("date_releve").dt.strftime('%Y-%m-%d') >= '2025-08-15') &
        (pl.col("date_releve").dt.strftime('%Y-%m-%d') <= '2025-10-15')
    ).select([
        "date_releve",
        "flux_origine",
        "index_base_kwh",
        "index_hp_kwh",
        "index_hc_kwh",
        "ordre_index"
    ]).sort("date_releve")

    print("📍 Index autour de septembre 2025 :")
    index_sept_2025
    return (index_sept_2025,)


@app.cell
def _(index_sept_2025, pl):
    """Calculer les delta entre relevés consécutifs"""

    delta_index = index_sept_2025.with_columns([
        (pl.col("index_base_kwh") - pl.col("index_base_kwh").shift(1)).alias("delta_base"),
        (pl.col("index_hp_kwh") - pl.col("index_hp_kwh").shift(1)).alias("delta_hp"),
        (pl.col("index_hc_kwh") - pl.col("index_hc_kwh").shift(1)).alias("delta_hc"),
    ])

    print("🔍 Delta entre relevés (consommations calculées) :")
    delta_index
    return (delta_index,)


@app.cell
def _(delta_index, pl):
    """Identifier les delta aberrants (> 10 MWh)"""

    SEUIL_ABERRANT_KWH = 10_000  # 10 MWh

    delta_aberrants = delta_index.filter(
        (pl.col("delta_base").abs() > SEUIL_ABERRANT_KWH) |
        (pl.col("delta_hp").abs() > SEUIL_ABERRANT_KWH) |
        (pl.col("delta_hc").abs() > SEUIL_ABERRANT_KWH)
    )

    print(f"⚠️ Nombre de delta aberrants (> {SEUIL_ABERRANT_KWH:,.0f} kWh) : {len(delta_aberrants)}")
    delta_aberrants
    return


@app.cell
def _(DB_PATH, PDL_CIBLE, c15, pl):
    """Vérifier l'historique de périmètre pour ce PDL"""

    historique_pdl = (
        c15(database_path=DB_PATH)
        .lazy()
        .filter(pl.col("pdl") == PDL_CIBLE)
        .sort("date_evenement")
        .collect()
    )

    print(f"📋 Événements dans l'historique : {len(historique_pdl)}")

    historique_pdl.select([
        "date_evenement",
        "evenement_declencheur",
        "type_evenement",
        "etat_contractuel",
        "formule_tarifaire_acheminement",
        "puissance_souscrite_kva",
        "type_compteur"
    ])
    return (historique_pdl,)


@app.cell
def _(historique_pdl, pl):
    """Chercher des événements suspects autour de sept 2025"""

    evt_sept_2025 = historique_pdl.filter(
        (pl.col("date_evenement").dt.strftime('%Y-%m-%d') >= '2025-08-01') &
        (pl.col("date_evenement").dt.strftime('%Y-%m-%d') <= '2025-10-31')
    )

    print("📅 Événements autour de septembre 2025 :")
    evt_sept_2025
    return


@app.cell
def _(pl, relevés_pdl):
    """Vérifier s'il y a des retours en arrière (ordre_index)"""

    retours_arriere = relevés_pdl.filter(
        pl.col("ordre_index") == True
    )

    print(f"🔙 Nombre de relevés avec ordre_index=True : {len(retours_arriere)}")

    if len(retours_arriere) > 0:
        retours_arriere.select([
            "date_releve",
            "flux_origine",
            "index_base_kwh",
            "ordre_index"
        ])
    return


@app.cell
def _():
    """
    🎯 SYNTHÈSE DU DEBUG

    Ce notebook permet de :

    1. ✅ Charger tous les relevés pour le PDL problématique
    2. ✅ Examiner les index bruts autour de la période suspecte
    3. ✅ Calculer les delta entre relevés consécutifs
    4. ✅ Identifier les delta aberrants (> 50 MWh)
    5. ✅ Vérifier l'historique de périmètre (événements contractuels)
    6. ✅ Détecter les retours en arrière (ordre_index=True)

    🔍 PROCHAINES ÉTAPES :

    Si on trouve un delta aberrant :
    - Vérifier si c'est un problème de changement de compteur
    - Vérifier si c'est un problème de reset d'index
    - Vérifier la qualité des données source (R151/R64)
    - Vérifier le parsing ETL (parsers.py)

    Si pas de delta aberrant dans les relevés harmonisés :
    - Le problème vient du calcul d'énergie dans pipeline_energie
    - Vérifier les périodes d'énergie calculées
    - Vérifier l'agrégation mensuelle
    """
    return


if __name__ == "__main__":
    app.run()
