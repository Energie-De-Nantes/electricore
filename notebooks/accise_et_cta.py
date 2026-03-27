import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import marimo as mo
    import polars as pl
    import sys
    from pathlib import Path
    from datetime import datetime, timezone, date
    import time
    import calendar
    from typing import Dict, List, Optional, Tuple

    # Ajouter le chemin du projet
    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    # Imports des loaders DuckDB
    from electricore.core.loaders import (
        c15, releves_harmonises
    )
    from electricore.core.pipelines.orchestration import (
        facturation
    )
    from electricore.core.pipelines.facturation import (
        expr_calculer_trimestre
    )

    # Import connecteur Odoo
    from electricore.core.loaders import (
        OdooReader,
        query,
        commandes_lignes
    )

    # Import pipeline Accise et CTA
    from electricore.core.pipelines.accise import pipeline_accise
    from electricore.core.pipelines.cta import pipeline_cta

    # Config Odoo centralisée depuis .env
    from electricore.config import charger_config_odoo


@app.cell(hide_code=True)
def _():
    """Configuration Odoo depuis .env"""
    try:
        config = charger_config_odoo()
        _msg = mo.md(f"""
        **Configuration Odoo chargée depuis `.env`**

        - URL: `{config.get('url', 'NON CONFIGURÉ')}`
        - Base: `{config.get('db', 'NON CONFIGURÉ')}`
        - Utilisateur: `{config.get('username', 'NON CONFIGURÉ')}`
        - Mot de passe: `{'***' if config.get('password') else 'NON CONFIGURÉ'}`
        """)
    except ValueError as e:
        config = {}
        _msg = mo.md(f"""
        ⚠️ **Configuration Odoo non trouvée**

        {e}

        Créez le fichier `.env` à la racine du projet avec :
        ```
        ODOO_URL=https://votre-instance.odoo.com
        ODOO_DB=votre_database
        ODOO_USERNAME=votre_username
        ODOO_PASSWORD=votre_password
        ```
        """)
    _msg
    return (config,)


@app.cell
def _():
    mo.md(r"""
    # CTA
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Récupération du périmètre PDL depuis Odoo
    """)
    return


@app.cell(hide_code=True)
def load_odoo_perimeter(config):
    """Récupération du périmètre de PDL depuis Odoo"""

    mo.md("## 🏢 Récupération du périmètre PDL (Odoo)")

    try:
        print("🔄 Connexion à Odoo...")
        with OdooReader(config=config) as odoo:
            df_pdl_odoo = (
                query(odoo, 'sale.order',
                    domain=[('x_pdl', '!=', False)],
                    fields=['name', 'x_pdl', 'partner_id'])
                .filter(pl.col('x_pdl').is_not_null())
                .select([
                    pl.col('x_pdl').str.strip_chars().alias('pdl'),
                    pl.col('name').alias('order_name')
                ])
                .collect()
                # .with_columns(pl.lit('EDN').alias('marque'))
                .unique('pdl')
            )

        nb_pdl_odoo = len(df_pdl_odoo)
        print(f"✅ {nb_pdl_odoo} PDL récupérés depuis Odoo")

        if nb_pdl_odoo > 0:
            print(f"📊 Exemples PDL: {df_pdl_odoo.select('pdl').to_series().to_list()[:5]}")

    except Exception as e:
        print(f"⚠️ Erreur connexion Odoo: {e}")
        print("📄 Continuons sans filtre Odoo (tous les PDL F15 seront analysés)")
        df_pdl_odoo = pl.DataFrame({'pdl': [], 'order_name': [], 'marque': []}, schema={'pdl': pl.Utf8, 'order_name': pl.Utf8, 'marque': pl.Utf8})

    # Ajouter des PDL supplémentaires
    pdl_supplementaires = pl.DataFrame({
        'pdl': ['14295224261882', '50070117855585', '50000508594660'],
        'order_name': ['PDL_MANUAL_1', 'PDL_MANUAL_2', 'PDL_MANUAL_3'],
        # 'marque': ['EDN', 'EDN', 'EDN']
    })
    df_pdl_odoo = pl.concat([df_pdl_odoo, pdl_supplementaires]).unique('pdl')
    return (df_pdl_odoo,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Chargement des données pour le pipeline de calcul
    """)
    return


@app.cell
def _():
    lf_historique = c15().lazy()
    lf_releves = releves_harmonises().lazy()
    return lf_historique, lf_releves


@app.cell
def _():
    mo.md(r"""
    ## Pipeline facturation
    """)
    return


@app.cell
def _(lf_historique, lf_releves):
    _,_,_,df_facturation = facturation(historique=lf_historique, releves=lf_releves)
    df_facturation
    return (df_facturation,)


@app.cell
def _(df_facturation, df_pdl_odoo):
    df_cta = (
        df_facturation
        .join(df_pdl_odoo, on='pdl', how='inner')
        .with_columns(
            expr_calculer_trimestre().alias('trimestre')
        )
    )
    df_cta
    return (df_cta,)


@app.cell
def _():
    mo.md(r"""
    ## 📊 Sélection du trimestre et taux CTA
    """)
    return


@app.cell(hide_code=True)
def _(df_cta):
    # Récupérer la liste des trimestres disponibles (triés)
    trimestres_disponibles = sorted(df_cta['trimestre'].unique().to_list())

    # Dropdown pour sélectionner le trimestre
    trimestre_selectionne = mo.ui.dropdown(
        options=trimestres_disponibles,
        value=trimestres_disponibles[-1] if trimestres_disponibles else None,
        label="Sélectionner le trimestre"
    )

    # Input pour le taux de CTA (en %)
    taux_cta = mo.ui.number(
        start=0,
        stop=100,
        step=0.01,
        value=21.93,  # Taux CTA 2025 par défaut
        label="Taux CTA (%)"
    )

    mo.vstack([trimestre_selectionne, taux_cta])
    return taux_cta, trimestre_selectionne


@app.cell(hide_code=True)
def _(df_facturation, df_pdl_odoo, taux_cta, trimestre_selectionne):
    df_detail_cta = pipeline_cta(
        df_facturation=df_facturation,
        df_pdl=df_pdl_odoo,
        taux_cta=taux_cta.value,
        trimestre=trimestre_selectionne.value,
    )

    turpe_fixe_total = df_detail_cta['turpe_fixe_total'].sum()
    cta_total = df_detail_cta['cta'].sum()
    nb_pdl = df_detail_cta['pdl'].n_unique()

    _result = mo.md(f"""
    ## 💰 Résultat CTA - {trimestre_selectionne.value}

    - **Nombre de PDL** : {nb_pdl}
    - **TURPE fixe total** : {turpe_fixe_total:,.2f} €
    - **Taux CTA** : {taux_cta.value} %
    - **CTA à facturer** : **{cta_total:,.2f} €**

    ---

    ### 📋 Détail par PDL
    """)

    mo.vstack([_result, df_detail_cta])
    return


@app.cell
def _():
    mo.md(r"""
    # Accise
    """)
    return


@app.cell
def _(config):
    with OdooReader(config=config) as _odoo:
        df_lignes = commandes_lignes(_odoo).collect()
    return (df_lignes,)


@app.cell
def _(df_lignes):
    # Pipeline complet : agrégation + calcul Accise
    df_accise = pipeline_accise(df_lignes.lazy())
    df_accise
    return (df_accise,)


@app.cell(hide_code=True)
def _(df_accise, trimestre_selectionne):
    # Filtrer sur le trimestre sélectionné
    df_accise_trimestre = df_accise.filter(pl.col('trimestre') == trimestre_selectionne.value)

    # Grouper par taux pour voir la répartition
    df_par_taux = (
        df_accise_trimestre
        .group_by('taux_accise_eur_mwh')
        .agg([
            pl.col('energie_mwh').sum().round(3),
            pl.col('accise_eur').sum().round(2),
            pl.col('pdl').n_unique().alias('nb_pdl')
        ])
        .sort('taux_accise_eur_mwh', descending=True)
    )

    # Totaux
    accise_total = df_accise_trimestre['accise_eur'].sum()
    energie_totale_mwh = df_accise_trimestre['energie_mwh'].sum()
    nb_pdl_total = df_accise_trimestre['pdl'].n_unique()

    _result = mo.md(f"""
    ## ⚡ Résultat Accise - {trimestre_selectionne.value}

    - **Nombre de PDL** : {nb_pdl_total}
    - **Énergie totale** : {energie_totale_mwh:,.2f} MWh
    - **Accise totale** : **{accise_total:,.2f} €**

    ### 📊 Répartition par taux (changements réglementaires)
    """)

    # Détail par PDL et mois
    df_detail_accise = df_accise_trimestre.sort(['pdl', 'mois_consommation'])

    mo.vstack([
        _result,
        mo.md("#### Vue agrégée par taux"),
        df_par_taux,
        mo.md("#### Vue détaillée par PDL et mois"),
        df_detail_accise
    ])
    return


if __name__ == "__main__":
    app.run()
