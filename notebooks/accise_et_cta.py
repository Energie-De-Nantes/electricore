import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import marimo as mo
    import polars as pl
    import pandas as pd
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
        lignes_factures
    )


@app.cell(hide_code=True)
def _():
    """Configuration Odoo depuis secrets.toml"""
    import tomllib

    # Chercher le fichier secrets.toml
    secrets_paths = [
        Path.cwd() / '.dlt' / 'secrets.toml',
        Path.cwd() / 'electricore' / 'etl' / '.dlt' / 'secrets.toml'
    ]

    config = {}
    secrets_file_found = None

    for secrets_path in secrets_paths:
        if secrets_path.exists():
            with open(secrets_path, 'rb') as f:
                config_data = tomllib.load(f)
                config = config_data.get('odoo', {})
                secrets_file_found = secrets_path
            break

    if not config:
        _msg = mo.md("""
        ⚠️ **Configuration Odoo non trouvée**

        Créez le fichier `.dlt/secrets.toml` ou `electricore/etl/.dlt/secrets.toml` avec :
        ```toml
        [odoo]
        url = "https://votre-instance.odoo.com"
        db = "votre_database"
        username = "votre_username"
        password = "votre_password"
        ```
        """)
    else:
        _msg = mo.md(f"""
        **Configuration chargée depuis**: `{secrets_file_found}`

        - URL: `{config.get('url', 'NON CONFIGURÉ')}`
        - Base: `{config.get('db', 'NON CONFIGURÉ')}`
        - Utilisateur: `{config.get('username', 'NON CONFIGURÉ')}`
        - Mot de passe: `{'***' if config.get('password') else 'NON CONFIGURÉ'}`
        """)
    _msg
    return (config,)


@app.cell
def _():
    mo.md(r"""# CTA""")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Récupération du périmètre PDL depuis Odoo""")
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
    mo.md(r"""## Chargement des données pour le pipeline de calcul""")
    return


@app.cell
def _():
    lf_historique = c15().lazy()
    lf_releves = releves_harmonises().lazy()
    return lf_historique, lf_releves


@app.cell
def _():
    mo.md(r"""## Pipeline facturation""")
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
    mo.md(r"""## 📊 Sélection du trimestre et taux CTA""")
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
def _(df_cta, taux_cta, trimestre_selectionne):
    # Filtrer sur le trimestre sélectionné
    df_trimestre = df_cta.filter(pl.col('trimestre') == trimestre_selectionne.value)

    # Calculer la somme du TURPE fixe
    turpe_fixe_total = df_trimestre['turpe_fixe_eur'].sum()

    # Calculer la CTA
    cta_total = turpe_fixe_total * (taux_cta.value / 100)

    # Nombre de PDL concernés
    nb_pdl = df_trimestre['pdl'].n_unique()

    _result = mo.md(f"""
    ## 💰 Résultat CTA - {trimestre_selectionne.value}

    - **Nombre de PDL** : {nb_pdl}
    - **TURPE fixe total** : {turpe_fixe_total:,.2f} €
    - **Taux CTA** : {taux_cta.value} %
    - **CTA à facturer** : **{cta_total:,.2f} €**

    ---

    ### 📋 Détail par PDL
    """)

    # Afficher le détail par PDL
    df_detail = (
        df_trimestre
        .group_by('pdl')
        .agg([
            pl.col('turpe_fixe_eur').sum().alias('turpe_fixe_total'),
            pl.col('order_name').first()
        ])
        .with_columns(
            (pl.col('turpe_fixe_total') * (taux_cta.value / 100)).alias('cta')
        )
        .sort('cta', descending=True)
    )

    mo.vstack([_result, df_detail])
    return


@app.cell
def _():
    mo.md(r"""# Accise""")
    return


@app.cell
def _(config):
    with OdooReader(config=config) as _odoo:
        df_factures = lignes_factures(_odoo).collect()
    df_factures
    return (df_factures,)


@app.cell
def _(df_factures):
    df_conso = (
        df_factures
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
