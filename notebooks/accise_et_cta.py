import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import os
    import sys
    from pathlib import Path

    import httpx
    import marimo as mo
    import polars as pl

    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    from electricore_client.arrow import ElectricoreArrowClient as ElectricoreClient

    # Client HTTP vers l'API electricore (cf. ADR-0009 — les notebooks
    # consomment l'API, pas la base DuckDB locale).
    _api_url = os.getenv("ELECTRICORE_API_URL", "https://electricore.localhost")
    _api_key = os.getenv("ELECTRICORE_API_KEY") or os.getenv("API_KEYS", "").split(",")[0].strip()
    _http_client = httpx.Client(verify=False, timeout=httpx.Timeout(30.0, read=300.0))
    client = ElectricoreClient(url=_api_url, api_key=_api_key, http_client=_http_client)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Accise & CTA — calcul trimestriel

    Les calculs sont délégués aux endpoints `/taxes/cta/detail.arrow` et `/taxes/accise/detail.arrow`
    de l'API electricore (cf. ADR-0009). Le notebook se contente de récupérer les
    DataFrames, de sélectionner un trimestre et d'afficher les agrégats.

    **Note** — depuis v1.7, les PDLs sans `sale.order` Odoo ne sont plus inclus
    dans la CTA. Si le périmètre de calcul doit dépasser le portefeuille Odoo,
    un endpoint dédié sera nécessaire.
    """)
    return


@app.cell(hide_code=True)
def _():
    """Charge les CTA mensuelles (tous trimestres) et l'accise détaillée."""
    df_cta_all = client.cta()
    df_accise_all = client.accise()
    mo.md(f"""
    - CTA chargée : **{len(df_cta_all)} lignes mensuelles**, **{df_cta_all["pdl"].n_unique()} PDLs**
    - Accise chargée : **{len(df_accise_all)} lignes mensuelles**, **{df_accise_all["pdl"].n_unique()} PDLs**
    """)
    return df_accise_all, df_cta_all


@app.cell(hide_code=True)
def _(df_accise_all, df_cta_all):
    """Sélecteur de trimestre — union des trimestres présents dans les deux flux."""
    trimestres_dispo = sorted(
        set(df_cta_all["trimestre"].unique().to_list()) | set(df_accise_all["trimestre"].unique().to_list())
    )
    trimestre_selectionne = mo.ui.dropdown(
        options=trimestres_dispo,
        value=trimestres_dispo[-1] if trimestres_dispo else None,
        label="Sélectionner le trimestre",
    )
    trimestre_selectionne
    return (trimestre_selectionne,)


@app.cell(hide_code=True)
def _(df_cta_all, trimestre_selectionne):
    """Vue CTA agrégée par PDL pour le trimestre sélectionné."""
    df_cta_trimestre = df_cta_all.filter(pl.col("trimestre") == trimestre_selectionne.value)

    df_detail_cta = (
        df_cta_trimestre.lazy()
        .group_by("pdl")
        .agg(
            [
                pl.col("order_name").first(),
                pl.col("turpe_fixe_eur").sum().round(2).alias("turpe_fixe_total"),
                pl.col("cta_eur").sum().round(2).alias("cta"),
                pl.col("taux_cta_pct").unique().sort().alias("taux_cta_appliques"),
            ]
        )
        .sort("cta", descending=True)
        .collect()
    )

    turpe_fixe_total = df_detail_cta["turpe_fixe_total"].sum()
    cta_total = df_detail_cta["cta"].sum()
    nb_pdl = df_detail_cta["pdl"].n_unique()
    taux_distincts = sorted({t for lst in df_detail_cta["taux_cta_appliques"].to_list() for t in lst})
    taux_str = " + ".join(f"{t:.2f} %" for t in taux_distincts) if taux_distincts else "—"

    mo.vstack(
        [
            mo.md(f"""
        ## 💰 CTA — {trimestre_selectionne.value}

        - **Nombre de PDL** : {nb_pdl}
        - **TURPE fixe total** : {turpe_fixe_total:,.2f} €
        - **Taux CTA appliqués** : {taux_str}
        - **CTA à facturer** : **{cta_total:,.2f} €**

        ### 📋 Détail par PDL
        """),
            df_detail_cta,
        ]
    )
    return


@app.cell(hide_code=True)
def _(df_accise_all, trimestre_selectionne):
    """Vue Accise par taux + détail pour le trimestre sélectionné."""
    df_accise_trimestre = df_accise_all.filter(pl.col("trimestre") == trimestre_selectionne.value)

    df_par_taux = (
        df_accise_trimestre.group_by("taux_accise_eur_mwh")
        .agg(
            [
                pl.col("energie_mwh").sum().round(3),
                pl.col("accise_eur").sum().round(2),
                pl.col("pdl").n_unique().alias("nb_pdl"),
            ]
        )
        .sort("taux_accise_eur_mwh", descending=True)
    )

    accise_total = df_accise_trimestre["accise_eur"].sum()
    energie_totale = df_accise_trimestre["energie_mwh"].sum()
    nb_pdl_total = df_accise_trimestre["pdl"].n_unique()
    df_detail_accise = df_accise_trimestre.sort(["pdl", "mois_consommation"])

    mo.vstack(
        [
            mo.md(f"""
        ## ⚡ Accise — {trimestre_selectionne.value}

        - **Nombre de PDL** : {nb_pdl_total}
        - **Énergie totale** : {energie_totale:,.2f} MWh
        - **Accise totale** : **{accise_total:,.2f} €**
        """),
            mo.md("#### Vue agrégée par taux"),
            df_par_taux,
            mo.md("#### Vue détaillée par PDL et mois"),
            df_detail_accise,
        ]
    )
    return


if __name__ == "__main__":
    app.run()
