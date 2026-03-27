import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import marimo as mo
    import polars as pl
    import plotly.express as px
    import sys
    from pathlib import Path

    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    from electricore.core.loaders import c15, releves_harmonises
    from electricore.core.pipelines.orchestration import facturation


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Pipeline Facturation

    Démonstration du pipeline de facturation Polars :
    **C15 + relevés → périmètre enrichi → abonnements → énergie → méta-périodes de facturation**
    """)
    return


@app.cell
def _():
    lf_historique = c15().lazy()
    lf_releves = releves_harmonises().lazy()
    return lf_historique, lf_releves


@app.cell
def _(lf_historique, lf_releves):
    hist, abo, energie, fact = facturation(
        historique=lf_historique,
        releves=lf_releves,
    )
    return abo, energie, fact, hist


@app.cell(hide_code=True)
def _(abo, energie, fact, hist):
    mo.md(f"""
    ## Résumé

    | Étape | Lignes |
    |---|---|
    | Historique périmètre enrichi | **{hist.collect().height}** |
    | Périodes d'abonnement | **{abo.collect().height}** |
    | Périodes d'énergie | **{energie.collect().height}** |
    | Méta-périodes de facturation | **{fact.height}** |
    """)
    return


@app.cell(hide_code=True)
def _(fact):
    mo.vstack([
        mo.md("## Méta-périodes de facturation"),
        mo.ui.table(fact),
    ])
    return


@app.cell(hide_code=True)
def _(fact):
    _df = fact.collect()
    fig = px.histogram(
        _df,
        x="puissance_souscrite_kva",
        nbins=20,
        title="Distribution des puissances souscrites (méta-périodes)",
        labels={"puissance_souscrite_kva": "Puissance souscrite (kVA)"},
    )
    fig.update_layout(height=400)
    fig
    return


@app.cell(hide_code=True)
def _(abo):
    mo.vstack([
        mo.md("## Périodes d'abonnement"),
        mo.ui.table(abo.collect()),
    ])
    return


@app.cell(hide_code=True)
def _(energie):
    mo.vstack([
        mo.md("## Périodes d'énergie"),
        mo.ui.table(energie.collect()),
    ])
    return


if __name__ == "__main__":
    app.run()
