import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo


@app.cell(hide_code=True)
def _():
    mo.md(
        "## Notebooks opérateur\n\n"
        "Suis l'ordre d'usage :\n\n"
        "1. [Injection RSC](/apps/injection_rsc) — réconcilie les RSC du mois\n"
        "2. [Facturation mensuelle](/apps/facturation)\n\n"
        "La facturation **dépend** de la réconciliation RSC du mois : lance d'abord "
        "l'injection RSC, puis la facturation.\n"
    )
    return


if __name__ == "__main__":
    app.run()
