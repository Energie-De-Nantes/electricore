import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    from electricore_kiosque import config, helpers


@app.cell(hide_code=True)
def _():
    mo.md(
        "## Exports\n\n"
        "Colle ta clé API pour consulter ta facturation mensuelle : tableau "
        "filtrable, téléchargeable en CSV (bouton en haut du tableau)."
    )
    return


@app.cell
def _():
    cle = mo.ui.text(label="Clé API", kind="password")
    mo.output.replace(cle)
    return (cle,)


@app.cell
def _(cle):
    if not cle.value:
        mo.stop(True, mo.md("_En attente d'une clé API…_"))

    try:
        client = helpers.construire_client(cle.value)
        lignes = helpers.recuperer_meta_periodes(client)
    except (helpers.CleApiRefusee, config.ApiUrlManquante) as exc:
        mo.stop(True, mo.md(f"⚠️ **{exc}**"))
    return (lignes,)


@app.cell
def _(lignes):
    mo.output.replace(mo.ui.table(lignes, label="Facturation mensuelle"))
    return


if __name__ == "__main__":
    app.run()
