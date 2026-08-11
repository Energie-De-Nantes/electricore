import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    from electricore_kiosque import config


@app.cell(hide_code=True)
def _():
    noms = sorted(config.apps_actives())
    liens = "\n".join(f"- [{nom}](/{nom})" for nom in noms) if noms else "_Aucune app active._"
    mo.md(f"## Kiosque {config.titre()}\n\n{liens}\n")
    return


if __name__ == "__main__":
    app.run()
