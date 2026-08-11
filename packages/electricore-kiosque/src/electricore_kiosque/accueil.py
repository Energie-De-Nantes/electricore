import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup:
    import os

    import marimo as mo


@app.cell(hide_code=True)
def _():
    titre = os.environ.get("KIOSQUE__TITRE", "ElectriCore")
    noms = sorted(n.strip() for n in os.environ.get("KIOSQUE__APPS", "").split(",") if n.strip())
    liens = "\n".join(f"- [{nom}](/{nom})" for nom in noms) if noms else "_Aucune app active._"
    mo.md(f"## Kiosque {titre}\n\n{liens}\n")
    return


if __name__ == "__main__":
    app.run()
