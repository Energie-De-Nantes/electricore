import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo


@app.cell(hide_code=True)
def _():
    mo.md(
        "## Notebooks opérateur\n\n"
        "- [Facturation mensuelle](/apps/facturation)\n"
        "- [Injection RSC](/apps/injection_rsc)\n"
    )
    return


if __name__ == "__main__":
    app.run()
