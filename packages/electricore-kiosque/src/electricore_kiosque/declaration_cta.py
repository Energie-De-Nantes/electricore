import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    from electricore_kiosque import config, helpers

    # Présentation pure (#708) : CTA, Par taux et empreintes viennent de l'API
    # (ADR-0058). Les seules opérations ici sont un filtre par trimestre et deux
    # sommes d'affichage ; le fichier téléchargé est l'XLSX servi par l'API.
    ERREURS_AFFICHABLES = (helpers.CleApiRefusee, helpers.ServiceIndisponible, config.ApiUrlManquante)


@app.cell(hide_code=True)
def _():
    mo.md(
        "## Déclaration CTA\n\n"
        "Colle ta clé API : la CTA de chaque **trimestre terminé**, sur tout le périmètre "
        "Enedis. Note l'**empreinte** avec ta déclaration — si elle change en "
        "retéléchargeant plus tard, un montant ou une ligne a bougé depuis."
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
        lignes = helpers.recuperer_declaration_cta(cle.value)
    except ERREURS_AFFICHABLES as exc:
        mo.stop(True, mo.md(f"⚠️ **{exc}**"))
    trimestres = sorted({ligne["trimestre"] for ligne in lignes}, reverse=True)
    if not trimestres:
        mo.stop(True, mo.md("_Aucun trimestre terminé à déclarer pour l'instant._"))
    # Widget lié à un nom global de cellule (jamais créé dans une fonction paresseuse) :
    # il reste au registre et la cellule d'affichage réagit à son changement (#721).
    trimestre = mo.ui.dropdown(trimestres, value=trimestres[0], label="Trimestre")
    return lignes, trimestre


@app.cell
def _(cle, lignes, trimestre):
    du_trimestre = [ligne for ligne in lignes if ligne["trimestre"] == trimestre.value]
    premiere = du_trimestre[0]
    resume = mo.md(
        f"**CTA {trimestre.value} : {sum(ligne['cta_eur'] for ligne in du_trimestre):,.2f} €** "
        f"(assiette TURPE fixe {sum(ligne['turpe_fixe_eur'] for ligne in du_trimestre):,.2f} €, "
        f"{len({ligne['pdl'] for ligne in du_trimestre})} PDL)\n\n"
        f"Empreinte du trimestre : `{premiere['empreinte_trimestre']}` "
        f"(version {premiere['version_empreinte']})"
    )
    try:
        telechargement = mo.download(
            helpers.telecharger_declaration_cta(cle.value, trimestre.value),
            filename=f"declaration_cta_{trimestre.value}.xlsx",
            label="⬇️ Télécharger la déclaration (XLSX)",
        )
    except ERREURS_AFFICHABLES as exc:
        telechargement = mo.md(f"⚠️ **{exc}**")
    mo.vstack([trimestre, resume, telechargement, mo.ui.table(du_trimestre, label="Détail RSC × mois")])
    return


if __name__ == "__main__":
    app.run()
