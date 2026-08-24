import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    from electricore_kiosque import config, helpers

    # Dropdown en dur (#720) : tables du registre flux DuckDB
    # (`electricore.core.loaders.duckdb.registry.FLUX_DESCRIPTORS`) — le kiosque
    # n'a pas accès à la base pour les découvrir dynamiquement. Une table absente
    # de cette box précise reste un message propre (`TableFluxAbsente`, 404 API).
    TABLES_FLUX = ["c15", "r151", "r15", "f15_detail", "f12_detail", "r64"]


@app.cell(hide_code=True)
def _():
    mo.md(
        "## Exports\n\n"
        "Colle ta clé API : **Facturation** mensuelle, **Relevés** harmonisés et "
        "**Flux bruts** Enedis, chacun dans son onglet — tableau filtrable, "
        "téléchargeable en CSV (bouton en haut du tableau)."
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
    return


@app.cell
def _(cle):
    # Onglet Facturation : comportement inchangé (#705, non-régression) — calculé
    # ici (pas dans un onglet paresseux) pour que clé refusée / KIOSQUE__API_URL
    # manquante restent des erreurs immédiates, avant même d'afficher les onglets.
    try:
        lignes_facturation = helpers.recuperer_meta_periodes(cle.value)
    except (helpers.CleApiRefusee, config.ApiUrlManquante) as exc:
        mo.stop(True, mo.md(f"⚠️ **{exc}**"))
    onglet_facturation = mo.ui.table(lignes_facturation, label="Facturation mensuelle")
    return (onglet_facturation,)


@app.cell
def _(cle, onglet_facturation):
    # Fonctions (pas des valeurs déjà calculées) : `mo.ui.tabs(..., lazy=True)`
    # ne fetch qu'à l'ouverture de l'onglet — jamais Relevés/Flux bruts au
    # chargement de la page. Exposées via `return` (pas seulement fermées sur
    # `cle`) pour rester testables unitairement (voir `test_exports.py`).
    def onglet_releves():
        debut_defaut, fin_defaut = helpers.fenetre_dernier_mois()
        pdl = mo.ui.text(label="PDL (optionnel)")
        debut = mo.ui.date(value=debut_defaut, label="Depuis")
        fin = mo.ui.date(value=fin_defaut, label="Jusqu'à")

        try:
            lignes, tronque = helpers.recuperer_releves(
                cle.value,
                prm=pdl.value or None,
                debut=str(debut.value),
                fin=str(fin.value),
            )
        except (helpers.CleApiRefusee, config.ApiUrlManquante) as exc:
            return mo.vstack([pdl, debut, fin, mo.md(f"⚠️ **{exc}**")])

        elements = [pdl, debut, fin]
        if tronque:
            elements.append(mo.callout("Vue tronquée : resserre tes filtres pour tout voir.", kind="warn"))
        elements.append(mo.ui.table(lignes, label="Relevés"))
        return mo.vstack(elements)

    def onglet_flux_bruts():
        table = mo.ui.dropdown(TABLES_FLUX, value=TABLES_FLUX[0], label="Table de flux")
        pdl = mo.ui.text(label="PDL (optionnel)")
        avertissement = mo.md(
            "⚠️ Données brutes fidèles à la source, conventions Enedis — pour des "
            "relevés harmonisés, onglet **Relevés**."
        )

        try:
            lignes = helpers.recuperer_flux(cle.value, table.value, prm=pdl.value or None)
        except (helpers.CleApiRefusee, helpers.TableFluxAbsente, config.ApiUrlManquante) as exc:
            return mo.vstack([table, pdl, avertissement, mo.md(f"⚠️ **{exc}**")])

        return mo.vstack([table, pdl, avertissement, mo.ui.table(lignes, label=f"Flux {table.value}")])

    mo.output.replace(
        mo.ui.tabs(
            {
                "Facturation": onglet_facturation,
                "Relevés": onglet_releves,
                "Flux bruts": onglet_flux_bruts,
            },
            lazy=True,
        )
    )
    return onglet_releves, onglet_flux_bruts


if __name__ == "__main__":
    app.run()
