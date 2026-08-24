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
def _():
    # Widgets de filtre Relevés — cell À PART, hors de toute fonction paresseuse
    # (#721, bug d'inertie). `mo.lazy` n'appelle `onglet_releves` qu'une fois par
    # ouverture d'onglet et ne retient pas l'objet résolu ; des widgets créés
    # DEDANS seraient donc des candidats GC, invisibles aux changements ultérieurs
    # (`UIElementRegistry` ne les garde qu'en weakref). Déclarés ici, ce sont des
    # cellules réactives normales : la cellule des onglets (plus bas) en dépend,
    # donc tout changement de filtre la refait tourner et redéclenche le fetch de
    # l'onglet ouvert. `_debut_defaut`/`_fin_defaut` préfixés `_` : privés à la
    # cellule (convention marimo), ils n'ont pas besoin d'exister pour l'onglet.
    _debut_defaut, _fin_defaut = helpers.fenetre_dernier_mois()
    pdl_releves = mo.ui.text(label="PDL (optionnel)")
    debut = mo.ui.date(value=_debut_defaut, label="Depuis")
    fin = mo.ui.date(value=_fin_defaut, label="Jusqu'à")
    return pdl_releves, debut, fin


@app.cell
def _():
    # Widgets de filtre Flux bruts — même raison qu'au-dessus.
    table_flux = mo.ui.dropdown(TABLES_FLUX, value=TABLES_FLUX[0], label="Table de flux")
    pdl_flux = mo.ui.text(label="PDL (optionnel)")
    return table_flux, pdl_flux


@app.cell
def _():
    # Onglet ouvert gardé en `mo.state` (#721) : la cellule des onglets est
    # reconstruite à chaque changement de filtre, et un `mo.ui.tabs` reconstruit
    # repart sur son premier onglet — sans ça, toucher un filtre Relevés
    # renverrait le visiteur sur Facturation. `allow_self_loops=False` (défaut) :
    # cliquer un onglet ne refait PAS tourner la cellule des onglets, donc pas
    # de boucle ni de re-fetch au simple changement d'onglet.
    onglet_ouvert, choisir_onglet = mo.state("Facturation")
    return onglet_ouvert, choisir_onglet


@app.cell
def _(cle):
    # Onglet Facturation : comportement inchangé (#705, non-régression) — calculé
    # ici (pas dans un onglet paresseux) pour que clé refusée / KIOSQUE__API_URL
    # manquante restent des erreurs immédiates, avant même d'afficher les onglets.
    # Le garde « pas de clé » vit ICI, dans le même cell que le fetch : `mo.stop`
    # ne coupe que les *descendants* par flux de données, et un cell qui ne
    # définit rien n'en a aucun — isolé, il laisserait partir un fetch à vide.
    if not cle.value:
        mo.stop(True, mo.md("_En attente d'une clé API…_"))

    try:
        lignes_facturation = helpers.recuperer_meta_periodes(cle.value)
    except (helpers.CleApiRefusee, config.ApiUrlManquante) as exc:
        mo.stop(True, mo.md(f"⚠️ **{exc}**"))
    onglet_facturation = mo.ui.table(lignes_facturation, label="Facturation mensuelle")
    return (onglet_facturation,)


@app.cell
def _(
    cle,
    onglet_facturation,
    pdl_releves,
    debut,
    fin,
    table_flux,
    pdl_flux,
    onglet_ouvert,
    choisir_onglet,
):
    # Fonctions (pas des valeurs déjà calculées) : `mo.ui.tabs(..., lazy=True)`
    # ne fetch qu'à l'ouverture de l'onglet — jamais Relevés/Flux bruts au
    # chargement de la page. Elles ne FONT que fetch + rendre, en fermant sur les
    # `.value` des widgets déclarés au-dessus (jamais recréés ici) — c'est ce qui
    # rend la cellule des onglets dépendante des widgets, donc réactive à leurs
    # changements (voir cellules précédentes). Exposées via `return` pour rester
    # testables unitairement (voir `test_exports.py`).
    def onglet_releves():
        try:
            lignes, tronque = helpers.recuperer_releves(
                cle.value,
                prm=pdl_releves.value or None,
                debut=str(debut.value),
                fin=str(fin.value),
            )
        except (helpers.CleApiRefusee, config.ApiUrlManquante) as exc:
            return mo.vstack([pdl_releves, debut, fin, mo.md(f"⚠️ **{exc}**")])

        elements = [pdl_releves, debut, fin]
        if tronque:
            elements.append(mo.callout("Vue tronquée : resserre tes filtres pour tout voir.", kind="warn"))
        elements.append(mo.ui.table(lignes, label="Relevés"))
        return mo.vstack(elements)

    def onglet_flux_bruts():
        avertissement = mo.md(
            "⚠️ Données brutes fidèles à la source, conventions Enedis — pour des "
            "relevés harmonisés, onglet **Relevés**."
        )

        try:
            lignes, tronque = helpers.recuperer_flux(cle.value, table_flux.value, prm=pdl_flux.value or None)
        except (helpers.CleApiRefusee, helpers.TableFluxAbsente, config.ApiUrlManquante) as exc:
            return mo.vstack([table_flux, pdl_flux, avertissement, mo.md(f"⚠️ **{exc}**")])

        elements = [table_flux, pdl_flux, avertissement]
        if tronque:
            elements.append(mo.callout("Vue tronquée : resserre tes filtres pour tout voir.", kind="warn"))
        elements.append(mo.ui.table(lignes, label=f"Flux {table_flux.value}"))
        return mo.vstack(elements)

    mo.output.replace(
        mo.ui.tabs(
            {
                "Facturation": onglet_facturation,
                "Relevés": onglet_releves,
                "Flux bruts": onglet_flux_bruts,
            },
            value=onglet_ouvert(),
            lazy=True,
            on_change=choisir_onglet,
        )
    )
    return onglet_releves, onglet_flux_bruts


if __name__ == "__main__":
    app.run()
