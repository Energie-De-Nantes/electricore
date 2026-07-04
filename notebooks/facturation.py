import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import logging
    import os
    import sys
    from pathlib import Path

    import httpx
    import marimo as mo
    import polars as pl

    # Ajouter le chemin vers electricore
    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    from datetime import date, timedelta

    from electricore_client import ContractVersionError, IngestionEnCours, PreconditionNonRemplie
    from electricore_client.arrow import ElectricoreArrowClient as ElectricoreClient

    # Configuration du logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    """Configuration Odoo + ElectricoreClient depuis .env"""
    from electricore.config import charger_config_odoo

    try:
        config = charger_config_odoo()
        config_msg = mo.md(
            "**Configuration Odoo chargée**\n\n"
            f"- URL: `{config['url']}`\n- Base: `{config['db']}`\n"
            f"- Utilisateur: `{config['username']}`\n- Mot de passe: `***`"
        )
    except ValueError as e:
        config = {}
        config_msg = mo.md(
            f"⚠️ **Configuration Odoo manquante**\n\n{e}\n\nDéfinissez les variables `ODOO__*` dans `.env`."
        )

    # Client API electricore (cf. ADR-0009 + ADR-0012)
    _api_url = os.getenv("ELECTRICORE_API_URL", "https://electricore.localhost")
    # Fallback : 1re clé de la liste API_KEYS (format serveur : "key1,key2,...")
    _api_key = os.getenv("ELECTRICORE_API_KEY") or os.getenv("API_KEYS", "").split(",")[0].strip()
    # verify=False : Caddy local TLS auto-signé (cf. docs/deploiement.md)
    _http_client = httpx.Client(verify=False, timeout=httpx.Timeout(30.0, read=120.0))
    client = ElectricoreClient(url=_api_url, api_key=_api_key, http_client=_http_client)

    # Vérif API à l'ouverture, même pattern que le bloc Odoo ci-dessus (#571) : URL,
    # clé, et un ping /health (endpoint public) — timeout court pour ne pas bloquer
    # l'ouverture. Le blocage dur reste dans la cellule de chargement (mo.stop).
    if not client.api_key:
        api_msg = mo.md(
            "⚠️ **Clé API electricore manquante**\n\n"
            "`ELECTRICORE_API_KEY` (ou `API_KEYS`) n'est pas définie dans `.env`. "
            "Le chargement des données restera bloqué tant qu'elle manque."
        )
    else:
        try:
            _health = _http_client.get(f"{client.url}/health", timeout=5.0)
            _health.raise_for_status()
            api_msg = mo.md(
                "**API electricore joignable**\n\n"
                f"- URL: `{client.url}`\n"
                f"- Version serveur: `{_health.json().get('version', '?')}`\n"
                f"- Clé API: `***`"
            )
        except Exception:
            api_msg = mo.md(
                f"⚠️ **API electricore injoignable** (`{client.url}`)\n\n"
                "Vérifiez `ELECTRICORE_API_URL` dans `.env` et que le serveur répond, "
                "sinon prévenez Virgile."
            )


@app.cell
def _():
    mo.vstack([config_msg, api_msg])
    return


@app.cell(hide_code=True)
def _():
    # Le serveur calcule le rapprochement Odoo × Enedis et les flags a_facturer /
    # a_supprimer indépendamment de l'état de facturation courant dans Odoo (ADR-0014).
    mo.md(r"""
    # Mois à traiter

    Choisissez ci-dessous le mois à facturer (format AAAA-MM). Par défaut : le dernier
    mois terminé — le mois en cours n'a jamais de données prêtes à facturer.
    """)
    return


@app.cell
def _():
    # Dernier mois révolu : le mois courant n'a jamais de périodes calculées (#179),
    # le proposer par défaut garantissait « 0 lignes » (#554).
    _dernier_mois_revolu = date.today().replace(day=1) - timedelta(days=1)
    mois_input = mo.ui.text(label="Mois (YYYY-MM)", value=_dernier_mois_revolu.strftime("%Y-%m"))
    mois_input
    return (mois_input,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Récupération des données du mois

    Le notebook interroge le serveur electricore pour le mois choisi et ne garde que
    les lignes à facturer ce mois-ci.

    **Si un encadré d'erreur apparaît ci-dessous** : suivez l'action qu'il indique. Les
    étapes suivantes du notebook ne s'exécutent pas tant que le problème n'est pas réglé —
    c'est voulu, pas un bug.
    """)
    return


@app.cell
def _(mois_input):
    # Fail-fast config (#571) : sans clé API, inutile de tenter l'appel réseau —
    # message actionnable immédiat plutôt qu'un 401 différé.
    mo.stop(
        not client.api_key,
        mo.callout(
            mo.md(
                "**Configuration API manquante**\n\n"
                "`ELECTRICORE_API_KEY` (ou `API_KEYS`) n'est pas définie. "
                "Renseignez-la dans `.env` puis relancez ce notebook."
            ),
            kind="danger",
        ),
    )

    _mois = f"{mois_input.value.strip()[:7]}-01"  # YYYY-MM → YYYY-MM-01 (tolère un YYYY-MM-DD saisi par habitude)

    # Zéro traceback pour le facturiste (#571, cf. #554) : chaque échec connu du
    # transport client (X-Error-Kind, #424) devient un message métier + arrêt propre
    # de l'aval (mo.stop empêche les cellules suivantes de tourner sur des données
    # absentes/périmées).
    try:
        fact_mois_brut = client.facturation(mois=_mois)
        # a_facturer / a_supprimer : flags calculés côté serveur (ADR-0014), indépendants
        # de l'état de facturation actuel dans Odoo. lignes_facture_rapprochees = rapprochement
        # Odoo × Enedis × compteur ; on ne garde que ce qui reste à facturer.
        fact_mois = fact_mois_brut.filter(pl.col("a_facturer"))
    except IngestionEnCours:
        mo.stop(
            True,
            mo.callout(mo.md("**Ingestion en cours** — réessayez dans environ 15 minutes."), kind="warn"),
        )
    except PreconditionNonRemplie as e:
        mo.stop(True, mo.callout(mo.md(f"**Précondition non remplie**\n\n{e}"), kind="warn"))
    except ContractVersionError:
        mo.stop(
            True,
            mo.callout(mo.md("**Mise à jour requise** — prévenez Virgile."), kind="danger"),
        )
    except httpx.TransportError:
        mo.stop(
            True,
            mo.callout(
                mo.md(
                    # L'URL tentée rend un défaut localhost inattendu auto-diagnostique
                    # (run direct hors lanceur opérateur, ELECTRICORE_API_URL non définie).
                    f"**Serveur electricore injoignable** (`{client.url}`) — vérifiez `/ingestion` "
                    "sur le bot Telegram, sinon prévenez Virgile."
                ),
                kind="danger",
            ),
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code in (401, 403):
            mo.stop(True, mo.callout(mo.md("**Clé API invalide** — prévenez Virgile."), kind="danger"))
        mo.stop(
            True,
            mo.callout(
                mo.md(f"**Erreur serveur inattendue** (HTTP {e.response.status_code}) — prévenez Virgile."),
                kind="danger",
            ),
        )
    except Exception as e:
        mo.stop(True, mo.callout(mo.md(f"**Erreur inattendue** — prévenez Virgile.\n\n`{e}`"), kind="danger"))

    # Complément #554 : le défaut « dernier mois révolu » évite déjà le cas courant,
    # ce garde-fou couvre le résiduel (mois saisi à la main, ingestion pas encore passée…).
    mo.stop(
        len(fact_mois_brut) == 0,
        mo.callout(
            mo.md(
                f"**Aucune donnée renvoyée par le serveur pour {_mois[:7]}.**\n\n"
                "À vérifier : l'ingestion du mois est-elle passée ? le mois est-il terminé ? "
                "la réconciliation des contrats a-t-elle été faite (notebook *Attribution des "
                "contrats*) ?"
            ),
            kind="warn",
        ),
    )

    mo.md(f"**{len(fact_mois_brut)}** lignes du mois côté serveur · **{len(fact_mois)}** à facturer")
    return (fact_mois,)


@app.cell
def _(fact_mois):
    fact_mois
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Clients ayant changé de contrat en cours de mois

    Un point de livraison peut changer de contrat pendant le mois (déménagement,
    renégociation…). Le tableau ci-dessous les repère : à vérifier avant validation,
    un cas peut demander un traitement à la main.
    """)
    return


@app.cell
def _(fact_mois):
    # Une ligne par invoice_line dans fact_mois : dédoublonner sur (pdl, contrat) pour
    # repérer les PDL à plusieurs situations contractuelles distinctes dans le mois.
    _pdl_rsc = fact_mois.select(["pdl", "ref_situation_contractuelle"]).unique()
    _pdl_counts = _pdl_rsc.group_by("pdl").agg(pl.len().alias("n"))
    pdls_doublons = _pdl_counts.filter(pl.col("n") > 1)["pdl"].to_list()

    fact_déménagements = fact_mois.filter(pl.col("pdl").is_in(pdls_doublons))
    mo.ui.table(fact_déménagements) if pdls_doublons else mo.md("✅ Aucun déménagement détecté")
    return


@app.cell(hide_code=True)
def _():
    # invoice_line_ids / sale_order_id / invoice_ids / x_lisse : identifiants Odoo
    # passe-plat conservés par `rapprocher` (core) — aucune lecture Odoo séparée ici,
    # tout vient déjà de l'API dans fact_mois.
    mo.md(r"""
    # Comparaison avec Odoo

    Les données chargées plus haut contiennent déjà le rapprochement complet entre
    Odoo et Enedis : pas de connexion Odoo séparée à cette étape.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Changements de puissance en cours de mois

    Lignes de facturation dont le PDL a changé de puissance souscrite pendant le mois.
    La quantité Enedis est une puissance moyenne pondérée par le nombre de jours — à vérifier
    avant validation de la facture.
    """)
    return


@app.cell
def _(fact_mois):
    fact_mois.filter(pl.col("memo_puissance") != "")
    return


@app.cell
def _(fact_mois):
    _sans_match = fact_mois.filter(pl.col("quantite_enedis").is_null())
    mo.md(
        f"❌ **{_sans_match['x_pdl'].n_unique()} PDL(s) sans correspondance Enedis**"
    ) if not _sans_match.is_empty() else mo.md("✅ Tous les PDLs ont une correspondance Enedis")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Préparation des quantités → Odoo

    Le tableau ci-dessous liste les lignes de facture dont la quantité va être remplacée
    par la mesure Enedis : `quantite` = valeur actuellement dans Odoo, `quantite_enedis` =
    valeur qui sera écrite. Les abonnements lissés et les lignes sans mesure Enedis ne
    sont pas touchés.

    **À vérifier avant d'injecter** : les écarts importants entre les deux colonnes, et
    les lignes portant une note de changement de puissance (`memo_puissance`).
    """)
    return


@app.cell
def _():
    filtre_a_injecter = pl.col("quantite_enedis").is_not_null() & ~pl.col("x_lisse").fill_null(False)
    return (filtre_a_injecter,)


@app.cell
def _(fact_mois, filtre_a_injecter):
    from electricore.integrations.odoo import OdooWriter

    _a_injecter = fact_mois.filter(filtre_a_injecter)
    _sans_match = fact_mois.filter(pl.col("quantite_enedis").is_null())

    sim_mode = mo.ui.checkbox(label="Mode simulation (aucune écriture réelle)", value=True)
    run_button = mo.ui.run_button(label="Injecter dans Odoo")

    mo.vstack(
        [
            mo.md(
                f"**{len(_a_injecter)}** lignes à mettre à jour "
                f"· **{len(_sans_match)}** sans correspondance Enedis (ignorées)"
            ),
            mo.ui.table(
                _a_injecter.select(
                    [
                        "name_account_move",
                        "x_pdl",
                        "categorie_produit",
                        "name_product_product",
                        "quantite",
                        "quantite_enedis",
                        "memo_puissance",
                    ]
                )
            ),
            sim_mode,
            run_button,
        ]
    )
    return OdooWriter, run_button, sim_mode


@app.cell
def _(fact_mois, filtre_a_injecter):
    lines_records = (
        fact_mois.filter(filtre_a_injecter)
        .select(
            [
                pl.col("invoice_line_ids").cast(pl.Int64).alias("id"),
                pl.col("quantite_enedis").alias("quantity"),
            ]
        )
        .to_dicts()
    )
    return (lines_records,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Préparation statut abonnement

    Chaque commande d'abonnement reçoit un statut pour le circuit de validation :

    - **draft** — le mois n'est pas calculable pour ce point de livraison : la commande
      est à reprendre à la main ;
    - **populated** — échantillon de contrôle tiré au hasard, à relire avant validation
      (le curseur ci-dessous règle la proportion) ;
    - **checked** — prêt à facturer.

    **Quand s'inquiéter** : un nombre de *draft* inhabituellement élevé signale un
    problème de données en amont (ingestion, relevés manquants) — dans ce cas, ne pas
    injecter et prévenir Virgile.
    """)
    return


@app.cell
def _():
    taux_verification = mo.ui.slider(
        0,
        20,
        value=5,
        step=1,
        label="% d'orders à vérifier (→ populated)",
        show_value=True,
    )
    taux_verification
    return (taux_verification,)


@app.cell
def _(fact_mois, taux_verification):
    import numpy as np

    # fact_mois porte déjà sale_order_id / x_lisse / qualite / ref_situation_contractuelle
    # (passe-plat de `rapprocher`) → 1 ligne par order après déduplication. L'ancienne
    # lecture Odoo directe + jointure sur la RSC était redondante (#417).
    # « à jour » ≡ énergie calculable (ADR-0033 : ancien data_complete=True ⇒ qualite ≠ incalculable).
    _orders = fact_mois.select(["sale_order_id", "x_lisse", "qualite", "ref_situation_contractuelle"]).unique()

    _df = _orders.with_columns(
        ((pl.col("qualite") != "incalculable") | pl.col("x_lisse")).alias("a_jour")
    ).with_columns(pl.Series("rand", np.random.rand(len(_orders))))

    orders_records = (
        _df.with_columns(
            pl.when(~pl.col("a_jour"))
            .then(pl.lit("draft"))
            .when(pl.col("rand") < taux_verification.value / 100)
            .then(pl.lit("populated"))
            .otherwise(pl.lit("checked"))
            .alias("x_invoicing_state")
        )
        .select(["sale_order_id", "x_invoicing_state"])
        .rename({"sale_order_id": "id"})
        .to_dicts()
    )

    # Schéma explicite : préserve les colonnes même si la liste est vide
    # (cas légitime hors période de facturation, cf. ADR-0014).
    _preview = pl.DataFrame(orders_records, schema={"id": pl.Int64, "x_invoicing_state": pl.Utf8})
    mo.vstack(
        [
            mo.md(
                f"**{_preview.filter(pl.col('x_invoicing_state') == 'draft')['id'].len()}** draft · "
                f"**{_preview.filter(pl.col('x_invoicing_state') == 'populated')['id'].len()}** populated · "
                f"**{_preview.filter(pl.col('x_invoicing_state') == 'checked')['id'].len()}** checked"
            ),
            mo.ui.table(_preview),
        ]
    )
    return (orders_records,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Préparation données factures

    Chaque facture du mois reçoit ses informations d'en-tête : la période facturée
    (début / fin), le montant d'acheminement réseau (TURPE, part fixe + part variable)
    et le compteur (type et numéro de série). Ces champs alimentent les mentions
    obligatoires de la facture — l'aperçu s'affiche ci-dessous, une ligne par facture.
    """)
    return


@app.cell
def _(fact_mois):
    # fact_mois porte déjà invoice_ids + debut/fin/turpe/compteur par invoice_line
    # (passe-plat de `rapprocher`). Déduplication sur (invoice_ids × RSC) → 1 ligne par
    # facture ; l'ancienne lecture Odoo directe + jointure sur la RSC était redondante (#417).
    _to_rename = {
        "invoice_ids": "id",
        "turpe": "x_turpe",
        "debut": "x_start_invoice_period",
        "fin": "x_end_invoice_period",
        "type_compteur": "x_type_compteur",
        "num_compteur": "x_num_serie_compteur",
    }

    _df = (
        fact_mois.select(
            [
                "invoice_ids",
                "ref_situation_contractuelle",
                "debut",
                "fin",
                "turpe_fixe_eur",
                "turpe_variable_eur",
                "num_compteur",
                "type_compteur",
            ]
        )
        .unique()
        .with_columns(
            [
                (pl.col("turpe_fixe_eur") + pl.col("turpe_variable_eur")).alias("turpe"),
                pl.col("debut").dt.strftime("%Y-%m-%d"),
                pl.col("fin").dt.strftime("%Y-%m-%d"),
            ]
        )
        .drop(["turpe_fixe_eur", "turpe_variable_eur", "ref_situation_contractuelle"])
        .rename(mapping=_to_rename)
    )
    invoices_records = _df.to_dicts()
    _df
    return (invoices_records,)


@app.cell
def _(
    OdooWriter,
    invoices_records,
    lines_records,
    orders_records,
    run_button,
    sim_mode,
):
    mo.stop(not run_button.value, mo.md("Vérifiez les données ci-dessus puis cliquez sur **Injecter**."))

    # Un échec par record n'interrompt plus la boucle : chaque update() rapporte ce
    # qui a réussi / échoué / a été ignoré (sans id) au lieu de stopper net (#571).
    with OdooWriter(config=config, sim=sim_mode.value) as _writer:
        _rapport_lignes = _writer.update("account.move.line", lines_records)
        _rapport_factures = _writer.update("account.move", invoices_records)
        _rapport_orders = _writer.update("sale.order", orders_records)

    _mode_label = "**simulation** (aucune écriture réelle)" if sim_mode.value else "**écriture réelle** dans Odoo"

    def _resume(nom, rapport):
        texte = f"- **{nom}** : {len(rapport.ids_reussis)} OK"
        if rapport.echecs:
            details = ", ".join(f"#{e.id} ({e.raison})" for e in rapport.echecs)
            texte += f", **{len(rapport.echecs)} échec(s)** → {details}"
        if rapport.sans_id:
            texte += f", {len(rapport.sans_id)} ignoré(s) (sans id)"
        return texte

    _rapports = (
        ("Lignes de facture", _rapport_lignes),
        ("Factures", _rapport_factures),
        ("Commandes", _rapport_orders),
    )
    _a_des_echecs = any(rapport.echecs for _, rapport in _rapports)

    mo.callout(
        mo.md(
            f"Injection en {_mode_label}.\n\n"
            + "\n".join(_resume(nom, rapport) for nom, rapport in _rapports)
            + "\n\n**Relancer cette étape ne risque rien** : les écritures sont idempotentes "
            "(mêmes valeurs réappliquées sur les mêmes lignes). Si des échecs apparaissent "
            "ci-dessus, corrigez la cause côté Odoo puis relancez l'injection."
        ),
        kind="danger" if _a_des_echecs else "success",
    )
    return


if __name__ == "__main__":
    app.run()
