import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from pathlib import Path
    import pandas as pd
    import numpy as np

    # ElectriFlux - Extract
    from electriflux.simple_reader import process_flux

    # ElectriCore - Transform
    from electricore.inputs.flux import lire_flux_c15, lire_flux_r151

    # ElectriCore - Process : Pipeline pur + orchestration moderne
    from electricore.core.pipeline_abonnements import pipeline_abonnement, generer_periodes_abonnement
    from electricore.core.orchestration import calculer_abonnements
    from electricore.core.utils.formatage import formater_date_francais

    # Debugging
    from icecream import ic
    return (
        Path,
        formater_date_francais,
        lire_flux_c15,
        lire_flux_r151,
        mo,
        pd,
        pipeline_abonnement,
        process_flux,
    )


@app.cell(hide_code=True)
def _(Path, mo):
    # Configuration des chemins de données
    data_path = Path('~/data/flux_enedis').expanduser()
    c15_path = data_path / 'C15'
    r151_path = data_path / 'R151'

    _status_message = mo.md(f"""
    ## Configuration des chemins

    - **Répertoire principal**: `{data_path}`
    - **Flux C15**: `{c15_path}` {'✅' if c15_path.exists() else '❌ (non trouvé)'}
    - **Flux R151**: `{r151_path}` {'✅' if r151_path.exists() else '❌ (non trouvé)'}
    """)

    _status_message
    return c15_path, r151_path


@app.cell(hide_code=True)
def _(c15_path, mo, process_flux, r151_path):
    # Étape 1: Extract - Chargement des données brutes avec ElectriFlux
    raw_c15, raw_r151, _extract_status = None, None, None

    try:
        raw_c15 = process_flux('C15', c15_path)
        raw_r151 = process_flux('R151', r151_path)
        _extract_status = mo.md(f"""
        ## 📁 **Extract - Données brutes chargées**

        - **C15 (Contrats)**: {len(raw_c15)} lignes, {len(raw_c15.columns)} colonnes
        - **R151 (Relevés)**: {len(raw_r151)} lignes, {len(raw_r151.columns)} colonnes
        """)
    except Exception as e:
        _extract_status = mo.md(f"❌ **Erreur lors du chargement**: {str(e)}")

    _extract_status
    return raw_c15, raw_r151


@app.cell(hide_code=True)
def _(lire_flux_c15, lire_flux_r151, mo, raw_c15, raw_r151):
    # Étape 2: Transform - Conversion vers les modèles Pandera
    historique, releves, transform_status, transform_success = None, None, None, False

    if raw_c15 is not None and raw_r151 is not None:
        try:
            # Transformation C15 → HistoriquePérimètre
            historique = lire_flux_c15(raw_c15)

            # Transformation R151 → RelevéIndex
            releves = lire_flux_r151(raw_r151)

            _transform_status = mo.md(f"""
            ## 🔄 **Transform - Données typées**

            - **HistoriquePérimètre**: {len(historique)} lignes validées ✅
            - **RelevéIndex**: {len(releves)} lignes validées ✅

            Les données respectent les schémas Pandera.
            """)
            transform_success = True
        except Exception as e:
            _transform_status = mo.md(f"❌ **Erreur de transformation**: {str(e)}")
    else:
        _transform_status = mo.md("⏭️ Étape Transform ignorée (données brutes manquantes)")

    _transform_status
    return historique, transform_success


@app.cell(hide_code=True)
def _(historique, mo):
    # Inspection de l'historique du périmètre
    _historique_display = (
        mo.vstack([mo.md("### Historique du Périmètre (sample)"), historique]) 
        if historique is not None 
        else mo.md("❌ Historique non disponible")
    )
    _historique_display
    return


@app.cell(hide_code=True)
def _(historique, mo, pd):
    # Analyse des événements impactant le TURPE fixe
    turpe_analysis = None

    if historique is not None:
        # Filtre les événements TURPE fixe
        evenements_turpe = historique[historique["impacte_abonnement"] == True] if "impacte_abonnement" in historique.columns else pd.DataFrame()

        if len(evenements_turpe) > 0:
            by_type = evenements_turpe.groupby("Evenement_Declencheur").size().to_dict()
            _turpe_analysis = mo.md(f"""
            ### 📊 Événements impactant TURPE fixe

            - **Total événements TURPE**: {len(evenements_turpe)}
            - **Répartition par type**:
            """ + "\n".join([f"  - {k}: {v}" for k, v in by_type.items()]))
        else:
            _turpe_analysis = mo.md("⚠️ Aucun événement impactant TURPE fixe trouvé")
    else:
        _turpe_analysis = mo.md("❌ Historique non disponible")

    _turpe_analysis
    return


@app.cell(hide_code=True)
def _(historique, mo, pipeline_abonnement, transform_success):
    # Étape 3: Process - Exécution du pipeline_abonnement complet
    periodes_abonnement, pipeline_status, pipeline_success = None, None, False

    if transform_success and historique is not None:
        try:
            periodes_abonnement = pipeline_abonnement(historique)
            _pipeline_status = mo.md(f"""
            ## 💳 **Process - Pipeline Abonnements**

            - **Périodes d'abonnement calculées**: {len(periodes_abonnement)} lignes ✅
            - **Colonnes**: {len(periodes_abonnement.columns)}
            - **PDL uniques**: {periodes_abonnement['pdl'].nunique() if 'pdl' in periodes_abonnement.columns else 'N/A'}

            Le pipeline d'abonnements s'est exécuté avec succès !
            """)
            pipeline_success = True
        except Exception as e:
            _pipeline_status = mo.md(f"❌ **Erreur pipeline_abonnement**: {str(e)}")
    else:
        _pipeline_status = mo.md("⏭️ Pipeline ignoré (données transformées manquantes)")

    _pipeline_status
    return (periodes_abonnement,)


@app.cell(hide_code=True)
def _(mo, periodes_abonnement):
    # Résultats du pipeline - Aperçu des colonnes
    colonnes_info = None

    if periodes_abonnement is not None:
        colonnes = list(periodes_abonnement.columns)
        _colonnes_info = mo.md(f"""
        ### 📋 Colonnes générées

        **Métadonnées**:
        - `Ref_Situation_Contractuelle`, `pdl`

        **Informations lisibles**:
        - `mois_annee`, `debut_lisible`, `fin_lisible`

        **Données techniques**:
        - `Formule_Tarifaire_Acheminement`, `Puissance_Souscrite`, `nb_jours`

        **Données brutes**:
        - `debut`, `fin`

        **Colonnes TURPE (si présentes)**:
        - `turpe_fixe_journalier`, `turpe_fixe`
        """)
    else:
        _colonnes_info = mo.md("❌ Pas de colonnes à afficher")

    _colonnes_info
    return


@app.cell(hide_code=True)
def _(mo, periodes_abonnement):
    # Résultats du pipeline - Table complète
    _periodes_display = (
        mo.vstack([mo.md("### 💳 Périodes d'Abonnement (résultat final)"), periodes_abonnement]) 
        if periodes_abonnement is not None 
        else mo.md("❌ Périodes d'abonnement non disponibles")
    )
    _periodes_display
    return


@app.cell(hide_code=True)
def _(mo, periodes_abonnement):
    # Métriques de qualité et validation
    quality_metrics = None

    if periodes_abonnement is not None:
        # Statistiques de base
        total_periodes = len(periodes_abonnement)
        pdls_uniques = periodes_abonnement['pdl'].nunique() if 'pdl' in periodes_abonnement.columns else "N/A"

        # Analyse des durées
        if 'nb_jours' in periodes_abonnement.columns:
            duree_moyenne = round(periodes_abonnement['nb_jours'].mean(), 1)
            duree_mediane = round(periodes_abonnement['nb_jours'].median(), 1)
            duree_min = periodes_abonnement['nb_jours'].min()
            duree_max = periodes_abonnement['nb_jours'].max()
        else:
            duree_moyenne = duree_mediane = duree_min = duree_max = "N/A"

        # Analyse des formules tarifaires
        if 'Formule_Tarifaire_Acheminement' in periodes_abonnement.columns:
            formules_uniques = periodes_abonnement['Formule_Tarifaire_Acheminement'].nunique()
            formules_top = periodes_abonnement['Formule_Tarifaire_Acheminement'].value_counts().head(3).to_dict()
        else:
            formules_uniques = "N/A"
            formules_top = {}

        _quality_metrics = mo.md(f"""
        ## 📊 **Métriques de Qualité**

        ### Volumes
        - **Total des périodes**: {total_periodes}
        - **PDL uniques**: {pdls_uniques}
        - **Formules tarifaires uniques**: {formules_uniques}

        ### Durées des périodes (jours)
        - **Moyenne**: {duree_moyenne}
        - **Médiane**: {duree_mediane}
        - **Min / Max**: {duree_min} / {duree_max}

        ### Top 3 Formules Tarifaires
        """ + ("\n".join([f"- **{k}**: {v} périodes" for k, v in formules_top.items()]) if formules_top else "N/A"))
    else:
        _quality_metrics = mo.md("⏭️ Métriques non disponibles")

    _quality_metrics
    return


@app.cell(hide_code=True)
def _(formater_date_francais, mo, periodes_abonnement):
    # Test des fonctions de formatage
    format_test = None

    if periodes_abonnement is not None and len(periodes_abonnement) > 0:
        # Prendre la première période comme exemple
        exemple_debut = periodes_abonnement.iloc[0]['debut'] if 'debut' in periodes_abonnement.columns else None

        if exemple_debut is not None:

            # Tests des différents formats
            format_jour = formater_date_francais(exemple_debut, "d MMMM yyyy")
            format_mois = formater_date_francais(exemple_debut, "LLLL yyyy")
            format_court = formater_date_francais(exemple_debut, "dd/MM/yyyy")

            _format_test = mo.md(f"""
            ### 🗓️ Test des Fonctions de Formatage

            **Date exemple**: `{exemple_debut}`

            - **Format complet**: {format_jour}
            - **Format mois**: {format_mois}  
            - **Format court**: {format_court}

            ✅ La fonction `formater_date_francais()` fonctionne correctement !
            """)
        else:
            _format_test = mo.md("⚠️ Pas de date exemple disponible pour le test")
    else:
        _format_test = mo.md("⏭️ Test formatage ignoré (données manquantes)")

    _format_test
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    ## 🔧 **Zone de Debug Interactive**

    Utilisez les cellules ci-dessous pour explorer les données plus en détail.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    # Interface pour sélectionner un PDL spécifique à inspecter
    pdl_input = mo.ui.text(placeholder="Entrez un PDL à inspecter", label="PDL à analyser")
    pdl_input
    return (pdl_input,)


@app.cell(hide_code=True)
def _(mo, pdl_input, periodes_abonnement):
    # Analyse détaillée d'un PDL spécifique
    pdl_analysis = None

    if periodes_abonnement is not None and pdl_input.value:
        pdl_data = periodes_abonnement[periodes_abonnement['pdl'] == pdl_input.value]
        if len(pdl_data) > 0:
            _pdl_analysis = mo.vstack([
                mo.md(f"### Analyse détaillée du PDL: {pdl_input.value}"),
                mo.md(f"**{len(pdl_data)} période(s) d'abonnement trouvée(s)**"),
                pdl_data
            ])
        else:
            _pdl_analysis = mo.md(f"❌ Aucune période d'abonnement trouvée pour le PDL: {pdl_input.value}")
    else:
        _pdl_analysis = mo.md("💡 Entrez un PDL ci-dessus pour voir l'analyse détaillée")

    _pdl_analysis
    return


@app.cell(hide_code=True)
def _(mo):
    # Interface pour tester le formatage de dates
    date_input = mo.ui.text(placeholder="2025-03-15", label="Date à formater (YYYY-MM-DD)")
    format_input = mo.ui.text(placeholder="d MMMM yyyy", label="Format Babel", value="d MMMM yyyy")

    mo.vstack([
        mo.md("### 🗓️ Test de Formatage de Dates"),
        date_input,
        format_input
    ])
    return date_input, format_input


@app.cell(hide_code=True)
def _(date_input, format_input, formater_date_francais, mo, pd):
    # Résultat du test de formatage
    format_result = None

    if date_input.value and format_input.value:
        try:
            test_date = pd.Timestamp(date_input.value)
            resultat = formater_date_francais(test_date, format_input.value)
            _format_result = mo.md(f"""
            **Résultat**: `{resultat}`
            """)
        except Exception as e:
            _format_result = mo.md(f"❌ Erreur: {str(e)}")
    else:
        _format_result = mo.md("💡 Entrez une date et un format ci-dessus")

    _format_result
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    ## 📝 **Notes de Développement**

    - Le notebook charge les données depuis `~/data/flux_enedis/`
    - Pipeline: ElectriFlux (extract) → ElectriCore/inputs (transform) → ElectriCore/core (process)
    - **Nouveau**: Fonction `formater_date_francais()` réutilisable dans `/core/utils/formatage.py`
    - **Refactoring**: Pipeline fonctionnel avec `.query()`, `.pipe()` et `.assign()`
    - Réactivité Marimo: toute modification en amont met à jour les résultats en aval

    **Architecture du pipeline d'abonnements**:
    1. `pipeline_commun()` - Détection des ruptures et événements de facturation
    2. `generer_periodes_abonnement()` - Calcul des périodes avec formatage
    3. `ajouter_turpe_fixe()` - Enrichissement avec tarifs TURPE

    **Prochaines étapes**: Améliorer les visualisations temporelles, ajouter des graphiques de durée
    """
    )
    return


if __name__ == "__main__":
    app.run()
