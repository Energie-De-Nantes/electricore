import marimo

__generated_with = "0.14.11"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    mo.md(
        r"""
        # Démonstration : Chaîne de traitement des abonnements

        Ce notebook présente la chaîne complète de traitement des abonnements dans ElectriCore.

        ## Objectif

        Transformer l'historique des événements contractuels en périodes homogènes de facturation pour calculer la part fixe du TURPE.
        """
    )
    return mo,


@app.cell
def __():
    import pandas as pd
    import numpy as np
    from datetime import datetime
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # Configuration d'affichage
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    # Imports ElectriCore
    from electricore.core.périmètre.fonctions import detecter_points_de_rupture, inserer_evenements_facturation
    from electricore.core.pipeline_abonnements import generer_periodes_abonnement

    return (
        datetime,
        detecter_points_de_rupture,
        generer_periodes_abonnement,
        go,
        inserer_evenements_facturation,
        make_subplots,
        np,
        pd,
        px,
    )


@app.cell
def __(mo):
    mo.md(
        r"""
        ## 1. Préparation des données d'exemple

        Créons un scénario réaliste avec :
        - Une mise en service (MES)
        - Un changement de puissance (MCT)
        - Une résiliation (RES)
        """
    )
    return


@app.cell
def __(pd):
    def create_historique_data(base_data: dict) -> pd.DataFrame:
        """Helper pour créer un DataFrame HistoriquePérimètre complet"""
        defaults = {
            "pdl": "PDL001",
            "Segment_Clientele": "C5",
            "Num_Compteur": "CPT001",
            "Categorie": "C5",
            "Etat_Contractuel": "SERVC",
            "Type_Compteur": "ELEC",
            "Type_Evenement": "contractuel",
            "Ref_Demandeur": "REF001",
            "Id_Affaire": "AFF001",
        }
        
        index_cols = ["BASE", "HP", "HC", "HCH", "HPH", "HPB", "HCB"]
        calendar_cols = ["Id_Calendrier_Distributeur"]
        
        n_rows = len(base_data.get("Ref_Situation_Contractuelle", []))
        full_data = {}
        
        # Ajouter les valeurs par défaut
        for key, default_value in defaults.items():
            if key not in base_data:
                full_data[key] = [default_value] * n_rows
            else:
                full_data[key] = base_data[key]
        
        # Ajouter les colonnes d'index et calendrier
        for col in index_cols + calendar_cols:
            if f"Avant_{col}" not in base_data:
                full_data[f"Avant_{col}"] = [None] * n_rows
            if f"Après_{col}" not in base_data:
                full_data[f"Après_{col}"] = [None] * n_rows
        
        # Ajouter les autres colonnes
        for key, value in base_data.items():
            if key not in full_data:
                full_data[key] = value
        
        return pd.DataFrame(full_data)

    return create_historique_data,


@app.cell
def __(create_historique_data, pd):
    # Créer les données d'exemple
    data_exemple = {
        "Ref_Situation_Contractuelle": ["PDL001", "PDL001", "PDL001"],
        "Date_Evenement": pd.to_datetime(["2024-01-15", "2024-02-10", "2024-03-20"], utc=True).tz_convert("Europe/Paris"),
        "Evenement_Declencheur": ["MES", "MCT", "RES"],
        "Puissance_Souscrite": [6.0, 9.0, 9.0],
        "Formule_Tarifaire_Acheminement": ["BASE", "BASE", "BASE"],
        "Avant_Puissance_Souscrite": [None, 6.0, 9.0],
        "Avant_Formule_Tarifaire_Acheminement": [None, "BASE", "BASE"],
        "Avant_Id_Calendrier_Distributeur": [None, "CAL001", "CAL001"],
        "Après_Id_Calendrier_Distributeur": ["CAL001", "CAL001", "CAL001"],
        "Etat_Contractuel": ["SERVC", "SERVC", "RESIL"],
        "Avant_BASE": [None, 1000.0, 2000.0],
        "Après_BASE": [1000.0, 2000.0, 2000.0],
    }

    historique_initial = create_historique_data(data_exemple)

    # Afficher les informations de base
    print("📊 Historique initial créé")
    print(f"   - {len(historique_initial)} événements")
    print(f"   - Période: {historique_initial['Date_Evenement'].min().strftime('%d/%m/%Y')} → {historique_initial['Date_Evenement'].max().strftime('%d/%m/%Y')}")
    print(f"   - Événements: {', '.join(historique_initial['Evenement_Declencheur'].tolist())}")

    # Afficher les colonnes principales
    colonnes_principales = ['Ref_Situation_Contractuelle', 'Date_Evenement', 'Evenement_Declencheur', 
                           'Puissance_Souscrite', 'Formule_Tarifaire_Acheminement', 'Etat_Contractuel']
    
    historique_initial[colonnes_principales]
    return colonnes_principales, data_exemple, historique_initial


@app.cell
def __(mo):
    mo.md(
        r"""
        ## 2. Chaîne de traitement complète

        Exécutons les trois étapes de la chaîne de traitement.
        """
    )
    return


@app.cell
def __(
    detecter_points_de_rupture,
    generer_periodes_abonnement,
    historique_initial,
    inserer_evenements_facturation,
):
    # Étape 1 : Détecter les points de rupture
    historique_enrichi = detecter_points_de_rupture(historique_initial)
    print("🔍 Points de rupture détectés")
    print(f"   - {historique_enrichi['impacte_abonnement'].sum()} impacts abonnement")
    print(f"   - {historique_enrichi['impacte_energie'].sum()} impacts énergie")

    # Étape 2 : Insérer les événements de facturation
    historique_etendu = inserer_evenements_facturation(historique_enrichi)
    print(f"\n📅 Événements de facturation insérés")
    print(f"   - {len(historique_etendu)} événements au total")
    print(f"   - {len(historique_etendu) - len(historique_enrichi)} événements artificiels ajoutés")

    # Étape 3 : Générer les périodes d'abonnement
    periodes_abonnement = generer_periodes_abonnement(historique_etendu)
    print(f"\n📋 Périodes d'abonnement générées")
    print(f"   - {len(periodes_abonnement)} périodes")
    print(f"   - Total: {periodes_abonnement['nb_jours'].sum()} jours")

    # Afficher les périodes
    colonnes_periodes = ['mois_annee', 'debut_lisible', 'fin_lisible', 
                        'Puissance_Souscrite', 'nb_jours']
    
    periodes_abonnement[colonnes_periodes]
    return (
        colonnes_periodes,
        historique_enrichi,
        historique_etendu,
        periodes_abonnement,
    )


@app.cell
def __(mo):
    mo.md(
        r"""
        ## 3. Visualisation des résultats

        Créons des graphiques interactifs pour visualiser les résultats.
        """
    )
    return


@app.cell
def __(go, historique_etendu, make_subplots):
    # Séparer les événements originaux et artificiels
    evenements_originaux = historique_etendu[historique_etendu['Type_Evenement'] == 'contractuel']
    evenements_artificiels = historique_etendu[historique_etendu['Type_Evenement'] == 'artificiel']

    # Créer un graphique interactif avec Plotly
    fig_timeline = go.Figure()

    # Ajouter les événements contractuels
    fig_timeline.add_trace(
        go.Scatter(
            x=evenements_originaux['Date_Evenement'],
            y=evenements_originaux['Puissance_Souscrite'],
            mode='markers',
            name='Événements contractuels',
            marker=dict(size=15, color='red'),
            text=evenements_originaux['Evenement_Declencheur'],
            hovertemplate='<b>%{text}</b><br>Date: %{x}<br>Puissance: %{y} kVA<extra></extra>'
        )
    )

    # Ajouter les événements de facturation
    fig_timeline.add_trace(
        go.Scatter(
            x=evenements_artificiels['Date_Evenement'],
            y=evenements_artificiels['Puissance_Souscrite'],
            mode='markers',
            name='Événements de facturation',
            marker=dict(size=10, color='blue'),
            text=evenements_artificiels['Evenement_Declencheur'],
            hovertemplate='<b>%{text}</b><br>Date: %{x}<br>Puissance: %{y} kVA<extra></extra>'
        )
    )

    fig_timeline.update_layout(
        title="Timeline des événements de la chaîne de traitement",
        xaxis_title="Date",
        yaxis_title="Puissance Souscrite (kVA)",
        hovermode='closest'
    )

    fig_timeline.show()
    return evenements_artificiels, evenements_originaux, fig_timeline


@app.cell
def __(go, periodes_abonnement):
    # Graphique des durées de périodes
    fig_durees = go.Figure()

    fig_durees.add_trace(
        go.Bar(
            x=list(range(1, len(periodes_abonnement) + 1)),
            y=periodes_abonnement['nb_jours'],
            name='Nombre de jours',
            marker_color='skyblue',
            text=periodes_abonnement['nb_jours'],
            textposition='outside',
            hovertemplate='<b>Période %{x}</b><br>Durée: %{y} jours<br>Puissance: %{customdata} kVA<extra></extra>',
            customdata=periodes_abonnement['Puissance_Souscrite']
        )
    )

    fig_durees.update_layout(
        title='Durée des périodes d\'abonnement',
        xaxis_title='Période',
        yaxis_title='Nombre de jours',
        showlegend=False
    )

    fig_durees.show()
    return fig_durees,


@app.cell
def __(go, pd, periodes_abonnement):
    # Graphique d'évolution de la puissance
    fig_puissance = go.Figure()

    periodes_viz = periodes_abonnement.copy()
    periodes_viz['debut_date'] = pd.to_datetime(periodes_viz['debut'])

    fig_puissance.add_trace(
        go.Scatter(
            x=periodes_viz['debut_date'],
            y=periodes_viz['Puissance_Souscrite'],
            mode='lines+markers',
            name='Puissance souscrite',
            line=dict(shape='hv', color='red', width=3),
            marker=dict(size=10),
            hovertemplate='<b>Période du %{x}</b><br>Puissance: %{y} kVA<extra></extra>'
        )
    )

    fig_puissance.update_layout(
        title='Évolution de la puissance par période',
        xaxis_title='Date',
        yaxis_title='Puissance Souscrite (kVA)',
        showlegend=False
    )

    fig_puissance.show()
    return fig_puissance, periodes_viz


@app.cell
def __(mo, periodes_abonnement):
    # Statistiques finales
    stats_html = f"""
    <div style="background-color: #f0f8ff; padding: 15px; border-radius: 10px; margin: 10px 0;">
        <h3>📊 Statistiques finales</h3>
        <ul>
            <li><strong>Nombre total de jours facturés:</strong> {periodes_abonnement['nb_jours'].sum()}</li>
            <li><strong>Puissance moyenne:</strong> {periodes_abonnement['Puissance_Souscrite'].mean():.1f} kVA</li>
            <li><strong>Durée moyenne des périodes:</strong> {periodes_abonnement['nb_jours'].mean():.1f} jours</li>
            <li><strong>Période la plus longue:</strong> {periodes_abonnement['nb_jours'].max()} jours</li>
            <li><strong>Période la plus courte:</strong> {periodes_abonnement['nb_jours'].min()} jours</li>
        </ul>
    </div>
    """

    mo.Html(stats_html)
    return stats_html,


@app.cell
def __(mo):
    mo.md(
        r"""
        ## 4. Exemple avec plusieurs PDL

        Démonstration avec plusieurs points de livraison.
        """
    )
    return


@app.cell
def __(
    create_historique_data,
    detecter_points_de_rupture,
    generer_periodes_abonnement,
    inserer_evenements_facturation,
    pd,
):
    # Créer un exemple avec plusieurs PDL
    data_multi_pdl = {
        "Ref_Situation_Contractuelle": ["PDL001", "PDL001", "PDL002", "PDL002"],
        "Date_Evenement": pd.to_datetime(["2024-01-15", "2024-02-10", "2024-01-20", "2024-03-15"], utc=True).tz_convert("Europe/Paris"),
        "Evenement_Declencheur": ["MES", "MCT", "MES", "RES"],
        "Puissance_Souscrite": [6.0, 9.0, 12.0, 12.0],
        "Formule_Tarifaire_Acheminement": ["BASE", "BASE", "HPHC", "HPHC"],
        "pdl": ["PDL001", "PDL001", "PDL002", "PDL002"],
        "Avant_Puissance_Souscrite": [None, 6.0, None, 12.0],
        "Avant_Formule_Tarifaire_Acheminement": [None, "BASE", None, "HPHC"],
        "Etat_Contractuel": ["SERVC", "SERVC", "SERVC", "RESIL"],
    }

    historique_multi = create_historique_data(data_multi_pdl)

    # Traitement complet
    historique_multi_enrichi = detecter_points_de_rupture(historique_multi)
    historique_multi_etendu = inserer_evenements_facturation(historique_multi_enrichi)
    periodes_multi = generer_periodes_abonnement(historique_multi_etendu)

    print("🏢 Exemple multi-PDL")
    print(f"   - {len(periodes_multi)} périodes au total")
    print(f"   - {len(periodes_multi['Ref_Situation_Contractuelle'].unique())} PDL différents")

    # Créer un résumé par PDL
    resume_multi = []
    for pdl_ref in periodes_multi['Ref_Situation_Contractuelle'].unique():
        periodes_pdl_data = periodes_multi[periodes_multi['Ref_Situation_Contractuelle'] == pdl_ref]
        resume_multi.append({
            'PDL': pdl_ref,
            'Nb_Periodes': len(periodes_pdl_data),
            'FTA': periodes_pdl_data['Formule_Tarifaire_Acheminement'].iloc[0],
            'Puissance_Min': periodes_pdl_data['Puissance_Souscrite'].min(),
            'Puissance_Max': periodes_pdl_data['Puissance_Souscrite'].max(),
            'Total_Jours': periodes_pdl_data['nb_jours'].sum()
        })

    resume_multi_df = pd.DataFrame(resume_multi)
    resume_multi_df
    return (
        data_multi_pdl,
        historique_multi,
        historique_multi_enrichi,
        historique_multi_etendu,
        periodes_multi,
        resume_multi,
        resume_multi_df,
    )


@app.cell
def __(go, periodes_multi):
    # Graphique comparatif multi-PDL
    fig_multi = go.Figure()

    colors = ['red', 'blue', 'green', 'orange', 'purple']
    
    for i, pdl_ref in enumerate(periodes_multi['Ref_Situation_Contractuelle'].unique()):
        periodes_pdl_data = periodes_multi[periodes_multi['Ref_Situation_Contractuelle'] == pdl_ref]
        
        fig_multi.add_trace(
            go.Bar(
                x=periodes_pdl_data['mois_annee'],
                y=periodes_pdl_data['nb_jours'],
                name=pdl_ref,
                marker_color=colors[i % len(colors)],
                text=periodes_pdl_data['Puissance_Souscrite'],
                textposition='inside',
                hovertemplate=f'<b>{pdl_ref}</b><br>Mois: %{{x}}<br>Jours: %{{y}}<br>Puissance: %{{text}} kVA<extra></extra>'
            )
        )

    fig_multi.update_layout(
        title='Comparaison multi-PDL - Nombre de jours par mois',
        xaxis_title='Mois',
        yaxis_title='Nombre de jours',
        barmode='group'
    )

    fig_multi.show()
    return colors, fig_multi


@app.cell
def __(mo, periodes_abonnement):
    # Résumé final interactif
    resume_final = f"""
    <div style="background-color: #e8f5e8; padding: 20px; border-radius: 10px; margin: 20px 0; border-left: 5px solid #4caf50;">
        <h2>✅ Résumé du traitement</h2>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 15px 0;">
            <div>
                <h4>📊 Données traitées</h4>
                <ul>
                    <li>Événements initiaux: 3</li>
                    <li>Périodes générées: {len(periodes_abonnement)}</li>
                    <li>Jours total: {periodes_abonnement['nb_jours'].sum()}</li>
                </ul>
            </div>
            <div>
                <h4>⚡ Caractéristiques</h4>
                <ul>
                    <li>Puissance: {periodes_abonnement['Puissance_Souscrite'].min():.0f}-{periodes_abonnement['Puissance_Souscrite'].max():.0f} kVA</li>
                    <li>FTA: {periodes_abonnement['Formule_Tarifaire_Acheminement'].iloc[0]}</li>
                    <li>Durée moy.: {periodes_abonnement['nb_jours'].mean():.1f} jours</li>
                </ul>
            </div>
        </div>
        <p><strong>🎯 Résultat:</strong> La chaîne de traitement fonctionne correctement et produit des périodes homogènes prêtes pour le calcul du TURPE fixe.</p>
    </div>
    """

    mo.Html(resume_final)
    return resume_final,


@app.cell
def __(mo):
    mo.md(
        r"""
        ## 5. Conclusion

        Cette démonstration illustre le fonctionnement complet de la chaîne de traitement des abonnements dans ElectriCore.

        ### Points clés :

        1. **Détection automatique** des changements impactant la facturation
        2. **Génération d'événements mensuels** pour faciliter le calcul
        3. **Création de périodes homogènes** avec durées précises
        4. **Gestion robuste** des cas limites et erreurs
        5. **Support multi-PDL** avec traitement indépendant

        ### Utilisation pratique :

        ```python
        # Chaîne complète en 3 étapes
        historique_enrichi = detecter_points_de_rupture(historique_initial)
        historique_etendu = inserer_evenements_facturation(historique_enrichi)
        periodes_abonnement = generer_periodes_abonnement(historique_etendu)
        ```

        Cette chaîne constitue la base du calcul précis des factures d'acheminement dans le système électrique français.
        """
    )
    return


if __name__ == "__main__":
    app.run()