import pytest
import pandas as pd
from pandera.typing import DataFrame
from electricore.core.périmètre.fonctions import detecter_points_de_rupture, inserer_evenements_facturation
from electricore.core.périmètre.modèles import HistoriquePérimètre
from electricore.core.pipeline_abonnements import generer_periodes_abonnement
from electricore.core.models import PeriodeAbonnement


def create_historique_data(base_data: dict) -> pd.DataFrame:
    """Helper pour créer un DataFrame HistoriquePérimètre avec toutes les colonnes requises"""
    # Colonnes par défaut pour HistoriquePérimètre
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
    
    # Colonnes d'index
    index_cols = ["BASE", "HP", "HC", "HCH", "HPH", "HPB", "HCB"]
    
    # Colonnes calendrier
    calendar_cols = ["Id_Calendrier_Distributeur"]
    
    # Fusionner avec les données de base
    n_rows = len(base_data.get("Ref_Situation_Contractuelle", []))
    
    # Créer le dictionnaire complet
    full_data = {}
    
    # Ajouter les valeurs par défaut
    for key, default_value in defaults.items():
        if key not in base_data:
            full_data[key] = [default_value] * n_rows
        else:
            full_data[key] = base_data[key]
    
    # Ajouter les colonnes d'index si non présentes
    for col in index_cols:
        if f"Avant_{col}" not in base_data:
            full_data[f"Avant_{col}"] = [None] * n_rows
        if f"Après_{col}" not in base_data:
            full_data[f"Après_{col}"] = [None] * n_rows
    
    # Ajouter les colonnes calendrier si non présentes
    for col in calendar_cols:
        if f"Avant_{col}" not in base_data:
            full_data[f"Avant_{col}"] = [None] * n_rows
        if f"Après_{col}" not in base_data:
            full_data[f"Après_{col}"] = [None] * n_rows
    
    # Ajouter toutes les autres colonnes de base_data
    for key, value in base_data.items():
        if key not in full_data:
            full_data[key] = value
    
    return pd.DataFrame(full_data)


class TestDetecterPointsDeRupture:
    """Tests pour la fonction detecter_points_de_rupture"""
    
    def test_detecte_changement_puissance(self):
        """Test que la fonction détecte correctement un changement de puissance"""
        # Préparer les données
        data = {
            "Ref_Situation_Contractuelle": ["PDL001", "PDL001"],
            "Date_Evenement": pd.to_datetime(["2024-01-01", "2024-02-01"], utc=True).tz_convert("Europe/Paris"),
            "Evenement_Declencheur": ["MES", "MCT"],
            "Puissance_Souscrite": [6.0, 9.0],
            "Formule_Tarifaire_Acheminement": ["BASE", "BASE"],
            "Avant_Puissance_Souscrite": [None, 6.0],
            "Avant_Formule_Tarifaire_Acheminement": [None, "BASE"],
            "Avant_Id_Calendrier_Distributeur": [None, "CAL001"],
            "Après_Id_Calendrier_Distributeur": ["CAL001", "CAL001"],
            "Avant_BASE": [None, 1000.0],
            "Après_BASE": [1000.0, 2000.0],
        }
        
        df = create_historique_data(data)
        
        # Exécuter la fonction
        resultat = detecter_points_de_rupture(df)
        
        # Vérifications
        assert len(resultat) == 2
        assert resultat.iloc[0]["impacte_abonnement"] == True  # MES a toujours un impact
        assert resultat.iloc[1]["impacte_abonnement"] == True  # Changement de puissance
        assert "P: 6.0 → 9.0" in resultat.iloc[1]["resume_modification"]
    
    def test_detecte_changement_fta(self):
        """Test que la fonction détecte correctement un changement de FTA"""
        data = {
            "Ref_Situation_Contractuelle": ["PDL001", "PDL001"],
            "Date_Evenement": pd.to_datetime(["2024-01-01", "2024-02-01"], utc=True).tz_convert("Europe/Paris"),
            "Evenement_Declencheur": ["MES", "MCT"],
            "Puissance_Souscrite": [6.0, 6.0],
            "Formule_Tarifaire_Acheminement": ["BASE", "HPHC"],
            "Avant_Puissance_Souscrite": [None, 6.0],
            "Avant_Formule_Tarifaire_Acheminement": [None, "BASE"],
            "Avant_Id_Calendrier_Distributeur": [None, "CAL001"],
            "Après_Id_Calendrier_Distributeur": ["CAL001", "CAL002"],
            "Avant_BASE": [None, 1000.0],
            "Après_BASE": [1000.0, 2000.0],
        }
        
        df = create_historique_data(data)
        resultat = detecter_points_de_rupture(df)
        
        assert resultat.iloc[1]["impacte_abonnement"] == True
        assert "FTA: BASE → HPHC" in resultat.iloc[1]["resume_modification"]
        assert "Cal: CAL001 → CAL002" in resultat.iloc[1]["resume_modification"]
    
    def test_evenements_entree_sortie_ont_impact(self):
        """Test que les événements d'entrée/sortie ont toujours un impact"""
        evenements = ["CFNE", "MES", "PMES", "CFNS", "RES"]
        data = {
            "Ref_Situation_Contractuelle": [f"PDL{i:03d}" for i in range(len(evenements))],
            "Date_Evenement": pd.to_datetime(["2024-01-01"] * len(evenements), utc=True).tz_convert("Europe/Paris"),
            "Evenement_Declencheur": evenements,
            "Puissance_Souscrite": [6.0] * len(evenements),
            "Formule_Tarifaire_Acheminement": ["BASE"] * len(evenements),
            "Avant_Puissance_Souscrite": [None] * len(evenements),
            "Avant_Formule_Tarifaire_Acheminement": [None] * len(evenements),
            "Avant_Id_Calendrier_Distributeur": [None] * len(evenements),
            "Après_Id_Calendrier_Distributeur": ["CAL001"] * len(evenements),
            "pdl": [f"PDL{i:03d}" for i in range(len(evenements))],
            "Num_Compteur": [f"CPT{i:03d}" for i in range(len(evenements))],
            "Avant_BASE": [None] * len(evenements),
            "Après_BASE": [1000.0] * len(evenements),
        }
        
        df = create_historique_data(data)
        resultat = detecter_points_de_rupture(df)
        
        for i, evt in enumerate(evenements):
            assert resultat.iloc[i]["impacte_abonnement"] == True
            assert resultat.iloc[i]["impacte_energie"] == True


class TestInsererEvenementsFacturation:
    """Tests pour la fonction inserer_evenements_facturation"""
    
    def test_insertion_evenements_mensuels(self):
        """Test que la fonction insère correctement les événements de facturation mensuels"""
        # Préparer les données avec une entrée et une sortie
        data = {
            "Ref_Situation_Contractuelle": ["PDL001", "PDL001"],
            "Date_Evenement": pd.to_datetime(["2024-01-15", "2024-04-20"], utc=True).tz_convert("Europe/Paris"),
            "Evenement_Declencheur": ["MES", "RES"],
            "Type_Evenement": ["contractuel", "contractuel"],
            "Source": ["flux_C15", "flux_C15"],
            "impacte_abonnement": [True, True],
            "impacte_energie": [True, True],
            "resume_modification": ["Mise en service", "Résiliation"],
            "Puissance_Souscrite": [6.0, 6.0],
            "Formule_Tarifaire_Acheminement": ["BASE", "BASE"],
            "Etat_Contractuel": ["SERVC", "RESIL"],
        }
        
        df = create_historique_data(data)
        
        # Exécuter la fonction
        resultat = inserer_evenements_facturation(df)
        
        # Vérifications
        evenements_facturation = resultat[resultat["Evenement_Declencheur"] == "FACTURATION"]
        
        # On devrait avoir des événements au 1er de chaque mois entre janvier et avril
        dates_attendues = pd.to_datetime(["2024-02-01", "2024-03-01", "2024-04-01"], utc=True).tz_convert("Europe/Paris")
        assert len(evenements_facturation) == 3
        
        # Vérifier qu'on a bien des événements aux dates attendues (tolérance pour les décalages de timezone)
        for date in dates_attendues:
            found = False
            for _, evt in evenements_facturation.iterrows():
                if evt["Date_Evenement"].date() == date.date():
                    found = True
                    break
            assert found, f"Événement manquant pour le {date.date()}"
        
        # Vérifier les propriétés des événements ajoutés
        for _, evt in evenements_facturation.iterrows():
            assert evt["Type_Evenement"] == "artificiel"
            assert evt["Source"] == "synthese_mensuelle"
            assert evt["impacte_abonnement"] == True
            assert evt["impacte_energie"] == True
    
    def test_propagation_donnees_contractuelles(self):
        """Test que les données contractuelles sont correctement propagées par ffill"""
        data = {
            "Ref_Situation_Contractuelle": ["PDL001", "PDL001"],
            "Date_Evenement": pd.to_datetime(["2024-01-15", "2024-02-20"], utc=True).tz_convert("Europe/Paris"),
            "Evenement_Declencheur": ["MES", "MCT"],
            "Puissance_Souscrite": [6.0, 9.0],
            "Formule_Tarifaire_Acheminement": ["BASE", "HPHC"],
            "Type_Evenement": ["contractuel", "contractuel"],
            "Source": ["flux_C15", "flux_C15"],
            "impacte_abonnement": [True, True],
            "impacte_energie": [True, True],
            "resume_modification": ["Mise en service", "Changement FTA"],
        }
        
        df = create_historique_data(data)
        resultat = inserer_evenements_facturation(df)
        
        # L'événement du 1er février devrait avoir les caractéristiques de janvier
        evenements_facturation = resultat[resultat["Evenement_Declencheur"] == "FACTURATION"]
        evt_fev = evenements_facturation[evenements_facturation["Date_Evenement"].dt.date == pd.Timestamp("2024-02-01").date()]
        
        if len(evt_fev) == 0:
            # Afficher les événements disponibles pour déboguer
            print("Événements de facturation disponibles:")
            print(evenements_facturation["Date_Evenement"].tolist())
            assert False, "Aucun événement de facturation trouvé pour février 2024"
        
        evt_fev = evt_fev.iloc[0]
        
        assert evt_fev["Puissance_Souscrite"] == 6.0
        assert evt_fev["Formule_Tarifaire_Acheminement"] == "BASE"
        assert evt_fev["pdl"] == "PDL001"
    
    def test_pas_evenements_apres_limite(self):
        """Test que la fonction ne génère pas d'événements après la date limite"""
        # PDL avec une résiliation
        data_avec_resiliation = {
            "Ref_Situation_Contractuelle": ["PDL001", "PDL001"],
            "Date_Evenement": pd.to_datetime(["2024-01-15", "2024-02-20"], utc=True).tz_convert("Europe/Paris"),
            "Evenement_Declencheur": ["MES", "RES"],
            "Puissance_Souscrite": [6.0, 6.0],
            "Formule_Tarifaire_Acheminement": ["BASE", "BASE"],
            "Type_Evenement": ["contractuel", "contractuel"],
            "Source": ["flux_C15", "flux_C15"],
            "impacte_abonnement": [True, True],
            "impacte_energie": [True, True],
            "resume_modification": ["Mise en service", "Résiliation"],
            "Etat_Contractuel": ["SERVC", "RESIL"],
        }
        
        df = create_historique_data(data_avec_resiliation)
        resultat = inserer_evenements_facturation(df)
        
        # Vérifier qu'il n'y a pas d'événements de facturation après la résiliation
        evenements_facturation = resultat[resultat["Evenement_Declencheur"] == "FACTURATION"]
        date_resiliation = pd.Timestamp("2024-02-20", tz="Europe/Paris")
        
        evenements_apres_resiliation = evenements_facturation[
            evenements_facturation["Date_Evenement"] > date_resiliation
        ]
        
        assert len(evenements_apres_resiliation) == 0, f"Trouvé {len(evenements_apres_resiliation)} événements après résiliation"
        
        # Vérifier qu'on a bien des événements avant la résiliation
        evenements_avant_resiliation = evenements_facturation[
            evenements_facturation["Date_Evenement"] <= date_resiliation
        ]
        
        assert len(evenements_avant_resiliation) > 0, "Aucun événement de facturation avant résiliation"


class TestGenererPeriodesAbonnement:
    """Tests pour la fonction generer_periodes_abonnement"""
    
    def test_generation_periodes_simples(self):
        """Test la génération de périodes d'abonnement simples"""
        # Préparer un historique avec des événements de facturation
        data = {
            "Ref_Situation_Contractuelle": ["PDL001", "PDL001", "PDL001"],
            "Date_Evenement": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"], utc=True).tz_convert("Europe/Paris"),
            "Evenement_Declencheur": ["FACTURATION", "FACTURATION", "FACTURATION"],
            "impacte_abonnement": [True, True, True],
            "Puissance_Souscrite": [6.0, 6.0, 6.0],
            "Formule_Tarifaire_Acheminement": ["BASE", "BASE", "BASE"],
        }
        
        df = create_historique_data(data)
        
        # Exécuter la fonction
        periodes = generer_periodes_abonnement(df)
        
        # Vérifications
        assert len(periodes) == 2  # Deux périodes complètes (la dernière n'a pas de fin)
        
        # Première période : janvier
        assert periodes.iloc[0]["nb_jours"] == 31
        assert periodes.iloc[0]["debut_lisible"] == "1 janvier 2024"
        assert periodes.iloc[0]["fin_lisible"] == "1 février 2024"
        assert periodes.iloc[0]["mois_annee"] == "janvier 2024"
        
        # Deuxième période : février
        assert periodes.iloc[1]["nb_jours"] == 29  # 2024 est bissextile
        assert periodes.iloc[1]["debut_lisible"] == "1 février 2024"
        assert periodes.iloc[1]["fin_lisible"] == "1 mars 2024"
        assert periodes.iloc[1]["mois_annee"] == "février 2024"
    
    def test_generation_avec_changement_puissance(self):
        """Test la génération avec un changement de puissance en cours de mois"""
        data = {
            "Ref_Situation_Contractuelle": ["PDL001", "PDL001", "PDL001", "PDL001"],
            "Date_Evenement": pd.to_datetime(["2024-01-01", "2024-01-15", "2024-02-01", "2024-03-01"], utc=True).tz_convert("Europe/Paris"),
            "Evenement_Declencheur": ["FACTURATION", "MCT", "FACTURATION", "FACTURATION"],
            "impacte_abonnement": [True, True, True, True],
            "Puissance_Souscrite": [6.0, 9.0, 9.0, 9.0],
            "Formule_Tarifaire_Acheminement": ["BASE", "BASE", "BASE", "BASE"],
        }
        
        df = create_historique_data(data)
        periodes = generer_periodes_abonnement(df)
        
        # On devrait avoir 4 périodes
        assert len(periodes) == 3
        
        # Première période : 1-15 janvier avec 6kVA
        assert periodes.iloc[0]["Puissance_Souscrite"] == 6.0
        assert periodes.iloc[0]["nb_jours"] == 14
        
        # Deuxième période : 15 janvier - 1 février avec 9kVA
        assert periodes.iloc[1]["Puissance_Souscrite"] == 9.0
        assert periodes.iloc[1]["nb_jours"] == 17
    
    def test_colonnes_resultat(self):
        """Test que le résultat contient toutes les colonnes attendues"""
        data = {
            "Ref_Situation_Contractuelle": ["PDL001", "PDL001"],
            "Date_Evenement": pd.to_datetime(["2024-01-01", "2024-02-01"], utc=True).tz_convert("Europe/Paris"),
            "Evenement_Declencheur": ["FACTURATION", "FACTURATION"],
            "impacte_abonnement": [True, True],
            "Puissance_Souscrite": [6.0, 6.0],
            "Formule_Tarifaire_Acheminement": ["BASE", "BASE"],
        }
        
        df = create_historique_data(data)
        periodes = generer_periodes_abonnement(df)
        
        colonnes_attendues = [
            "Ref_Situation_Contractuelle",
            "mois_annee",
            "debut_lisible",
            "fin_lisible",
            "Formule_Tarifaire_Acheminement",
            "Puissance_Souscrite",
            "nb_jours",
            "debut"
        ]
        
        for col in colonnes_attendues:
            assert col in periodes.columns


class TestIntegrationChaineAbonnements:
    """Test d'intégration de la chaîne complète de traitement des abonnements"""
    
    def test_chaine_complete_scenario_reel(self):
        """Test d'intégration avec un scénario réaliste complet"""
        # Scénario : Un PDL avec MES, changement de puissance, puis résiliation
        data = {
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
        
        df_initial = create_historique_data(data)
        
        # Étape 1 : Détecter les points de rupture
        historique_enrichi = detecter_points_de_rupture(df_initial)
        
        # Vérifications de l'étape 1
        assert len(historique_enrichi) == 3
        assert all(historique_enrichi["impacte_abonnement"] == True)
        assert "P: 6.0 → 9.0" in historique_enrichi.iloc[1]["resume_modification"]
        
        # Étape 2 : Insérer les événements de facturation
        historique_etendu = inserer_evenements_facturation(historique_enrichi)
        
        # Vérifications de l'étape 2
        evenements_facturation = historique_etendu[historique_etendu["Evenement_Declencheur"] == "FACTURATION"]
        assert len(evenements_facturation) == 2  # Février, mars (avril arrêté par RES le 20/03)
        
        # Vérifier que les événements artificiels ont bien hérité des caractéristiques contractuelles
        evt_fev = evenements_facturation[evenements_facturation["Date_Evenement"].dt.month == 2].iloc[0]
        assert evt_fev["Puissance_Souscrite"] == 6.0
        
        evt_mars = evenements_facturation[evenements_facturation["Date_Evenement"].dt.month == 3].iloc[0]
        assert evt_mars["Puissance_Souscrite"] == 9.0
        
        # Étape 3 : Générer les périodes d'abonnement
        periodes_abonnement = generer_periodes_abonnement(historique_etendu)
        
        # Vérifications de l'étape 3
        assert len(periodes_abonnement) >= 4  # Plusieurs périodes dues aux changements
        
        # Vérifier qu'on a bien des périodes avec différentes puissances
        puissances = set(periodes_abonnement["Puissance_Souscrite"])
        assert 6.0 in puissances
        assert 9.0 in puissances
        
        # Vérifier que les périodes couvrent bien la durée du contrat
        debut_contrat = pd.Timestamp("2024-01-15").date()
        fin_contrat = pd.Timestamp("2024-03-20").date()
        
        premiere_periode = periodes_abonnement.iloc[0]
        derniere_periode = periodes_abonnement.iloc[-1]
        
        assert premiere_periode["debut"].date() == debut_contrat
        # La dernière période devrait commencer avant ou à la fin du contrat
        assert derniere_periode["debut"].date() <= fin_contrat
        
        # Vérifier que chaque période a un nombre de jours cohérent
        for _, periode in periodes_abonnement.iterrows():
            assert periode["nb_jours"] > 0
            assert periode["nb_jours"] <= 31
        
        # Vérifier la cohérence des colonnes
        colonnes_attendues = [
            "Ref_Situation_Contractuelle",
            "mois_annee",
            "debut_lisible",
            "fin_lisible",
            "Formule_Tarifaire_Acheminement",
            "Puissance_Souscrite",
            "nb_jours",
            "debut"
        ]
        
        for col in colonnes_attendues:
            assert col in periodes_abonnement.columns
        
        # Vérifier que toutes les périodes concernent le même PDL
        assert all(periodes_abonnement["Ref_Situation_Contractuelle"] == "PDL001")
    
    def test_chaine_complete_avec_plusieurs_pdl(self):
        """Test d'intégration avec plusieurs PDL"""
        data = {
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
        
        df_initial = create_historique_data(data)
        
        # Exécuter la chaîne complète
        historique_enrichi = detecter_points_de_rupture(df_initial)
        historique_etendu = inserer_evenements_facturation(historique_enrichi)
        periodes_abonnement = generer_periodes_abonnement(historique_etendu)
        
        # Vérifications
        # On devrait avoir des périodes pour les deux PDL
        pdl_list = periodes_abonnement["Ref_Situation_Contractuelle"].unique()
        assert "PDL001" in pdl_list
        assert "PDL002" in pdl_list
        
        # Chaque PDL devrait avoir ses propres périodes
        periodes_pdl001 = periodes_abonnement[periodes_abonnement["Ref_Situation_Contractuelle"] == "PDL001"]
        periodes_pdl002 = periodes_abonnement[periodes_abonnement["Ref_Situation_Contractuelle"] == "PDL002"]
        
        assert len(periodes_pdl001) > 0
        assert len(periodes_pdl002) > 0
        
        # Vérifier que les FTA sont correctes
        assert all(periodes_pdl001["Formule_Tarifaire_Acheminement"] == "BASE")
        assert all(periodes_pdl002["Formule_Tarifaire_Acheminement"] == "HPHC")
    
    def test_chaine_complete_cas_limite_changement_meme_jour(self):
        """Test avec plusieurs changements le même jour"""
        data = {
            "Ref_Situation_Contractuelle": ["PDL001", "PDL001", "PDL001"],
            "Date_Evenement": pd.to_datetime(["2024-01-15", "2024-02-10", "2024-02-10"], utc=True).tz_convert("Europe/Paris"),
            "Evenement_Declencheur": ["MES", "MCT", "MCT"],
            "Puissance_Souscrite": [6.0, 9.0, 12.0],
            "Formule_Tarifaire_Acheminement": ["BASE", "BASE", "HPHC"],
            "Avant_Puissance_Souscrite": [None, 6.0, 9.0],
            "Avant_Formule_Tarifaire_Acheminement": [None, "BASE", "BASE"],
        }
        
        df_initial = create_historique_data(data)
        
        # Exécuter la chaîne complète
        historique_enrichi = detecter_points_de_rupture(df_initial)
        historique_etendu = inserer_evenements_facturation(historique_enrichi)
        periodes_abonnement = generer_periodes_abonnement(historique_etendu)
        
        # Vérifications
        # La fonction devrait gérer correctement les changements multiples le même jour
        assert len(periodes_abonnement) > 0
        
        # La dernière période devrait avoir la puissance et FTA finales
        derniere_periode = periodes_abonnement[periodes_abonnement["debut"] == periodes_abonnement["debut"].max()]
        if len(derniere_periode) > 0:
            assert derniere_periode.iloc[0]["Puissance_Souscrite"] == 12.0
            assert derniere_periode.iloc[0]["Formule_Tarifaire_Acheminement"] == "HPHC"