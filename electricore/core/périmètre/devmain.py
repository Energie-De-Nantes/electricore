# 📌 Main pour tester en dev
import pandas as pd
from electricore.core.périmètre import extraire_situation, variations_mct_dans_periode

if __name__ == "__main__":
    print("🚀 Test de génération de la situation du périmètre")

    # Historique réaliste avec des MCT bien placés
    historique = pd.DataFrame({
        "pdl": [
            "12345", "12345", "12345",  # PDL 12345
            "67890", "67890", "67890",  # PDL 67890
            "11111", "11111"  # PDL 11111
        ],
        "Ref_Situation_Contractuelle": [
            "A1", "A1", "A1",  # Même Ref pour PDL 12345
            "B1", "B1", "B1",  # Même Ref pour PDL 67890
            "C1", "C1"  # Même Ref pour PDL 11111
        ],
        "Date_Evenement": pd.to_datetime([
            "2024-01-01", "2024-02-10", "2024-03-05",  # PDL 12345
            "2024-01-15", "2024-02-20", "2024-03-10",  # PDL 67890
            "2024-02-01", "2024-03-12"  # PDL 11111
        ]).tz_localize("Europe/Paris"),
        "Etat_Contractuel": [
            "EN SERVICE", "EN SERVICE", "RESILIE",  # PDL 12345
            "EN SERVICE", "EN SERVICE", "RESILIE",  # PDL 67890
            "EN SERVICE", "EN SERVICE"  # PDL 11111
        ],
        "Evenement_Declencheur": [
            "MES", "MCT", "RES",  # PDL 12345 (MCT entre MES et RES)
            "MES", "MCT", "RES",  # PDL 67890 (MCT entre MES et RES)
            "MES", "MCT"  # PDL 11111 (MCT en période active)
        ],
        "Type_Evenement": [
            "CONTRAT", "CONTRAT", "CONTRAT",  # PDL 12345
            "CONTRAT", "CONTRAT", "CONTRAT",  # PDL 67890
            "CONTRAT", "CONTRAT"  # PDL 11111
        ],
        "Segment_Clientele": [
            "RES", "RES", "RES",  # PDL 12345
            "RES", "RES", "RES",  # PDL 67890
            "PRO", "PRO"  # PDL 11111
        ],
        "Categorie": [
            "C2", "C2", "C2",  # PDL 12345
            "C2", "C2", "C2",  # PDL 67890
            "C5", "C5"  # PDL 11111
        ],
        "Type_Compteur": [
            "CCB", "CCB", "CCB",  # PDL 12345
            "CCB", "CCB", "CCB",  # PDL 67890
            "CCB", "CCB"  # PDL 11111
        ],
        "Num_Compteur": [
            "C1", "C1", "C1",  # PDL 12345
            "C2", "C2", "C2",  # PDL 67890
            "C3", "C3"  # PDL 11111
        ],
        "Puissance_Souscrite": [
            6, 9, 9,  # Changement à 9 kVA après MCT pour PDL 12345
            12, 15, 15,  # Changement à 15 kVA après MCT pour PDL 67890
            3, 6  # Changement à 6 kVA après MCT pour PDL 11111
        ],
        "Formule_Tarifaire_Acheminement": [
            "BTINFCU4", "BTINFMU4", "BTINFMU4",  # PDL 12345
            "BTINFCUST", "BTINFCU4", "BTINFCU4",  # PDL 67890
            "BTINFCU4", "BTINFMU4"  # PDL 11111
        ],
        "Ref_Demandeur": [
            None, "Dem1", None,  # PDL 12345
            None, "Dem2", None,  # PDL 67890
            None, "Dem3"  # PDL 11111
        ],
        "Id_Affaire": [
            None, "Aff1", None,  # PDL 12345
            None, "Aff2", None,  # PDL 67890
            None, "Aff3"  # PDL 11111
        ]
    })


    # Définir la date de référence
    date_reference = pd.Timestamp("2024-02-15", tz="Europe/Paris")

    # Extraire la situation
    situation = extraire_situation(date_reference, historique)

    # Afficher le résultat
    print("\n📊 Situation du périmètre au", date_reference)
    print(situation)

    # Définir la période d'analyse
    deb = pd.Timestamp("2024-01-01", tz="Europe/Paris")
    fin = pd.Timestamp("2024-03-15", tz="Europe/Paris")

    # Extraire les variations MCT
    variations_mct = variations_mct_dans_periode(deb, fin, historique)

    # Afficher les résultats
    print("\n🔄 Variations MCT dans la période du", deb, "au", fin)
    print(variations_mct)