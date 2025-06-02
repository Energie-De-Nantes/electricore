from electricore.core.périmètre import HistoriquePérimètre
from electricore.core.abonnements.modèles import PeriodeAbonnement

import pandera.pandas as pa
from pandera.typing import DataFrame

import pandas as pd

from babel.dates import format_date

@pa.check_types
def generer_periodes_abonnement(historique: DataFrame[HistoriquePérimètre]) -> DataFrame[PeriodeAbonnement]:
    """
    Génère les périodes homogènes d'abonnement à partir des événements impactant le TURPE fixe.
    """
    # 1. Filtrer les événements pertinents
    filtres = (
        (historique["impact_turpe_fixe"] == True) &
        (historique["Ref_Situation_Contractuelle"].notna())
    )
    abonnements = historique[filtres].copy()

    # 2. Trier par ref et date
    abonnements = abonnements.sort_values(["Ref_Situation_Contractuelle", "Date_Evenement"])

    # 3. Construire les débuts et fins de période
    abonnements["periode_debut"] = abonnements["Date_Evenement"]
    abonnements["periode_fin"] = abonnements.groupby("Ref_Situation_Contractuelle")["Date_Evenement"].shift(-1)

    # 4. Ne garder que les lignes valides
    periodes = abonnements.dropna(subset=["periode_fin"]).copy()

    # 5. Ajouter FTA, Puissance, et nb jours (arrondi à la journée, pas de time)
    periodes["Formule_Tarifaire_Acheminement"] = periodes["Formule_Tarifaire_Acheminement"]
    periodes["Puissance_Souscrite"] = periodes["Puissance_Souscrite"]
    periodes["nb_jours"] = (periodes["periode_fin"].dt.normalize() - periodes["periode_debut"].dt.normalize()).dt.days

    # 5. Ajouter FTA, Puissance, et nb jours (arrondi à la journée, pas de time)
    periodes["Formule_Tarifaire_Acheminement"] = periodes["Formule_Tarifaire_Acheminement"]
    periodes["Puissance_Souscrite"] = periodes["Puissance_Souscrite"]
    periodes["nb_jours"] = (periodes["periode_fin"].dt.normalize() - periodes["periode_debut"].dt.normalize()).dt.days

    # 6. Ajout lisibles
    periodes['periode_debut_lisible'] = periodes['periode_debut'].apply(
        lambda d: format_date(d, "d MMMM yyyy", locale="fr_FR") if not pd.isna(d) else None
    )
    periodes['periode_fin_lisible'] = periodes['periode_fin'].apply(
        lambda d: format_date(d, "d MMMM yyyy", locale="fr_FR") if not pd.isna(d) else "en cours"
    )

    periodes['mois_annee'] = periodes['periode_debut'].apply(
        lambda d: format_date(d, "LLLL yyyy", locale="fr_FR")
    )
    return periodes[[
        "Ref_Situation_Contractuelle",
        "mois_annee",
        "periode_debut_lisible",
        "periode_fin_lisible",
        "Formule_Tarifaire_Acheminement",
        "Puissance_Souscrite",
        "nb_jours",
        "periode_debut",
    ]].reset_index(drop=True)