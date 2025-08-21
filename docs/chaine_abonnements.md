# Chaîne de traitement des abonnements

Cette documentation décrit la chaîne de traitement spécifique pour calculer les périodes d'abonnement et la part fixe du TURPE à partir de l'historique périmètre.

## Vue d'ensemble

La chaîne de traitement des abonnements permet de transformer l'historique des événements contractuels en périodes homogènes de facturation pour la part fixe (TURPE). Elle suit une approche en trois étapes :

```
HistoriquePérimètre → Points de rupture → Événements de facturation → Périodes d'abonnement
```

## Architecture

### Étape 1 : Détection des points de rupture
**Fonction :** `detecter_points_de_rupture()`  
**Fichier :** `electricore/core/périmètre/fonctions.py:151`

Cette fonction enrichit l'historique périmètre avec trois types d'impacts :
- **`impact_turpe_fixe`** : Changements affectant la part fixe du TURPE (puissance, FTA)
- **`impact_energie`** : Changements affectant le calcul des énergies (calendrier, index)
- **`impact_turpe_variable`** : Changements affectant la part variable du TURPE

#### Logique de détection

```python
# Impact TURPE fixe
impact_turpe_fixe = (
    changement_puissance_souscrite OR changement_formule_tarifaire_acheminement
)

# Impact énergie
impact_energie = (
    changement_calendrier_distributeur OR changement_index_compteur
)

# Impact TURPE variable
impact_turpe_variable = (
    impact_energie OR changement_formule_tarifaire_acheminement
)
```

Les événements d'entrée/sortie (`MES`, `PMES`, `CFNE`, `CFNS`, `RES`) ont automatiquement tous les impacts à `True`.

### Étape 2 : Insertion des événements de facturation
**Fonction :** `inserer_evenements_facturation()`  
**Fichier :** `electricore/core/périmètre/fonctions.py:224`

Cette fonction génère des événements artificiels `FACTURATION` au 1er de chaque mois pour permettre un calcul mensuel.

#### Algorithme

1. **Détection des périodes actives** : Pour chaque `Ref_Situation_Contractuelle`, identifier les dates d'entrée et de sortie
2. **Génération des dates mensuelles** : Créer tous les 1er du mois entre `min(entrée)` et `max(sortie)`
3. **Filtrage par périmètre** : Associer chaque date mensuelle aux références actives à cette date
4. **Création des événements** : Générer les événements `FACTURATION` avec les propriétés par défaut
5. **Propagation des données** : Utiliser `ffill()` pour propager les caractéristiques contractuelles aux événements artificiels

#### Exemple

```python
# Données d'entrée
MES: 2024-01-15 (PDL001)
RES: 2024-03-20 (PDL001)

# Événements générés
FACTURATION: 2024-02-01 (PDL001) - hérite des caractéristiques du 15/01
FACTURATION: 2024-03-01 (PDL001) - hérite des caractéristiques du 15/01
```

### Étape 3 : Génération des périodes d'abonnement
**Fonction :** `generer_periodes_abonnement()`  
**Fichier :** `electricore/core/abonnements/fonctions.py:12`

Cette fonction convertit les événements enrichis en périodes homogènes de facturation.

#### Algorithme

1. **Filtrage** : Sélectionner uniquement les événements avec `impact_turpe_fixe == True`
2. **Tri** : Ordonner par `Ref_Situation_Contractuelle` et `Date_Evenement`
3. **Construction des périodes** : Utiliser `shift(-1)` pour créer des paires début/fin
4. **Calcul des durées** : Calculer `nb_jours` entre début et fin de période
5. **Formatage** : Ajouter les colonnes lisibles avec formatage français

#### Modèle de sortie

```python
class PeriodeAbonnement:
    Ref_Situation_Contractuelle: str
    mois_annee: str                    # "janvier 2024"
    debut_lisible: str                 # "15 janvier 2024"
    fin_lisible: str                   # "1 février 2024"
    Formule_Tarifaire_Acheminement: str
    Puissance_Souscrite: float
    nb_jours: int
    debut: pd.Timestamp
    fin: pd.Timestamp
```

## Utilisation

### Utilisation de base

```python
from electricore.core.périmètre.fonctions import detecter_points_de_rupture, inserer_evenements_facturation
from electricore.core.pipeline_abonnements import generer_periodes_abonnement

# Chaîne complète
historique_enrichi = detecter_points_de_rupture(historique_initial)
historique_etendu = inserer_evenements_facturation(historique_enrichi)
periodes_abonnement = generer_periodes_abonnement(historique_etendu)
```

### Exemple concret

```python
# Scénario : MES, changement de puissance, résiliation
import pandas as pd

data = {
    "Ref_Situation_Contractuelle": ["PDL001", "PDL001", "PDL001"],
    "Date_Evenement": pd.to_datetime(["2024-01-15", "2024-02-10", "2024-03-20"]),
    "Evenement_Declencheur": ["MES", "MCT", "RES"],
    "Puissance_Souscrite": [6.0, 9.0, 9.0],
    "Formule_Tarifaire_Acheminement": ["BASE", "BASE", "BASE"],
    # ... autres colonnes requises
}

df = pd.DataFrame(data)

# Traitement
historique_enrichi = detecter_points_de_rupture(df)
historique_etendu = inserer_evenements_facturation(historique_enrichi)
periodes = generer_periodes_abonnement(historique_etendu)

# Résultat attendu
# Période 1: 15/01 → 10/02 (26 jours, 6kVA)
# Période 2: 10/02 → 01/03 (19 jours, 9kVA)  
# Période 3: 01/03 → 20/03 (19 jours, 9kVA)
```

## Cas d'usage spécifiques

### Changements multiples le même jour

La chaîne gère correctement les changements multiples le même jour. L'ordre des événements dans l'historique détermine l'état final.

### Plusieurs PDL

Chaque `Ref_Situation_Contractuelle` est traitée indépendamment. Les périodes sont générées séparément pour chaque PDL.

### Gestion des fuseaux horaires

Toutes les dates utilisent le fuseau `Europe/Paris`. La fonction `inserer_evenements_facturation` génère des événements au 1er du mois à 00:00:00 UTC+1/UTC+2 selon la saison.

## Validation et tests

### Tests unitaires

- **`TestDetecterPointsDeRupture`** : Validation de la détection des impacts
- **`TestInsererEvenementsFacturation`** : Validation de l'insertion des événements mensuels
- **`TestGenererPeriodesAbonnement`** : Validation de la génération des périodes

### Tests d'intégration

- **`TestIntegrationChaineAbonnements`** : Tests de la chaîne complète avec différents scénarios

### Commandes de test

```bash
# Tests unitaires
pytest tests/test_abonnements.py -v

# Tests d'intégration uniquement
pytest tests/test_abonnements.py::TestIntegrationChaineAbonnements -v

# Test spécifique
pytest tests/test_abonnements.py::TestDetecterPointsDeRupture::test_detecte_changement_puissance -v
```

## Limitations et améliorations futures

### Limitations actuelles

1. **Gestion des changements simultanés** : Les changements le même jour sont traités séquentiellement
2. **Précision temporelle** : Les calculs sont arrondis à la journée
3. **Validation des données** : Dépendance forte aux schémas Pandera

### Améliorations possibles

1. **Performance** : Optimisation pour de gros volumes de données
2. **Logging** : Ajout de logs détaillés pour le debugging
3. **Métriques** : Calcul automatique du TURPE fixe dans les périodes
4. **Validation** : Contrôles de cohérence plus stricts

## Intégration avec le reste du système

### Avec le module taxes

Les périodes générées peuvent être directement utilisées par le module `taxes` pour calculer le TURPE fixe :

```python
from electricore.core.taxes.turpe import calculer_turpe_fixe

for _, periode in periodes_abonnement.iterrows():
    turpe_fixe = calculer_turpe_fixe(
        puissance=periode["Puissance_Souscrite"],
        fta=periode["Formule_Tarifaire_Acheminement"],
        nb_jours=periode["nb_jours"]
    )
```

### Avec le module énergies

Les impacts détectés permettent de savoir quand recalculer les énergies :

```python
ruptures_energie = historique_enrichi[historique_enrichi["impact_energie"] == True]
```

## Fichiers impliqués

- **`electricore/core/périmètre/fonctions.py`** : Fonctions de détection et d'insertion
- **`electricore/core/abonnements/fonctions.py`** : Génération des périodes
- **`electricore/core/abonnements/modèles.py`** : Modèle PeriodeAbonnement
- **`tests/test_abonnements.py`** : Tests complets de la chaîne

Cette chaîne de traitement constitue une brique essentielle pour le calcul précis des factures d'acheminement dans le domaine énergétique français.