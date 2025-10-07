# Conventions de Nommage

---

## 1. Principe Directeur

**Tout le code est en français** : variables, fonctions, classes, modules, colonnes, documentation.

**Exception** : Méthodes des bibliothèques tierces (Polars, Pandas, etc.) restent en anglais car imposées par les bibliothèques.

**Justification** : Cette bibliothèque est spécifiquement destinée au marché français de l'électricité (Enedis, CRE, fournisseurs français). Les concepts métier (HPH, HCH, HPB, HCB, TURPE, CSPE, etc.) sont intrinsèquement français et non-traduisibles sans perte de sens.

---

## 2. Format Standard pour Colonnes DataFrame

### Règle Générale

```
<grandeur>_<cadran>_<unité>
```

### Exemples Complets

```python
# Index compteurs (valeurs cumulées)
index_hph_kwh
index_hch_kwh
index_hpb_kwh
index_hcb_kwh

# Énergies consommées (différences entre relevés)
energie_hph_kwh
energie_hch_kwh
energie_hpb_kwh
energie_hcb_kwh

# Puissances souscrites (contrat)
puissance_souscrite_hph_kva
puissance_souscrite_hch_kva
puissance_souscrite_hpb_kva
puissance_souscrite_hcb_kva

# Tarifs (prix unitaire)
tarif_hph_eur_par_kwh
tarif_hch_eur_par_kwh
tarif_hpb_eur_par_kwh
tarif_hcb_eur_par_kwh

# Coûts calculés
cout_hph_eur
cout_hch_eur
cout_hpb_eur
cout_hcb_eur
cout_total_eur

# Taxes et contributions
turpe_hph_eur_par_kwh
turpe_hch_eur_par_kwh
cspe_eur_par_kwh

# Métadonnées
identifiant_pdl
date_releve
type_tarif
statut_compteur
```

---

## 3. Cadrans EJP (Codes Standardisés)

Les codes cadrans sont **immuables**, mais les plages horaires peuvent changer.  

| Code | Signification | Période |
|------|---------------|---------|
| `hph` | Heures Pleines Hiver | 6h-22h, novembre-mars (hors jours EJP) |
| `hch` | Heures Creuses Hiver | 22h-6h, novembre-mars (hors jours EJP) |
| `hpb` | Heures Pleines saison Basse | 6h-22h, jours normaux (reste de l'année) |
| `hcb` | Heures Creuses saison Basse | 22h-6h, jours normaux (reste de l'année) |

**Note** : Les 22 jours EJP (pointe mobile) sont facturés en HPH avec tarif majoré.

---

## 4. Nommage des Fonctions (Français)

### Convention : `verbe_complement_complement()`

**Format** : Verbe à l'infinitif + compléments en snake_case

```python
def calculer_consommation():
    """Calcule la consommation entre deux relevés."""
    pass

def valider_releve_compteur():
    """Valide la cohérence d'un relevé de compteur."""
    pass

def recuperer_donnees_tarif():
    """Récupère les données tarifaires depuis l'API."""
    pass

def generer_facture():
    """Génère une facture à partir des consommations."""
    pass

def extraire_index_compteur():
    """Extrait les index d'un relevé de compteur."""
    pass

def transformer_format_enedis():
    """Transforme les données au format Enedis standard."""
    pass

def charger_donnees_depuis_csv():
    """Charge les données depuis un fichier CSV."""
    pass

def exporter_vers_parquet():
    """Exporte les résultats au format Parquet."""
    pass

def aggreger_par_periode():
    """Agrège les données par période (jour/mois/an)."""
    pass

def filtrer_compteurs_actifs():
    """Filtre les compteurs ayant un statut actif."""
    pass
```

### Verbes d'Action Recommandés

| Verbe | Usage |
|-------|-------|
| `calculer` | Opérations de calcul (consommation, coût, etc.) |
| `valider` | Vérification de cohérence |
| `recuperer` / `obtenir` | Récupération de données |
| `generer` / `creer` | Création de nouveaux objets |
| `extraire` | Extraction de sous-ensembles |
| `transformer` / `convertir` | Transformation de format |
| `charger` | Chargement depuis fichier/API |
| `exporter` / `sauvegarder` | Écriture vers fichier |
| `aggreger` / `grouper` | Opérations d'agrégation |
| `filtrer` / `selectionner` | Filtrage de données |

---

## 5. Nommage des Classes (Français)

### Convention : `SubstantifDescriptif` (PascalCase)

**Format** : Substantif(s) en PascalCase (première lettre de chaque mot en majuscule)

```python
class DonneesCompteur(pa.Model):
    """Schéma pour les données de compteur."""
    pass

class MoteurFacturation:
    """Moteur de calcul de facturation électrique."""
    pass

class ValidateurReleve:
    """Validateur de relevés de compteurs."""
    pass

class GestionnaireTarifs:
    """Gestionnaire des grilles tarifaires."""
    pass

class CalculateurConsommation:
    """Calculateur de consommations électriques."""
    pass

class ExportateurDonnees:
    """Exportateur de données vers différents formats."""
    pass

class ClientApiEnedis:
    """Client pour l'API Enedis."""
    pass

class ReleveCompteur(pa.Model):
    """Modèle de données pour un relevé de compteur."""
    pass

class ConfigurationTarif:
    """Configuration d'une grille tarifaire."""
    pass
```

### Patterns de Nommage Courants

| Pattern | Exemple | Usage |
|---------|---------|-------|
| `Donnees*` | `DonneesCompteur` | Structures de données |
| `Moteur*` | `MoteurFacturation` | Composants de calcul |
| `Validateur*` | `ValidateurReleve` | Validateurs |
| `Gestionnaire*` | `GestionnaireTarifs` | Gestionnaires de ressources |
| `Calculateur*` | `CalculateurConsommation` | Calculateurs spécialisés |
| `Exportateur*` | `ExportateurDonnees` | Exportateurs |
| `Client*` | `ClientApiEnedis` | Clients API |
| `Configuration*` | `ConfigurationTarif` | Configurations |

---

## 6. Nommage des Variables (Français)

### Variables Locales

**Format** : snake_case descriptif

```python
# Variables simples
identifiant_pdl = "PDL12345678901234"
date_releve = datetime(2025, 1, 15)
consommation_totale = 805.6
nombre_jours = 31
tarif_applique = 0.1821

# Variables complexes
liste_compteurs_actifs = [...]
dictionnaire_tarifs = {...}
dataframe_releves = pl.DataFrame(...)

# Boucles
for compteur in liste_compteurs:
    releve = recuperer_dernier_releve(compteur)
    consommation = calculer_consommation(releve)

for cadran in CADRANS_EJP:
    energie = df[f"energie_{cadran}_kwh"]
    cout = energie * tarif[f"tarif_{cadran}_eur_par_kwh"]

# Compréhensions
energies_totales = [
    sum(df[f"energie_{c}_kwh"]) 
    for c in CADRANS_EJP
]
```

### Variables de Classe

```python
class MoteurFacturation:
    """Moteur de facturation."""
    
    def __init__(self):
        self.grille_tarifaire = {}
        self.taux_tva = 0.20
        self.compteurs_traites = []
        self._cache_calculs = {}  # Privé (underscore)
```

---

## 7. Constantes (Français - MAJUSCULES)

### Convention : `MAJUSCULES_AVEC_UNDERSCORES`

```python
# Constantes métier
CADRANS_EJP = ["hph", "hch", "hpb", "hcb"]
NOMBRE_JOURS_EJP_PAR_AN = 22
PUISSANCE_MAX_STANDARD_KVA = 36.0
NOMBRE_CARACTERES_PDL = 14

# Tarifs (exemple 2025)
TARIF_HPH_EUR_PAR_KWH = 0.1821
TARIF_HCH_EUR_PAR_KWH = 0.1249
TARIF_HPB_EUR_PAR_KWH = 0.1054
TARIF_HCB_EUR_PAR_KWH = 0.0823
TARIF_EJP_EUR_PAR_KWH = 0.6854

# Taxes et contributions
TAUX_TVA = 0.20
TAUX_CSPE_EUR_PAR_KWH = 0.0225

# Configuration technique
TAILLE_PAGE_PAR_DEFAUT = 100
NOMBRE_TENTATIVES_MAX = 3
DELAI_EXPIRATION_SECONDES = 30
URL_API_ENEDIS = "https://api.enedis.fr/v1"

# Périodes
DEBUT_HIVER = (11, 1)  # 1er novembre
FIN_HIVER = (3, 31)    # 31 mars
```

### Dictionnaires de Configuration

```python
TARIFS_ENEDIS_2025 = {
    "tarif_hph_eur_par_kwh": 0.1821,
    "tarif_hch_eur_par_kwh": 0.1249,
    "tarif_hpb_eur_par_kwh": 0.1054,
    "tarif_hcb_eur_par_kwh": 0.0823,
}

TURPE_2025 = {
    "turpe_hph_eur_par_kwh": 0.0458,
    "turpe_hch_eur_par_kwh": 0.0458,
    "turpe_hpb_eur_par_kwh": 0.0334,
    "turpe_hcb_eur_par_kwh": 0.0334,
}

PLAGES_HORAIRES = {
    "heures_pleines": (6, 22),   # 6h-22h
    "heures_creuses": (22, 6),   # 22h-6h
}
```

### Convention pour Noms de Fichiers

- **snake_case** pour tous les fichiers Python
- **Descriptif et explicite** : `calcul_consommation.py` plutôt que `calc.py`
- **Pas de caractères spéciaux** (éviter accents pour compatibilité système)

---

## 9. Documentation (Français)

### Docstrings - Format Google Style Adapté

```python
def calculer_cout_periode(
    df_releves: pl.DataFrame,
    date_debut: datetime,
    date_fin: datetime,
    grille_tarifaire: dict[str, float]
) -> pl.DataFrame:
    """Calcule le coût de consommation sur une période donnée.
    
    Cette fonction prend en entrée une DataFrame de relevés de compteurs
    et calcule le coût total de consommation pour chaque compteur sur la
    période spécifiée, en appliquant la grille tarifaire fournie.
    
    La formule appliquée est :
        Coût_cadran = Énergie_cadran × Tarif_cadran
        Coût_total = Σ(Coût_cadran) pour tous les cadrans
    
    Args:
        df_releves: DataFrame contenant les relevés avec colonnes :
            - identifiant_pdl (str) : Identifiant Point de Livraison (14 chiffres)
            - date_releve (datetime) : Date du relevé
            - energie_hph_kwh (float) : Énergie consommée HPH en kWh
            - energie_hch_kwh (float) : Énergie consommée HCH en kWh
            - energie_hpb_kwh (float) : Énergie consommée HPB en kWh
            - energie_hcb_kwh (float) : Énergie consommée HCB en kWh
        date_debut: Date de début de période (inclusive)
        date_fin: Date de fin de période (inclusive)
        grille_tarifaire: Dictionnaire des tarifs avec clés :
            - tarif_hph_eur_par_kwh : Tarif HPH en €/kWh
            - tarif_hch_eur_par_kwh : Tarif HCH en €/kWh
            - tarif_hpb_eur_par_kwh : Tarif HPB en €/kWh
            - tarif_hcb_eur_par_kwh : Tarif HCB en €/kWh
    
    Returns:
        DataFrame enrichi avec colonnes supplémentaires :
            - cout_hph_eur : Coût HPH en euros
            - cout_hch_eur : Coût HCH en euros
            - cout_hpb_eur : Coût HPB en euros
            - cout_hcb_eur : Coût HCB en euros
            - cout_total_eur : Coût total sur la période en euros
    
    Raises:
        ValueError: Si la période est invalide (date_debut > date_fin)
        KeyError: Si colonnes obligatoires manquantes dans df_releves
        TypeError: Si grille_tarifaire n'est pas un dictionnaire
    
    Example:
        >>> from datetime import datetime
        >>> import polars as pl
        >>> 
        >>> # Données d'exemple
        >>> df = pl.DataFrame({
        ...     "identifiant_pdl": ["PDL12345678901234"],
        ...     "date_releve": [datetime(2025, 1, 15)],
        ...     "energie_hph_kwh": [125.3],
        ...     "energie_hch_kwh": [234.7],
        ...     "energie_hpb_kwh": [156.2],
        ...     "energie_hcb_kwh": [289.4],
        ... })
        >>> 
        >>> # Grille tarifaire
        >>> tarifs = {
        ...     "tarif_hph_eur_par_kwh": 0.1821,
        ...     "tarif_hch_eur_par_kwh": 0.1249,
        ...     "tarif_hpb_eur_par_kwh": 0.1054,
        ...     "tarif_hcb_eur_par_kwh": 0.0823,
        ... }
        >>> 
        >>> # Calcul
        >>> df_couts = calculer_cout_periode(
        ...     df,
        ...     datetime(2025, 1, 1),
        ...     datetime(2025, 1, 31),
        ...     tarifs
        ... )
        >>> 
        >>> print(df_couts["cout_total_eur"][0])
        98.45
    
    Note:
        Les jours EJP sont comptabilisés dans le cadran HPH avec
        un coefficient tarifaire majoré spécifique (environ 3,76× le tarif HPB).
        
        La TVA n'est pas incluse dans ce calcul et doit être ajoutée
        séparément si nécessaire (taux standard : 20%).
    
    See Also:
        - calculer_consommation() : Pour calculer les énergies consommées
        - generer_facture() : Pour générer une facture complète
    
    References:
        - Cahier des charges Enedis v2.3, section 4.2
        - Arrêté tarifaire CRE 2025
        - Documentation TURPE 6
    
    Version:
        Ajouté en v1.0.0
        Modifié en v1.2.0 : Ajout support jours EJP
    """
    # Implémentation...
    pass
```

### Docstrings de Classes

```python
class MoteurFacturation:
    """Moteur de calcul de facturation électrique.
    
    Cette classe fournit les méthodes nécessaires pour calculer
    les factures d'électricité à partir de relevés de compteurs
    et de grilles tarifaires.
    
    Attributes:
        grille_tarifaire (dict): Grille tarifaire en vigueur
        taux_tva (float): Taux de TVA applicable (par défaut 0.20)
        historique_calculs (list): Historique des calculs effectués
    
    Example:
        >>> moteur = MoteurFacturation(TARIFS_ENEDIS_2025)
        >>> facture = moteur.calculer_facture(df_releves)
        >>> print(facture["cout_total_eur"].sum())
    """
    
    def __init__(self, grille_tarifaire: dict[str, float]):
        """Initialise le moteur de facturation.
        
        Args:
            grille_tarifaire: Dictionnaire des tarifs par cadran
        """
        self.grille_tarifaire = grille_tarifaire
        self.taux_tva = 0.20
        self.historique_calculs = []
```

---

### Commentaires de Section

```python
# ═══════════════════════════════════════════════════════════════
# VALIDATION DES DONNÉES
# ═══════════════════════════════════════════════════════════════

# Vérification de la cohérence des index (doivent être croissants)
# ...

# ═══════════════════════════════════════════════════════════════
# CALCULS DE CONSOMMATION
# ═══════════════════════════════════════════════════════════════

# Calcul des différences d'index entre relevés successifs
# ...

# ═══════════════════════════════════════════════════════════════
# CALCULS DE COÛTS
# ═══════════════════════════════════════════════════════════════

# Application de la grille tarifaire
# ...
```

### TODO et Annotations

```python
# TODO: Implémenter gestion des jours EJP avec tarif majoré
# FIXME: Bug dans le calcul quand changement d'heure été/hiver
# HACK: Solution temporaire en attendant correction API Enedis
# NOTE: Cette méthode sera dépréciée en v2.0
# OPTIMIZE: Possibilité d'optimisation avec vectorisation Polars
# WARNING: Cette fonction modifie le DataFrame en place
```

---

## 13. Vocabulaire Technique Standardisé

### Glossaire Français-Français (Termes Autorisés)

| Domaine | Concept | Terme Standard | Exemple |
|---------|---------|----------------|---------|
| **Identification** | Compteur | `compteur` | `identifiant_compteur` |
| | Point de Livraison | `pdl` | `identifiant_pdl` |
| | Identifiant | `identifiant` | `identifiant_unique` |
| **Temporel** | Relevé | `releve` | `date_releve` |
| | Période | `periode` | `periode_facturation` |
| | Date | `date` | `date_debut`, `date_fin` |
| | Heure | `heure` | `heure_releve` |
| **Énergie** | Index | `index` | `index_hph_kwh` |
| | Énergie | `energie` | `energie_hph_kwh` |
| | Consommation | `consommation` | `consommation_totale` |
| | Puissance | `puissance` | `puissance_souscrite_kva` |
| **Tarification** | Tarif | `tarif` | `tarif_hph_eur_par_kwh` |
| | Coût | `cout` | `cout_total_eur` |
| | Prix | `prix` | `prix_unitaire` |
| | Facture | `facture` | `generer_facture()` |
| | Grille tarifaire | `grille_tarifaire` | `appliquer_grille_tarifaire()` |
| **Acteurs** | Fournisseur | `fournisseur` | `identifiant_fournisseur` |
| | Gestionnaire | `gestionnaire` | `gestionnaire_reseau` |
| | Client | `client` | `identifiant_client` |
| **Statuts** | Actif | `actif` | `statut_actif` |
| | Résilié | `resilie` | `statut_resilie` |
| | Suspendu | `suspendu` | `statut_suspendu` |
| **Calculs** | Calculer | `calculer` | `calculer_consommation()` |
| | Générer | `generer` | `generer_facture()` |
| | Valider | `valider` | `valider_releve()` |
| | Agréger | `aggreger` | `aggreger_par_mois()` |

### Termes à Éviter (Ambigus)

| ❌ À Éviter | ✅ Préférer | Raison |
|------------|------------|--------|
| `mesure` | `releve` ou `index` | Trop générique |
| `valeur` | Terme spécifique (`index`, `energie`, `cout`) | Trop vague |
| `data` | `donnees` | Anglicisme |
| `id` | `identifiant` | Abréviation anglaise |
| `info` | `information` | Abréviation informelle |
| `config` | `configuration` | Abréviation à éviter |
| `param` | `parametre` | Abréviation à éviter |

---

## 14. Gestion des Accents

### Position Officielle

**Python 3 supporte parfaitement les accents** dans les identifiants (noms de variables, fonctions, classes).

### Recommandation Pragmatique

Pour **compatibilité maximale** et **éviter problèmes d'encodage** :

#### ✅ CODE : Sans Accents

```python
# ✅ Recommandé : code sans accents
energie_consommee = 125.5  # plutôt que énergie_consommée
periode_ete = (date(2025, 6, 1), date(2025, 9, 1))  # plutôt que été
cout_total = 1234.56  # plutôt que coût
electricite = "..."  # plutôt que électricité

def calculer_electricite():  # plutôt que calculer_électricité
    """Calcule..."""
    pass
```

#### ✅ DOCUMENTATION : Accents Complets

```python
def calculer_cout_periode():
    """Calcule le coût de consommation sur une période donnée.
    
    La période d'été bénéficie de tarifs préférentiels selon
    la réglementation CRE en vigueur.
    
    Note: L'été est défini du 1er juin au 31 août inclus.
    """
    pass
```

#### ✅ COLONNES DATAFRAME : Sans Accents (Recommandé)

```python
# ✅ Préféré : colonnes sans accents
df = pl.DataFrame({
    "identifiant_pdl": [...],
    "cout_total_eur": [...],      # "cout" plutôt que "coût"
    "energie_ete_kwh": [...],     # "ete" plutôt que "été"
    "periode_hiver": [...],        # OK (pas d'accent dans "période")
})
```

#### ✅ MESSAGES UTILISATEUR : Accents Complets

```python
# ✅ Messages avec accents complets
print("Période d'été détectée : application tarif préférentiel")
logger.info(f"Coût total calculé : {cout_total:.2f} €")
raise ValueError("La puissance souscrite doit être > 0")
```

### Table de Conversion Accents → Sans Accents

| Avec Accent | Sans Accent | Contexte |
|-------------|-------------|----------|
| `énergie` | `energie` | Variable/colonne |
| `été` | `ete` | Variable/colonne |
| `coût` | `cout` | Variable/colonne |
| `électricité` | `electricite` | Variable/fonction |
| `période` | `periode` | Variable/colonne |
| `résilié` | `resilie` | Variable/enum |
| `créer` | `creer` | Fonction |
| `générer` | `generer` | Fonction |
| `récupérer` | `recuperer` | Fonction |

### Justification

- **Raison 1** : Certains éditeurs/terminaux ont problèmes encodage UTF-8
- **Raison 2** : Saisie au clavier plus rapide (pas de touches spéciales)
- **Raison 3** : Compatibilité systèmes Windows/Linux/Mac garantie
- **Raison 4** : Évite bugs subtils liés à l'encodage

---

