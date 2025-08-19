# Plan d'Intégration ElectriFlux - Monorepo

> Focus sur la migration d'ElectriFlux vers le monorepo ElectriCore  
> Créé: 2025-01-19  
> Base: Décision architecturale du document 01-vision-architecture.md

## Objectif

Intégrer ElectriFlux dans ElectriCore pour créer un monorepo unifié, supprimant la duplication des schémas et simplifiant la maintenance.

## Structure Cible

### Organisation Finale
```
electricore/  (monorepo)
├── electricore/
│   ├── etl/              # 🆕 ElectriFlux intégré ici
│   │   ├── __init__.py
│   │   ├── extract.py    # SFTP, decrypt, unzip
│   │   ├── transform.py  # XML → DataFrame + validation Pandera
│   │   ├── load.py       # Stockage Parquet/DB
│   │   └── config/       # Mappings YAML Enedis
│   ├── models/           # 🆕 Schémas Pandera centralisés
│   │   ├── __init__.py
│   │   ├── perimetre.py  # HistoriquePérimètre
│   │   ├── releves.py    # RelevéIndex
│   │   └── energie.py    # PeriodeEnergie
│   ├── core/            # Business logic existant
│   │   ├── périmètre/
│   │   ├── relevés/
│   │   ├── énergies/
│   │   ├── taxes/
│   │   ├── abonnements/
│   │   └── services.py
│   └── inputs/          # ❓ À supprimer ou conserver temporairement
├── notebooks/           # Développement interactif
├── tests/
│   ├── unit/           # Tests par module
│   └── integration/    # 🆕 Tests end-to-end facilités
├── docs/
└── pyproject.toml      # Configuration unifiée
```

### Points Clés de l'Organisation

#### 1. `electricore/etl/` - ETL Complet
Reprend les fonctionnalités d'ElectriFlux mais avec accès direct aux modèles Pandera :

```python
# electricore/etl/transform.py
from electricore.models.perimetre import HistoriquePérimètre
from electricore.models.releves import RelevéIndex

def transform_c15(raw_xml_data: pd.DataFrame) -> HistoriquePérimètre:
    """Transform C15 raw data vers HistoriquePérimètre validé"""
    # Logique de mapping YAML → DataFrame
    # Validation Pandera directe
    return HistoriquePérimètre.validate(result)
```

#### 2. `electricore/models/` - Single Source of Truth
Tous les schémas Pandera centralisés, utilisables par ETL et Core :

```python
# electricore/models/perimetre.py
import pandera.pandas as pa
from pandera.typing import DataFrame, Series

class HistoriquePérimètre(pa.DataFrameModel):
    pdl: Series[str]
    Date_Evenement: Series[pd.DatetimeTZDtype] = pa.Field(dtype_kwargs={"tz": "Europe/Paris"})
    Ref_Situation_Contractuelle: Series[str]
    # ... autres champs
```

#### 3. Flux de Données Simplifié
```
Raw XML → ETL (transform.py) → Models (Pandera) → Core (business logic)
   ↓              ↓                 ↓              ↓
SFTP Enedis → electricore.etl → electricore.models → electricore.core
```

## Plan de Migration

### Étape 1 : Préparation (Avant migration)

#### 1.1 Audit du Code ElectriFlux
```bash
# Identifier les dépendances et fonctionnalités
find electriflux/ -name "*.py" | xargs grep -l "import"
```

**Inventaire à faire :**
- Dépendances Python spécifiques (paramiko, cryptography, etc.)
- Configuration SFTP et secrets
- Mappings YAML Enedis
- Tests existants à préserver

#### 1.2 Créer la Structure de Base
```bash
cd electricore/
mkdir -p electricore/{etl,models}
mkdir -p electricore/etl/config
mkdir -p tests/integration
```

### Étape 2 : Migration Progressive

#### 2.1 Migration des Schémas (Week 1)
```bash
# 1. Extraire les modèles Pandera existants vers electricore/models/
mv electricore/inputs/flux/modèles.py electricore/models/enedis_raw.py

# 2. Créer les modèles canoniques
# Manuellement créer electricore/models/{perimetre,releves,energie}.py
```

#### 2.2 Intégration ElectriFlux (Week 2)
```bash
# Ajouter ElectriFlux comme subtree (préserve l'historique)
git subtree add --prefix=temp_electriflux git@github.com:ton/electriflux.git main

# Réorganiser vers la structure cible
mv temp_electriflux/src/electriflux/* electricore/etl/
rm -rf temp_electriflux/
```

#### 2.3 Adaptation du Code (Week 2-3)
```python
# Avant (ElectriFlux)
from electriflux.models import SomeModel

# Après (Monorepo)
from electricore.models.perimetre import HistoriquePérimètre
```

#### 2.4 Migration des Tests (Week 3)
```bash
# Migrer les tests ElectriFlux
mv electriflux_tests/* tests/unit/etl/

# Créer tests d'intégration end-to-end
# tests/integration/test_full_pipeline.py
```

### Étape 3 : Nettoyage et Optimisation

#### 3.1 Suppression des Doublons
- Supprimer `electricore/inputs/flux/` (redondant avec ETL)
- Fusionner les configurations
- Unifier les dépendances dans `pyproject.toml`

#### 3.2 Validation Complète
```python
# Test d'intégration complet
def test_full_etl_to_core():
    # ETL: Raw XML → Validated DataFrames
    raw_data = etl.extract_from_sftp()
    validated_data = etl.transform_and_validate(raw_data)
    
    # Core: Business logic
    result = core.pipeline_energie(validated_data)
    
    assert result.validate()  # Pandera OK
    assert len(result) > 0    # Données traitées
```

## Dépendances et Configuration

### pyproject.toml Unifié
```toml
[project]
name = "electricore"
dependencies = [
    # Core existant
    "pandas>=2.2.3",
    "pandera>=0.24.0",
    "babel>=2.17.0",
    "toolz>=0.12.0",
    
    # ETL (ex-ElectriFlux) 
    "paramiko>=3.0.0",      # SFTP
    "cryptography>=40.0.0", # Decrypt
    "lxml>=4.9.0",          # XML parsing
    "pyyaml>=6.0.0",        # Config YAML
]

[project.optional-dependencies]
etl = ["paramiko", "cryptography", "lxml", "pyyaml"]
dev = ["pytest", "marimo", "icecream"]
```

### Configuration ETL
```yaml
# electricore/etl/config/enedis_mapping.yaml
flux_c15:
  fields:
    pdl: "Point_De_Livraison/Id_Point_De_Livraison"
    Date_Evenement: "Evenement/Date_Evenement"
    # ... mappings XML vers DataFrame
    
flux_r151:
  fields:
    pdl: "Releve/Point_De_Livraison"
    Date_Releve: "Releve/Date_Releve"
    # ... mappings
```

## Points d'Attention

### 1. Gestion des Secrets
```python
# electricore/etl/config.py
import os
from pathlib import Path

def get_sftp_config():
    return {
        'host': os.getenv('ENEDIS_SFTP_HOST'),
        'username': os.getenv('ENEDIS_USERNAME'), 
        'key_file': Path.home() / '.ssh' / 'enedis_key.pem'
    }
```

### 2. Rétrocompatibilité Temporaire
Maintenir une interface de compatibilité pendant la migration :

```python
# electricore/compat.py - Temporaire
from electricore.etl.transform import transform_c15 as lire_flux_c15
from electricore.etl.transform import transform_r151 as lire_flux_r151

# Permet aux notebooks existants de continuer à fonctionner
```

### 3. Tests de Régression
```python
# tests/integration/test_migration_compatibility.py
def test_same_results_before_after_migration():
    """Vérifie que la migration ne change pas les résultats"""
    # Données de test
    # Comparaison old vs new pipeline
    assert old_result.equals(new_result)
```

## Validation du Succès

### Critères d'Acceptation
- [ ] **Fonctionnalité** : Tous les tests ElectriFlux passent dans le monorepo
- [ ] **Performance** : Pas de régression sur les temps de traitement
- [ ] **Simplicité** : Un seul `pyproject.toml`, une seule CI/CD
- [ ] **Maintenabilité** : Schémas Pandera unifiés et partagés
- [ ] **Tests** : Suite d'intégration end-to-end fonctionnelle

### Timeline Estimée
- **Week 1** : Préparation et migration des schémas
- **Week 2** : Intégration du code ElectriFlux  
- **Week 3** : Adaptation, tests, nettoyage
- **Week 4** : Validation finale et décommissioning de l'ancien repo

---

*Ce plan de migration constitue la feuille de route concrète vers le monorepo unifié.*