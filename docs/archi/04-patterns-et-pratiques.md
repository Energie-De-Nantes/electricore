# Patterns et Pratiques - Monorepo ElectriCore

> Guide des bonnes pratiques pour développer dans le monorepo ElectriCore  
> Créé: 2025-01-19  
> Public: Développeurs contribuant au projet

## Organisation du Code

### Structure Recommandée

```
electricore/
├── electricore/                  # Package principal Python
│   ├── __init__.py
│   ├── etl/                     # Extract, Transform, Load
│   │   ├── __init__.py
│   │   ├── extract.py           # SFTP, decrypt, unzip
│   │   ├── transform.py         # XML → DataFrame validated
│   │   ├── load.py              # Stockage (Parquet/DB)
│   │   └── config/              # Mappings YAML
│   │       ├── enedis_c15.yaml
│   │       └── enedis_r151.yaml
│   ├── models/                  # Schémas Pandera (Single Source of Truth)
│   │   ├── __init__.py
│   │   ├── base.py              # Types communs, utilitaires
│   │   ├── perimetre.py         # HistoriquePérimètre
│   │   ├── releves.py           # RelevéIndex, RequêteRelevé
│   │   └── energie.py           # PeriodeEnergie, BaseCalculEnergies
│   ├── core/                    # Business logic métier
│   │   ├── __init__.py
│   │   ├── périmètre/
│   │   ├── relevés/
│   │   ├── énergies/
│   │   ├── taxes/
│   │   ├── abonnements/
│   │   └── services.py          # Orchestration (facturation, pipeline_energie)
│   └── api/                     # Future API REST (Phase 3)
│       ├── __init__.py
│       ├── routes/
│       └── schemas/
├── notebooks/                   # Développement interactif Marimo
│   ├── pipeline_energie_dev.py
│   ├── data_exploration.py
│   └── performance_analysis.py
├── tests/                       # Tests complets
│   ├── __init__.py
│   ├── unit/                    # Tests par module
│   │   ├── test_etl/
│   │   ├── test_models/
│   │   └── test_core/
│   ├── integration/             # Tests end-to-end
│   │   ├── test_full_pipeline.py
│   │   └── test_etl_to_core.py
│   └── fixtures/                # Données de test partagées
│       ├── sample_c15.xml
│       └── sample_r151.xml
├── scripts/                     # Utilitaires et automatisation
│   ├── migrate_data.py
│   ├── benchmark.py
│   └── generate_docs.py
├── docs/                        # Documentation
│   ├── archi/                   # Documents architecturaux
│   ├── api/                     # Documentation API (auto-générée)
│   └── guides/                  # Guides utilisateurs
└── pyproject.toml              # Configuration unique
```

### Principes d'Organisation

#### 1. Séparation des Responsabilités
- **ETL** : Responsable de l'extraction et transformation des données brutes
- **Models** : Définition des contrats de données (Pandera)
- **Core** : Logique métier pure, agnostique de la source
- **API** : Exposition des fonctionnalités (future)

#### 2. Dependencies Flow
```
ETL → Models ← Core
 ↓      ↑       ↓
Load   Validate Process
```

**Règle** : Core ne dépend jamais d'ETL, seulement de Models.

#### 3. Import Pattern
```python
# ✅ Correct
from electricore.models.perimetre import HistoriquePérimètre
from electricore.core.services import pipeline_energie

# ❌ Éviter
from electricore.etl.extract import sftp_client  # Dans core/
from electricore.core.services import *         # Import global
```

## Conventions de Nommage

### Modules et Packages
```python
# Snake_case pour les modules
electricore.etl.extract
electricore.core.énergies.fonctions  # OK: terme métier français

# PascalCase pour les classes
class HistoriquePérimètre(pa.DataFrameModel):
class RelevéIndex(pa.DataFrameModel):
```

### Fonctions et Variables
```python
# Snake_case français acceptable pour domaine métier
def préparer_base_énergies(historique, deb, fin):
    période_données = extraire_période(deb, fin, historique)
    return période_données

# Variables temporaires en anglais OK
def process_data():
    temp_df = pd.DataFrame()
    result = temp_df.pipe(transform)
    return result
```

### Fichiers et Dossiers
```
# Dossiers: minuscules avec tirets
docs/architecture-decisions/
tests/integration-tests/

# Fichiers Python: snake_case
pipeline_energie_dev.py
test_full_etl_process.py

# Fichiers config: extensions claires
enedis_c15.yaml
sample_data.parquet
```

## Patterns de Code

### 1. Pandera Models Pattern

#### Définition Centralisée
```python
# electricore/models/base.py
import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame, Series

# Types communs réutilisables
PDLType = pa.Field(str, regex=r'^\d{14}$')  # Format PDL Enedis
DateTZType = pa.Field(dtype=pd.DatetimeTZDtype(tz='Europe/Paris'))

class BaseEnergieModel(pa.DataFrameModel):
    """Classe de base avec validations communes"""
    pdl: Series[str] = PDLType
    
    @pa.check('pdl')
    def pdl_valide(cls, pdl_series):
        return pdl_series.str.len() == 14
```

#### Usage dans ETL
```python
# electricore/etl/transform.py
from electricore.models.perimetre import HistoriquePérimètre

@pa.check_types
def transform_c15(raw_data: pd.DataFrame) -> DataFrame[HistoriquePérimètre]:
    """Transform C15 avec validation automatique au retour"""
    result = apply_yaml_mapping(raw_data, 'enedis_c15.yaml')
    # La validation Pandera se fait automatiquement grâce au décorateur
    return result
```

#### Usage dans Core
```python
# electricore/core/services.py  
@pa.check_types
def pipeline_energie(
    historique: DataFrame[HistoriquePérimètre],
    relevés: DataFrame[RelevéIndex]
) -> DataFrame[PeriodeEnergie]:
    """Pipeline avec validation d'entrée ET sortie"""
    # La logique métier assume des données validées
    return result
```

### 2. Error Handling Pattern

#### ETL Layer
```python
# electricore/etl/extract.py
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class ETLError(Exception):
    """Erreurs spécifiques à l'ETL"""
    pass

async def extract_from_sftp(file_pattern: str) -> Optional[List[Path]]:
    """Extract avec gestion d'erreur robuste"""
    try:
        files = await sftp_client.list_files(file_pattern)
        logger.info(f"Found {len(files)} files matching {file_pattern}")
        return files
    except ConnectionError as e:
        logger.error(f"SFTP connection failed: {e}")
        raise ETLError(f"Cannot connect to SFTP: {e}") from e
    except Exception as e:
        logger.error(f"Unexpected error in extract: {e}")
        raise ETLError(f"Extract failed: {e}") from e
```

#### Core Layer
```python
# electricore/core/services.py
from electricore.models.exceptions import ValidationError, BusinessLogicError

def pipeline_energie(historique, relevés):
    """Pipeline avec validation métier"""
    try:
        # Validation des pré-conditions métier
        if historique.empty:
            raise BusinessLogicError("Historique périmètre vide")
        
        result = process_pipeline(historique, relevés)
        
        # Post-conditions
        if result.empty:
            logger.warning("Pipeline returned empty result")
        
        return result
    except pa.errors.SchemaError as e:
        raise ValidationError(f"Schema validation failed: {e}") from e
```

### 3. Configuration Pattern

#### Configuration Centralisée
```python
# electricore/config.py
from pathlib import Path
from typing import Dict, Any
import yaml
import os

class Config:
    """Configuration centralisée avec overrides d'environnement"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        config_file = self.base_dir / 'config.yaml'
        with open(config_file) as f:
            config = yaml.safe_load(f)
        
        # Override avec variables d'environnement
        config['sftp']['host'] = os.getenv('ENEDIS_SFTP_HOST', config['sftp']['host'])
        return config
    
    @property 
    def sftp_config(self) -> Dict[str, str]:
        return self._config['sftp']

# Singleton global
config = Config()
```

#### Usage
```python
# Dans n'importe quel module
from electricore.config import config

def connect_sftp():
    return paramiko.SSHClient().connect(**config.sftp_config)
```

### 4. Testing Patterns

#### Fixtures Partagées
```python
# tests/conftest.py
import pytest
import pandas as pd
from electricore.models.perimetre import HistoriquePérimètre

@pytest.fixture
def sample_historique():
    """Historique de test validé"""
    data = {
        'pdl': ['12345678901234', '12345678901235'],
        'Date_Evenement': pd.to_datetime(['2024-01-01', '2024-01-02'], tz='Europe/Paris'),
        'Ref_Situation_Contractuelle': ['REF001', 'REF002']
    }
    return HistoriquePérimètre.validate(pd.DataFrame(data))

@pytest.fixture  
def sample_releves():
    """Relevés de test validés"""
    # ... similaire
```

#### Tests Paramétrés
```python
# tests/unit/test_core/test_energies.py
import pytest
from electricore.core.énergies.fonctions import calculer_energies

@pytest.mark.parametrize("inclure_jour_fin,expected_days", [
    (False, 30),
    (True, 31),
])
def test_calculer_energies_duree(sample_base_energies, inclure_jour_fin, expected_days):
    """Test la logique d'inclusion du jour de fin"""
    result = calculer_energies(sample_base_energies, inclure_jour_fin)
    assert result['j'].iloc[0] == expected_days
```

#### Tests d'Intégration
```python  
# tests/integration/test_full_pipeline.py
def test_complete_etl_to_business_logic(tmp_path):
    """Test end-to-end avec fichiers réels"""
    # Arrange: Préparer données test
    sample_files = copy_sample_files(tmp_path)
    
    # Act: Pipeline complet
    extracted = etl.extract(sample_files)
    transformed = etl.transform(extracted)
    business_result = core.pipeline_energie(transformed['historique'], transformed['relevés'])
    
    # Assert: Validation métier
    assert len(business_result) > 0
    assert business_result['data_complete'].all()
    assert not business_result['periode_irreguliere'].any()
```

## Patterns d'Évolutivité

### 1. Plugin Architecture (Future)
```python
# electricore/plugins/base.py
from abc import ABC, abstractmethod

class DataConnector(ABC):
    """Interface pour nouveaux connecteurs de données"""
    
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        pass
    
    @abstractmethod 
    def get_schema(self) -> pa.DataFrameModel:
        pass

# electricore/plugins/engie.py (futur)
class EngieConnector(DataConnector):
    """Connecteur pour données Engie (différent d'Enedis)"""
    pass
```

### 2. Versioning des Schémas
```python
# electricore/models/versioning.py
from typing import Union
from electricore.models.perimetre import HistoriquePérimètreV1, HistoriquePérimètreV2

def migrate_schema(data: pd.DataFrame, from_version: str, to_version: str) -> pd.DataFrame:
    """Migration automatique entre versions de schémas"""
    migrations = {
        ('v1', 'v2'): migrate_perimetre_v1_to_v2,
        # autres migrations...
    }
    return migrations[(from_version, to_version)](data)
```

### 3. Async/Await Ready
```python
# Préparer le terrain pour l'async (Phase 3)
from typing import Union, Awaitable

# Type hints compatibles sync/async
ProcessResult = Union[pd.DataFrame, Awaitable[pd.DataFrame]]

async def async_pipeline_energie(historique, relevés) -> pd.DataFrame:
    """Version async du pipeline pour futures performances"""
    # Parallélisation des traitements lourds
    tasks = [
        asyncio.create_task(process_perimetre(historique)),
        asyncio.create_task(process_releves(relevés))
    ]
    perimetre_result, releves_result = await asyncio.gather(*tasks)
    return combine_results(perimetre_result, releves_result)
```

## Outils et Automatisation

### Pre-commit Hooks
```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.1.0
    hooks:
      - id: black
  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort
  - repo: local
    hooks:
      - id: pytest-check
        name: pytest-check
        entry: pytest tests/unit
        language: system
        always_run: true
```

### Makefile Utilitaire
```makefile
# Makefile
.PHONY: test install lint format

install:
	poetry install

test:
	poetry run pytest tests/ -v

test-integration:
	poetry run pytest tests/integration/ -v

lint:
	poetry run ruff check electricore/
	poetry run mypy electricore/

format:
	poetry run black electricore/ tests/
	poetry run isort electricore/ tests/

notebook:
	poetry run marimo edit notebooks/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete
```

### Documentation Automatique
```python
# scripts/generate_docs.py
"""Génère la documentation API automatiquement"""
import pdoc

def generate_api_docs():
    pdoc.pdoc('electricore', output_dir='docs/api/')

if __name__ == '__main__':
    generate_api_docs()
```

## Métriques et Monitoring

### Logging Structuré
```python
# electricore/utils/logging.py
import logging
import json
from datetime import datetime

class StructuredLogger:
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        
    def log_processing_start(self, pipeline: str, data_size: int):
        self.logger.info(json.dumps({
            'event': 'processing_start',
            'pipeline': pipeline,
            'data_size': data_size,
            'timestamp': datetime.utcnow().isoformat()
        }))

# Usage
logger = StructuredLogger('electricore.pipeline')
logger.log_processing_start('energie', len(historique))
```

### Performance Monitoring
```python
# electricore/utils/metrics.py
import time
import functools
from typing import Callable

def monitor_performance(func: Callable) -> Callable:
    """Décorateur pour monitorer les performances"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        
        # Log metrics (future: send to Prometheus)
        print(f"{func.__name__} took {duration:.2f}s")
        return result
    return wrapper

# Usage
@monitor_performance
def pipeline_energie(historique, relevés):
    # ... logique métier
    pass
```

---

*Ce guide constitue notre standard de développement. Il évoluera avec les besoins du projet et les retours de l'équipe.*