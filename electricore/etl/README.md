# ETL ElectriCore

Pipeline ETL pour les flux énergétiques Enedis avec architecture modulaire et fonctionnelle.

## 🏗️ Architecture

```
electricore/etl/
├── lib/                     # Fonctions pures, aucune dépendance DLT
│   ├── crypto.py           # Cryptographie AES (decrypt_file_aes, load_aes_credentials)
│   ├── xml_parser.py       # Parsing XML avec lxml (xml_to_dict_from_bytes)
│   ├── transformers.py     # Transformation des données (enrich_record)
│   ├── processors.py       # Orchestration fonctionnelle (process_flux_items)
│   └── logging.py          # Helpers de logging structuré
│
├── sources/                 # Sources DLT (@dlt.source)
│   └── sftp_enedis.py      # Source SFTP Enedis multi-ressources
│
├── pipelines/               # Orchestration et exécution DLT
│   └── flux_enedis.py      # Pipeline principal avec vérification
│
├── config/                  # Configuration centralisée
│   ├── settings.py         # Chargement YAML et constantes
│   └── flux.yaml           # Configuration des flux (C15, F12, F15, R15, R151)
│
├── run_pipeline.py          # Point d'entrée simple
└── .dlt/                    # Configuration DLT
    ├── config.toml
    └── secrets.toml
```

## 🚀 Utilisation

### Exécution Simple
```bash
# Depuis le dossier etl/
poetry run python run_pipeline.py
```

### Exécution Avancée
```bash
# Pipeline principal avec configuration custom
poetry run python -m pipelines.flux_enedis

# Test des fonctions pures (sans DLT)
poetry run python -c "from lib.crypto import decrypt_file_aes; print('✅ Fonctions pures OK')"
```

### Commandes DLT Utiles
```bash
# Vérifier l'état du pipeline
poetry run dlt pipeline flux_enedis info

# Reset complet si nécessaire
poetry run dlt pipeline flux_enedis drop --drop-all

# Logs détaillés
poetry run dlt pipeline flux_enedis trace
```

## 📊 Flux Supportés

| Flux | Description | Tables générées |
|------|-------------|-----------------|
| **C15** | Changements contractuels | `flux_c15` |
| **F12** | Facturation distributeur | `flux_f12` |
| **F15** | Facturation détaillée | `flux_f15_detail` |
| **R15** | Relevés index | `flux_r15`, `flux_r15_acc` |
| **R151** | Relevés courbe de charge | `flux_r151` |

## 🔧 Configuration

### Secrets DLT (`.dlt/secrets.toml`)
```toml
[sftp]
url = "sftp://user:pass@host/path/"
file_pattern = "**/*.zip"

[aes]
key = "hex_encoded_key"
iv = "hex_encoded_iv"
```

### Configuration des Flux (`config/flux.yaml`)
Chaque flux définit :
- `zip_pattern` : Pattern des fichiers ZIP à traiter
- `xml_configs` : Liste des configurations XML avec row_level, data_fields, nested_fields

## 🎯 Avantages de cette Architecture

✅ **Séparation claire** : Fonctions pures vs logique DLT  
✅ **Testabilité** : `lib/` testable sans dépendances DLT  
✅ **Extensibilité** : Ajouter de nouvelles sources dans `sources/`  
✅ **Maintenabilité** : Responsabilités claires par module  
✅ **Réutilisabilité** : Fonctions pures réutilisables dans d'autres contextes  

## 🔄 Incrémental

Le pipeline utilise l'incrémental DLT basé sur `modification_date` des fichiers SFTP.
- **1er run** : Charge tous les fichiers
- **Runs suivants** : Charge uniquement les nouveaux fichiers

## 🔮 Extensions Futures

- `sources/api_enedis.py` : API REST Enedis
- `sources/sftp_axpo.py` : SFTP Axpo
- `sources/odoo_connector.py` : Import depuis Odoo
- `lib/validators.py` : Validation Pandera des données