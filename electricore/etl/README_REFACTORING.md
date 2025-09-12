# Architecture ETL Refactorée - Transformers Modulaires

## 🎯 Vue d'ensemble

L'ETL Enedis a été refactorisé pour passer d'une architecture **quasi-monolithique** à une architecture **modulaire avec chaînage de transformers**.

### Avant (Architecture monolithique)
```python
@dlt.resource
def xml_resource():
    # Tout dans une seule fonction
    sftp_files = filesystem(...)
    for file in sftp_files:
        encrypted_data = read_sftp_file(file)
        decrypted_data = decrypt_file_aes(encrypted_data, key, iv)
        xml_files = extract_files_from_zip(decrypted_data)
        for xml_name, xml_content in xml_files:
            for record in xml_to_dict_from_bytes(xml_content, ...):
                yield record
```

### Après (Architecture modulaire)
```python
# Chaînage de transformers spécialisés
pipeline = (
    sftp_resource |          # SFTP files
    decrypt_transformer |    # AES decryption
    unzip_transformer |      # ZIP extraction + filtering
    xml_parser_transformer   # XML parsing
).with_name("flux_r151")
```

## 📁 Structure des fichiers

```
electricore/etl/
├── sources/
│   ├── base.py                    # 🆕 Resource SFTP réutilisable
│   ├── sftp_enedis.py            # 🔄 Ancienne architecture (conservée)
│   ├── sftp_enedis_modular.py    # 🆕 Test architecture modulaire
│   └── sftp_enedis_refactored.py # 🆕 Architecture complète refactorée
├── transformers/                  # 🆕 Dossier des transformers
│   ├── __init__.py
│   ├── crypto.py                 # 🆕 Transformer déchiffrement AES
│   ├── archive.py                # 🆕 Transformer extraction ZIP
│   └── parsers.py                # 🆕 Transformers XML/CSV Polars
├── lib/                          # ✅ Fonctions pures (inchangé)
│   ├── crypto.py
│   ├── processors.py
│   ├── transformers.py
│   └── xml_parser.py
├── test_modular_architecture.py  # 🆕 Script de test
├── migration_guide.py            # 🆕 Guide de migration
└── README_REFACTORING.md         # 🆕 Ce fichier
```

## 🔗 Chaînes de transformation

### Flux XML (R15, R151, C15, F12, F15)
```
SFTP Files → Decrypt AES → Unzip XML → Parse XML → Table DuckDB
```

### Flux CSV (R64)
```  
SFTP Files → Decrypt AES → Unzip CSV → Parse CSV (Polars) → Table DuckDB
```

## 🧪 Comment tester

### Test simple
```bash
cd electricore/etl
python test_modular_architecture.py
```

### Test complet avec votre configuration
```python
from sources.sftp_enedis_refactored import sftp_flux_enedis_refactored
from config.settings import FLUX_CONFIG  # Votre config existante

# Créer pipeline
pipeline = dlt.pipeline(
    pipeline_name="test_refactored",
    destination="duckdb",
    dataset_name="enedis_refactored"
)

# Charger avec nouvelle architecture
source = sftp_flux_enedis_refactored(FLUX_CONFIG)
load_info = pipeline.run(source)
```

## ✅ Bénéfices validés

### 🧪 Testabilité améliorée
- Chaque transformer est testable isolément
- Mocking facilité (un transformer à la fois)
- Tests unitaires plus ciblés

### 🔄 Réutilisabilité
- `decrypt_transformer` utilisé par tous les flux
- `unzip_transformer` commun XML et CSV  
- `xml_parser_transformer` configurable pour tous les XML

### 🛠️ Maintenabilité
- Séparation claire des responsabilités
- Débug plus facile (erreurs isolées par transformer)
- Ajout de nouveaux flux simplifié

### ⚡ Performance
- Clés AES chargées une seule fois (optimisation)
- Polars préservé pour les CSV
- Possibilité d'ajouter `@dlt.defer` pour parallélisation

## 📋 Checklist de migration

- [x] Créer modules de base (sources, transformers)
- [x] Implémenter chaînage pour tous les flux
- [x] Scripts de test et comparaison
- [ ] Tests unitaires des transformers
- [ ] Validation sur données réelles
- [ ] Migration progressive flux par flux
- [ ] Mesures de performance
- [ ] Mise en production

## 🚀 Utilisation

### Migration douce
L'ancienne interface est préservée via un alias :
```python
# Utilise la nouvelle architecture avec l'ancienne interface
from sources.sftp_enedis_refactored import sftp_flux_enedis_multi_v2 as sftp_flux_enedis_multi
```

### Utilisation directe de la nouvelle architecture
```python
from sources.sftp_enedis_refactored import sftp_flux_enedis_refactored

source = sftp_flux_enedis_refactored(flux_config)
pipeline.run(source)
```

## 🔮 Évolutions futures possibles

Grâce à l'architecture modulaire, il devient facile d'ajouter :

### Nouveaux transformers
- **Validation Pandera** : `| pandera_validator`
- **Enrichissement API** : `| api_enrichment_transformer`
- **Calculs métier** : `| business_logic_transformer`

### Parallélisation
```python
@dlt.defer  # Parallélisation automatique
@dlt.transformer
def heavy_processing_transformer(items):
    # Traitement lourd parallélisé
    return processed_items
```

### Chaînage conditionnel
```python
# Différentes chaînes selon le type de flux
if flux_type == "R64":
    pipeline = sftp | decrypt | unzip | csv_parser
else:
    pipeline = sftp | decrypt | unzip | xml_parser | business_rules
```

## 💡 Recommandations

1. **Commencer petit** : Testez sur R151 d'abord
2. **Migration progressive** : Un flux à la fois avec validation
3. **Tests systématiques** : Comparer ancienne vs nouvelle version
4. **Monitoring** : Surveillez les performances pendant la migration

---

**🎉 Résultat** : Une architecture ETL modulaire, testable, maintenable et évolutive, prête pour les besoins futurs d'ElectriCore !