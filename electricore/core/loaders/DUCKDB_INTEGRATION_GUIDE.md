# Guide d'Intégration DuckDB - ElectriCore

## Vue d'ensemble

Cette intégration permet d'utiliser DuckDB comme source de données pour les pipelines ElectriCore modernes basés sur **Polars pur**. Fini pandas - nous migrons vers une architecture 100% Polars pour des performances optimales.

## 🚀 Fonctionnalités

- **Polars pur** : Architecture moderne sans dépendances pandas legacy
- **Chargement lazy** : Utilise Polars LazyFrames pour l'optimisation des requêtes
- **Expressions composables** : Utilise les expressions Polars pour transformations pures
- **Filtrage SQL** : Pousse les filtres vers DuckDB pour de meilleures performances
- **Zero-copy** : Transferts de données optimaux entre DuckDB et Polars

## 📖 Utilisation

### 1. Chargement Direct depuis DuckDB

```python
from electricore.core.loaders.duckdb_loader import load_historique_perimetre, load_releves

# Charger l'historique de périmètre
historique_lf = load_historique_perimetre(
    filters={"Date_Evenement": ">= '2024-01-01'"},
    limit=1000
)

# Charger les relevés
releves_lf = load_releves(
    filters={"pdl": ["PDL123", "PDL456"]},
    limit=5000
)

# Collecter les données
historique_df = historique_lf.collect()
releves_df = releves_lf.collect()
```

### 2. Pipeline Polars Moderne

```python
from electricore.core.pipeline_perimetre import pipeline_perimetre_polars

# Depuis DuckDB avec filtres - retourne un LazyFrame
result_lf = pipeline_perimetre_polars(
    source="duckdb",  # ou None pour défaut
    filters={"Date_Evenement": ">= '2024-01-01'"},
    limit=10000
)

# Collecter quand nécessaire
result_df = result_lf.collect()

# Depuis LazyFrame existant - pipeline composable
result_lf = pipeline_perimetre_polars(source=mon_lazyframe)

# Avec chemin de base personnalisé
result_lf = pipeline_perimetre_polars(
    source="/custom/path/ma_base.duckdb",
    filters={"pdl": ["PDL123"]}
)

# Chaîner avec d'autres transformations Polars
final_result = (
    pipeline_perimetre_polars(source="duckdb")
    .filter(pl.col("impacte_energie") == True)
    .select(["pdl", "Date_Evenement", "resume_modification"])
    .collect()
)
```

### 3. Requêtes Personnalisées

```python
from electricore.core.loaders.duckdb_loader import execute_custom_query

# Requête personnalisée (lazy)
lf = execute_custom_query("""
    SELECT pdl, COUNT(*) as nb_evenements
    FROM enedis_production.flux_c15
    WHERE Date_Evenement >= '2024-01-01'
    GROUP BY pdl
    ORDER BY nb_evenements DESC
""", lazy=True)

# Requête personnalisée (eager)
df = execute_custom_query("""
    SELECT DISTINCT Evenement_Declencheur
    FROM enedis_production.flux_c15
""", lazy=False)
```

## 🔧 Configuration

### Configuration Database (optionnelle)

Le fichier `electricore/config/database.yaml` contient la configuration centralisée :

```yaml
# Configuration par défaut
default_database:
  path: "electricore/etl/flux_enedis.duckdb"
  read_only: true

# Filtres prédéfinis
predefined_filters:
  last_month:
    Date_Evenement: ">= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')"

  current_year:
    Date_Evenement: ">= DATE_TRUNC('year', CURRENT_DATE)"
```

## 📊 Structure des Données

### Tables Principales

- **`enedis_production.flux_c15`** : Historique des événements contractuels
- **`enedis_production.flux_r151`** : Relevés périodiques
- **`enedis_production.flux_r15`** : Relevés avec événements

### Mapping des Colonnes

Le loader adapte automatiquement les colonnes DuckDB aux modèles Pandera :

```python
# DuckDB → Modèle Pandera
date_evenement → Date_Evenement
pdl → pdl
avant_hp → Avant_HP (avec cast DOUBLE)
apres_hc → Après_HC (avec cast DOUBLE)
```

## ⚡ Performances

### Optimisations Automatiques

- **Lazy Evaluation** : Les transformations sont optimisées par Polars
- **Pushdown Filters** : Les filtres sont appliqués dans DuckDB
- **Type Casting** : Conversion des types dans la requête SQL

### Exemple de Performance

```python
# Efficient: filtre poussé vers DuckDB
lf = load_historique_perimetre(
    filters={"Date_Evenement": ">= '2024-01-01'", "pdl": "PDL123"},
    limit=1000
)

# Less efficient: filtre après chargement
lf = load_historique_perimetre(limit=10000)
df = lf.filter(pl.col("pdl") == "PDL123").collect()
```

## 🧪 Tests

### Lancer les Tests

```bash
# Tests unitaires du loader
poetry run pytest tests/core/loaders/test_duckdb_loader.py -v

# Tests d'intégration pipeline Polars
poetry run pytest tests/core/test_pipeline_perimetre_duckdb.py -v

# Tests avec données réelles (si DB disponible)
poetry run pytest tests/core/ -k "real_database" -v
```

### Test Manuel Rapide

```python
from electricore.core.loaders.duckdb_loader import get_available_tables

# Vérifier la connectivité
tables = get_available_tables()
print(f"Tables disponibles: {len(tables)}")

# Test de pipeline Polars moderne
from electricore.core.pipeline_perimetre import pipeline_perimetre_polars
result_lf = pipeline_perimetre_polars(source="duckdb", limit=5)
result_df = result_lf.collect()
print(f"Pipeline Polars OK: {len(result_df)} lignes")

# Test des expressions Polars
print("Expressions disponibles:")
from electricore.core.pipelines_polars.perimetre_polars import (
    expr_impacte_abonnement,
    expr_impacte_energie,
    expr_resume_modification
)
```

## 🔄 Migration vers Polars Pur

### Code Legacy Pandas (à migrer)

```python
# Code pandas legacy - en cours de migration
from electricore.core.loaders import charger_historique
historique = charger_historique("fichier.parquet")  # pandas DataFrame
result = pipeline_perimetre(historique)  # pandas processing
```

### Code Moderne Polars

```python
# Code Polars moderne - recommandé
from electricore.core.pipeline_perimetre import pipeline_perimetre_polars

# LazyFrame optimisé avec chargement DuckDB
result_lf = pipeline_perimetre_polars(
    source="duckdb",
    filters={"Date_Evenement": ">= '2024-01-01'"}
)

# Transformation continue avec expressions Polars
final = (
    result_lf
    .filter(pl.col("impacte_abonnement"))
    .group_by("pdl")
    .agg([
        pl.count().alias("nb_evenements"),
        pl.col("Date_Evenement").min().alias("premier_evenement")
    ])
    .collect()
)
```

### Expressions Polars Composables

```python
# Utiliser les expressions pures directement
from electricore.core.pipelines_polars.perimetre_polars import (
    expr_impacte_abonnement,
    expr_impacte_energie,
    expr_resume_modification
)

# Pipeline personnalisé avec expressions
custom_pipeline = (
    load_historique_perimetre(source="duckdb")
    .with_columns([
        expr_impacte_abonnement().alias("impacte_abonnement"),
        expr_impacte_energie().alias("impacte_energie"),
        expr_resume_modification().alias("resume_modification")
    ])
    .filter(pl.col("impacte_energie"))
)
```

## 🐛 Dépannage

### Erreurs Communes

1. **Base non trouvée** : Vérifier le chemin `electricore/etl/flux_enedis.duckdb`
2. **Import DuckDB** : `pip install duckdb` si manquant
3. **Colonnes manquantes** : Vérifier la structure des tables source

### Debug

```python
# Activer le logging SQL (dans config/database.yaml)
logging:
  log_queries: true

# Tester une requête simple
from electricore.core.loaders.duckdb_loader import execute_custom_query
df = execute_custom_query("SELECT COUNT(*) FROM enedis_production.flux_c15")
print(df)
```

## 📈 Prochaines Étapes

1. **Migration Polars complète** : Finaliser la migration des pipelines relevés, énergies, taxes vers Polars pur
2. **Expressions avancées** : Développer plus d'expressions composables pour logiques métier complexes
3. **Optimisation lazy** : Maximiser les optimisations Polars avec pushdown vers DuckDB
4. **Monitoring** des performances en production avec métriques Polars
5. **Extension** à d'autres sources (Arrow, Parquet, PostgreSQL) via Polars

---

## 🎯 Résumé des Avantages Polars

- **Performance maximale** : Zero-copy, vectorisation SIMD, multi-threading
- **Lazy evaluation** : Optimisations automatiques des requêtes
- **Expressions pures** : Code fonctionnel composable et testable
- **Écosystème moderne** : Compatible Arrow, DuckDB, Cloud
- **Future-proof** : Abandon des dépendances pandas legacy

Cette architecture **Polars pur + DuckDB** positionne ElectriCore comme une solution moderne haute performance pour le traitement de données énergétiques à l'échelle.