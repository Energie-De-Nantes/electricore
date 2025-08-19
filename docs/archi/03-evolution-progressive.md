# Évolution Progressive - Roadmap ElectriCore

> Roadmap détaillée de l'évolution d'ElectriCore vers un système complet  
> Créé: 2025-01-19  
> Horizon: 12-18 mois

## Vue d'Ensemble

L'évolution d'ElectriCore se fait en 4 phases distinctes, chacune apportant de la valeur incrémentale tout en préparant les étapes suivantes.

```
Phase 1: Consolidation     Phase 2: Monorepo ETL      Phase 3: Base de Données    Phase 4: Système Distribué
     (3 mois)                   (2 mois)                    (4 mois)                    (6+ mois)

[ElectriFlux] ──┐          [ElectriCore Monorepo]    [ETL] → [DB] → [Core]       [Workers] → [DB] ← [API]
[ElectriCore] ──┘     →         [ETL + Core]      →    Parquet → PostgreSQL  →    Auto-ingestion + API
Notebooks validés            Tests intégration           TimescaleDB               Temps réel + Scale
```

## Phase 1 : Consolidation (État Actuel → 3 mois)

### Objectifs
- Stabiliser l'architecture actuelle
- Valider le `pipeline_energie` avec des données réelles
- Préparer les outils de migration

### Livrables

#### 1.1 Pipeline Énergies Fonctionnel ✅
- [x] Notebook Marimo de développement créé
- [ ] Validation sur données production complètes
- [ ] Tests unitaires pour chaque fonction du pipeline
- [ ] Documentation des cas d'usage et limitations

#### 1.2 Suite de Tests Robuste
```python
tests/
├── unit/
│   ├── test_perimetre_functions.py
│   ├── test_releves_functions.py
│   └── test_energies_pipeline.py
└── integration/
    ├── test_full_facturation.py
    └── test_pipeline_energie.py
```

#### 1.3 Outils de Migration
```bash
# Scripts utilitaires pour la Phase 2
scripts/
├── audit_electriflux.py      # Analyse code ElectriFlux
├── migrate_schemas.py        # Migration modèles Pandera
└── validate_migration.py     # Tests de régression
```

#### 1.4 Documentation Complète
- [x] Architecture et décisions documentées
- [ ] Guide de contribution et conventions
- [ ] Documentation API des fonctions core
- [ ] Guide d'utilisation des notebooks

### Critères de Succès Phase 1
- `pipeline_energie()` traite avec succès un dataset production complet
- Couverture de tests > 80% sur le code core
- Temps de traitement < 2 minutes pour 1 mois de données
- Documentation permettant à un développeur externe de contribuer

## Phase 2 : Monorepo ETL (4-6 mois)

### Objectifs
- Intégrer ElectriFlux dans le monorepo
- Unifier les schémas et éliminer les doublons
- Créer un ETL complet et autonome

### Architecture Cible Phase 2
```
electricore/ (monorepo)
├── electricore/
│   ├── etl/              # ElectriFlux intégré
│   │   ├── extract.py    # SFTP, decrypt
│   │   ├── transform.py  # XML → Pandera
│   │   └── load.py       # Parquet local
│   ├── models/           # Schémas unifiés
│   │   ├── perimetre.py
│   │   ├── releves.py
│   │   └── energie.py
│   └── core/             # Business logic
├── cli/                  # Interface ligne de commande
├── notebooks/
└── tests/integration/    # End-to-end tests
```

### Livrables

#### 2.1 Migration ElectriFlux (Mois 4)
- Migration git subtree avec préservation historique
- Adaptation du code aux modèles Pandera unifiés
- Conservation des fonctionnalités SFTP/decrypt existantes

#### 2.2 CLI Interface (Mois 5)
```bash
# Interface unifiée pour l'ETL et le processing
electricore etl --flux C15 --source ~/data/enedis/
electricore process --pipeline energie --period 2024-01 
electricore export --format parquet --output ~/processed/
```

#### 2.3 Tests d'Intégration End-to-End (Mois 5-6)
```python
def test_full_etl_pipeline():
    """Test complet : XML brut → Résultats métier"""
    # ETL
    raw_files = ["C15_sample.xml", "R151_sample.xml"]
    etl_result = etl.process_files(raw_files)
    
    # Core processing
    result = core.pipeline_energie(etl_result.historique, etl_result.releves)
    
    # Validation métier
    assert result.validate_business_rules()
```

### Critères de Succès Phase 2
- Un seul repository avec ETL et Core intégrés
- Pipeline complet XML → Parquet → Business logic fonctionnel
- Suppression complète du repository ElectriFlux séparé
- Interface CLI simple pour les opérations courantes

## Phase 3 : Base de Données (7-10 mois)

### Objectifs
- Remplacer le stockage Parquet par une vraie base de données
- Préparer l'architecture pour le temps réel
- Optimiser les performances sur de gros volumes

### Architecture Cible Phase 3
```
[ETL Workers]           [PostgreSQL + TimescaleDB]         [Core API]
SFTP monitoring    →    Hypertables par flux         →     FastAPI server
Auto-processing         Partitioning temporel              Endpoints métier
Parquet → DB           Indexing optimisé                   JSON responses
```

### Stack Technique
```yaml
Database:
  - PostgreSQL 15+ (JSONB, partitioning)
  - TimescaleDB (time-series optimization)
  - pgBouncer (connection pooling)

Processing:
  - Pandas → Polars migration (performance)
  - Dask (distributed computing si nécessaire)
  - Redis (cache et queuing)

Monitoring:
  - Prometheus + Grafana
  - Structured logging (JSON)
  - Health checks
```

### Livrables

#### 3.1 Schema Base de Données (Mois 7)
```sql
-- Hypertables TimescaleDB
CREATE TABLE historique_perimetre (
    time TIMESTAMPTZ NOT NULL,
    pdl TEXT NOT NULL,
    data JSONB NOT NULL
);
SELECT create_hypertable('historique_perimetre', 'time');

CREATE TABLE releves_index (
    time TIMESTAMPTZ NOT NULL,
    pdl TEXT NOT NULL,
    cadrans JSONB NOT NULL
);
SELECT create_hypertable('releves_index', 'time');
```

#### 3.2 Migration ETL → DB (Mois 7-8)
```python
# electricore/etl/load.py
async def load_to_database(data: DataFrame, table: str):
    """Chargement optimisé en base avec upserts"""
    async with get_db_connection() as conn:
        await conn.execute_many(
            f"INSERT INTO {table} VALUES (...) ON CONFLICT UPDATE ...",
            data.to_records()
        )
```

#### 3.3 API Core (Mois 8-9)
```python
# FastAPI server
@app.get("/api/v1/pipeline-energie")
async def get_energie_pipeline(
    deb: date, fin: date, pdl: Optional[List[str]] = None
):
    historique = await db.get_historique(deb, fin, pdl)
    releves = await db.get_releves(deb, fin, pdl)
    return pipeline_energie(historique, releves)
```

#### 3.4 Worker ETL Automatique (Mois 9-10)
```python
# Monitoring SFTP + processing automatique
class ETLWorker:
    async def monitor_sftp(self):
        while True:
            new_files = await sftp.list_new_files()
            for file in new_files:
                await self.process_file_async(file)
            await asyncio.sleep(3600)  # Check hourly
```

### Critères de Succès Phase 3
- Base de données PostgreSQL+TimescaleDB opérationnelle
- API REST exposant les fonctions core principales
- Worker ETL automatique pour l'ingestion continue
- Performance : < 30s pour traiter un fichier R151 moyen
- Capacité : Stocker 2+ années d'historique complet

## Phase 4 : Système Distribué (11+ mois)

### Objectifs
- Architecture scalable pour production
- Haute disponibilité et récupération d'erreurs
- API publique pour applications tierces

### Architecture Cible Phase 4
```
[Load Balancer]
       ↓
[API Gateway]                    [Message Queue]
    ↓                                 ↓
[API Servers] ←→ [Cache] ←→ [ETL Workers] ←→ [Database Cluster]
    ↓                                 ↓              ↓
[WebSocket]                    [Monitoring]      [Replicas]
```

### Stack Technique Complète
```yaml
Infrastructure:
  - Docker + Kubernetes (orchestration)
  - Traefik (load balancing + SSL)
  - Redis Cluster (cache distribué)

Processing:
  - Celery (task queue)
  - Apache Airflow (ETL orchestration)
  - dbt (data transformations)

API & Frontend:
  - FastAPI (async REST API)
  - WebSockets (real-time updates)
  - OpenAPI (documentation auto)

Observability:
  - ELK Stack (logs)
  - Jaeger (distributed tracing)
  - Sentry (error tracking)
```

### Livrables

#### 4.1 Containerisation (Mois 11)
```dockerfile
# Multi-stage build optimisé
FROM python:3.12-slim as base
# ETL worker container
# API server container  
# DB migration container
```

#### 4.2 Orchestration (Mois 12)
```yaml
# kubernetes/
├── namespace.yaml
├── database/
│   ├── postgres-cluster.yaml
│   └── timescaledb-config.yaml
├── api/
│   ├── deployment.yaml
│   └── service.yaml
└── etl/
    ├── cronjob.yaml
    └── worker-deployment.yaml
```

#### 4.3 API Publique (Mois 13+)
- Documentation OpenAPI complète
- Authentification JWT 
- Rate limiting et quotas
- SDK Python pour développeurs tiers

### Critères de Succès Phase 4
- Système déployé en production avec 99.9% uptime
- API publique utilisée par au moins 2 applications externes
- Capacité de traitement : 100+ PDLs simultanément
- Récupération automatique des pannes ETL

## Jalons et Métriques

### Métriques Techniques
- **Performance** : Temps de traitement par volume de données
- **Fiabilité** : Uptime, taux d'erreur, recovery time
- **Scalabilité** : Nombre de PDLs traités, débit par heure
- **Qualité** : Couverture tests, bugs en production

### Métriques Métier
- **Adoption** : Nombre d'applications clientes
- **Volume** : Go de données traitées par mois  
- **Réactivité** : Délai entre réception flux et disponibilité API
- **Précision** : Taux d'erreur sur les calculs énergétiques

## Décisions Reportées

### Choix Technologiques Futurs
- **Message Queue** : RabbitMQ vs Apache Kafka vs Redis Streams
- **Frontend** : Dashboard de monitoring (React vs Vue vs Streamlit)
- **ML/AI** : Détection d'anomalies, prédiction de consommation
- **Multi-tenant** : Support de plusieurs fournisseurs d'énergie

### Intégrations Potentielles
- **LibreWatt** : Interface directe sans API REST
- **Odoo** : Module ElectriCore natif
- **Business Intelligence** : Connecteurs Tableau, PowerBI
- **IoT** : Ingestion capteurs temps réel

---

*Cette roadmap constitue notre vision à 18 mois. Elle sera ajustée en fonction des retours utilisateurs et des contraintes techniques rencontrées.*