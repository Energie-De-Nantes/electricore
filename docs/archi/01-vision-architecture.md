# Vision Architecture - ElectriCore

> Document de réflexion sur l'évolution architecturale du projet ElectriCore  
> Créé: 2025-01-19  
> Contexte: Solo developer, backend pour LibreWatt et modules Odoo

## Contexte du Projet

ElectriCore est un moteur de traitement de données énergétiques françaises qui transforme les flux Enedis en formats métier. Il sert de backend pour des applications comme LibreWatt et des modules Odoo.

### Architecture Actuelle

```
[ElectriFlux]           [ElectriCore]
XML Enedis       →      inputs/          →     core/
- SFTP download         - Transform              - Business logic
- Decrypt/unzip         - Pandera validation     - Périmètre, relevés
- XML → DataFrame       - Format canonique       - Énergies, taxes
```

**Problématiques identifiées :**
- Double définition des schémas (YAML dans ElectriFlux, Pandera dans ElectriCore)
- Maintenance de 2 dépôts séparés pour un domaine cohésif
- Couplage fort mais séparation artificielle
- Complexité des tests d'intégration

### Vision Cible

```
[SFTP Enedis] → [ETL Automatique] → [DB] → [API ElectriCore] → [Applications]
                     ↓                ↑           ↓
              détection nouveaux    stockage   pipeline_energie()
                    fichiers        persistant   facturation()
```

**Objectifs :**
1. **Système temps réel** : Ingestion automatique des nouvelles archives
2. **Stockage persistant** : Base de données sur serveur
3. **API de calculs** : Expose `pipeline_energie()`, `facturation()` etc.
4. **Évolutivité** : Capacité à traiter des volumes croissants

## Décision Architecturale : Monorepo

### Analyse des Options

#### Option A : Multi-repos (3 packages séparés)
- `electricore-models` : Schémas Pandera partagés
- `electriflux` : ETL complet
- `electricore` : Business logic pure

**Inconvénients :**
- 3x la maintenance (CI/CD, releases, versioning)
- Synchronisation complexe des versions
- Overhead de coordination pour 1 développeur
- Pas de vraie réutilisabilité (domaine spécifique)

#### Option B : Monorepo structuré ✅

**Avantages :**
- **Refactoring atomique** : Changement de schéma en 1 commit
- **Tests d'intégration triviaux** : Tout est disponible localement  
- **Une seule CI/CD** : Pipeline unifié
- **Développement rapide** : Pas de gymnastique entre packages
- **Maintenance simplifiée** : Un seul point d'entrée

### Justification

Pour un **développeur solo** travaillant sur un **domaine cohésif** (données énergétiques Enedis), le monorepo est la solution optimale :

1. **Cohésion fonctionnelle** : ElectriFlux et ElectriCore travaillent sur les mêmes données
2. **Cycles de développement synchrones** : Un bug Enedis impacte souvent les deux parties
3. **Pas de réutilisabilité externe** : Personne d'autre n'utilisera ces modèles spécifiques
4. **Simplicité = Vélocité** : Plus de temps sur la valeur métier, moins sur l'infrastructure

## Principes Architecturaux

### 1. Single Source of Truth
- **Schémas Pandera** : Définition canonique dans `electricore/models/`
- **Configuration** : Un seul `pyproject.toml` pour tout le projet
- **Tests** : Suite d'intégration unique et complète

### 2. Séparation des Responsabilités
```python
electricore/
├── etl/          # Extract, Transform, Load (ex-ElectriFlux)
├── models/       # Schémas Pandera canoniques
├── core/         # Business logic métier
└── inputs/       # Connecteurs (peut rester temporairement)
```

### 3. Évolution Progressive
- **Phase 1** : Monorepo avec structure actuelle
- **Phase 2** : Migration d'ElectriFlux vers `electricore/etl/`
- **Phase 3** : Ajout base de données
- **Phase 4** : API server et workers

### 4. Testabilité
- Tests unitaires par module
- **Tests d'intégration end-to-end** facilités par le monorepo
- Validation Pandera à chaque étape
- Fixtures communes partagées

## Anti-Patterns à Éviter

### "Microservices Envy"
- Tendance à sur-découper par mimétisme des grandes entreprises
- Complexité injustifiée pour un contexte solo developer

### "Clean Architecture Porn"
- Abstractions excessives sans valeur métier
- Maintenance d'interfaces qui ne servent que la "propreté"

### "Premature Optimization"
- Anticiper des besoins de scalabilité non validés
- Complexité technique sans ROI démontré

## Références et Inspiration

### Exemples de Monorepos Réussis
- **Django** : Framework Python monolithique et efficace
- **Ruby on Rails** : Convention over configuration
- **FastAPI** : Simplicité et performance

### Outils et Pratiques
- **Poetry workspaces** : Gestion des dépendances dans le monorepo
- **Pytest** : Suite de tests unifiée
- **Pandera** : Validation de schémas comme contrat
- **Git subtree** : Migration progressive depuis ElectriFlux

---

*Ce document constitue la base de notre réflexion architecturale. Il sera enrichi au fur et à mesure de l'évolution du projet.*