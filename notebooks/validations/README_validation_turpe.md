# Notebook de validation TURPE F15

Ce notebook Marimo permet de valider les calculs TURPE d'ElectriCore en les comparant avec les données de facturation F15 d'Enedis.

## 🎯 Objectifs

- **Validation multi-échelle** : global, par PDL, temporelle
- **Identification des écarts** et analyse de leurs causes
- **Gestion des différences attendues** (compteurs non-intelligents, relevés manquants)
- **Rapport de synthèse interactif** avec métriques de qualité

## 🚀 Utilisation

### Mode interactif (recommandé)

```bash
poetry run marimo edit notebooks/validation_turpe_f15.py
```

Le notebook s'ouvre dans le navigateur avec interface interactive permettant :
- Navigation entre les sections d'analyse
- Tableaux interactifs avec tri et filtrage
- Widgets de sélection pour approfondir l'analyse
- Exports CSV des données filtrées

### Mode script (automatisé)

```bash
poetry run marimo run notebooks/validation_turpe_f15.py --headless
```

Génère automatiquement un rapport de validation avec :
- Métriques globales d'écart
- Liste des PDL avec écarts significatifs
- Analyse temporelle des différences
- Recommandations d'actions

### Mode développement

```bash
poetry run python notebooks/validation_turpe_f15.py
```

Lance le notebook en mode développement avec rechargement automatique des modifications.

## 📊 Structure des analyses

### 1. Extraction des données F15
- Requête sur `flux_enedis.flux_f15_detail` avec `nature_ev = '01'`
- Classification des composantes TURPE (Gestion, Comptage, Soutirage)
- Agrégations par PDL, mois et type de composante

### 2. Calcul TURPE via le pipeline
- Pipeline énergie pour les périodes de consommation
- Pipeline abonnements pour les périodes contractuelles
- Application des règles TURPE tarifaires
- Calcul du TURPE fixe et variable

### 3. Validation multi-échelle

#### Niveau global
- Écart total en € et %
- Taux de couverture des PDL
- Évaluation qualitative (🟢 🟡 🟠 🔴)

#### Niveau PDL
- Comparaison individuelle par PDL
- Identification des écarts > 5%
- Détection des PDL manquants (compteurs non-intelligents)

#### Niveau temporel
- Évolution mensuelle des écarts
- Identification des périodes problématiques
- Analyse des tendances

### 4. Gestion des cas particuliers

Le notebook identifie automatiquement :
- **PDL sans compteurs intelligents** : exclus de la comparaison
- **Relevés manquants** : premiers mois d'adhésion ou données indisponibles
- **Écarts techniques** : différences de méthode de calcul acceptables

## 📈 Métriques de qualité

Le notebook génère une évaluation automatique :

| Évaluation | Écart relatif | Couverture PDL | Recommandation |
|------------|---------------|----------------|----------------|
| 🟢 **EXCELLENTE** | < 2% | > 95% | Validation très satisfaisante |
| 🟡 **BONNE** | < 5% | > 90% | Quelques écarts à analyser |
| 🟠 **CORRECTE** | < 10% | > 80% | Ajustements nécessaires |
| 🔴 **À AMÉLIORER** | > 10% | < 80% | Révision du pipeline |

## 🔧 Configuration requise

### Données nécessaires
- Base DuckDB avec données F15 : `flux_enedis.flux_f15_detail`
- Historique C15 et relevés R151 dans la même base
- Règles TURPE dans `electricore/config/turpe_rules.csv`

### Dépendances
- Polars >= 0.20
- Marimo >= 0.16
- ElectriCore pipelines

### Variables d'environnement
```bash
# Chemin vers la base DuckDB (optionnel, utilise la config par défaut)
DUCKDB__PATH="electricore/ingestion/flux_enedis_pipeline.duckdb"
```

## 🚨 Résolution des problèmes

### Erreurs courantes

#### "FileNotFoundError: Base DuckDB non trouvée"
```bash
# Vérifier l'existence de la base
ls -la electricore/ingestion/flux_enedis_pipeline.duckdb

# Exécuter le pipeline de données si nécessaire
cd electricore/ingestion && poetry run python pipeline_production.py
```

#### "ImportError: cannot import name"
```bash
# Réinstaller les dépendances
poetry install

# Vérifier les imports du projet
python -c "from electricore.core.pipelines_polars import turpe_polars"
```

#### "Aucune donnée TURPE trouvée"
Vérifier la requête F15 :
```sql
SELECT COUNT(*) FROM flux_enedis.flux_f15_detail WHERE nature_ev = '01';
```

### Performance

Pour les gros volumes de données (> 100k lignes) :
- Utiliser des filtres temporels : `{"date_facture": ">= '2024-01-01'"}`
- Limiter les PDL : `{"pdl": ["PDL123", "PDL456"]}`
- Activer le mode lazy avec `.lazy()` sur les LazyFrames

## 📋 Outputs

### Tableaux exportables
- `ecarts_pdl.csv` : Écarts détaillés par PDL
- `evolution_mensuelle.csv` : Comparaison temporelle
- `pdl_manquants.csv` : PDL sans données calculées

### Métriques de synthèse
- Écart relatif global
- Taux de couverture PDL
- Nombre d'écarts significatifs
- Recommandations d'actions

## 🔄 Intégration CI/CD

Pour automatiser la validation :

```yaml
# .github/workflows/validation-turpe.yml
name: Validation TURPE
on:
  schedule:
    - cron: '0 6 * * 1'  # Tous les lundis à 6h

jobs:
  validate-turpe:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: poetry install
      - name: Run TURPE validation
        run: |
          poetry run marimo run notebooks/validation_turpe_f15.py --headless
          # Parser les résultats et alerter si écart > seuil
```

## 📞 Support

Pour toute question sur ce notebook :
- Vérifier les logs de validation dans le notebook
- Consulter la documentation des pipelines ElectriCore
- Ouvrir une issue avec les métriques d'écart observées