# 🚀 Plan de Migration Pipeline Facturation vers Polars

## Architecture Proposée

### Structure des fichiers
```
electricore/core/
├── pipelines_polars/
│   └── facturation_polars.py         # Pipeline facturation Polars
├── models_polars/
│   └── periode_meta_polars.py        # Modèle Pandera PeriodeMetaPolars
└── tests/
    ├── unit/
    │   └── test_expressions_facturation_polars.py  # Tests unitaires
    └── integration/
        └── test_facturation_polars_vs_pandas.py    # Comparaison avec pandas
```

## Implémentation par étapes

### 1️⃣ Créer le modèle PeriodeMetaPolars
- Adapter PeriodeMeta pour Polars avec types natifs (`pl.Utf8`, `pl.Float64`, etc.)
- Ajouter champs pour tracer l'incomplétude :
  - `coverage_abo`: Taux de couverture temporelle des abonnements
  - `coverage_energie`: Taux de couverture temporelle des énergies
- Validation Pandera des méta-périodes avec checks de cohérence

### 2️⃣ Expressions atomiques pour agrégations
```python
# Expressions pour abonnements
expr_puissance_ponderee()     # Calcul puissance * nb_jours
expr_puissance_moyenne()       # Division par nb_jours total
expr_memo_puissance()          # Construction du mémo changements

# Expressions pour énergies
expr_somme_energies()          # Agrégation des cadrans
expr_coverage_temporelle()     # Calcul taux de couverture
```

### 3️⃣ Expressions d'agrégation mensuelle
```python
agreger_abonnements_mensuel()  # Groupby avec puissance pondérée
agreger_energies_mensuel()     # Groupby avec sommes simples
```
- Gestion des flags de changement (plusieurs sous-périodes)
- Calcul des métriques de complétude

### 4️⃣ Expression de jointure et réconciliation
```python
joindre_meta_periodes()        # Jointure externe abo/energie
expr_reconcilier_dates()       # Priorité abo > energie
expr_reconcilier_valeurs()     # Gestion des nulls avec defaults
expr_flags_completude()        # Calcul coverage et data_complete
```

### 5️⃣ Pipeline principal
```python
def pipeline_facturation_polars(
    abonnements_lf: pl.LazyFrame,
    energies_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """Pipeline pur d'agrégation avec LazyFrames."""
```
- Orchestration des expressions
- Validation entrée/sortie avec `@pa.check_types`
- Support scan_parquet optionnel

### 6️⃣ Tests unitaires par expression
- Test de chaque expression isolément
- Cas nominaux et cas limites :
  - Périodes complètes
  - Données manquantes (abo sans energie, energie sans abo)
  - Changements de puissance multiples
  - Mois incomplets
- Vérification types et valeurs

### 7️⃣ Tests d'intégration et validation
- Comparaison résultats pandas vs Polars sur données réelles
- Notebook de validation visuelle
- Tests de performance (objectif : x10 plus rapide)

## Principes directeurs

✅ **DRY** : Expressions réutilisables et composables
✅ **KISS** : Une expression = une responsabilité unique
✅ **Idiomatique Polars** : LazyFrames, expressions vectorisées, pas de loops
✅ **Testabilité** : Chaque fonction testable unitairement
✅ **Traçabilité** : Mémos et flags pour suivre les transformations

## Ordre d'implémentation

1. **Modèle PeriodeMetaPolars** avec validation Pandera
2. **Expressions atomiques** + tests unitaires
3. **Expressions d'agrégation** + tests unitaires
4. **Expression de jointure** + tests unitaires
5. **Pipeline principal** avec orchestration
6. **Tests d'intégration** pandas vs Polars
7. **Notebook de validation** pour analyse visuelle

## Points d'attention

### Gestion de l'incomplétude
- Une méta-période est créée même si données partielles
- Flags `coverage_abo` et `coverage_energie` tracent la complétude
- `data_complete = True` seulement si couverture 100% des deux côtés

### Mémo puissance
- Construit uniquement si changements réels de puissance
- Format : "14j à 6kVA, 17j à 9kVA"
- Chaîne vide si puissance constante

### Réconciliation des dates
- Priorité aux dates d'abonnement si présentes
- Sinon utilisation des dates d'énergie
- Calcul nb_jours si manquant

### Performance
- Utilisation systématique de LazyFrames
- Pas de collect() intermédiaire
- Expressions vectorisées uniquement
- Groupby optimisés avec aggregations natives

## Validation finale
- Comparaison sur 3 mois de données réelles
- Vérification exactitude des montants TURPE
- Tests de régression sur cas limites identifiés
- Validation performance (objectif < 100ms pour 10k lignes)