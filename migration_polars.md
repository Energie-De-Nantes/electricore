# Plan de migration pandas → polars pour ElectriCore

## 📊 État des lieux

### Métriques du projet
- **Volume de code**: ~7000 lignes Python
- **Fichiers impactés**: 32 fichiers utilisant pandas
- **Architecture**: Pipelines fonctionnels avec patterns chainés (.pipe(), .assign(), .groupby())
- **Validation**: Pandera pour schémas stricts + Hypothesis pour tests property-based
- **Domaine métier**: Traitement de données énergétiques françaises (flux Enedis)

### Analyse d'impact
- **Core pipelines**: ~1000 lignes (energie, facturation, perimetre, abonnements)
- **Modèles Pandera**: ~400 lignes de schémas de validation
- **Parsers de flux**: ~300 lignes d'I/O
- **Tests**: ~2000 lignes à adapter
- **Notebooks marimo**: ~1900 lignes d'analyse interactive

## ⚡ Enjeux de la migration

### Bénéfices attendus
1. **Performance**: Gains de 3-10x sur les opérations groupby/agg (97 occurrences identifiées)
2. **Expressivité**: Les expressions polars remplaceront avantageusement les lambdas Python
3. **Scalabilité**: Lazy evaluation et streaming pour traiter de gros volumes de données
4. **Maintenabilité**: Code plus concis, type-safe et idiomatique
5. **Mémoire**: Empreinte mémoire réduite grâce au columnar storage optimisé

### Opportunités spécifiques
- Réécriture des pipelines avec expressions natives polars
- Optimisation des window functions (.shift(), .ffill())
- Parallélisation automatique des opérations
- Meilleure gestion des types et des nulls

## ⏱️ Planning et estimation

**Durée totale estimée**: 3-4 semaines pour une migration complète avec tests

### Phase 1: Infrastructure et préparation (1 semaine)

#### Objectifs
- Mettre en place les fondations pour une migration progressive
- Assurer la compatibilité bidirectionnelle pendant la transition

#### Tâches
1. **Configuration du projet**
   ```toml
   # pyproject.toml
   dependencies = [
       "polars>=0.20.0",
       "pandera[polars]>=0.18.0",
       # garder pandas temporairement pour transition
       "pandas>=2.2.3",
   ]
   ```

2. **Module de compatibilité**
   - Créer `electricore/core/compat.py` avec fonctions de conversion
   - Helpers pour pandas↔polars DataFrame conversion
   - Wrappers pour API communes

3. **Adaptation Pandera**
   - Migrer les DataFrameModel vers polars schemas
   - Créer décorateurs de validation polars
   - Maintenir rétrocompatibilité temporaire

4. **Tests de non-régression**
   - Framework de comparaison pandas vs polars
   - Golden tests avec données de référence
   - CI/CD pour vérifier parité

### Phase 2: Migration des pipelines isolés (1 semaine)

#### Ordre de migration (du plus simple au plus complexe)
1. **pipeline_releves** (moins de dépendances)
   - `interroger_relevés()` 
   - Fonctions de filtrage et sélection

2. **pipeline_abonnements**
   - `calculer_bornes_periodes()`
   - `selectionner_colonnes_abonnement()`

3. **pipeline_energie**
   - `calculer_decalages_par_pdl()`
   - `calculer_differences_cadrans()`
   - `enrichir_cadrans_principaux()`

#### Patterns de conversion clés
```python
# Pandas
df.groupby('pdl')['value'].shift(1)

# Polars
df.with_columns(
    pl.col('value').shift(1).over('pdl')
)
```

### Phase 3: Pipeline principal et orchestration (1 semaine)

#### Composants critiques
1. **pipeline_facturation**
   - `agreger_abonnements_mensuel()` - puissance moyenne pondérée
   - `agreger_energies_mensuel()` - agrégations mensuelles
   - `joindre_agregats()` - jointures complexes

2. **orchestration.py**
   - Optimisation avec lazy evaluation
   - Chaînage des pipelines
   - Gestion des erreurs

#### Optimisations polars spécifiques
- Utiliser `.lazy()` pour différer l'exécution
- Exploiter le query optimizer de polars
- Streaming pour gros fichiers flux

### Phase 4: Tests et notebooks (1 semaine)

#### Migration des tests
1. **Tests unitaires** (~2000 lignes)
   - Adapter fixtures pour polars DataFrames
   - Modifier assertions (pas d'index en polars)
   - Garder Hypothesis pour génération de données

2. **Tests d'intégration**
   - Valider la chaîne complète de traitement
   - Benchmarks performance avant/après
   - Tests de charge avec gros volumes

#### Modernisation des notebooks marimo
- Exploiter les expressions polars pour visualisations
- Utiliser lazy frames pour exploration interactive
- Profiter du meilleur support des types

## 📋 Décomposition détaillée des tâches

### Tâches de setup initial
- [ ] Ajouter polars aux dépendances
- [ ] Créer module `electricore/core/compat.py`
- [ ] Configurer Pandera pour polars
- [ ] Mettre en place tests de parité
- [ ] Créer golden tests de référence

### Tâches de migration des modèles
- [ ] Migrer `HistoriquePérimètre` vers polars schema
- [ ] Migrer `RelevéIndex` vers polars schema
- [ ] Migrer `PeriodeEnergie` vers polars schema
- [ ] Migrer `PeriodeAbonnement` vers polars schema
- [ ] Migrer `PeriodeMeta` vers polars schema
- [ ] Adapter décorateurs `@pa.check_types`

### Tâches par pipeline

#### Pipeline relevés
- [ ] Migrer `extraire_releves_evenements()`
- [ ] Migrer `interroger_relevés()`
- [ ] Adapter tests unitaires

#### Pipeline périmètre
- [ ] Migrer `detecter_points_de_rupture()`
- [ ] Migrer `inserer_evenements_facturation()`
- [ ] Optimiser avec expressions polars

#### Pipeline énergie
- [ ] Migrer `reconstituer_chronologie_relevés()`
- [ ] Migrer `calculer_decalages_par_pdl()`
- [ ] Migrer `calculer_differences_cadrans()`
- [ ] Migrer `filtrer_periodes_valides()`
- [ ] Migrer `enrichir_cadrans_principaux()`

#### Pipeline facturation
- [ ] Migrer `agreger_abonnements_mensuel()`
- [ ] Migrer `agreger_energies_mensuel()`
- [ ] Migrer `joindre_agregats()`
- [ ] Implémenter lazy evaluation

### Tâches de test et validation
- [ ] Créer harness de comparaison pandas/polars
- [ ] Implémenter tests de performance
- [ ] Valider calculs TURPE
- [ ] Tester avec données réelles Enedis
- [ ] Benchmarker mémoire et CPU

### Tâches d'optimisation polars
- [ ] Implémenter lazy evaluation dans orchestration
- [ ] Utiliser window functions natives
- [ ] Activer streaming pour gros fichiers
- [ ] Optimiser expressions complexes
- [ ] Profiler et identifier bottlenecks

## ⚠️ Points d'attention critiques

### Différences d'API principales

| Fonctionnalité | Pandas | Polars |
|---------------|--------|---------|
| Timezone | `tz_localize()`, `tz_convert()` | `dt.convert_time_zone()` |
| Index | `.reset_index()`, `.set_index()` | Pas d'index, utiliser colonnes |
| Null handling | `fillna()` | `fill_null()` |
| Forward fill | `.ffill()` | `.forward_fill()` |
| String ops | `.str.contains()` | `.str.contains()` mais syntaxe différente |
| Merge | `pd.merge()` | `.join()` avec API différente |

### Risques identifiés
1. **Régression silencieuse** sur calculs de dates/périodes
2. **Incompatibilité** avec dépendances (electriflux?)
3. **Courbe d'apprentissage** pour l'équipe
4. **Différences subtiles** dans le handling des nulls
5. **Performance dégradée** si mauvaise utilisation du lazy API

### Stratégies de mitigation
- Tests de parité systématiques
- Migration progressive avec rollback possible
- Documentation des patterns de conversion
- Formation équipe sur polars
- Monitoring des performances en production

## 🧪 Stratégie de test

### Structure proposée
```
tests/
├── migration/
│   ├── test_parity.py          # Compare outputs pandas vs polars
│   ├── test_performance.py     # Benchmarks temps et mémoire
│   ├── test_regression.py      # Tests de non-régression
│   └── golden/
│       ├── facturation_ref.parquet
│       ├── energie_ref.parquet
│       └── abonnements_ref.parquet
```

### Méthodologie de validation
1. **Tests de parité**: Comparer bit-à-bit les résultats
2. **Property-based testing**: Garder Hypothesis avec adaptations polars
3. **Tests de performance**: Mesurer gains/pertes
4. **Tests de charge**: Valider avec gros volumes
5. **Tests d'intégration**: Chaîne complète end-to-end

### Exemple de test de parité
```python
def test_pipeline_parity(sample_data):
    # Version pandas
    result_pandas = pipeline_pandas(sample_data.to_pandas())
    
    # Version polars
    result_polars = pipeline_polars(sample_data)
    
    # Comparaison
    assert_frame_equal(
        result_pandas,
        result_polars.to_pandas(),
        check_dtype=False  # Types peuvent différer légèrement
    )
```

## ✅ Critères de succès

1. **Fonctionnalité**: 100% des tests passent
2. **Performance**: Amélioration ≥2x sur pipelines principaux
3. **Mémoire**: Réduction ≥30% empreinte mémoire
4. **Code**: Réduction ~30% du volume de code
5. **Maintenabilité**: Code plus expressif et idiomatique

## 🚀 Opportunités futures post-migration

1. **Streaming processing** pour très gros fichiers flux
2. **Parallélisation** des traitements multi-PDL
3. **Cache intelligent** avec lazy evaluation
4. **Intégration GPU** via polars-gpu (futur)
5. **API REST** avec streaming responses

## 📝 Notes et recommandations

### Pourquoi maintenant?
- Projet jeune, dette technique limitée
- Pas encore de dépendances externes fortes
- Équipe réduite, coordination simple
- ROI maximal avant croissance du code

### Approche recommandée
- Migration bottom-up (modules simples d'abord)
- Maintenir compatibilité pendant transition
- Tests de parité à chaque étape
- Documentation au fur et à mesure
- Formation continue de l'équipe

### Ressources utiles
- [Polars User Guide](https://docs.pola.rs/)
- [Pandas to Polars migration guide](https://docs.pola.rs/user-guide/migration/pandas/)
- [Pandera Polars support](https://pandera.readthedocs.io/en/stable/polars.html)
- [Polars cookbook](https://github.com/pola-rs/polars-cookbook)

## Conclusion

La migration vers polars est une opportunité stratégique pour ElectriCore. Le timing est optimal (projet jeune), les bénéfices sont clairs (performance, expressivité), et l'approche progressive minimise les risques. L'investissement de 3-4 semaines sera rapidement rentabilisé par les gains en performance et maintenabilité.