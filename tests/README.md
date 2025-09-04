# Stratégie de Test pour ElectriCore

## Philosophy générale

ElectriCore suit une approche de traitement de données **fonctionnelle** utilisant Polars. La stratégie de test doit être **pragmatique** et **maintenable**, sans devenir plus complexe que le code testé.

## Problématique initiale

Les données énergétiques Enedis ont des **dépendances complexes** :
- **Temporelles** : Les relevés dépendent de l'historique du périmètre
- **Métier** : Séquences d'événements logiques (MES → vie du contrat → RES)  
- **Structurelles** : Le calendrier distributeur détermine les mesures présentes

❌ **Anti-pattern évité** : Générer des données parfaitement cohérentes avec Hypothesis devient plus complexe que le pipeline lui-même.

## Approche retenue : Fixtures + Snapshots

### 🎯 Objectifs prioritaires
1. **Prévenir les régressions** (priorité 1)
2. **Documenter le comportement attendu** via les fixtures
3. **Tests rapides** et **maintenables**

### 📋 Stratégie en 4 niveaux

#### Niveau 1 : Fixtures de données réelles (80% de l'effort)
- **Source** : Extraction de cas réels anonymisés
- **Couverture** : 6 cas métier critiques
- **Tests** : Snapshots pour détecter les régressions

#### Niveau 2 : Tests unitaires d'expressions Polars (15% de l'effort)
- **Focus** : Tester chaque transformation individuellement
- **Données** : Minimales, créées à la main (5-10 lignes)
- **Objectif** : Valider la logique métier

#### Niveau 3 : Tests de contrat Pandera (4% de l'effort)
- **Validation** : Schémas entrée/sortie respectés
- **Pas besoin** de données parfaitement cohérentes

#### Niveau 4 : Hypothesis pour invariants simples (1% de l'effort)
- **Usage** : Propriétés mathématiques simples uniquement
- **Exemple** : `BASE = HP + HC`

## Cas métier critiques couverts

### 1. MCT avec changement de calendrier
- **Scénario** : Passage de BASE vers HP/HC
- **Fixture** : `cas_mct_changement_calendrier`
- **Test** : Vérification de la continuité des index

### 2. Entrée sur le périmètre
- **Scénarios** : MES, PMES, CFNE
- **Fixture** : `cas_entree_perimetre`
- **Test** : Premier relevé correct

### 3. Sortie du périmètre  
- **Scénarios** : RES, CFNS
- **Fixture** : `cas_sortie_perimetre`
- **Test** : Dernier relevé et clôture

### 4. Changement de compteur
- **Scénario** : Remplacement compteur avec remise à zéro
- **Fixture** : `cas_changement_compteur`
- **Test** : Gestion des index de départ

### 5. Changement de puissance
- **Scénario** : Modification puissance souscrite
- **Fixture** : `cas_changement_puissance`
- **Test** : Impact sur les calculs de taxes

### 6. Changement de FTA
- **Scénario** : Changement Formule Tarifaire Acheminement
- **Fixture** : `cas_changement_fta`
- **Test** : Nouveau calcul des coûts

## Structure des tests

```
tests/
├── fixtures/
│   ├── donnees_anonymisees/     # Fichiers parquet anonymisés
│   └── cas_metier.py            # Fixtures pytest
├── unit/
│   ├── test_expressions.py     # Tests unitaires expressions Polars
│   └── test_invariants.py      # Tests Hypothesis simples  
├── integration/
│   └── test_pipeline.py        # Tests avec fixtures + snapshots
└── README.md                   # Ce fichier
```

## Exemples d'implémentation

### Fixture de cas métier
```python
@pytest.fixture
def cas_mct_changement_calendrier():
    """MCT avec passage de BASE vers HP/HC"""
    return {
        "historique": pl.read_parquet("fixtures/mct_calendrier_historique.parquet"),
        "releves": pl.read_parquet("fixtures/mct_calendrier_releves.parquet"),
        "expected": pl.read_parquet("fixtures/mct_calendrier_expected.parquet")
    }
```

### Test de non-régression
```python
def test_pipeline_mct_calendrier(cas_mct_changement_calendrier, snapshot):
    """Vérifie que le pipeline produit toujours le même résultat"""
    result = pipeline_facturation(
        cas_mct_changement_calendrier["historique"],
        cas_mct_changement_calendrier["releves"]
    )
    
    # Le snapshot alerte si le résultat change
    assert result.to_dict() == snapshot
```

### Test unitaire d'expression
```python
def test_calcul_energie_consommee():
    """Test unitaire d'une transformation Polars"""
    df = pl.DataFrame({
        "index_fin": [1000.0, 2000.0],  
        "index_debut": [900.0, 1800.0],
    })
    
    result = df.with_columns(
        (pl.col("index_fin") - pl.col("index_debut")).alias("energie")
    )
    
    assert result["energie"].to_list() == [100.0, 200.0]
```

### Test d'invariant simple
```python
@given(
    hp=st.floats(min_value=0, max_value=10000, allow_nan=False),
    hc=st.floats(min_value=0, max_value=10000, allow_nan=False)
)
def test_invariant_base_equals_hp_plus_hc(hp, hc):
    """BASE doit toujours égaler HP + HC"""
    df = pl.DataFrame({"HP": [hp], "HC": [hc]})
    
    result = df.with_columns(
        (pl.col("HP") + pl.col("HC")).alias("BASE")
    )
    
    assert abs(result["BASE"][0] - (hp + hc)) < 1e-10
```

## Processus d'anonymisation

### Script d'anonymisation type
```python
def anonymiser_cas(historique_df, releves_df):
    """
    Anonymise un cas métier tout en préservant la cohérence.
    """
    # 1. Décaler toutes les dates de X jours aléatoires
    offset_days = random.randint(1000, 2000)
    
    # 2. Remplacer PDL par valeurs génériques séquentielles
    pdl_mapping = {pdl: f"PDL{i:05d}" for i, pdl in enumerate(historique_df["pdl"].unique())}
    
    # 3. Arrondir les index pour masquer les vraies consommations
    # Mais garder les différences relatives !
    
    # 4. Anonymiser les références contractuelles
    
    return historique_anonymise, releves_anonymises
```

## Outils recommandés

- **pytest** : Framework de test principal
- **syrupy** ou **pytest-snapshot** : Tests de snapshot
- **hypothesis** : Tests de propriétés (usage limité)
- **polars** : Manipulation des fixtures
- **pandera** : Validation des schémas

## Avantages de cette approche

✅ **Tests réalistes** : Basés sur de vrais cas métier  
✅ **Détection de régression** : Les snapshots alertent sur tout changement  
✅ **Maintenabilité** : Pas de génération complexe à maintenir  
✅ **Documentation** : Chaque fixture documente un cas métier  
✅ **Performance** : Tests rapides avec données pré-extraites  
✅ **Évolutivité** : Facile d'ajouter de nouveaux cas  

## Migration depuis Hypothesis

### Étapes de migration
1. **Garder** les tests d'invariants simples existants
2. **Supprimer** la génération de séquences temporelles complexes  
3. **Extraire** les fixtures des données réelles
4. **Réécrire** les tests d'intégration avec snapshots

### Ce qui est conservé
- Tests de propriétés mathématiques simples
- Validation des modèles Pandera
- Structure des fixtures simples

### Ce qui est abandonné
- Génération de chaînes d'événements cohérentes
- Stratégies complexes avec dépendances temporelles
- Tentatives de reproduire toute la logique métier dans les tests

## Prochaines étapes

1. **Extraire les cas métier** des données de production
2. **Créer le script d'anonymisation**  
3. **Implémenter les tests de snapshot**
4. **Migrer progressivement** les tests existants
5. **Documenter** les nouveaux cas au fur et à mesure

---

## Historique : Stratégies Hypothesis (conservées pour référence)

*Cette section documente l'approche précédente basée sur la génération complexe avec Hypothesis, conservée pour référence.*

### Stratégies disponibles (usage limité recommandé)

#### RelevéIndex (simplifié)
```python
from electricore.core.testing.strategies_polars import releve_index_strategy

# Pour tests d'invariants uniquement
df = releve_index_strategy(min_size=5, max_size=10).example()
```

#### Données générées
- **PDL** : 14 chiffres numériques
- **Dates** : 2024-2025 avec timezone Europe/Paris  
- **Valeurs d'énergie** : 0-99999 avec 3 décimales
- **Calendriers** : DI000001 (BASE), DI000002 (HP/HC), DI000003 (Tempo)

---

*Cette approche privilégie le **pragmatisme** et la **maintenabilité** pour se concentrer sur l'essentiel : garantir que le pipeline Polars fonctionne correctement et détecter rapidement les régressions.*