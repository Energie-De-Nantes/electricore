# Stratégie de Test pour ElectriCore

## Philosophy générale

ElectriCore suit une approche de traitement de données **fonctionnelle** utilisant Polars. La stratégie de test doit être **pragmatique** et **maintenable**, sans devenir plus complexe que le code testé.

**✨ Mise à jour 2025** : La batterie de tests a été modernisée avec :
- Configuration pytest centralisée avec markers
- Fixtures partagées via `conftest.py`
- Tests paramétrés pour réduire la duplication
- Tests snapshot avec Syrupy pour détection de régression automatique
- Script d'anonymisation pour fixtures basées sur données réelles

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

#### Niveau 4 : Property-based testing pour invariants simples (1% de l'effort)
- **Usage** : Propriétés mathématiques simples sur entrées *plates* générées
- **Implémentation** : [tests/property/](property/) — stratégies dérivées des schémas Pandera
  via `strategie_depuis_schema` ([tests/property/strategies.py](property/strategies.py)),
  construit sur `polars.testing.parametric` (issue #194)
- **Exemples** : TURPE fixe/variable — contributions ≥ 0, tarifs nuls → contribution nulle,
  préservation des lignes par la jointure, validité temporelle des règles
- **Profil CI** : `max_examples=25`, pas de deadline (enregistré dans [conftest.py](conftest.py)) ;
  lancement : `uv run --group test pytest -m hypothesis`

**🚧 Garde-fou (leçon de 2025)** : les stratégies restent **plates** — colonnes indépendantes,
contraintes simples (bornes `ge`/`le`, regex) dérivées des schémas Pandera, surcharges
ponctuelles par test. Dès qu'une stratégie exige des séquences d'événements C15 cohérentes
(MES → vie du contrat → RES) ou des dépendances entre colonnes (debut < fin), **on s'arrête** :
c'est le territoire des fixtures + snapshots (Niveau 1). La tentative de 2025 (commit `cea13af`,
supprimé) réinventait ~500 lignes de stratégies à la main pour générer des données métier
cohérentes — plus complexe que les pipelines testés. Ne pas recommencer.

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
├── conftest.py                      # 🆕 Fixtures globales + hooks pytest
├── fixtures/
│   ├── donnees_anonymisees/         # Fichiers parquet anonymisés (à venir)
│   └── cas_metier.py                # ✅ Fixtures pytest (2 implémentées)
├── unit/
│   ├── test_expressions_*.py        # Tests unitaires expressions Polars
│   └── test_*_parametrized.py       # 🆕 Tests paramétrés
├── property/                        # 🆕 Property-based tests (Niveau 4, issue #194)
│   ├── strategies.py                # Helper : schéma Pandera → stratégie parametric
│   ├── test_strategies.py           # Balle traçante du socle
│   ├── test_turpe_invariants.py     # Invariants TURPE fixe + variable
│   ├── test_taxes_invariants.py     # Invariants taxes : taux en vigueur, accise, CTA (#195)
│   └── test_facturation_invariants.py # Conservation à l'agrégation facturation (#196)
├── integration/
│   ├── test_pipelines_snapshot.py   # 🆕 Tests snapshot avec Syrupy
│   └── test_pipeline.py             # Tests avec fixtures
├── __snapshots__/                   # 🆕 Snapshots Syrupy (auto-créés)
└── README.md                        # Ce fichier
```

## Configuration pytest

Le fichier [pyproject.toml](../pyproject.toml) contient la configuration centralisée :

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "unit: Unit tests - fast, isolated",
    "integration: Integration tests",
    "slow: Tests > 5 seconds",
    "smoke: Critical tests for CI",
    "duckdb: Tests requiring DuckDB",
    "hypothesis: Property-based tests",
]
```

### Utiliser les markers

```bash
# Tests unitaires rapides uniquement
pytest -m unit

# Tests smoke pour CI rapide
pytest -m smoke

# Exclure les tests lents
pytest -m "not slow"

# Tests parallèles (avec pytest-xdist)
pytest -n auto
```

## Exemples d'implémentation

### 1. Fixture de cas métier (implémentée)

```python
@pytest.fixture
def cas_mct_changement_calendrier():
    """MCT avec passage de BASE vers HP/HC"""
    historique = pl.LazyFrame({
        "pdl": ["PDL00001", "PDL00001"],
        "Date_Evenement": [datetime(2024, 1, 1), datetime(2024, 6, 1)],
        "Id_Calendrier_Distributeur": ["DI000001", "DI000002"],  # BASE → HP/HC
        # ... autres colonnes
    })

    releves = pl.LazyFrame({
        "pdl": ["PDL00001", "PDL00001", "PDL00001"],
        "date_releve": [datetime(2024, 2, 1), datetime(2024, 7, 1), datetime(2024, 8, 1)],
        "BASE": [1500.0, None, None],
        "HP": [None, 2000.0, 2300.0],
        "HC": [None, 1200.0, 1400.0],
        # ... autres colonnes
    })

    return {"historique": historique, "releves": releves}
```

Voir [tests/fixtures/cas_metier.py](fixtures/cas_metier.py) pour les fixtures complètes.

### 2. Test snapshot avec Syrupy

```python
@pytest.mark.integration
@pytest.mark.smoke
def test_pipeline_perimetre_snapshot(
    historique_snapshot_test: pl.LazyFrame,
    snapshot: SnapshotAssertion
):
    """Test snapshot du pipeline périmètre - détecte automatiquement les régressions."""
    result = pipeline_perimetre(historique_snapshot_test).collect()

    # Syrupy capture et compare automatiquement
    assert result.to_dicts() == snapshot
```

**Workflow snapshot** :
```bash
# Première exécution : crée les snapshots
pytest tests/integration/test_pipelines_snapshot.py

# Exécutions suivantes : compare avec snapshots
pytest tests/integration/test_pipelines_snapshot.py

# Si changement volontaire : mettre à jour
pytest --snapshot-update
```

Voir [tests/integration/test_pipelines_snapshot.py](integration/test_pipelines_snapshot.py) pour exemples complets.

### 3. Test unitaire paramétré

```python
@pytest.mark.unit
@pytest.mark.parametrize(
    "valeurs,expected_changements,description",
    [
        ([6.0, 6.0, 9.0, 9.0, 3.0], [False, False, True, False, True], "changement_simple"),
        ([None, 6.0, 6.0, None, 3.0], [False, False, False, False, False], "avec_nulls"),
        ([6.0, 6.0, 6.0], [False, False, False], "sequence_constante"),
    ],
    ids=lambda x: x if isinstance(x, str) else ""
)
def test_expr_changement_cases(valeurs, expected_changements, description):
    """Teste expr_changement avec différents patterns de données."""
    df = pl.DataFrame({
        "ref_situation_contractuelle": ["A"] * len(valeurs),
        "valeur": valeurs,
    })

    result = df.select(expr_changement("valeur").alias("changement"))

    assert result["changement"].to_list() == expected_changements
```

**Avantages** :
- Un seul test pour 3+ cas
- Noms de cas clairs dans le rapport
- Facile d'ajouter de nouveaux cas

Voir [tests/unit/test_expressions_perimetre_parametrized.py](unit/test_expressions_perimetre_parametrized.py) et [tests/unit/test_turpe_parametrized.py](unit/test_turpe_parametrized.py).

### Test d'invariant property-based (Niveau 4)
```python
from tests.property.strategies import strategie_depuis_schema

@pytest.mark.hypothesis
@given(periodes=strategie_depuis_schema(
    PeriodeAbonnement,
    colonnes=["pdl", "formule_tarifaire_acheminement", "puissance_souscrite_kva", "nb_jours", "debut"],
    surcharges={  # uniquement ce que le schéma ne sait pas
        "formule_tarifaire_acheminement": st.sampled_from(FTAS_REELLES),
        "nb_jours": st.integers(1, 366),
    },
))
def test_turpe_fixe_contributions_positives(periodes):
    """Puissances ≥ 0 → TURPE fixe ≥ 0 (les tarifs réels sont positifs)."""
    result = ajouter_turpe_fixe(periodes.lazy(), REGLES.lazy()).collect()
    assert (result["turpe_fixe_eur"] >= 0).all()
```

Dtypes, nullabilité, bornes (`ge=0`) et regex viennent du schéma Pandera — pas de
quatrième copie du savoir de typage. Voir [tests/property/](property/).

## Processus d'anonymisation

### Script d'anonymisation (implémenté)

Un script complet est disponible : [scripts/anonymiser_donnees.py](../scripts/anonymiser_donnees.py)

**Usage** :
```bash
poetry run python scripts/anonymiser_donnees.py \
    --input-historique data/prod/historique.parquet \
    --input-releves data/prod/releves.parquet \
    --output-dir tests/fixtures/donnees_anonymisees \
    --cas-name "mct_changement_calendrier"
```

**Principes d'anonymisation** :
- **PDL** : Remplacés par séquences génériques (PDL00001, PDL00002, ...)
- **Dates** : Décalage aléatoire uniforme de 365-730 jours
- **Index** : Arrondis avec bruit (+/- 2%) pour masquer consommations exactes
- **Références** : Anonymisées séquentiellement
- **✅ Préservé** : Relations temporelles, séquences d'événements, cohérence métier

Exemple de fonction :
```python
def anonymiser_cas_metier(historique_df, releves_df, seed=None):
    """Anonymise tout en préservant la cohérence."""
    # 1. Générer mapping PDL
    mapping_pdl = generer_mapping_pdl(tous_pdls, seed)

    # 2. Décalage temporel uniforme
    offset_days = random.randint(365, 730)

    # 3. Anonymiser historique + relevés
    # 4. Arrondir index avec bruit

    return historique_anonymise, releves_anonymises
```

## Outils utilisés

- ✅ **pytest** (8.4+) : Framework de test principal
- ✅ **syrupy** (4.9+) : Tests de snapshot automatiques
- ✅ **pytest-xdist** (3.8+) : Exécution parallèle
- ✅ **hypothesis** (6.0+) : Tests de propriétés (usage limité)
- ✅ **polars** (1.0+) : Manipulation des fixtures
- ✅ **pandera[polars]** (0.24+) : Validation des schémas

Installation :
```bash
poetry install --with test
```

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

### ✅ Complété (2025)
1. ✅ Configuration pytest centralisée avec markers
2. ✅ Fixtures globales via `conftest.py`
3. ✅ Tests paramétrés (périmètre + TURPE)
4. ✅ Tests snapshot avec Syrupy
5. ✅ Script d'anonymisation
6. ✅ Fixtures cas métier (2/6 implémentées)

### 🔄 En cours
- Extraire et anonymiser les 4 cas métier restants
- Ajouter tests snapshot pour pipeline facturation
- Migration progressive des tests existants vers parametrize

### 📋 À venir
- Coverage report automatique en CI
- Tests de performance (benchmarks)
- Documentation auto-générée des cas métier

## Commandes utiles

```bash
# Tous les tests
pytest

# Tests rapides seulement
pytest -m unit

# Tests critiques (CI rapide)
pytest -m smoke

# Tests parallèles
pytest -n auto

# Avec coverage
pytest --cov=electricore --cov-report=html

# Mettre à jour snapshots
pytest --snapshot-update

# Tests verbeux avec locals
pytest -vv --showlocals
```

---

## Historique : la tentative Hypothesis de 2025 (abandonnée)

La première tentative de property-based testing (commit `cea13af`, supprimé par `052c99f`)
maintenait ~500 lignes de stratégies manuelles (`electricore/core/testing/strategies_polars.py`,
module supprimé) pour générer des données métier cohérentes : PDL à 14 chiffres, calendriers
distributeur, séquences temporelles. La génération était devenue plus complexe que les
pipelines testés — c'est l'anti-pattern documenté en tête de ce fichier.

La reprise de 2026 (issue #194, [tests/property/](property/)) repose sur
`polars.testing.parametric` (inexistant pour nous à l'époque) et dérive les stratégies
des schémas Pandera au lieu de les écrire à la main.

---

*Cette approche privilégie le **pragmatisme** et la **maintenabilité** pour se concentrer sur l'essentiel : garantir que le pipeline Polars fonctionne correctement et détecter rapidement les régressions.*