# Guide de transmission — ElectriCore

**Pour** : ingénieur Rust/Java, premier contact avec le projet.
**Objectif** : comprendre ce que fait le code, comment le faire tourner, et comment s'y retrouver.

---

## 1. Contexte métier

### Le problème

En France, la distribution d'électricité est gérée par **Enedis** (gestionnaire du réseau). Chaque point de livraison (PDL, le "compteur" Linky) produit des **flux de données** : relevés d'index, événements contractuels, factures réseau.

Ces flux sont transmis aux fournisseurs d'énergie en format XML/CSV propriétaire, appelés **flux Enedis** (R15, R151, C15, F15…). ElectriCore transforme ces fichiers bruts en données exploitables pour la facturation et l'analyse.

### Ce que fait ElectriCore

```
Fichiers XML/CSV Enedis   →   Base DuckDB   →   Calculs métier   →   Odoo / API
     (flux R15, C15…)           (ETL)          (pipelines Polars)     (résultats)
```

Concrètement, le pipeline de facturation calcule pour chaque PDL et chaque mois :
- La **puissance souscrite** et sa durée (abonnement)
- La **consommation** par cadran horaire (heures pleines/creuses)
- Le **TURPE** : taxe réseau réglementaire (fixe + variable)
- L'**Accise** (ancienne TICFE) : taxe intérieure sur l'électricité
- La **facturation mensuelle** agrégée prête pour Odoo

---

## 2. Toolchain

### Analogie Cargo → uv

Si tu viens de Rust, `uv` joue le même rôle que `cargo` :

| Cargo | uv | Rôle |
|---|---|---|
| `Cargo.toml` | `pyproject.toml` | Manifeste du projet |
| `Cargo.lock` | `uv.lock` | Lockfile déterministe |
| `cargo build` | `uv build` | Build du package |
| `cargo test` | `uv run --group test pytest` | Tests |
| `cargo run` | `uv run python …` | Exécution |
| `[dev-dependencies]` | `[dependency-groups] test` | Dépendances de dev |

### Setup

```bash
# Prérequis : Python 3.12+, uv
uv sync              # installe tout (équivalent : cargo build)
uv sync --extra etl  # + dépendances SFTP pour l'ETL (serveur de collecte)

# Vérifier que ça fonctionne
uv run --group test pytest -q
# → 183 passed, 12 skipped
```

---

## 3. Architecture des modules

```
electricore/
├── etl/          # Collecte : SFTP Enedis → DuckDB  (optionnel, --extra etl)
├── core/         # Calculs : DuckDB → résultats métier
│   ├── loaders/  # Lecture des données (DuckDB, Odoo, Parquet)
│   ├── pipelines/# Transformations métier
│   ├── models/   # Schémas de validation (Pandera)
│   └── writers/  # Écriture vers Odoo
├── api/          # API REST FastAPI
└── config/       # Fichiers CSV de règles tarifaires (TURPE, Accise)
```

Le **core** est le cœur : les pipelines transforment des données lues depuis DuckDB en résultats de facturation, sans effet de bord.

---

## 4. Comprendre Polars (l'équivalent des iterators Rust)

Le code utilise **Polars**, une bibliothèque de traitement de données en colonnes. Les analogies avec Rust :

### LazyFrame ≈ Iterator non-consommé / query plan

```python
# Équivalent Rust : iter().filter(...).map(...)  (pas encore exécuté)
lf = c15().filter({"pdl": ["PDL123"]}).lazy()

# .collect() = .collect::<Vec<_>>()  → exécute et matérialise
df = lf.collect()
```

Un `LazyFrame` représente un **plan de calcul** qui n'est pas encore exécuté. Polars optimise ce plan (réordonne les filtres, fusionne les opérations) avant de l'exécuter. C'est l'équivalent d'un `Iterator` Rust paresseux, ou d'un `Stream` Java.

### pl.Expr ≈ fonction composable

```python
# Une expression = une transformation réutilisable sur une colonne
expr = pl.col("energie_hp_kwh") * pl.col("c_hp") / 100

# On l'applique via .with_columns() ou .select()
df.with_columns(expr.alias("turpe_hp"))
```

Les expressions sont **pures et composables** — comme des fonctions Rust qui prennent et retournent des valeurs sans muter l'état.

### Exemple complet : calcul TURPE variable

```python
# Dans electricore/core/pipelines/turpe.py
def expr_calculer_turpe_cadran(cadran: str) -> pl.Expr:
    """Retourne l'expression de calcul TURPE pour un cadran donné."""
    return (
        pl.when(pl.col(f"energie_{cadran}_kwh").is_not_null())
        .then(pl.col(f"energie_{cadran}_kwh") * pl.col(f"c_{cadran}") / 100)
        .otherwise(0.0)
    )

# Usage
df.with_columns(
    expr_calculer_turpe_cadran("hp").alias("turpe_hp_eur")
)
```

---

## 5. Flux de données complet

### Les flux Enedis (entrées)

| Flux | Contenu | Table DuckDB |
|------|---------|--------------|
| **C15** | Événements contractuels (mise en service, résiliation, changement FTA…) | `flux_c15` |
| **R151** | Relevés périodiques Linky (index HP/HC/Base) | `flux_r151` |
| **R15** | Relevés à la demande + événements | `flux_r15` |
| **F15** | Factures réseau Enedis | `flux_f15_detail` |

### Le pipeline de facturation

```
C15 (historique contractuel)          R151 (relevés d'index)
        │                                      │
        ▼                                      │
pipeline_perimetre()                           │
  → détecte les ruptures de période           │
  → enrichit avec les événements              │
        │                                      │
        ├──────────────────────────────────────┤
        │                                      │
        ▼                                      ▼
pipeline_abonnements()           pipeline_energie()
  → calcule durée de chaque         → calcule consommation
    période (nb_jours)                par cadran (HP/HC/Base)
  → ajoute TURPE fixe               → ajoute TURPE variable
        │                                      │
        └──────────────┬───────────────────────┘
                       ▼
               pipeline_facturation()
                 → agrège par mois
                 → méta-périodes mensuelles
                 → validation Pandera
                       │
                       ▼
              ResultatFacturationPolars
              (NamedTuple immutable)
```

### Appel du pipeline complet

```python
from electricore.core.pipelines.orchestration import facturation
from electricore.core.loaders import c15, releves

# Charger les données sources (lazy — pas encore exécuté)
historique_lf = c15().lazy()
releves_lf = releves().lazy()

# Lancer le pipeline complet
result = facturation(historique_lf, releves_lf)

# Accéder aux résultats
df_mensuel = result.facturation          # DataFrame déjà collecté
df_abos = result.abonnements.collect()   # LazyFrame → DataFrame
```

`ResultatFacturationPolars` est un `NamedTuple` (≈ struct Rust avec champs nommés) :

```python
class ResultatFacturationPolars(NamedTuple):
    historique_enrichi: pl.LazyFrame
    abonnements: Optional[pl.LazyFrame] = None
    energie: Optional[pl.LazyFrame] = None
    facturation: Optional[pl.DataFrame] = None
```

---

## 6. Query Builders (lecture des données)

L'accès à DuckDB passe par des **query builders immutables** avec une API fluente :

```python
from electricore.core.loaders import c15, r151, releves

# Équivalent d'un query builder SQL typé
historique = (
    c15()
    .filter({"pdl": ["PDL001", "PDL002"]})
    .filter({"Date_Evenement": ">= '2024-01-01'"})
    .limit(1000)
    .collect()  # → exécute la requête SQL sous-jacente
)
```

Chaque méthode retourne une **nouvelle instance** (immutable, comme les méthodes Rust sur `Iterator`). `.collect()` déclenche l'exécution.

---

## 7. Validation des données (Pandera)

Les pipelines critiques (facturation) utilisent **Pandera** pour valider les schémas :

```python
# @pa.check_types valide les types des colonnes en entrée ET en sortie
@pa.check_types(lazy=True)
def pipeline_facturation(
    abonnements: LazyFrame[PeriodeAbonnementModel],
    energie: LazyFrame[PeriodeEnergieModel]
) -> DataFrame[ConsommationMensuelleModel]:
    ...
```

C'est l'équivalent d'une signature de type stricte avec validation à l'exécution — proche des traits Rust mais au niveau des colonnes du DataFrame.

---

## 8. Règles tarifaires (config CSV)

TURPE et Accise ne sont **pas codés en dur** : les taux sont dans des fichiers CSV avec des plages de validité temporelle.

```
electricore/config/
├── turpe_rules.csv   # Taux TURPE par FTA (formule tarifaire) + période de validité
└── accise_rules.csv  # Taux Accise par période (modification législative)
```

Les pipelines chargent ces règles via `load_turpe_rules()` / `load_accise_rules()` et font une jointure temporelle pour appliquer le bon taux selon la date de la période.

---

## 9. Domaine : glossaire minimal

| Terme | Signification |
|-------|---------------|
| **PDL** | Point De Livraison — identifiant unique d'un compteur |
| **FTA** | Formule Tarifaire d'Acheminement — type de contrat réseau (BTINFCUST, BTSUPCU4…) |
| **C5/C4** | Segments de clientèle : C5 = BT ≤ 36 kVA, C4 = BT > 36 kVA |
| **HP/HC** | Heures Pleines / Heures Creuses |
| **HPH/HCH/HPB/HCB** | Saison Haute/Basse × Pleines/Creuses (Tempo/EJP) |
| **TURPE** | Taxe réseau réglementaire (fixe/puissance + variable/consommation) |
| **Accise** | Ancienne TICFE, taxe intérieure sur l'électricité |
| **Flux C15** | Événements contractuels : MES (mise en service), RES (résiliation), MCT (modification)… |
| **Flux R151** | Relevés périodiques Linky (envoyés tous les mois par Enedis) |

---

## 10. Tests et développement

```bash
# Lancer tous les tests
uv run --group test pytest -q

# Tests d'un module spécifique
uv run --group test pytest tests/unit/test_turpe.py -v

# Comprendre les marqueurs de test
# unit        → tests unitaires purs, rapides
# integration → pipelines complets (certains nécessitent des données réelles)
# duckdb      → nécessite la vraie base DuckDB locale
# skip_ci     → données de prod, ne pas lancer en CI
```

### État de la couverture

| Module | Tests |
|--------|-------|
| `perimetre`, `abonnements`, `energie`, `turpe` | ✅ Complets |
| `facturation` | ⚠️ Expressions seulement |
| `accise`, `orchestration` | ❌ À faire |
| `api`, `etl` | ❌ Non couverts |

### Notebooks d'exploration

Des notebooks **Marimo** (interactifs, réactifs) sont disponibles dans `notebooks/`. Pour les lancer :

```bash
uv run marimo edit notebooks/
```

Marimo est à Python ce qu'un Jupyter notebook est à R — mais avec une réactivité garantie (recalcul automatique des cellules dépendantes).

---

## 11. Points d'entrée recommandés

Pour comprendre le code dans l'ordre :

1. [electricore/core/pipelines/perimetre.py](../electricore/core/pipelines/perimetre.py) — pipeline le plus simple, bien testé
2. [electricore/core/pipelines/turpe.py](../electricore/core/pipelines/turpe.py) — expressions composables, bonne illustration du pattern
3. [electricore/core/pipelines/orchestration.py](../electricore/core/pipelines/orchestration.py) — comment tout s'assemble
4. [tests/unit/test_turpe.py](../tests/unit/test_turpe.py) — tests = documentation exécutable
5. [docs/conventions-dates-enedis.md](conventions-dates-enedis.md) — convention critique pour les dates
