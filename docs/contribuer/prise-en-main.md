---
fraicheur: 2026-07-08
---

# Prise en main

Point d'entrée pour qui découvre le code : comprendre ce que fait ElectriCore, comment le
faire tourner, et par où commencer à lire. Pour l'architecture détaillée, les patterns
établis et les exemples de code une fois le contexte posé, voir [Développer](developper.md)
— ce document ne duplique pas ce qu'il couvre déjà.

> Si tu viens d'un langage compilé (Rust, Java, Go…), quelques analogies reviennent dans ce
> guide (`uv` ≈ `cargo`, `LazyFrame` ≈ itérateur paresseux) — elles sont optionnelles,
> passe-les si elles ne t'aident pas.

---

## 1. Contexte métier

### Le problème

En France, la distribution d'électricité est gérée par **Enedis** (gestionnaire du réseau). Chaque point de livraison (PDL, le "compteur" Linky) produit des **flux de données** : relevés d'index, événements contractuels, factures réseau.

Ces flux sont transmis aux fournisseurs d'énergie en format XML/CSV propriétaire, appelés **flux Enedis** (R15, R151, C15, F15…). ElectriCore transforme ces fichiers bruts en données exploitables pour la facturation et l'analyse.

### Ce que fait ElectriCore

```
Fichiers XML/CSV Enedis   →   Base DuckDB   →   Calculs métier   →   Odoo / API
     (flux R15, C15…)           (ingestion)    (pipelines Polars)     (résultats)
```

Concrètement, le pipeline de facturation calcule pour chaque PDL et chaque mois :
- La **puissance souscrite** et sa durée (abonnement)
- La **consommation** par cadran horaire (heures pleines/creuses)
- Le **TURPE** : taxe réseau réglementaire (fixe + variable)
- L'**Accise** (ancienne TICFE) : taxe intérieure sur l'électricité
- La **facturation mensuelle** agrégée prête pour Odoo

---

## 2. Toolchain

<details>
<summary>Si tu viens de Rust : <code>uv</code> joue le rôle de <code>cargo</code></summary>

| Cargo | uv | Rôle |
|---|---|---|
| `Cargo.toml` | `pyproject.toml` | Manifeste du projet |
| `Cargo.lock` | `uv.lock` | Lockfile déterministe |
| `cargo build` | `uv build` | Build du package |
| `cargo test` | `uv run --group test pytest` | Tests |
| `cargo run` | `uv run python …` | Exécution |
| `[dev-dependencies]` | `[dependency-groups] test` | Dépendances de dev |

</details>

### Setup

```bash
# Prérequis : Python 3.12+, uv
uv sync                                    # installe le runtime (core + API + bot)
uv sync --extra ingestion --extra dbt      # + pipeline d'ingestion SFTP (dlt + dbt)

# Vérifier que ça fonctionne
uv run --group test pytest -q
```

---

## 3. Comprendre Polars

Le code utilise **Polars**, une bibliothèque de traitement de données en colonnes.

### LazyFrame ≈ plan de requête non exécuté

```python
lf = c15().filter({"pdl": ["PDL123"]}).lazy()

# .collect() exécute et matérialise le résultat
df = lf.collect()
```

Un `LazyFrame` représente un **plan de calcul** qui n'est pas encore exécuté. Polars optimise ce plan (réordonne les filtres, fusionne les opérations) avant de l'exécuter — un peu comme un itérateur paresseux (`Iterator` Rust, `Stream` Java) qui ne consomme rien tant qu'on ne le collecte pas.

### pl.Expr ≈ transformation composable

```python
# Une expression = une transformation réutilisable sur une colonne
expr = pl.col("energie_hp_kwh") * pl.col("c_hp") / 100

# On l'applique via .with_columns() ou .select()
df.with_columns(expr.alias("turpe_hp"))
```

Les expressions sont **pures et composables** — elles prennent et retournent des valeurs sans muter d'état.

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

## 4. Architecture, pipelines, query builders, validation

L'architecture des modules (`ingestion/`, `core/`, `integrations/`, `api/`, `bot/`), le
pipeline de facturation complet (`contexte_du_mois()`), les query builders DuckDB/Odoo et
la validation Pandera sont couverts dans [Développer](developper.md) — pas dupliqués ici.

---

## 5. Règles tarifaires (config CSV)

TURPE et Accise ne sont **pas codés en dur** : les taux sont dans des fichiers CSV avec des plages de validité temporelle.

```
electricore/config/
├── turpe_rules.csv   # Taux TURPE par FTA (formule tarifaire) + période de validité
├── accise_rules.csv  # Taux Accise par période (modification législative)
└── cta_rules.csv     # Taux CTA par période
```

Les pipelines chargent ces règles via `load_turpe_rules()` / `load_accise_rules()` et font une jointure temporelle pour appliquer le bon taux selon la date de la période.

---

## 6. Domaine : glossaire

Le vocabulaire métier est éclaté par module : voir [CONTEXT-MAP.md](https://github.com/Energie-De-Nantes/electricore/blob/main/CONTEXT-MAP.md) à la racine. L'essentiel (PDL, FTA, C5/C4, HP/HC, TURPE, accise, événements C15, périmètre, abonnement…) vit dans [`electricore/core/CONTEXT.md`](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/core/CONTEXT.md).

---

## 7. Points d'entrée recommandés

Pour comprendre le code dans l'ordre :

1. [electricore/core/pipelines/historique.py](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/core/pipelines/historique.py) — pipeline le plus simple, bien testé
2. [electricore/core/pipelines/turpe.py](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/core/pipelines/turpe.py) — expressions composables, bonne illustration du pattern
3. [electricore/core/builds/contexte_mensuel.py](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/core/builds/contexte_mensuel.py) — comment tout s'assemble (détaillé dans [Développer](developper.md))
4. [tests/unit/test_turpe.py](https://github.com/Energie-De-Nantes/electricore/blob/main/tests/unit/test_turpe.py) — tests = documentation exécutable
5. [Conventions de dates Enedis](../conventions-dates-enedis.md) — convention critique pour les dates

Pour la suite (installer pour développer, tests, contribution, feuille de route) :
[Développer](developper.md).

[Retour à Contribuer](index.md).
