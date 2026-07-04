# Tutoriel : Utilisation des fonctions TURPE en mode standalone

Ce guide explique comment utiliser les fonctions de calcul TURPE indépendamment du pipeline principal, pour des analyses ad-hoc, des tests, ou des intégrations personnalisées.

## Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Fonctions Pipeline (Usage Recommandé)](#fonctions-pipeline-usage-recommandé)
3. [Expressions individuelles (Usage Avancé)](#expressions-individuelles-usage-avancé)
4. [Cas d'usage pratiques](#cas-dusage-pratiques)
5. [Bonnes pratiques](#bonnes-pratiques)

---

## Vue d'ensemble

Le module TURPE fournit **2 fonctions principales** pour un usage externe :

- **`ajouter_turpe_fixe(periodes, regles=None)`** - Calcul du TURPE fixe
- **`ajouter_turpe_variable(periodes, regles=None)`** - Calcul du TURPE variable

Ces fonctions encapsulent toute la logique métier (jointure règles, filtrage temporel, détection C4/C5) et sont **prêtes à l'emploi**.

### Import et configuration

```python
import polars as pl
from electricore.core.pipelines.turpe import (
    # Fonctions pipeline (usage recommandé)
    ajouter_turpe_fixe,
    ajouter_turpe_variable,

    # Utilitaires
    load_turpe_rules,
)
```

---

## Fonctions Pipeline (Usage Recommandé)

### `ajouter_turpe_fixe()` - TURPE Fixe

**Signature** :
```python
def ajouter_turpe_fixe(
    periodes: pl.LazyFrame,
    regles: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame
```

**Fonctionnalités** :
- ✅ Joint automatiquement les règles TURPE (chargées si `regles=None`)
- ✅ Filtre les règles sur les dates d'application
- ✅ Détecte C4 vs C5 automatiquement
- ✅ Calcule le TURPE fixe pour la période
- ✅ Conserve toutes les colonnes originales

**Prérequis des données en entrée** :
```python
periodes = pl.LazyFrame({
    "pdl": [...],                                # Identifiant point de livraison
    "formule_tarifaire_acheminement": [...],     # FTA (ex: "BTINFCU4", "BTSUPCU")
    "puissance_souscrite": [...],                # Puissance C5 (kVA)
    "date_debut": [...],                         # Date début période
    "date_fin": [...],                           # Date fin période

    # Optionnel pour C4 (BT > 36 kVA) :
    "puissance_souscrite_hph": [...],            # 4 puissances souscrites
    "puissance_souscrite_hch": [...],
    "puissance_souscrite_hpb": [...],
    "puissance_souscrite_hcb": [...],
})
```

**Colonnes ajoutées** :
- `turpe_fixe` (€) - Montant TURPE fixe pour la période

#### Exemple 1 : Calcul TURPE fixe C5 simple

```python
# Périodes d'abonnement résidentiel (BT ≤ 36 kVA)
periodes = pl.LazyFrame({
    "pdl": ["PDL001", "PDL002"],
    "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCUST"],
    "puissance_souscrite": [9.0, 6.0],
    "date_debut": ["2025-01-01", "2025-01-01"],
    "date_fin": ["2025-03-31", "2025-03-31"],
})

# Calcul automatique avec règles officielles
result = ajouter_turpe_fixe(periodes).collect()

print(result.select(["pdl", "puissance_souscrite", "turpe_fixe"]))
```

**Résultat** :
```
┌────────┬──────────────────────┬────────────┐
│ pdl    │ puissance_souscrite  │ turpe_fixe │
│ str    │ f64                  │ f64        │
├────────┼──────────────────────┼────────────┤
│ PDL001 │ 9.0                  │ 30.55      │
│ PDL002 │ 6.0                  │ 25.13      │
└────────┴──────────────────────┴────────────┘
```

#### Exemple 2 : Calcul TURPE fixe C4 avec modulation saisonnière

```python
# Point industriel avec 4 puissances souscrites (BT > 36 kVA)
periodes_c4 = pl.LazyFrame({
    "pdl": ["PDL_INDUSTRIE"],
    "formule_tarifaire_acheminement": ["BTSUPCU"],
    "puissance_souscrite": [60.0],  # Utilisé pour info, pas pour calcul C4

    # 4 puissances par cadran temporel (économies via modulation)
    "puissance_souscrite_hph": [36.0],  # Hiver Pleines Heures (P₁)
    "puissance_souscrite_hch": [36.0],  # Hiver Creuses Heures (P₂)
    "puissance_souscrite_hpb": [60.0],  # Été Pleines Heures (P₃)
    "puissance_souscrite_hcb": [60.0],  # Été Creuses Heures (P₄)

    "date_debut": ["2025-01-01"],
    "date_fin": ["2025-12-31"],
})

result = ajouter_turpe_fixe(periodes_c4).collect()

print(f"TURPE fixe annuel C4 : {result['turpe_fixe'][0]:.2f} €")
```

**Résultat** :
```
TURPE fixe annuel C4 : 1484.47 €

💡 Contrainte réglementaire C4 : P₁ ≤ P₂ ≤ P₃ ≤ P₄
💡 Économies : ~5% vs puissance constante 60 kVA
```

---

### `ajouter_turpe_variable()` - TURPE Variable

**Signature** :
```python
def ajouter_turpe_variable(
    periodes: pl.LazyFrame,
    regles: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame
```

**Fonctionnalités** :
- ✅ Joint automatiquement les règles TURPE
- ✅ Filtre les règles sur les dates d'application
- ✅ Calcule le TURPE variable par cadran horaire
- ✅ Ajoute la composante dépassement (C4 uniquement)
- ✅ Conserve toutes les colonnes originales

**Prérequis des données en entrée** :
```python
periodes = pl.LazyFrame({
    "pdl": [...],
    "formule_tarifaire_acheminement": [...],
    "date_debut": [...],
    "date_fin": [...],

    # Énergies par cadran (kWh) - selon FTA :
    # C4 (4 cadrans) :
    "energie_hph_kwh": [...],  # Hiver Pleines Heures
    "energie_hch_kwh": [...],  # Hiver Creuses Heures
    "energie_hpb_kwh": [...],  # Été Pleines Heures
    "energie_hcb_kwh": [...],  # Été Creuses Heures

    # OU C5 HP/HC (2 cadrans) :
    "energie_hp_kwh": [...],   # Heures Pleines
    "energie_hc_kwh": [...],   # Heures Creuses

    # OU C5 Base (1 cadran) :
    "energie_base_kwh": [...],

    # Optionnel pour C4 - Dépassements de puissance :
    "duree_depassement_h": [...],  # Durée totale dépassement (heures)
})
```

**Colonnes ajoutées** :
- `turpe_variable` (€) - Montant TURPE variable (cadrans + dépassement)

#### Exemple 3 : Calcul TURPE variable C4 avec 4 cadrans

```python
# Période avec énergies par cadran horaire
periodes = pl.LazyFrame({
    "pdl": ["PDL001"],
    "formule_tarifaire_acheminement": ["BTSUPCU"],
    "date_debut": ["2025-01-01"],
    "date_fin": ["2025-01-31"],

    # Énergies C4 (kWh)
    "energie_hph_kwh": [1000.0],  # Hiver Pleines Heures
    "energie_hch_kwh": [800.0],   # Hiver Creuses Heures
    "energie_hpb_kwh": [600.0],   # Été Pleines Heures
    "energie_hcb_kwh": [400.0],   # Été Creuses Heures

    # Dépassement (optionnel)
    "duree_depassement_h": [10.0],  # 10h de dépassement total
})

result = ajouter_turpe_variable(periodes).collect()

print(f"TURPE variable : {result['turpe_variable'][0]:.2f} €")
```

**Résultat** :
```
TURPE variable : 245.74 €

Détail : 121.64 € (cadrans) + 124.10 € (dépassement)
```

#### Exemple 4 : Calcul TURPE variable C5 HP/HC simple

```python
# Point résidentiel avec heures pleines/creuses
periodes_hphc = pl.LazyFrame({
    "pdl": ["PDL002"],
    "formule_tarifaire_acheminement": ["BTINFCU4"],
    "date_debut": ["2025-01-01"],
    "date_fin": ["2025-01-31"],

    # Énergies HP/HC (kWh)
    "energie_hp_kwh": [2000.0],
    "energie_hc_kwh": [1500.0],
})

result = ajouter_turpe_variable(periodes_hphc).collect()

print(result.select(["pdl", "energie_hp_kwh", "energie_hc_kwh", "turpe_variable"]))
```

**Résultat** :
```
┌────────┬────────────────┬────────────────┬────────────────┐
│ pdl    │ energie_hp_kwh │ energie_hc_kwh │ turpe_variable │
│ str    │ f64            │ f64            │ f64            │
├────────┼────────────────┼────────────────┼────────────────┤
│ PDL002 │ 2000.0         │ 1500.0         │ 176.80         │
└────────┴────────────────┴────────────────┴────────────────┘
```

---

## Expressions individuelles (Usage Avancé)

> ⚠️ **Avertissement** : Ces expressions sont conçues pour un usage **interne au pipeline**.
> Leur utilisation directe nécessite de gérer manuellement :
> - Le chargement et la jointure avec les règles TURPE
> - Le filtrage temporel des règles applicables
> - La validation des colonnes requises
> - La création/gestion de toutes les colonnes intermédiaires
>
> **Usage recommandé** : Utilisez `ajouter_turpe_fixe()` et `ajouter_turpe_variable()` sauf besoin très spécifique.

### Expressions TURPE Fixe

```python
from electricore.core.pipelines.turpe import (
    expr_calculer_turpe_fixe_annuel,      # Calcul annuel C4/C5
    expr_calculer_turpe_fixe_journalier,  # Proratisation journalière
    expr_calculer_turpe_fixe_periode,     # Montant période
    expr_valider_puissances_croissantes_c4,  # Validation P₁≤P₂≤P₃≤P₄
)
```

#### Exemple minimaliste (non recommandé)

```python
# ⚠️ Vous devez gérer manuellement les règles et colonnes
regles = load_turpe_rules()

df = pl.DataFrame({
    "formule_tarifaire_acheminement": ["BTINFCU4"],
    "puissance_souscrite": [9.0],
    "date_debut": ["2025-01-01"],
    "nb_jours": [92],

    # Colonnes C4 obligatoires même pour C5 !
    "b_hph": [None], "b_hch": [None], "b_hpb": [None], "b_hcb": [None],
    "puissance_souscrite_hph": [None],
    "puissance_souscrite_hch": [None],
    "puissance_souscrite_hpb": [None],
    "puissance_souscrite_hcb": [None],
})

# Jointure et filtrage manuel
result = (
    df.join(regles, left_on="formule_tarifaire_acheminement",
            right_on="Formule_Tarifaire_Acheminement", how="left")
    .filter(pl.col("start") <= pl.col("date_debut"))
    .with_columns([
        expr_calculer_turpe_fixe_annuel().alias("turpe_annuel"),
        expr_calculer_turpe_fixe_periode().alias("turpe_periode"),
    ])
)
```

### Expressions TURPE Variable

```python
from electricore.core.pipelines.turpe import (
    expr_calculer_turpe_cadran,              # Calcul par cadran individuel
    expr_calculer_turpe_contributions_cadrans,  # Tous cadrans (dict)
    expr_sommer_turpe_cadrans,               # Somme des cadrans
    expr_calculer_composante_depassement,    # Pénalités dépassement (C4)
)
```

#### Exemple calcul par cadran individuel

```python
# Calcul manuel pour un cadran spécifique
df = pl.DataFrame({
    "energie_hph_kwh": [1000.0],
    "c_hph": [6.91],  # c€/kWh (doit être récupéré des règles)
})

result = df.with_columns(
    expr_calculer_turpe_cadran("hph").alias("turpe_hph")
)

print(result["turpe_hph"][0])  # → 69.10 €
```

### Composante dépassement (C4 uniquement)

```python
from electricore.core.pipelines.turpe import expr_calculer_composante_depassement
```

**⚠️ Important** : Cette expression attend :
- `duree_depassement_h` (float) - Durée totale de dépassement en **heures** (tous cadrans confondus)
- `cmdps` (float) - Tarif €/h issu des règles TURPE

**Le calcul des dépassements est à la charge de l'appelant** (mesure Linky/analyseur).

#### Exemple réaliste

```python
# Vous devez calculer vous-même les dépassements par cadran et les sommer
df = pl.DataFrame({
    "pdl": ["PDL_C4"],
    "cmdps": [12.41],  # €/h (règles TURPE pour C4)

    # Durée totale de dépassement calculée en amont
    # Exemple : somme des heures où P_réelle > P_souscrite_cadran
    "duree_depassement_h": [10.0],  # 10 heures de dépassement
})

result = df.with_columns(
    expr_calculer_composante_depassement().alias("penalite_depassement")
)

print(f"Pénalité : {result['penalite_depassement'][0]:.2f} €")
```

**Résultat** :
```
Pénalité : 124.10 €

Formule : duree_depassement_h × cmdps
        = 10 h × 12.41 €/h = 124.10 €
```

---

## Cas d'usage pratiques

### Cas 1 : Pipeline complet TURPE fixe + variable

```python
from electricore.core.pipelines.turpe import ajouter_turpe_fixe, ajouter_turpe_variable

# Données d'entrée combinées
periodes = pl.LazyFrame({
    "pdl": ["PDL001"],
    "formule_tarifaire_acheminement": ["BTINFCU4"],
    "puissance_souscrite": [9.0],
    "date_debut": ["2025-01-01"],
    "date_fin": ["2025-01-31"],

    # Énergies HP/HC
    "energie_hp_kwh": [500.0],
    "energie_hc_kwh": [300.0],
})

# Chaînage des 2 fonctions
result = (
    periodes
    .pipe(ajouter_turpe_fixe)
    .pipe(ajouter_turpe_variable)
    .collect()
)

print(result.select(["pdl", "turpe_fixe", "turpe_variable"]))
```

### Cas 2 : Comparaison tarifaire multi-FTA

```python
# Simuler plusieurs formules tarifaires pour un même profil
profils = pl.LazyFrame({
    "scenario": ["Base 6kVA", "HP/HC 9kVA", "Tempo 12kVA"],
    "formule_tarifaire_acheminement": ["BTINFBASE", "BTINFCU4", "BTINFMU4"],
    "puissance_souscrite": [6.0, 9.0, 12.0],
    "date_debut": ["2025-01-01"] * 3,
    "date_fin": ["2025-12-31"] * 3,

    # Même consommation pour tous
    "energie_base_kwh": [3500.0, None, None],
    "energie_hp_kwh": [None, 2100.0, None],
    "energie_hc_kwh": [None, 1400.0, None],
})

# Calculs
result = (
    profils
    .pipe(ajouter_turpe_fixe)
    .pipe(ajouter_turpe_variable)
    .with_columns(
        (pl.col("turpe_fixe") + pl.col("turpe_variable")).alias("turpe_total")
    )
    .collect()
)

print(result.select(["scenario", "turpe_fixe", "turpe_variable", "turpe_total"]))
```

### Cas 3 : Analyse de sensibilité puissance souscrite

```python
# Simuler l'impact de différentes puissances
puissances = [6.0, 9.0, 12.0, 15.0, 18.0]

scenarios = pl.LazyFrame({
    "puissance_souscrite": puissances,
    "formule_tarifaire_acheminement": ["BTINFCU4"] * len(puissances),
    "date_debut": ["2025-01-01"] * len(puissances),
    "date_fin": ["2025-12-31"] * len(puissances),
})

result = (
    scenarios
    .pipe(ajouter_turpe_fixe)
    .with_columns(
        (pl.col("turpe_fixe") / pl.col("puissance_souscrite")).alias("cout_par_kva")
    )
    .collect()
)

print(result.select(["puissance_souscrite", "turpe_fixe", "cout_par_kva"]))
```

### Cas 4 : Intégration avec données Odoo

```python
from electricore.core.loaders import OdooReader

# Charger les abonnements depuis Odoo
with OdooReader(config) as odoo:
    abonnements = (
        odoo.query('x_pdl', domain=[('x_actif', '=', True)])
        .select(['x_name', 'x_fta', 'x_puissance_souscrite', 'x_date_debut'])
        .collect()
    )

# Renommer pour compatibilité pipeline TURPE
periodes = abonnements.rename({
    "x_name": "pdl",
    "x_fta": "formule_tarifaire_acheminement",
    "x_puissance_souscrite": "puissance_souscrite",
    "x_date_debut": "date_debut",
}).with_columns(
    pl.col("date_debut").dt.offset_by("1y").alias("date_fin")  # 1 an
)

# Calcul TURPE
result = periodes.lazy().pipe(ajouter_turpe_fixe).collect()
```

---

## Bonnes pratiques

### 1. Toujours utiliser les fonctions pipeline

```python
# ✅ BON - Gestion automatique des règles et validations
result = ajouter_turpe_fixe(periodes)

# ❌ MAUVAIS - Réinvention de la roue, risque d'erreurs
result = periodes.with_columns(expr_calculer_turpe_fixe_annuel())
```

### 2. Charger les règles TURPE une seule fois

```python
# ✅ BON - Réutilisation des règles
regles = load_turpe_rules()
result1 = ajouter_turpe_fixe(periodes1, regles=regles)
result2 = ajouter_turpe_fixe(periodes2, regles=regles)

# ❌ MAUVAIS - Rechargement inutile
result1 = ajouter_turpe_fixe(periodes1)  # charge les règles
result2 = ajouter_turpe_fixe(periodes2)  # recharge les règles
```

### 3. Validation des données C4

```python
from electricore.core.pipelines.turpe import expr_valider_puissances_croissantes_c4

# Toujours valider la contrainte P₁ ≤ P₂ ≤ P₃ ≤ P₄
periodes_c4 = periodes_c4.with_columns(
    expr_valider_puissances_croissantes_c4().alias("contrainte_ok")
)

# Filtrer les lignes invalides
invalides = periodes_c4.filter(~pl.col("contrainte_ok"))
if invalides.height > 0:
    print(f"⚠️ {invalides.height} points C4 ne respectent pas P₁≤P₂≤P₃≤P₄")
```

### 4. Gestion des colonnes optionnelles

```python
# Les fonctions pipeline gèrent automatiquement les colonnes manquantes
# Inutile de créer manuellement les colonnes C4 pour un point C5

periodes_c5 = pl.LazyFrame({
    "pdl": ["PDL001"],
    "formule_tarifaire_acheminement": ["BTINFCU4"],
    "puissance_souscrite": [9.0],
    "date_debut": ["2025-01-01"],
    "date_fin": ["2025-03-31"],
    # ✅ Pas besoin de créer puissance_souscrite_hph/hch/hpb/hcb
})

result = ajouter_turpe_fixe(periodes_c5)  # Fonctionne directement
```

### 5. Utiliser LazyFrame pour optimiser les performances

```python
# ✅ BON - LazyFrame optimisé
periodes_lf = pl.scan_parquet("periodes.parquet")
result = ajouter_turpe_fixe(periodes_lf).collect()

# ⚠️ Moins optimal - DataFrame eager
periodes_df = pl.read_parquet("periodes.parquet")
result = ajouter_turpe_fixe(periodes_df.lazy()).collect()
```

---

## Ressources complémentaires

- **Code source** : [electricore/core/pipelines/turpe.py](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/core/pipelines/turpe.py)
- **Tests exhaustifs** : [tests/unit/test_turpe.py](https://github.com/Energie-De-Nantes/electricore/blob/main/tests/unit/test_turpe.py)
- **Documentation C4** : [turpe-fixe-c4-btsup36kva.md](./turpe-fixe-c4-btsup36kva.md)
- **Tarifs CRE officiels** : [electricore/config/turpe_rules.csv](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/config/turpe_rules.csv)
- **Nomenclature CRE** : Délibération CRE 2025-40 du 30 janvier 2025

---

## Support

Pour toute question ou cas d'usage spécifique, consultez les tests unitaires qui couvrent 38 scénarios différents avec des exemples concrets et validés.

Les fonctions `ajouter_turpe_fixe()` et `ajouter_turpe_variable()` sont conçues pour être **directement réutilisables** sans modification. Si vous avez besoin d'une personnalisation, ouvrez une issue pour discussion.
