# Tutoriel : Utilisation des fonctions TURPE en mode standalone

Ce guide explique comment utiliser les fonctions de calcul TURPE indépendamment du pipeline principal, pour des analyses ad-hoc, des tests, ou des intégrations personnalisées.

## Table des matières

1. [Import et configuration](#import-et-configuration)
2. [TURPE Fixe - Calculs de base](#turpe-fixe---calculs-de-base)
3. [TURPE Fixe C4 - 4 puissances souscrites](#turpe-fixe-c4---4-puissances-souscrites)
4. [TURPE Variable - Calculs par cadran](#turpe-variable---calculs-par-cadran)
5. [CMDPS - Pénalités de dépassement](#cmdps---pénalités-de-dépassement)
6. [Cas d'usage avancés](#cas-dusage-avancés)

---

## Import et configuration

```python
import polars as pl
from electricore.core.pipelines.turpe import (
    # Expressions de calcul
    expr_calculer_turpe_fixe_annuel,
    expr_calculer_turpe_fixe_journalier,
    expr_calculer_turpe_fixe_periode,
    expr_calculer_turpe_cadran,
    expr_calculer_cmdps,
    expr_valider_puissances_croissantes_c4,

    # Fonctions de pipeline
    ajouter_turpe_fixe,
    ajouter_turpe_variable,

    # Utilitaires
    load_turpe_rules,
)
```

---

## TURPE Fixe - Calculs de base

### Exemple 1 : Calcul TURPE fixe annuel C5 (BT ≤ 36 kVA)

```python
# Créer un DataFrame avec les données nécessaires
df = pl.DataFrame({
    "pdl": ["PDL001", "PDL002", "PDL003"],
    "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCUST", "BTINFMU4"],
    "puissance_souscrite": [9.0, 6.0, 12.0],
    "b": [9.36, 10.44, 8.40],      # Coefficient puissance (€/kVA/an)
    "cg": [16.2, 16.2, 16.2],      # Composante gestion (€/an)
    "cc": [20.88, 20.88, 20.88],   # Composante comptage (€/an)
    # Colonnes C4 avec NULL pour C5
    "b_hph": [None, None, None],
    "b_hch": [None, None, None],
    "b_hpb": [None, None, None],
    "b_hcb": [None, None, None],
    "puissance_souscrite_hph": [None, None, None],
    "puissance_souscrite_hch": [None, None, None],
    "puissance_souscrite_hpb": [None, None, None],
    "puissance_souscrite_hcb": [None, None, None],
})

# Calculer le TURPE fixe annuel
result = df.with_columns(
    expr_calculer_turpe_fixe_annuel().alias("turpe_fixe_annuel")
)

print(result.select(["pdl", "puissance_souscrite", "turpe_fixe_annuel"]))
```

**Résultat attendu** :
```
┌────────┬──────────────────────┬────────────────────┐
│ pdl    │ puissance_souscrite  │ turpe_fixe_annuel  │
│ str    │ f64                  │ f64                │
├────────┼──────────────────────┼────────────────────┤
│ PDL001 │ 9.0                  │ 121.32             │
│ PDL002 │ 6.0                  │ 99.72              │
│ PDL003 │ 12.0                 │ 137.88             │
└────────┴──────────────────────┴────────────────────┘

Formule C5 : turpe_annuel = (b × P) + cg + cc
Exemple PDL001 : (9.36 × 9) + 16.2 + 20.88 = 121.32 €
```

### Exemple 2 : Calcul TURPE fixe pour une période

```python
df = pl.DataFrame({
    "pdl": ["PDL001"],
    "nb_jours": [92],  # Trimestre (jan-mars)
    "turpe_fixe_annuel": [121.32],
})

result = df.with_columns([
    expr_calculer_turpe_fixe_journalier().alias("turpe_journalier"),
    expr_calculer_turpe_fixe_periode().alias("turpe_periode"),
])

print(result)
```

**Résultat** :
```
┌────────┬──────────┬────────────────────┬──────────────────┬───────────────┐
│ pdl    │ nb_jours │ turpe_fixe_annuel  │ turpe_journalier │ turpe_periode │
│ str    │ i64      │ f64                │ f64              │ f64           │
├────────┼──────────┼────────────────────┼──────────────────┼───────────────┤
│ PDL001 │ 92       │ 121.32             │ 0.332            │ 30.55         │
└────────┴──────────┴────────────────────┴──────────────────┴───────────────┘

Formule : turpe_periode = turpe_annuel × (nb_jours / 365)
Exemple : 121.32 × (92 / 365) = 30.55 €
```

---

## TURPE Fixe C4 - 4 puissances souscrites

### Exemple 3 : Calcul C4 avec modulation saisonnière (BT > 36 kVA)

```python
# Point C4 avec 4 puissances différentes (économies via modulation)
df = pl.DataFrame({
    "pdl": ["PDL_INDUSTRIE"],
    "formule_tarifaire_acheminement": ["BTSUPCU"],
    "puissance_souscrite": [60.0],  # Non utilisé en C4

    # 4 puissances souscrites par cadran temporel (kVA)
    "puissance_souscrite_hph": [36.0],  # P₁ - Hiver Pleines Heures (le plus cher)
    "puissance_souscrite_hch": [36.0],  # P₂ - Hiver Creuses Heures
    "puissance_souscrite_hpb": [60.0],  # P₃ - Été Pleines Heures
    "puissance_souscrite_hcb": [60.0],  # P₄ - Été Creuses Heures (le moins cher)

    # Coefficients puissance CRE officiels (€/kVA/an)
    "b_hph": [17.61],  # b₁ - Coefficient HPH (le plus élevé)
    "b_hch": [15.96],  # b₂
    "b_hpb": [14.56],  # b₃
    "b_hcb": [11.98],  # b₄ - Coefficient HCB (le plus faible)

    # Composantes fixes
    "cg": [217.8],
    "cc": [283.27],
})

# Valider la contrainte réglementaire P₁ ≤ P₂ ≤ P₃ ≤ P₄
validation = df.with_columns(
    expr_valider_puissances_croissantes_c4().alias("contrainte_ok")
)
print(f"Contrainte P₁≤P₂≤P₃≤P₄ respectée : {validation['contrainte_ok'][0]}")

# Calculer le TURPE fixe annuel C4
result = df.with_columns(
    expr_calculer_turpe_fixe_annuel().alias("turpe_fixe_annuel")
)

print(f"\nTURPE fixe annuel C4 : {result['turpe_fixe_annuel'][0]:.2f} €")
```

**Résultat** :
```
Contrainte P₁≤P₂≤P₃≤P₄ respectée : True

TURPE fixe annuel C4 : 1484.47 €

Formule C4 progressive :
turpe_annuel = b₁×P₁ + b₂×(P₂-P₁) + b₃×(P₃-P₂) + b₄×(P₄-P₃) + cg + cc
             = 17.61×36 + 15.96×(36-36) + 14.56×(60-36) + 11.98×(60-60) + 217.8 + 283.27
             = 633.96 + 0 + 349.44 + 0 + 501.07
             = 1484.47 €

💡 Économies : ~5% vs puissance constante 60 kVA (1557.67 €)
```

### Exemple 4 : Comparaison C4 vs C5

```python
# Comparaison entre un point C4 et un point C5 équivalent
df_comparaison = pl.DataFrame({
    "type": ["C5 (36 kVA)", "C4 (36/36/60/60)"],
    "puissance_souscrite": [60.0, 60.0],

    # C5 : b standard
    "b": [10.44, None],

    # C4 : 4 coefficients b
    "b_hph": [None, 17.61],
    "b_hch": [None, 15.96],
    "b_hpb": [None, 14.56],
    "b_hcb": [None, 11.98],

    # C4 : 4 puissances
    "puissance_souscrite_hph": [None, 36.0],
    "puissance_souscrite_hch": [None, 36.0],
    "puissance_souscrite_hpb": [None, 60.0],
    "puissance_souscrite_hcb": [None, 60.0],

    "cg": [16.2, 217.8],
    "cc": [20.88, 283.27],
})

result = df_comparaison.with_columns(
    expr_calculer_turpe_fixe_annuel().alias("turpe_annuel")
).with_columns(
    (pl.col("turpe_annuel") - pl.col("turpe_annuel").first()).alias("economie")
)

print(result.select(["type", "turpe_annuel", "economie"]))
```

---

## TURPE Variable - Calculs par cadran

### Exemple 5 : Calcul TURPE variable pour chaque cadran horaire

```python
df = pl.DataFrame({
    "pdl": ["PDL001"],

    # Énergies par cadran (kWh)
    "hph_energie": [1000.0],  # Hiver Pleines Heures
    "hch_energie": [800.0],   # Hiver Creuses Heures
    "hpb_energie": [600.0],   # Été Pleines Heures
    "hcb_energie": [400.0],   # Été Creuses Heures

    # Tarifs CRE (c€/kWh) - Nomenclature officielle
    "c_hph": [6.91],
    "c_hch": [4.21],
    "c_hpb": [2.13],
    "c_hcb": [1.52],
})

# Calculer le TURPE variable pour chaque cadran
result = df.with_columns([
    expr_calculer_turpe_cadran("hph").alias("turpe_hph"),
    expr_calculer_turpe_cadran("hch").alias("turpe_hch"),
    expr_calculer_turpe_cadran("hpb").alias("turpe_hpb"),
    expr_calculer_turpe_cadran("hcb").alias("turpe_hcb"),
]).with_columns(
    # Total TURPE variable
    (pl.col("turpe_hph") + pl.col("turpe_hch") +
     pl.col("turpe_hpb") + pl.col("turpe_hcb")).alias("turpe_variable_total")
)

print(result.select([
    "pdl",
    "turpe_hph", "turpe_hch", "turpe_hpb", "turpe_hcb",
    "turpe_variable_total"
]))
```

**Résultat** :
```
┌────────┬───────────┬───────────┬───────────┬───────────┬──────────────────────┐
│ pdl    │ turpe_hph │ turpe_hch │ turpe_hpb │ turpe_hcb │ turpe_variable_total │
│ str    │ f64       │ f64       │ f64       │ f64       │ f64                  │
├────────┼───────────┼───────────┼───────────┼───────────┼──────────────────────┤
│ PDL001 │ 69.10     │ 33.68     │ 12.78     │ 6.08      │ 121.64               │
└────────┴───────────┴───────────┴───────────┴───────────┴──────────────────────┘

Formule : turpe_cadran = (energie_cadran × c_cadran) / 100
Exemple HPH : (1000 × 6.91) / 100 = 69.10 €
         HCH : (800 × 4.21) / 100 = 33.68 €
```

### Exemple 6 : Calcul simplifié HP/HC (sans cadrans été/hiver)

```python
df = pl.DataFrame({
    "pdl": ["PDL002"],

    # Énergies HP/HC simples
    "hp_energie": [2000.0],
    "hc_energie": [1500.0],
    "base_energie": [None],  # Pas utilisé en HP/HC

    # Tarifs CRE
    "c_hp": [5.75],
    "c_hc": [4.12],
    "c_base": [None],
})

result = df.with_columns([
    expr_calculer_turpe_cadran("hp").alias("turpe_hp"),
    expr_calculer_turpe_cadran("hc").alias("turpe_hc"),
    expr_calculer_turpe_cadran("base").alias("turpe_base"),
]).with_columns(
    (pl.col("turpe_hp") + pl.col("turpe_hc") + pl.col("turpe_base"))
    .alias("turpe_variable_total")
)

print(result.select(["pdl", "turpe_hp", "turpe_hc", "turpe_variable_total"]))
```

---

## CMDPS - Pénalités de dépassement

### Exemple 7 : Calcul des pénalités de dépassement de puissance

```python
df = pl.DataFrame({
    "pdl": ["PDL001", "PDL002", "PDL003"],
    "puissance_souscrite": [9.0, 12.0, 6.0],
    "puissance_max_atteinte": [10.5, 11.0, 8.0],  # Puissance de pointe
    "cmdps": [12.41, 12.41, 12.41],  # Tarif dépassement (€/kVA)
    "nb_jours": [31, 31, 31],
})

result = df.with_columns([
    # Dépassement constaté
    (pl.col("puissance_max_atteinte") - pl.col("puissance_souscrite"))
    .clip(lower_bound=0)
    .alias("depassement_kva"),

    # Pénalité CMDPS
    expr_calculer_cmdps().alias("penalite_cmdps"),
])

print(result.select([
    "pdl", "puissance_souscrite", "puissance_max_atteinte",
    "depassement_kva", "penalite_cmdps"
]))
```

**Résultat** :
```
┌────────┬──────────────────────┬────────────────────────┬─────────────────┬─────────────────┐
│ pdl    │ puissance_souscrite  │ puissance_max_atteinte │ depassement_kva │ penalite_cmdps  │
│ str    │ f64                  │ f64                    │ f64             │ f64             │
├────────┼──────────────────────┼────────────────────────┼─────────────────┼─────────────────┤
│ PDL001 │ 9.0                  │ 10.5                   │ 1.5             │ 1.58            │
│ PDL002 │ 12.0                 │ 11.0                   │ 0.0             │ 0.0             │
│ PDL003 │ 6.0                  │ 8.0                    │ 2.0             │ 2.11            │
└────────┴──────────────────────┴────────────────────────┴─────────────────┴─────────────────┘

Formule CMDPS : pénalité = max(0, P_max - P_souscrite) × cmdps × (nb_jours / 365)
Exemple PDL001 : (10.5 - 9.0) × 12.41 × (31 / 365) = 1.58 €
```

---

## Cas d'usage avancés

### Exemple 8 : Utilisation avec les règles TURPE du CSV

```python
# Charger les règles officielles depuis le CSV
regles = load_turpe_rules()
print("Règles TURPE disponibles :")
print(regles.select(["Formule_Tarifaire_Acheminement", "start", "end"]))

# Joindre avec vos données pour récupérer automatiquement les coefficients
df = pl.DataFrame({
    "pdl": ["PDL001", "PDL002"],
    "formule_tarifaire_acheminement": ["BTINFCU4", "BTSUPCU"],
    "puissance_souscrite": [9.0, 60.0],
    "date_debut": ["2025-08-01", "2025-08-01"],
})

# Jointure avec les règles
result = df.join(
    regles,
    left_on="formule_tarifaire_acheminement",
    right_on="Formule_Tarifaire_Acheminement",
    how="left"
).filter(
    # Filtrer sur la date d'application
    pl.col("start") <= pl.col("date_debut")
)

print("\nDonnées enrichies avec les règles TURPE :")
print(result.select([
    "pdl", "formule_tarifaire_acheminement",
    "b", "cg", "cc", "cmdps"
]))
```

### Exemple 9 : Pipeline complet avec LazyFrame

```python
# Charger vos périodes d'abonnement
periodes_lf = pl.LazyFrame({
    "pdl": ["PDL001", "PDL002"],
    "date_debut": ["2025-01-01", "2025-01-01"],
    "date_fin": ["2025-03-31", "2025-03-31"],
    "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCUST"],
    "puissance_souscrite": [9.0, 6.0],
})

# Ajouter le TURPE fixe via la fonction pipeline
periodes_avec_turpe = ajouter_turpe_fixe(periodes_lf)

# Collecter et afficher
result = periodes_avec_turpe.collect()
print(result.select([
    "pdl", "puissance_souscrite",
    "turpe_fixe_annuel", "turpe_fixe_journalier", "turpe_fixe_periode"
]))
```

### Exemple 10 : Analyse de sensibilité tarifaire

```python
# Simuler l'impact de différentes puissances souscrites
puissances = [6.0, 9.0, 12.0, 15.0, 18.0, 24.0, 36.0]

df = pl.DataFrame({
    "puissance_souscrite": puissances,
    "b": [10.44] * len(puissances),
    "cg": [16.2] * len(puissances),
    "cc": [20.88] * len(puissances),
    # Colonnes C4 NULL
    "b_hph": [None] * len(puissances),
    "b_hch": [None] * len(puissances),
    "b_hpb": [None] * len(puissances),
    "b_hcb": [None] * len(puissances),
    "puissance_souscrite_hph": [None] * len(puissances),
    "puissance_souscrite_hch": [None] * len(puissances),
    "puissance_souscrite_hpb": [None] * len(puissances),
    "puissance_souscrite_hcb": [None] * len(puissances),
})

result = df.with_columns([
    expr_calculer_turpe_fixe_annuel().alias("turpe_annuel"),
]).with_columns([
    # Coût unitaire par kVA
    (pl.col("turpe_annuel") / pl.col("puissance_souscrite")).alias("cout_par_kva"),
])

print("Analyse de sensibilité tarifaire :")
print(result.select(["puissance_souscrite", "turpe_annuel", "cout_par_kva"]))
```

---

## Bonnes pratiques

### 1. Toujours inclure les colonnes C4 pour la compatibilité

```python
# ✅ BON - Inclut les colonnes C4 avec NULL pour C5
df = pl.DataFrame({
    "puissance_souscrite": [9.0],
    "b": [9.36],
    "cg": [16.2],
    "cc": [20.88],
    "b_hph": [None],
    "b_hch": [None],
    "b_hpb": [None],
    "b_hcb": [None],
    "puissance_souscrite_hph": [None],
    "puissance_souscrite_hch": [None],
    "puissance_souscrite_hpb": [None],
    "puissance_souscrite_hcb": [None],
})

# ❌ MAUVAIS - Manque les colonnes C4
df = pl.DataFrame({
    "puissance_souscrite": [9.0],
    "b": [9.36],
    "cg": [16.2],
    "cc": [20.88],
})
# → ColumnNotFoundError: unable to find column "b_hph"
```

### 2. Validation des contraintes C4

```python
# Toujours valider P₁ ≤ P₂ ≤ P₃ ≤ P₄ pour les points C4
df_c4 = pl.DataFrame({
    "puissance_souscrite_hph": [40.0],
    "puissance_souscrite_hch": [35.0],  # ⚠️ Invalide : P₂ < P₁
    "puissance_souscrite_hpb": [60.0],
    "puissance_souscrite_hcb": [60.0],
})

validation = df_c4.with_columns(
    expr_valider_puissances_croissantes_c4().alias("valide")
)

if not validation["valide"][0]:
    print("⚠️ Contrainte réglementaire non respectée : P₁ ≤ P₂ ≤ P₃ ≤ P₄")
```

### 3. Utiliser load_turpe_rules() pour les tarifs officiels

```python
# ✅ BON - Utilise les tarifs officiels du CSV
regles = load_turpe_rules()
df = df.join(regles, on="Formule_Tarifaire_Acheminement", how="left")

# ❌ MAUVAIS - Tarifs codés en dur (risque d'obsolescence)
df = df.with_columns(pl.lit(10.44).alias("b"))
```

---

## Ressources complémentaires

- **Code source** : [electricore/core/pipelines/turpe.py](../electricore/core/pipelines/turpe.py)
- **Tests exhaustifs** : [tests/unit/test_turpe.py](../tests/unit/test_turpe.py)
- **Documentation C4** : [turpe-fixe-c4-btsup36kva.md](./turpe-fixe-c4-btsup36kva.md)
- **Tarifs CRE** : [electricore/config/turpe_rules.csv](../electricore/config/turpe_rules.csv)
- **Nomenclature CRE** : Délibération CRE 2025-40 du 30 janvier 2025

---

## Support

Pour toute question ou cas d'usage spécifique, consultez les tests unitaires qui couvrent 38 scénarios différents avec des exemples concrets et validés.
