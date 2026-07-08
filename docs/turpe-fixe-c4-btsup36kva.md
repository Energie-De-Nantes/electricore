---
fraicheur: 2026-07-08
---

# TURPE fixe C4 (BT > 36 kVA)

## Vue d'ensemble

Le TURPE fixe pour les points de connexion **BT > 36 kVA** (catégorie C4) présente une **spécificité majeure** par rapport aux points C5 (BT ≤ 36 kVA) : la possibilité de souscrire **4 puissances différentes** selon les plages temporelles.

Cette modulation permet des **économies significatives** (jusqu'à 20%) pour les profils de consommation saisonniers.

---

## Formule du TURPE fixe C4

### Formule générale

```
CS = cg + cc + b₁×P₁ + Σ(i=2 to 4) bᵢ×(Pᵢ - Pᵢ₋₁) + Σ(i=1 to 4) cᵢ×Eᵢ
```

Développée :
```
CS = cg + cc + b₁×P₁ + b₂×(P₂-P₁) + b₃×(P₃-P₂) + b₄×(P₄-P₃) + c₁×E₁ + c₂×E₂ + c₃×E₃ + c₄×E₄
```

### Composantes

- **Composante d'accès** (fixe, indépendante de la modulation) : `cg` (gestion) + `cc`
  (comptage) — les mêmes composantes qu'en C5 ([turpe-usage-standalone.md](turpe-usage-standalone.md)).
- **Composante puissance** (fixe, modulable) : `b₁×P₁ + Σ bᵢ×(Pᵢ - Pᵢ₋₁)`
- **Composante énergie** (variable, TURPE variable — hors de ce document) : `Σ cᵢ×Eᵢ`

### Définition des plages temporelles

| Indice i | Plage temporelle | Abréviation | Période |
|----------|------------------|-------------|---------|
| **i=1** | Heures Pleines Hiver | HPH | Saison haute (nov-mars), 16h hors HC |
| **i=2** | Heures Creuses Hiver | HCH | Saison haute (nov-mar), 8h HC |
| **i=3** | Heures Pleines Été | HPB | Saison basse (avr-oct), 16h hors HC |
| **i=4** | Heures Creuses Été | HCB | Saison basse (avr-oct), 8h HC |

---

## Contrainte réglementaire : Puissances croissantes

### Règle obligatoire (Délibération CRE 2025-40, p.155)

> **"En outre, quel que soit i, les puissances souscrites apparentes doivent être telles que Pi+1 ≥ Pi."**

C'est-à-dire : **P₁ ≤ P₂ ≤ P₃ ≤ P₄**

### Interprétation

- **P₁ (HPH)** : Puissance **minimale** (période de pointe nationale)
- **P₄ (HCB)** : Puissance **maximale** (période creuse)

Le gestionnaire de réseau **impose** une souscription croissante pour refléter les contraintes réseau :
- **Hiver HPH** : Forte tension réseau → encourage à limiter la puissance
- **Été HCB** : Faible tension réseau → autorise des puissances élevées

---

## Coefficients tarifaires (au 1er août 2025)

### Tarif Courte Utilisation (CU)

| Plage | Coefficient bᵢ (€/kVA/an) | Coefficient cᵢ (c€/kWh) |
|-------|---------------------------|-------------------------|
| HPH (i=1) | **17,61** | 6,91 |
| HCH (i=2) | 15,96 | 4,21 |
| HPB (i=3) | 14,56 | 2,13 |
| HCB (i=4) | **11,98** | 1,52 |

### Tarif Longue Utilisation (LU)

| Plage | Coefficient bᵢ (€/kVA/an) | Coefficient cᵢ (c€/kWh) |
|-------|---------------------------|-------------------------|
| HPH (i=1) | **30,16** | 5,69 |
| HCH (i=2) | 21,18 | 3,47 |
| HPB (i=3) | 16,64 | 2,01 |
| HCB (i=4) | **12,37** | 1,49 |

**Observation** : Les coefficients bᵢ sont **décroissants** (b₁ > b₂ > b₃ > b₄), ce qui rend la modulation avantageuse.

---

## Exemples de calcul

Coefficients CU utilisés ci-dessous (`turpe_rules.csv`, ligne `BTSUPCU` en vigueur depuis
le 2025-08-01) : `cg=217.8`, `cc=283.27`, `b_hph=17.61`, `b_hch=15.96`, `b_hpb=14.56`,
`b_hcb=11.98`. Valeurs recalculées via `ajouter_turpe_fixe()` contre le CSV courant
(pas recopiées d'une version antérieure de ce document).

### Exemple 1 : Puissance constante (baseline)

**Profil** : 60 kVA toute l'année
P₁ = P₂ = P₃ = P₄ = 60 kVA

**Calcul (CU)** :
```
CS = 217.8 + 283.27 + 17.61×60 + 15.96×(60-60) + 14.56×(60-60) + 11.98×(60-60)
   = 501.07 + 1056.6 + 0 + 0 + 0
   = 1557.67 €/an
```

### Exemple 2 : Modulation modérée

**Profil** : Bureau avec climatisation été
P₁ = 36 kVA, P₂ = 36 kVA, P₃ = 60 kVA, P₄ = 60 kVA

**Calcul (CU)** :
```
CS = 217.8 + 283.27 + 17.61×36 + 15.96×(36-36) + 14.56×(60-36) + 11.98×(60-60)
   = 501.07 + 633.96 + 0 + 349.44 + 0
   = 1484.47 €/an
```

**Économie vs baseline 60 kVA constant (exemple 1)** : 1557.67 - 1484.47 = **73.20 €/an**
(soit environ -4.7 %)

### Exemple 3 : Modulation extrême

**Profil** : Climatisation massive uniquement en heures creuses été
P₁ = 36 kVA, P₂ = 36 kVA, P₃ = 36 kVA, P₄ = 100 kVA

**Calcul (CU)** :
```
CS = 217.8 + 283.27 + 17.61×36 + 15.96×(36-36) + 14.56×(36-36) + 11.98×(100-36)
   = 501.07 + 633.96 + 0 + 0 + 766.72
   = 1901.75 €/an
```

**Baseline 100 kVA constant** :
```
CS = 217.8 + 283.27 + 17.61×100 = 501.07 + 1761 = 2262.07 €/an
```

**Économie** : 2262.07 - 1901.75 = **360.32 €/an** (soit environ -15.9 %)

---

## Logique économique et incitations

### Pourquoi cette tarification ?

Le TURPE C4 avec 4 puissances reflète les **contraintes réelles du réseau** :

| Période | Contrainte réseau | Coefficient (CU) | Stratégie tarifaire |
|---------|------------------|-------------|---------------------|
| **HPH** | ⚠️ Pointe nationale (chauffage) | b_hph = 17.61 (max) | **Pénalise** les fortes puissances |
| **HCH** | Tension moyenne | b_hch = 15.96 | Tarif intermédiaire |
| **HPB** | Charge modérée | b_hpb = 14.56 | Tarif réduit |
| **HCB** | ✅ Période creuse | b_hcb = 11.98 (min) | **Encourage** les fortes puissances |

### Incitations comportementales

**Profils gagnants** (économies potentielles) :
- 🏢 **Bureaux/commerces** : Faible consommation hiver, clim été
- 🏭 **Industries saisonnières** : Production concentrée en été
- ⚡ **Bornes de recharge** : Charge nocturne en HCB

**Profils neutres** :
- 🏭 **Industries 24/7** : Charge constante toute l'année

**Profils pénalisés** :
- 🏠 **Chauffage électrique dominant** : Forte puissance en HPH/HCH

---

## Comparaison C4 vs C5

| Aspect | C5 (BT ≤ 36 kVA) | C4 (BT > 36 kVA) |
|--------|------------------|------------------|
| **Puissances souscrites** | 1 seule (P) | 4 différentes (P₁, P₂, P₃, P₄) |
| **Formule TURPE fixe** | `cg + cc + b×P` | `cg + cc + b₁×P₁ + Σ bᵢ×(Pᵢ-Pᵢ₋₁)` |
| **Optimisation possible** | ❌ Non | ✅ Oui (modulation saisonnière) |
| **Gestion dépassements** | Disjoncteur (coupure) | CMDPS (pénalité financière) |
| **Complexité contractuelle** | Faible | Élevée (4 choix de puissance) |

---

## Implémentation dans electricore

Implémenté et validé (pas une intention future) : `expr_calculer_turpe_fixe_annuel()`
gère C4 **et** C5 dans une seule expression unifiée
([electricore/core/pipelines/turpe.py:103-145](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/core/pipelines/turpe.py)),
appelée par `ajouter_turpe_fixe()` — voir [turpe-usage-standalone.md](turpe-usage-standalone.md)
pour l'usage complet du pipeline.

### Données d'entrée nécessaires

Pour calculer le TURPE fixe C4, les périodes d'abonnement doivent contenir :

```python
# C5 (1 puissance) — colonne toujours requise par expr_calculer_turpe_fixe_annuel(),
# même sur une période C4 (le schéma Polars évalue les deux branches C4/C5).
puissance_souscrite_kva: float

# C4 (4 puissances)
puissance_souscrite_hph_kva: float  # P₁ - Heures Pleines Hiver
puissance_souscrite_hch_kva: float  # P₂ - Heures Creuses Hiver
puissance_souscrite_hpb_kva: float  # P₃ - Heures Pleines Été
puissance_souscrite_hcb_kva: float  # P₄ - Heures Creuses Été
```

La **détection C4 vs C5** se fait sur la présence des 4 coefficients `b_hph`/`b_hch`/`b_hpb`/`b_hcb`
dans la règle jointe (non-NULL) — pas sur une colonne dédiée côté périodes.

**Validation** : Vérifier la contrainte `P₁ ≤ P₂ ≤ P₃ ≤ P₄`

### Structure turpe_rules.csv

```csv
Formule_Tarifaire_Acheminement,start,end,cg,cc,b,b_hph,b_hch,b_hpb,b_hcb,c_hph,c_hch,c_hpb,c_hcb,c_hp,c_hc,c_base,cmdps,reference
BTSUPCU,2025-08-01,,217.8,283.27,,17.61,15.96,14.56,11.98,6.91,4.21,2.13,1.52,0,0,0,12.41,Délibération CRE n° 2025-40…
BTSUPLU,2025-08-01,,217.8,283.27,,30.16,21.18,16.64,12.37,5.69,3.47,2.01,1.49,0,0,0,12.41,Délibération CRE n° 2025-40…
```

**Notes** (colonnes réelles du fichier — une seule table pour toutes les FTA, C4 et C5) :
- `b` : coefficient de puissance C5 (vide pour les lignes C4)
- `b_hph, b_hch, b_hpb, b_hcb` : coefficients pondérateurs de puissance C4 (€/kVA/an) —
  vides pour les lignes C5
- `c_hph, c_hch, c_hpb, c_hcb` : coefficients pondérateurs d'énergie (c€/kWh) — utilisés
  pour toute FTA à 4 cadrans temporels (Tempo/EJP), C4 **ou** C5
- `c_hp, c_hc, c_base` : coefficients d'énergie pour les FTA C5 à 2 cadrans (HP/HC) ou
  1 cadran (Base)
- `cmdps` : Composante Mensuelle de Dépassement de Puissance Souscrite (€/h), vide pour
  les FTA sans CMDPS (C5)

### Expression Polars pour le calcul

```python
def expr_calculer_turpe_fixe_annuel() -> pl.Expr:
    """
    Expression pour calculer le TURPE fixe annuel avec détection automatique C4/C5.

    Formule C5 (BT ≤ 36 kVA) : (b × P) + cg + cc (par défaut)
    Formule C4 (BT > 36 kVA) : b_hph×P₁ + b_hch×(P₂-P₁) + b_hpb×(P₃-P₂) + b_hcb×(P₄-P₃) + cg + cc

    La détection se fait sur la présence des 4 coefficients C4 (b_hph, b_hch, b_hpb, b_hcb).
    """
    turpe_c5 = (pl.col("b") * pl.col("puissance_souscrite_kva")) + pl.col("cg") + pl.col("cc")

    turpe_c4 = (
        (pl.col("b_hph") * pl.col("puissance_souscrite_hph_kva"))
        + (pl.col("b_hch") * (pl.col("puissance_souscrite_hch_kva") - pl.col("puissance_souscrite_hph_kva")))
        + (pl.col("b_hpb") * (pl.col("puissance_souscrite_hpb_kva") - pl.col("puissance_souscrite_hch_kva")))
        + (pl.col("b_hcb") * (pl.col("puissance_souscrite_hcb_kva") - pl.col("puissance_souscrite_hpb_kva")))
        + pl.col("cg")
        + pl.col("cc")
    )

    is_c4 = (
        pl.col("b_hph").is_not_null()
        & pl.col("b_hch").is_not_null()
        & pl.col("b_hpb").is_not_null()
        & pl.col("b_hcb").is_not_null()
    )

    return pl.when(is_c4).then(turpe_c4).otherwise(turpe_c5)
```

### Validation de la contrainte

```python
def expr_valider_puissances_croissantes_c4() -> pl.Expr:
    """
    Vérifie que les puissances souscrites respectent P₁ ≤ P₂ ≤ P₃ ≤ P₄.

    Returns:
        Expression booléenne : True si contrainte respectée, False sinon
    """
    return (
        (pl.col("puissance_souscrite_hph_kva") <= pl.col("puissance_souscrite_hch_kva"))
        & (pl.col("puissance_souscrite_hch_kva") <= pl.col("puissance_souscrite_hpb_kva"))
        & (pl.col("puissance_souscrite_hpb_kva") <= pl.col("puissance_souscrite_hcb_kva"))
    )
```

Cette expression est disponible mais **pas appliquée automatiquement** par `ajouter_turpe_fixe()` —
à l'appelant de filtrer/valider les périodes C4 en amont si la contrainte doit être imposée.

---

## Références

- **Délibération CRE n°2025-40** (4 février 2025) - TURPE 7 HTA/BT
  - Section 5.2.1.6, pages 154-156
- **Brochure tarifaire Enedis** - Tarifs en vigueur au 1er août 2025
- **Code de l'énergie** - Article L. 341-2 (TURPE)

---

## État de l'implémentation

Le TURPE fixe C4 (4 puissances, détection automatique, validation `P₁≤P₂≤P₃≤P₄`) et la
composante CMDPS de dépassement sont **implémentés et testés** — voir
[tests/unit/test_turpe.py](https://github.com/Energie-De-Nantes/electricore/blob/main/tests/unit/test_turpe.py)
(40 scénarios, C4 et C5). Rien de ce document ne décrit une fonctionnalité future.
