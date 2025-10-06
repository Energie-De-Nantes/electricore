# TURPE Fixe C4 (BT > 36 kVA) - Spécificités et Optimisation

## Vue d'ensemble

Le TURPE fixe pour les points de connexion **BT > 36 kVA** (catégorie C4) présente une **spécificité majeure** par rapport aux points C5 (BT ≤ 36 kVA) : la possibilité de souscrire **4 puissances différentes** selon les plages temporelles.

Cette modulation permet des **économies significatives** (jusqu'à 20%) pour les profils de consommation saisonniers.

---

## Formule du TURPE fixe C4

### Formule générale

```
CS = b₁×P₁ + Σ(i=2 to 4) bᵢ×(Pᵢ - Pᵢ₋₁) + Σ(i=1 to 4) cᵢ×Eᵢ
```

Développée :
```
CS = b₁×P₁ + b₂×(P₂-P₁) + b₃×(P₃-P₂) + b₄×(P₄-P₃) + c₁×E₁ + c₂×E₂ + c₃×E₃ + c₄×E₄
```

### Composantes

- **Composante puissance** (fixe) : `b₁×P₁ + Σ bᵢ×(Pᵢ - Pᵢ₋₁)`
- **Composante énergie** (variable) : `Σ cᵢ×Eᵢ`

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

### Exemple 1 : Puissance constante (baseline)

**Profil** : 60 kVA toute l'année
P₁ = P₂ = P₃ = P₄ = 60 kVA

**Calcul (CU)** :
```
CS = 17.96×60 + 16.28×(60-60) + 14.85×(60-60) + 12.21×(60-60)
   = 1077.6 + 0 + 0 + 0
   = 1077.6 €/an
```

### Exemple 2 : Modulation modérée

**Profil** : Bureau avec climatisation été
P₁ = 36 kVA, P₂ = 36 kVA, P₃ = 60 kVA, P₄ = 60 kVA

**Calcul (CU)** :
```
CS = 17.96×36 + 16.28×(36-36) + 14.85×(60-36) + 12.21×(60-60)
   = 646.56 + 0 + 356.4 + 0
   = 1002.96 €/an
```

**Économie** : 1077.6 - 1002.96 = **74.64 €/an** (soit -7%)

### Exemple 3 : Modulation extrême

**Profil** : Climatisation massive uniquement en heures creuses été
P₁ = 36 kVA, P₂ = 36 kVA, P₃ = 36 kVA, P₄ = 100 kVA

**Calcul (CU)** :
```
CS = 17.96×36 + 16.28×(36-36) + 14.85×(36-36) + 12.21×(100-36)
   = 646.56 + 0 + 0 + 781.44
   = 1428 €/an
```

**Baseline 100 kVA constant** :
```
CS = 17.96×100 = 1796 €/an
```

**Économie** : 1796 - 1428 = **368 €/an** (soit -20% !)

---

## Logique économique et incitations

### Pourquoi cette tarification ?

Le TURPE C4 avec 4 puissances reflète les **contraintes réelles du réseau** :

| Période | Contrainte réseau | Coefficient | Stratégie tarifaire |
|---------|------------------|-------------|---------------------|
| **HPH** | ⚠️ Pointe nationale (chauffage) | b₁ = 17.96 (max) | **Pénalise** les fortes puissances |
| **HCH** | Tension moyenne | b₂ = 16.28 | Tarif intermédiaire |
| **HPB** | Charge modérée | b₃ = 14.85 | Tarif réduit |
| **HCB** | ✅ Période creuse | b₄ = 12.21 (min) | **Encourage** les fortes puissances |

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
| **Formule TURPE fixe** | `cg + cc + b×P` | `b₁×P₁ + Σ bᵢ×(Pᵢ-Pᵢ₋₁)` |
| **Optimisation possible** | ❌ Non | ✅ Oui (modulation saisonnière) |
| **Gestion dépassements** | Disjoncteur (coupure) | CMDPS (pénalité financière) |
| **Complexité contractuelle** | Faible | Élevée (4 choix de puissance) |

---

## Implémentation dans electricore

### Données d'entrée nécessaires

Pour calculer le TURPE fixe C4, les périodes d'abonnement doivent contenir :

```python
# C5 actuel (1 puissance)
puissance_souscrite: float  # kVA

# C4 (4 puissances)
puissance_souscrite_hph: float  # P₁ - Heures Pleines Hiver
puissance_souscrite_hch: float  # P₂ - Heures Creuses Hiver
puissance_souscrite_hpb: float  # P₃ - Heures Pleines Été
puissance_souscrite_hcb: float  # P₄ - Heures Creuses Été
```

**Validation** : Vérifier la contrainte `P₁ ≤ P₂ ≤ P₃ ≤ P₄`

### Structure turpe_rules.csv

```csv
Formule_Tarifaire_Acheminement,start,end,cg,cc,b1,b2,b3,b4,c1,c2,c3,c4,cmdps
BTSUPCU,2025-08-01,,16.8,22,17.96,16.28,14.85,12.21,7.04,4.29,2.18,1.55,12.41
BTSUPLU,2025-08-01,,16.8,22,30.75,21.59,16.97,12.61,5.81,3.53,2.05,1.52,12.41
```

**Notes** :
- `b1, b2, b3, b4` : Coefficients pondérateurs de puissance (€/kVA/an)
- `c1, c2, c3, c4` : Coefficients pondérateurs d'énergie (c€/kWh)
- `cmdps` : Composante Mensuelle de Dépassement de Puissance Souscrite (€/h)

### Expression Polars pour le calcul

```python
def expr_calculer_turpe_fixe_annuel_c4() -> pl.Expr:
    """
    Expression pour calculer le TURPE fixe annuel C4 (BT > 36 kVA).

    Formule : b₁×P₁ + b₂×(P₂-P₁) + b₃×(P₃-P₂) + b₄×(P₄-P₃)

    Returns:
        Expression Polars retournant le TURPE fixe annuel en €

    Prérequis:
        - Colonnes : puissance_souscrite_hph, _hch, _hpb, _hcb (kVA)
        - Colonnes : b1, b2, b3, b4 (€/kVA/an)
        - Contrainte : P₁ ≤ P₂ ≤ P₃ ≤ P₄

    Example:
        >>> df.with_columns(expr_calculer_turpe_fixe_annuel_c4().alias("turpe_fixe_annuel"))
    """
    return (
        pl.col("b1") * pl.col("puissance_souscrite_hph") +
        pl.col("b2") * (pl.col("puissance_souscrite_hch") - pl.col("puissance_souscrite_hph")) +
        pl.col("b3") * (pl.col("puissance_souscrite_hpb") - pl.col("puissance_souscrite_hch")) +
        pl.col("b4") * (pl.col("puissance_souscrite_hcb") - pl.col("puissance_souscrite_hpb"))
    )
```

### Validation de la contrainte

```python
def expr_valider_puissances_croissantes_c4() -> pl.Expr:
    """
    Vérifie que les puissances souscrites respectent P₁ ≤ P₂ ≤ P₃ ≤ P₄.

    Returns:
        Expression booléenne : True si contrainte respectée
    """
    return (
        (pl.col("puissance_souscrite_hph") <= pl.col("puissance_souscrite_hch")) &
        (pl.col("puissance_souscrite_hch") <= pl.col("puissance_souscrite_hpb")) &
        (pl.col("puissance_souscrite_hpb") <= pl.col("puissance_souscrite_hcb"))
    )
```

---

## Références

- **Délibération CRE n°2025-40** (4 février 2025) - TURPE 7 HTA/BT
  - Section 5.2.1.6, pages 154-156
- **Brochure tarifaire Enedis** - Tarifs en vigueur au 1er août 2025
- **Code de l'énergie** - Article L. 341-2 (TURPE)

---

## Notes de mise en œuvre

### Phase 1 : CMDPS uniquement (priorité actuelle)
- ✅ Ajouter colonne `cmdps` dans `turpe_rules.csv`
- ✅ Implémenter `expr_calculer_composante_depassement()`
- ✅ Intégrer dans `turpe_variable`

### Phase 2 : TURPE fixe C4 complet (future)
- ⏳ Refonte du pipeline abonnements pour supporter 4 puissances
- ⏳ Modification de `expr_calculer_turpe_fixe_annuel()` avec détection C4/C5
- ⏳ Ajout validation `P₁ ≤ P₂ ≤ P₃ ≤ P₄`
- ⏳ Tests avec données réelles Enedis

### Limitations actuelles
- electricore calcule le TURPE fixe C5 uniquement (1 puissance)
- Pour C4, les utilisateurs doivent calculer manuellement ou attendre Phase 2
- La composante CMDPS (Phase 1) fonctionne indépendamment du TURPE fixe
