# Problèmes de qualité des données R151

## Résumé

Les flux R151 d'Enedis contiennent des relevés avec des index aberrants qui génèrent des énergies calculées irréalistes (plusieurs centaines de MWh/jour pour des PDL résidentiels). Ces anomalies impactent la validation TURPE variable avec des écarts de +40% par rapport aux factures Enedis.

## Problèmes identifiés

### 1. Calendriers distributeur invalides (✅ RÉSOLU)

**Description** : Présence de calendriers invalides dans R151
- `INCONNU` : 841 lignes (229 PDL) - 0.70%
- `DN999999` : 10 lignes (10 PDL) - 0.01%

**Solution implémentée** :
- Filtre dans `electricore/core/loaders/duckdb/sql.py` (lignes 234 et 383)
- Ne conserve que les calendriers valides : `DI000001`, `DI000002`, `DI000003`
- Impact : 0.7% des données R151 exclues

### 2. Index aberrants avec calendriers valides (❌ NON RÉSOLU)

**Description** : Données sources Enedis corrompues même avec calendriers valides

**Exemple PDL** : `14290738060355`
- **Période** : 9 septembre 2025
- **Calendrier** : DI000003 (valide ✅)
- **Anomalie** : Saut d'index de 522 kWh → 29,982 kWh en une journée (x57)
- **Impact** : 962 MWh calculés en septembre 2025 (impossible pour un PDL résidentiel de 3 kVA)

**Données brutes R151** :
```
Date         Calendrier   HPB (kWh)   HCB (kWh)
2025-09-08   DI000003     522.9       6,549.4
2025-09-09   DI000003     29,982.7    9,148.6    ← SAUT ABERRANT
2025-09-10   DI000003     59,505.3    23,909.8
2025-09-11   DI000003     89,025.9    38,668.9
...
2025-09-30   DI000003     649,914.1   319,116.0
```

**Delta quotidiens** :
- HPB : ~29,520 kWh/jour (29.5 MWh/jour)
- HCB : ~14,760 kWh/jour (14.8 MWh/jour)
- **Total : ~44 MWh/jour** pour un PDL de 3 kVA (physiquement impossible)

**Impact sur TURPE variable** :
- TURPE calculé : 14,405.91 €
- TURPE F15 facturé : 76.79 €
- Écart : +18,720% (!!!)

## Analyse technique

### Conversion Wh → kWh

✅ La conversion fonctionne correctement :
- Données R151 stockées en **Wh** dans DuckDB
- Colonne `unite = "Wh"` permet la conversion automatique
- `releves_harmonises()` applique `/1000` pour obtenir des kWh
- Vérifié avec `releves_harmonises().validate(True).lazy().collect()`

### Pipeline énergie

✅ Le pipeline fonctionne correctement :
- Calcule les deltas entre relevés consécutifs
- Regex `^energie_.*_kwh$` sélectionne bien toutes les colonnes d'énergie
- Les énergies aberrantes proviennent des **données sources**, pas du code

### Origine du problème

❌ Les données sources R151 d'Enedis sont corrompues :
- Pas de calendrier INCONNU/DN999999 pour ce PDL
- Calendrier DI000003 valide utilisé
- Aucun événement C15 de changement de compteur
- Les index aberrants sont **directement dans les fichiers XML R151 source**

## Impact sur la validation TURPE variable

**Statistiques globales** :
- PDL avec TURPE calculé : 716
- PDL avec TURPE F15 : 796
- **Écart moyen : +41.86%** (au lieu de ~0.5% attendu)

**Cause principale** :
- Quelques PDL avec données aberrantes (comme `14290738060355`)
- Génèrent des énergies et TURPE x1000 trop élevés
- Faussent les statistiques de validation globales

## Solutions envisagées

### Option A : Filtrage calendriers invalides (✅ IMPLÉMENTÉE)
- Exclut INCONNU et DN999999
- Nettoie 0.7% des données
- **Ne résout PAS le problème des index aberrants avec calendriers valides**

### Option B : Détection de qualité des données (🔄 À IMPLÉMENTER)

**Approche proposée** :
1. Détecter les PDL avec deltas quotidiens aberrants (> seuil, ex: 1000 kWh/jour)
2. Les marquer comme "données corrompues"
3. Les exclure de la validation TURPE ou les traiter séparément

**Implémentation suggérée** :
```python
def detecter_pdl_aberrants(
    periodes_energie: pl.LazyFrame,
    seuil_quotidien_kwh: float = 1000
) -> pl.LazyFrame:
    """
    Identifie les PDL avec des consommations quotidiennes aberrantes.

    Args:
        periodes_energie: LazyFrame des périodes d'énergie
        seuil_quotidien_kwh: Seuil de détection (kWh/jour)

    Returns:
        LazyFrame avec colonne `data_aberrante: bool`
    """
    # Calculer énergie totale par période
    # Normaliser par nombre de jours
    # Marquer les périodes > seuil
    # Propager le flag au niveau PDL
```

**Critères de détection** :
- Delta index > 1000 kWh/jour entre relevés consécutifs
- Énergie totale mensuelle > 10 MWh pour PDL résidentiel (< 36 kVA)
- Ratio énergie calculée / énergie F15 > 10x

### Option C : Exclusion manuelle (⚠️ TEMPORAIRE)

Liste des PDL à exclure de la validation :
- `14290738060355` - Index aberrants sept 2025

## Recommandations

1. **Court terme** :
   - ✅ Filtre des calendriers invalides implémenté
   - 📝 Documenter le problème (ce fichier)
   - ⏭️ Implémenter détection de qualité (Option B)

2. **Moyen terme** :
   - Ajouter des contrôles qualité dans l'ETL
   - Logger les anomalies détectées
   - Créer un rapport de qualité des données

3. **Long terme** :
   - Remonter les anomalies à Enedis
   - Demander correction des fichiers sources
   - Mettre en place une validation automatique des flux entrants

## Fichiers concernés

- `electricore/core/loaders/duckdb/sql.py` - Filtrage R151 (lignes 234, 383)
- `electricore/core/pipelines/energie.py` - Calcul des énergies
- `notebooks/validation_turpe_variable.py` - Validation TURPE
- `notebooks/debug_pdl_anomalie.py` - Notebook d'investigation

## Références

- Convention de nommage : [docs/conventions-nommage.md](conventions-nommage.md)
- Conventions de dates : [docs/conventions-dates-enedis.md](conventions-dates-enedis.md)
- Calendriers distributeur Enedis :
  - `DI000001` : Base (tarif simple)
  - `DI000002` : HP/HC (heures pleines/creuses)
  - `DI000003` : 4 cadrans (HPH/HPB/HCH/HCB pour C4/C5)

---
*Document créé le 2025-10-07*
*Dernière mise à jour : 2025-10-07*
