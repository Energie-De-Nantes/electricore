# Plan de refonte du pipeline énergie Polars

## Contexte
Refonte du pipeline énergie pandas vers Polars avec une approche fonctionnelle et testable.
L'objectif est de créer des périodes homogènes d'énergie où les propriétés permettant de calculer l'énergie ET le turpe variable sont constantes.

## Principe de base
- Les événements avec `impacte_energie=True` (incluant FACTURATION) déterminent les points de rupture
- Combiner relevés C15 (intégrés aux événements) + relevés R151 (à interroger)
- Tri chronologique par `['pdl', 'date_releve', 'ordre_index']`
- Calcul des périodes par différence entre relevés consécutifs

## Fonctions à implémenter (ordre d'implémentation)

### 1. `extraire_releves_evenements_polars(evt_contractuels: pl.LazyFrame) -> pl.LazyFrame`
**Objectif** : Extraire les relevés intégrés dans les événements C15

**Entrée** : LazyFrame des événements contractuels (hors FACTURATION)

**Sortie** : LazyFrame avec :
- Colonnes d'index : BASE, HP, HC, HCH, HPH, HCB, HPB
- date_releve, ordre_index, source
- ref_situation_contractuelle, formule_tarifaire_acheminement
- pdl

**Points clés** :
- Les relevés sont déjà dans les colonnes des événements
- ordre_index provient de la position avant/après dans l'événement

### 2. `interroger_releves_polars(requete: pl.LazyFrame, releves: pl.LazyFrame) -> pl.LazyFrame`
**Objectif** : Chercher les relevés R151 et GARANTIR une sortie de même taille que la requête

**Entrée** :
- requete : LazyFrame avec colonnes `pdl`, `date_releve`
- releves : LazyFrame base de relevés R151

**Sortie** : LazyFrame de MÊME TAILLE que requête avec :
- Toutes les colonnes de la requête
- Colonnes d'index (valeurs ou NaN si non trouvé)
- `releve_manquant` : bool indiquant l'absence de données

**Points clés** :
- Utiliser jointure LEFT pour garder toutes les lignes
- Ajouter flag `releve_manquant = source.is_null()`
- Pas de création de relevés factices séparés

### 3. `reconstituer_chronologie_releves_polars(evenements: pl.LazyFrame, releves: pl.LazyFrame) -> pl.LazyFrame`
**Objectif** : Combiner tous les relevés dans l'ordre chronologique

**Étapes** :
1. Séparer événements contractuels vs FACTURATION
2. Extraire relevés des événements contractuels via `extraire_releves_evenements_polars`
3. Pour FACTURATION : construire requête et appeler `interroger_releves_polars`
4. Combiner les deux sources
5. Trier par `['pdl', 'date_releve', 'ordre_index']`
6. Propager ref_situation_contractuelle avec forward fill
7. Appliquer priorité des sources (C15 > R151 via tri alphabétique + drop_duplicates)

**Sortie** : LazyFrame chronologique complet des relevés

### 4. `calculer_periodes_energie_polars(lf: pl.LazyFrame) -> pl.LazyFrame`
**À adapter** : Ajouter gestion du flag `releve_manquant`

**Modifications** :
- Propager le flag `releve_manquant` dans les périodes
- L'utiliser pour enrichir `data_complete` ou créer un indicateur spécifique

### 5. Modifications du modèle `PeriodeEnergiePolars`
Ajouter :
```python
releve_manquant_debut: Optional[pl.Boolean] = pa.Field(nullable=True)
releve_manquant_fin: Optional[pl.Boolean] = pa.Field(nullable=True)
```

## Structure des tests

### Tests unitaires (un par fonction)
1. **test_extraire_releves_evenements_polars**
   - Cas nominal : événements avec relevés complets
   - Cas avec valeurs manquantes
   - Cas avec ordre_index multiples

2. **test_interroger_releves_polars**
   - Cas nominal : tous les relevés trouvés
   - Cas mixte : certains trouvés, d'autres non
   - Cas vide : aucun relevé trouvé
   - Vérifier taille sortie = taille entrée

3. **test_reconstituer_chronologie_releves_polars**
   - Test priorité C15 > R151
   - Test propagation références contractuelles
   - Test tri chronologique
   - Test avec changement de compteur (ordre_index)

### Test d'intégration
- Comparer résultats Polars vs Pandas sur données réelles
- Vérifier cohérence des périodes générées
- Performance : mesurer temps d'exécution

## Points techniques importants

1. **LazyFrames partout** : Optimisation des requêtes par Polars
2. **Expressions pures** : Facilite composition et tests
3. **Gestion explicite des NaN** : Pas de masquage des données manquantes
4. **Traçabilité** : Flag `releve_manquant` pour audit

## Avantages de cette approche

- **Simplicité** : Pas de création artificielle de relevés factices
- **Robustesse** : Taille garantie dans interroger_releves_polars
- **Traçabilité** : Flags explicites pour données manquantes
- **Performance** : LazyFrames et optimisations Polars
- **Testabilité** : Fonctions pures facilement testables

## Prochaines étapes

1. Implémenter et tester chaque fonction individuellement
2. Valider contre le pipeline pandas existant
3. Optimiser si nécessaire
4. Documenter les différences comportementales éventuelles