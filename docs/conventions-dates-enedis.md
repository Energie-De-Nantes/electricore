# Conventions de Date dans les Flux Enedis

## Problématique

Les différents flux Enedis utilisent des conventions de date incompatibles, causant des écarts systématiques dans les relevés d'index.

## Conventions par Flux

### Convention "Début de journée" (majoritaire)
- **Flux concernés** : R64, R15, C15
- **Principe** : `date J = index mesuré au début du jour J`
- **Exemple** : Date 2024-01-15 = index relevé à 00h00 le 15 janvier
- **Note** : C15 et R15 semblent également adopter cette convention "début de journée"

### Convention "Fin de journée" (minoritaire)
- **Flux concernés** : R151
- **Principe** : `date J = index mesuré en fin du jour J`
- **Exemple** : Date 2024-01-15 = index relevé à 23h59 le 15 janvier

## Impact Métier

Sans harmonisation, on observe des écarts systématiques :
- **R64 vs R151** : ~0.4% d'écart sur même PDL+date
- **Cause** : Décalage de 24h dans l'interprétation des dates

## Solution d'Harmonisation

### Stratégie
1. **Convention cible** : "Début de journée" (majoritaire)
2. **Ajustement R151** : `date J → J+1`
3. **Traçabilité** : Colonne `date_ajustee` pour identifier les ajustements

### Transformation R151
```sql
-- AVANT harmonisation
date_releve = '2024-01-15'  -- Index fin jour 15/01

-- APRÈS harmonisation
date_releve = '2024-01-16'  -- Index début jour 16/01 (équivalent)
date_ajustee = true         -- Traçabilité
```

## Validation

- **Test de correspondance** : 244 matches exactes R151/R64 après harmonisation
- **Avant harmonisation** : 0 match exact
- **Après harmonisation** : 244 matches (100% des cas testés)

## Exclusions

### Flux R15
- **Statut** : Exclu des relevés harmonisés
- **Raison** : Cause erreurs TURPE sur clients professionnels
- **Alternative** : Utiliser R151 + R64 pour couverture complète

## Implémentation

Voir `electricore/core/loaders/duckdb_loader.py` :
- Fonction `BASE_QUERY_R151` : Ajustement `+ INTERVAL '1 day'`
- Fonction `BASE_QUERY_RELEVES_HARMONISES` : Documentation et traçabilité