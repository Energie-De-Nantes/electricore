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

> **Où vit l'harmonisation (#294)** : le `+1 jour` R151 est porté en **un seul endroit**,
> le mart dbt `releves` (la ligne de temps consommée par la chaîne énergie). Il a été
> **retiré du loader** : l'endpoint brut `/flux/r151` sert la date **nue** (fin de journée),
> fidèle à la source, et est dépréciable (cf. [ADR-0003](adr/0003-r151-date-harmonisation.md)).

### Stratégie
1. **Convention cible** : "Début de journée" (majoritaire)
2. **Ajustement R151** : `date J → J+1` (dans le mart `releves`)
3. **Traçabilité** : la convention se lit via `source = 'flux_R151'`

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

## Fuseau horaire : heure légale française, propriété par flux

Concern distinct de la convention *jour* ci-dessus. **Tous les flux Enedis expriment leurs
horodatages en heure légale française (Europe/Paris)** — vérifié sur les guides SGE, les XSD
et les JSON-schemas (`Documents/guides_flux/`). Il n'y a donc **pas** de fuseaux divergents
entre flux. Mais la donnée se présente sous **deux formes** :

- **Offset explicite** (`xsd:dateTime`, ex. `2024-04-03T00:01:00+02:00`) — C15
  (`Date_Evenement`, relevés avant/après), R15 (`Date_Releve`), X12/X13 (`jalon dateHeure`).
  L'instant est **absolu** : le fuseau est porté *par la donnée*, aucune hypothèse à faire.
  dbt les stocke en `TIMESTAMPTZ`.
- **Implicite Paris** (sans offset) — R64 (JSON, champ `d` = horodate nue, ex.
  `2022-01-01 00:00:00` ; le guide R6X ne mentionne aucun fuseau). C'est la **seule**
  hypothèse de fuseau du système : convention Europe/Paris, à *déclarer* explicitement.
  Les champs `xs:date` (R151 `Date_Releve`, F15) sont des jours nus — ils relèvent de la
  convention *jour* ci-dessus, pas d'un instant.

**Le fuseau est une propriété *par flux*, pas un réglage global de connexion.** Chaque flux
porte la forme de son horodate ; seuls les flux **naïfs** (R64) déclarent une hypothèse de
fuseau (Europe/Paris, sourcée de la doc), les flux à offset n'en portent aucune. Côté
loaders, les filtres de date sur une colonne tz-aware doivent interpréter le littéral en
heure de Paris (le fuseau du flux), de façon **déterministe** quel que soit le fuseau de
session (UTC sur le VPS, Paris en dev) — sans `SET TimeZone` global qui masquerait cette
propriété de domaine.

## Implémentation

Voir [electricore/core/loaders/duckdb/sql.py](../electricore/core/loaders/duckdb/sql.py) :
- Spécification de la table R151 : ajustement `+ INTERVAL '1 day'` sur `date_releve`
- Requêtes unifiées `releves()` / `releves_harmonises()` : traçabilité via `flux_origine` et `date_ajustee`