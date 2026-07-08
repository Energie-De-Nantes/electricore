---
fraicheur: 2026-07-08
---

# Conventions de dates Enedis

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

> **Où vit l'harmonisation (ADR-0042, révise l'amendement #294 d'[ADR-0003](adr/0003-r151-date-harmonisation.md))** :
> le `+1 jour` R151 est porté au **boundary d'ingestion**, dans le modèle dbt `flux_r151`
> lui-même — comme la conversion native de chaque source (R64 fait de même pour son
> ancrage en instant). Après ce boundary, `flux_r151.date_releve` sert directement
> l'**instant harmonisé** (convention « début de journée », comme R64/R15/C15) : ce n'est
> plus « fidèle au label brut », l'endpoint `/flux/r151` sert donc la date **déjà
> harmonisée**, pas la date nue. Le mart `releves` ne réapplique plus le `+1j` — il
> consomme `flux_r151.date_releve` en passthrough.

### Stratégie
1. **Convention cible** : "Début de journée" (majoritaire)
2. **Ajustement R151** : `date J → J+1`, posé dans `flux_r151` (dbt) — le mart `releves` ne
   fait plus que consommer l'instant déjà harmonisé
3. **Traçabilité** : la convention se lit via `source = 'flux_R151'` ; l'identité du relevé
   (`releve_id`, ADR-0028) reste, elle, mintée sur la date **brute** — stable malgré le `+1j`

### Transformation R151 (dbt, `flux_r151.sql`)
```sql
-- Date BRUTE (xs:date, convention « fin de journée »), lue depuis le document Enedis
date_releve_brute := cast(releve ->> '$.Date_Releve' as date)  -- ex. 2024-01-15

-- date_releve = INSTANT minuit Paris du jour J+1 (ADR-0042) : la conversion
-- « fin de journée → début de journée » devient la conversion NATIVE de R151.
date_releve := timezone('Europe/Paris', date_releve_brute::timestamp + interval '1 day')
-- → 2024-01-16T00:00:00+01:00 (équivalent, en convention "début de journée")
```

## Validation

- **Test de correspondance** : 244 matches exactes R151/R64 après harmonisation
- **Avant harmonisation** : 0 match exact
- **Après harmonisation** : 244 matches (100% des cas testés)

## Exclusions

### Flux R15
- **Statut** : Exclu du mart canonique `releves` (union C15 + R64 + R151, ADR-0029)
- **Raison** : Cause erreurs TURPE sur clients professionnels
- **Alternative** : Le mart `releves` (C15 + R64 + R151) couvre le besoin ; R15 reste
  interrogeable directement via le loader `r15()` pour ses propres cas d'usage

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

- [electricore/ingestion/dbt/models/flux/flux_r151.sql](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/ingestion/dbt/models/flux/flux_r151.sql) —
  le `+ interval '1 day'` sur `date_releve_brute`, posé au **boundary d'ingestion**
  (ADR-0042).
- [electricore/ingestion/dbt/models/marts/releves.sql](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/ingestion/dbt/models/marts/releves.sql) —
  le mart canonique des relevés (ADR-0029) : l'adapter R151 lit `flux_r151.date_releve`
  **en passthrough**, sans plus réappliquer d'ajustement.
- [electricore/core/loaders/duckdb/registry.py](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/core/loaders/duckdb/registry.py) —
  `DESCRIPTOR_R151` : le loader `r151()` est un `SELECT *` sur `flux_enedis.flux_r151`
  (aucune projection résiduelle côté Python) et son `where_clause` filtre sur les
  calendriers distributeur valides (`DI000001`/`DI000002`/`DI000003`). Il n'y a plus de
  `releves_harmonises()` ni de flag `date_ajustee` : l'arbitrage entre sources de relevés
  (C15 > R64 > R151) est entièrement porté par le mart dbt `releves`
  ([ADR-0029](adr/0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md)), lu via le
  loader `releves()`.