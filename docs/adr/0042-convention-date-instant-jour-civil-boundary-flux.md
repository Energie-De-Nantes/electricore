# Convention de date : instant (TIMESTAMPTZ) vs jour civil (DATE), harmonisés au boundary `flux_*`, fuseau posé à la lecture

## Contexte

Le traitement des dates était éclaté et **harmonisé deux fois** : les marts (`releves`,
`spine_contrat`) ré-ancraient les dates naïves en instants Paris, et le loader le refaisait
à sa façon (forme par colonne `FormeTemporelle` + ancrage + filtre déterministe, #391, PR
#392). Les modèles `flux_*` émettaient des types **mixtes** — `TIMESTAMPTZ` (offsets
C15/R15), `TIMESTAMP` naïf (R64), `DATE` (R151/F15) — sans convention unique, chaque
consommateur re-décidant l'ancrage. La grille FACTURATION (1ᵉʳ du mois) doit, elle,
calculer à **minuit Paris** et non UTC, sous peine de mauvais mois au bord.

Trois fins de partie incompatibles flottaient : (a) `SET TimeZone` global, rejeté par #391 ;
(b) forme par colonne dans le loader (#389-391, livré) qui empêche le `SELECT *` pur visé
par [ADR-0035](0035-typage-chaine-ingestion-coeur-proprietaire-par-fait.md) ; (c) forme
dérivée de Pandera.

## Décision

### 1. Deux représentations selon la *nature* (instant vs jour civil)

Le canon (java.time / Noda Time) distingue le **moment** du **jour calendaire** :

| Nature | Type physique | Exemples Enedis |
|---|---|---|
| **Instant** (point sur la ligne du temps) | `TIMESTAMPTZ` (UTC) | *événement* C15 (`date_evenement`), *relevé* (R15/R64/R151) |
| **Jour civil** (journée, sans heure ni fuseau) | `DATE` | *date de facture* F15, borne de période, bascule de niveau C15, date d'effet d'affaire |

« Tout en `TIMESTAMPTZ` » est **rejeté** : coller un fuseau à un jour civil injecte un faux
« minuit » + un fuseau porteur → bug de bord (le `2024-10-04` relu en UTC devient le 03).
La règle « store UTC, convert at edges » ne vaut que pour les **instants** ; un jour civil
ne porte jamais de fuseau.

### 2. Harmonisation au boundary `flux_*`, une seule fois (pattern adapter)

Chaque source convertit son encodage vers le type canonique **dans son modèle `flux_*`** (le
bord), comme un adapter hexagonal normalise au seuil :

- **R64** : `TIMESTAMP` naïf → instant (ancrage heure-mur Paris).
- **R151** : label `xs:date` *fin-de-journée* → instant **minuit Paris du jour J+1**. Le
  « +1 jour » n'est pas une harmonisation baladeuse mais la **conversion native** de R151
  (fin de J = début de J+1), appliquée au bord comme R64 fait son ancrage. Après ça
  **R151 ≡ R64**, une seule convention « début de journée » en aval. L'*identité de relevé*
  (`releve_id`, [ADR-0028](0028-identite-releve-cle-metier-priorite-sources.md)) reste mintée
  sur la **date brute** à l'intérieur de `flux_r151` (identité stable, zéro re-hash).
- **C15 / R15** (offsets `xsd:dateTime`) : déjà `TIMESTAMPTZ`, rien à faire.
- **Jours civils** (F15, bascule de niveau C15, date d'effet d'affaire) : restent `DATE`.

Les marts cessent de ré-ancrer ; plus aucun `TIMESTAMP` naïf ne survit. **Aucun
consommateur en aval ne re-décide la convention.**

### 3. Fuseau posé à la lecture → loader = `SELECT *` pur

`SET TimeZone='Europe/Paris'` sur `duckdb_readonly_conn` : les `TIMESTAMPTZ` sortent tagués
Paris **déterministes** (poste / CI / VPS) ET les filtres interprètent leurs littéraux en
Paris (déterminisme du `WHERE` *gratuit*, sans enveloppe par colonne). Les `DATE` sont
insensibles au fuseau. Le loader devient le `SELECT *` typeless d'ADR-0035 : **plus de
`FormeTemporelle`, plus de `convert_time_zone`, plus d'enveloppe de filtre**.

### 4. Conversions civil ↔ instant explicites au point d'usage

Un calcul calendaire qui croise les deux natures (grille FACTURATION qui pose le 1ᵉʳ du mois
sur la timeline d'instants des événements ; troncature au mois) convertit **explicitement**
en `AT TIME ZONE 'Europe/Paris'` au point d'usage — le build dbt tourne en session UTC, donc
ne jamais s'appuyer sur la session. `spine_contrat.sql` le fait déjà ; côté cœur,
`dt.truncate("1mo")` est correct parce que la lecture tague Paris.

## Le `SET TimeZone` global, rejeté par #391, est ré-adopté

#391 l'avait rejeté (« la responsabilité du fuseau doit être par flux »). **Renversé ici par
le haut** : le *fuseau* est un **invariant de domaine uniforme** — tous les flux Enedis sont
en heure légale française (cf. `docs/conventions-dates-enedis.md`). Ce n'est pas une
responsabilité qui varie par flux ; seule la *forme de stockage* varie, et elle est résolue
en dbt (point 2). Poser le fuseau une fois à la lecture **encode fidèlement** cet invariant,
il ne le masque pas.

## Conséquences

- **Supersède #391** (PR #392) : `FormeTemporelle` + l'enveloppe de filtre par colonne sont
  retirées du loader.
- **Révise [ADR-0035](0035-typage-chaine-ingestion-coeur-proprietaire-par-fait.md) §1** : le
  loader atteint vraiment le `SELECT *` typeless — la dimension tz, laissée « à inventorier »
  par ADR-0035, est ici tranchée.
- **Révise l'amendement #294 d'[ADR-0003](0003-r151-date-harmonisation.md)** : le +1 jour R151
  repasse du mart `releves` au boundary `flux_r151` ; `flux_r151` n'est plus *fidèle au label
  brut* mais *fidèle à l'instant de relevé*. `/flux/r151` (déprécié) sert l'instant.
- Réalise **#311** (loaders = `SELECT *`), sous un cadrage tz différent de l'issue d'origine
  (qui supposait le `SET TimeZone` global comme seul enabler, sans la distinction
  instant/jour civil).
- Discipline dbt à généraliser : tout calcul calendaire en `AT TIME ZONE 'Europe/Paris'`
  explicite (le build tourne en session UTC).

## Statut

Accepté. Supersède #391 (PR #392) ; révise ADR-0003 (amendement #294) et ADR-0035 §1.
