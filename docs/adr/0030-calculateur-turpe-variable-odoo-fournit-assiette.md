# Calculateur TURPE variable : Odoo fournit l'assiette, electricore price

## Contexte

[ADR-0027](0027-endpoint-lecture-meta-periodes-odoo-tire.md) a posé le feed
`GET /facturation/meta-periodes` : electricore calcule les méta-périodes **depuis sa propre
énergie** (DuckDB), Odoo *tire*. Cette même ADR a **écarté un `POST recompute`** au motif que,
le calcul étant déjà à la volée et sans cache, *un GET recalcule déjà* — et a posé la règle
*« electricore livre le **montant** € quand il possède l'assiette, le **taux** quand Odoo la
possède »*.

L'ADR `souscriptions_odoo` 0005 (granularité de l'énergie pilotée par le calendrier de comptage)
introduit ensuite une énergie **saisissable à la main dans Odoo** (cascade dérivée-mais-
surchargeable `compute store readonly=False`) pour le cas où le flux Enedis manque. Conséquence
explicite de cette ADR : *« TURPE variable reste saisi à la main pour l'instant ; son recalcul
par electricore depuis les cadrans dépend du contrat d'intégration (#12). »*

Le nœud : l'énergie saisie à la main dans Odoo **n'atteint jamais le DuckDB d'electricore**. Le
`GET` calcule donc le TURPE sur une assiette absente ou périmée pour ces périodes. Le besoin réel
que l'ADR-0027 avait anticipé (« réintroduisible trivialement si un besoin réel émerge ») **a
émergé** : recalculer le TURPE variable à partir d'une assiette que **seul Odoo possède**.

## Décision

**Ajouter `POST /facturation/turpe-variable`** : un **calculateur sans état** où *l'appelant
fournit l'assiette* (énergies par cadran) et electricore renvoie le **montant** TURPE variable.

- **Calculateur, pas feed.** Odoo POST les énergies (+ FTA + `debut`), electricore applique la
  formule et renvoie. C'est une **évaluation de fonction**, pas une synchro : electricore ne
  *possède* pas l'assiette (Odoo en reste le système de référence), il l'**évalue** sur des
  entrées prêtées le temps de la requête.

- **Renvoie le montant € (pas le taux).** Exception **raisonnée** à la règle « taux quand Odoo
  possède l'assiette » d'ADR-0027 : cette règle protège electricore de devoir une assiette
  *qu'il ne voit pas*, or ici **l'assiette arrive dans la requête**, donc la crainte se dissout.
  Garder le montant : (1) la **formule reste en un seul endroit** (electricore — conversion
  c€→€ /100, sélection temporelle de la règle, sommation des cadrans) ; (2) **symétrie avec le
  GET**, qui renvoie aussi un montant → Odoo consomme `turpe_variable_eur` de façon **uniforme**,
  quelle que soit la provenance de l'énergie.

- **TURPE variable uniquement (pas le fixe).** Le déclencheur est l'énergie saisie à la main →
  le variable. Le fixe ne dépend pas des cadrans et arrive déjà correct via le GET. KISS/YAGNI :
  le fixe est **trivialement ajoutable** plus tard (même machinerie `ajouter_turpe_fixe`), au
  prix d'ajouter `puissance_souscrite` à la requête.

- **Lot + `id` opaque ré-émis.** Requête = liste de lignes, chacune avec un `id` opaque fourni
  par l'appelant (ex. `periode_id` Odoo) ; electricore le **ré-émet** tel quel à côté du
  résultat. Columnar-natif (une passe Polars sur le lot), indépendant de l'ordre, ERP-agnostique
  (electricore n'interprète jamais l'`id`). Le cas mono-période est `n = 1`.

- **Succès partiel, par ligne.** Chaque `id` envoyé revient, portant **soit** `turpe_variable_eur`
  **soit** un motif d'erreur (FTA inconnue / aucune règle pour la date). Remplace **délibérément**
  le *silent-drop* du pipeline (`valider_regles_presentes` filtre les lignes sans règle) — un
  calculateur requête/réponse ne doit jamais omettre silencieusement une réponse demandée.

- **Passe les 7 cadrans, electricore arbitre par la sommation.** La requête porte les 7
  `energie_*_kwh` (nullables ; null → 0). `turpe_variable_eur = Σ energie_cadran × c_cadran(FTA,
  debut) / 100`. **Aucun roll-up** côté electricore : l'arbitrage de granularité est porté par
  les **coefficients à zéro** de la règle FTA. Repose sur l'invariant *« chaque FTA a des
  coefficients non-nuls à exactement une granularité »* — **vérifié** sur `turpe_rules.csv` (38
  règles : 26 `4_cadrans`, 8 `base`, 4 `hp_hc`, **zéro** chevauchement). À **garder sous test**
  (le contrat se casse en silence — double comptage — si une future règle viole l'invariant).
  Corollaire métier : les FTA C5 *compteur communicant* (`BTINF*`) sont tarifées **à 4 cadrans**.

- **ERP-agnostique, JSON enveloppé.** Router sans import `integrations/odoo`
  ([ADR-0016](0016-core-erp-agnostique.md)), `contract_version` dans l'enveloppe, auth
  `X-API-Key` — mêmes conventions que `GET /facturation/meta-periodes`.

## Raison

electricore reste l'**autorité de la formule** TURPE (réglementée — un des trois registres de
savoir, [ADR-0024](0024-trois-registres-de-savoir.md)) ; Odoo reste l'autorité de **l'assiette
saisie**. Le calculateur matérialise cette frontière sans la franchir : Odoo prête l'assiette
qu'il possède, electricore applique la formule qu'il possède, et le montant repart. La forme
*pull-and-compute* met le système d'écriture (Odoo) aux commandes ; aucun service automatique
d'electricore ne modifie Odoo ([ADR-0012](0012-api-read-only-odoo.md)).

## Conséquences

- Débloque la consigne d'ADR-0005 « TURPE variable reste saisi à la main *pour l'instant* » :
  Odoo peut désormais le **recalculer** dès qu'il a l'énergie (flux ou saisie).
- Deux endpoints renvoient `turpe_variable_eur` en montant — GET (énergie electricore) et POST
  (énergie Odoo) — **consommés identiquement** côté Odoo. Différence : la provenance de l'assiette.
- Nouvel invariant **load-bearing** sur `turpe_rules.csv` (une granularité non-nulle par FTA) ;
  ajouter un test de garde.
- Le pipeline TURPE variable doit exposer un chemin **sans silent-drop** (motif d'erreur par
  ligne) ; le `valider_regles_presentes` actuel reste pour les chemins feed.
- Extension additive connue : TURPE **fixe** (ajouter `puissance_souscrite*`), composante de
  **dépassement** C4 (ajouter `duree_depassement_h`) — hors v1 (parc C5).

## Alternatives écartées

- **Renvoyer le taux (grille de coefficients), Odoo multiplie.** Strictement conforme à la règle
  d'ADR-0027, cacheable, sans aller-retour par période. Écarté : **duplique la formule TURPE dans
  Odoo** (même classe d'erreur que la « formule d'abonnement codée en dur » pointée par
  l'audit refonte), et donne à Odoo **deux** chemins TURPE (confiance-montant pour le GET,
  calcul-depuis-taux ici).
- **Recompute général `(RSC, mois)` depuis les données electricore** — exactement le `POST
  recompute` qu'ADR-0027 a écarté : sans valeur tant que l'énergie vient du DuckDB (le GET
  recalcule déjà). Le présent endpoint **n'est pas** celui-là : son assiette est *fournie par
  l'appelant*.
- **`POST /facturation/meta-periodes` avec overrides d'énergie** (recompute de la ligne complète).
  Plus puissant mais rouvre le large recompute écarté et dépasse le périmètre *variable seul*.
- **Roll-up dans electricore** (Odoo envoie 4 cadrans, electricore replie hph+hpb→hp selon la
  FTA). Inutile : les coefficients à zéro arbitrent déjà ; ajouterait une logique FTA-aware et un
  garde-fou contre la double granularité.
- **Garder le silent-drop du pipeline.** Un calculateur qui omet des réponses demandées est un
  piège de débogage pour le·la facturiste.
