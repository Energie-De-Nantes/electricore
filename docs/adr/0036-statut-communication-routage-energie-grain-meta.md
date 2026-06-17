---
status: accepted
---

# Statut de communication : axe miroir de la qualité, routage de l'énergie au grain méta

## Contexte

L'issue [#189](https://github.com/Energie-De-Nantes/electricore/issues/189) pointait deux
populations « infacturables en réel » : les compteurs non communicants (pas de télérelevé)
et les Linky dont la collecte n'est pas ouverte / refusée. La crainte était qu'elles soient
« difficiles à savoir ». La balise C15 `Niveau_Ouverture_Services` (∈ {0, 1, 2}, **toujours
transmise**, portée au niveau PRM) les **couvre toutes les deux** d'une seule lecture — c'est
la *cause déclarative* du statut de communication, surfacée par #314 (PR #320) jusqu'au modèle
`Historique` (typée, reportée par RSC en forward-fill).

Restait à figer **comment ce statut route le calcul d'énergie**. Un spike de faisabilité
([#315](https://github.com/Energie-De-Nantes/electricore/issues/315)) a tourné sur les données
prod (1372 PRM, avril 2024 → juin 2026) pour valider l'alignement structurel **avant** de
graver la décision.

**Chiffres du spike (#315) :**

- **Distribution du parc** (dernier niveau par PRM) : niveau 2 = **98,2 %**, niveau 0 = **1,8 %**,
  niveau 1 = **0 PRM**. Le niveau 1 n'existe qu'en *transit* (27 événements), personne n'y
  stationne.
- **Bascules** 0 → ≥1 : 25 PRM (1,8 %). **Non-monotones (communicant → non → communicant) : 0 %.**
- **Alignement relevé-au-1er ⇆ niveau** : une borne communicante a un relevé au 1er dans
  74,8 % des cas ; sources à la borne **R64 7338 / R151 3607 / C15 285** (le pull R64 est
  majoritaire). Les bornes non-communicantes sont sans relevé à 99,3 % (attendu).
- **Anomalie *communicant ∧ incalculable*** brute ~23 %, mais **dominée par la couverture de
  pull / la fenêtre d'extraction**, pas par des défaillances : un mois à **0,4 %** de trou
  (2026-05) prouve l'alignement quasi-total quand le R64 est tiré ; seuls **7 / 1324** PRM
  communicants sont « noirs ». **Prouvé par round-trip** : 2 demandes R64 M023 aux bornes
  01/02 et 01/03 ramènent **351/361** et **316/323** des manquants (manquants 361→**10**,
  323→**7**) ; les restants sont exactement les non-recevables (résiliés / MES tardive).

## Décision

### 1. Cause vs effet — deux axes orthogonaux, jumeaux de plomberie

Le **statut de communication** est la *cause déclarative* (C15 `Niveau_Ouverture_Services`)
qui route un contrat-mois vers la **voie communicante** ou **non-communicante**. Il est
**orthogonal** à la *qualité de période d'énergie* ([ADR-0033](0033-qualite-periode-remplace-data-complete-coverage.md)),
qui en est l'*effet mesuré* période par période (un mois communicant peut produire une période
*estimée*). Les deux partagent la même plomberie (portage sur les relevés, rollup pire-gagne),
d'où « jumeaux ».

_Réserver « réelle/estimée » à la qualité ; « communicant/non-communicant » au statut. Ne pas
confondre avec la capacité matérielle (`Palier_Technologique` = LINKY)._

### 2. Seuil : **communicant ⇔ niveau ≥ 1**

Retenu sur la donnée : le parc n'a **aucun** PRM au niveau 1 → seuils **≥1 et ≥2 sont
empiriquement équivalents** ; on retient **≥1**, sémantiquement correct (le niveau 1 ouvre la
collecte quotidienne d'index, ce qui suffit à la facturation calendaire ; le niveau 2 y ajoute
l'infra-journalier).

### 3. Routage au **grain méta**, paradigme **plein ou rien**

- Le pipeline énergie calcule **toutes les sous-périodes** (entre relevés consécutifs) — **pas
  de gate au grain relevé**.
- Le `niveau_ouverture` est reporté du C15 sur chaque relevé de la *chronologie des relevés* ;
  une *période d'énergie* est **communicante** ssi ses deux bornes sont à niveau ≥ 1.
- Le verdict mensuel est le **rollup pire-gagne** au grain *méta-période* : un `(situation
  contractuelle, mois)` part en voie communicante (facturable en réel) ssi **toutes** ses
  sous-périodes du segment actif sont communicantes — seule la **troncature calendaire
  d'entrée/sortie** reste éligible. Sinon → **non-communicant / incalculable, mois écarté du
  réel** (pour ne pas confondre en aval). Une bascule **en cours de mois** rend donc le mois
  entièrement non-communicant (sa borne à niveau 0 suffit) : **« pas de demi-mois ».**

### 4. L'abonnement n'est **pas** routé par ce statut

La part fixe (abonnement) est calculée pour **tous** ; le statut est un **filtre en amont de
l'axe énergie seul**, pas un troisième axe pair de l'abonnement.

### 5. Non-monotone **écarté en P1**

Les bascules non-monotones (communicant → non → communicant) sont ignorées en P1. Coût mesuré :
**0 %** du parc.

### 6. R64-pull = source maîtrisée ; l'anomalie est un trou de pull réparable

Niveau ≥ 1 ouvre l'**éligibilité** aux flux périodiques, pas leur *réception*. Le **R64 est
demandé à la borne** (*pull* ciblé sur le 1er) — donc disponible dès que le point est
communicant et **maîtrisé** (prioritaire sur R151 dans `PRIORITE_SOURCES`). L'anomalie
*communicant ∧ incalculable* est donc majoritairement un **trou de pull**, **opérationnellement
réparable** : la doctrine est *détecter → demander un R64 M023 → ré-ingérer*, pas estimer /
abandonner. (Note opérationnelle : le téléchargement portail M023 est un **ZIP clair**, il
**contourne** le blocage de la bascule de chiffrement AES-256 côté SFTP — [#221](https://github.com/Energie-De-Nantes/electricore/issues/221).)

## Alternatives écartées

- **Gate au grain relevé** (filtrer l'entrée du relevé de bascule) **vs rollup au grain méta** →
  **rollup retenu**. Le gate au grain relevé est fragile : `impacte_energie` est un détecteur de
  *delta avant/après* (deux valeurs non-nulles requises) **aveugle aux apparitions `null→valeur`**.
  Le spike l'a prouvé : le relevé d'activation `CMAT` (calendrier qui *apparaît*, `avant=null`)
  **n'entre pas** dans la chronologie (27/61). Sous le rollup plein-ou-rien c'est **sans
  conséquence** (la borne niveau-0 écarte le mois) → #314 reste à **0 code**. Le besoin de
  matérialiser le relevé de bascule ne réapparaît que pour le *salvage* des sous-périodes
  partielles, **déféré au chantier #322** (« Gestion/estimation des NC »).
- **Enum étendue** (ajouter le niveau dans l'énumération de qualité) **vs champ orthogonal** →
  **orthogonal retenu**. Porter le statut en champ distinct de la qualité sur la méta-période est
  ce qui rend l'*anomalie de communication* (communicant ∧ incalculable) distinguable d'un
  non-communicant attendu (même `incalculable`, cause différente).
- **Seuil ≥1 vs ≥2** → **≥1**. Chiffré équivalent (0 PRM au niveau 1) et sémantiquement correct.

## Conséquences

- **#314 livré** (PR #320) : `niveau_ouverture_services` surfacé C15 → `flux_c15` → `Historique`.
  L'implémentation de la *voie communicante* (report du niveau sur la chronologie, verdict de
  période, rollup méta) reste à faire — issue de suite.
- **CONTEXT.md** corrigé : l'entrée *Voie communicante* ne prétend plus que le relevé `CMAT`
  « borne proprement la première sous-période communicante » (prématuré — c'est le salvage du
  chantier #322, pas le paradigme plein-ou-rien actuel).
- **Chantier #322** créé (« Gestion/estimation des NC ») : salvage des sous-périodes partielles
  (qui exigera de lever l'angle mort `impacte_energie` `null→valeur`) + estimation des périodes
  NC (ML / estimées Enedis) + articulation lissé/régularisation (#191).
- **Raffinement méthodo** : la détection « communicant à la borne » doit borner sur l'état
  `EN SERVICE` à la date exacte (exclure RES imminent / post-MES) — sinon surestime l'anomalie
  (le résidu 10/7 du round-trip était surtout du cycle de vie contractuel).

Lie [ADR-0023](0023-periodisations-separees-abonnement-energie.md) (périodisations séparées
abonnement/énergie) et [ADR-0033](0033-qualite-periode-remplace-data-complete-coverage.md)
(qualité de période — le jumeau dont ce statut partage la plomberie).

## Statut

Accepté (épique #313, chiffré par le spike #315). Surfaçage livré (#314) ; voie communicante et
chantier NC (#322) en suite.
