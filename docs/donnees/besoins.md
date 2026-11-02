---
fraicheur: 2026-11-02
---

# Catalogue des besoins

Une fiche par besoin courant. Chacune donne le **statut** (calculé, lu, à construire,
ailleurs — voir la [méthode](index.md#la-methode-en-cinq-questions)), la **profondeur**,
**où regarder**, **comment y accéder** et les **pièges**. Les noms de tables, d'endpoints
et de fonctions sont exacts : un test les confronte au code pour que ce catalogue ne
dérive pas.

**Profondeur par défaut** : celle de l'instance — rien avant son déploiement, rien avant
l'entrée du PDL au périmètre, sauf archives rejouées ou flux commandés à la demande
(R64, R67 : jusqu'à 36 mois, voir [Obtenir les données Enedis](sources-enedis.md)). Les
fiches ne la répètent que quand elle diffère.

Les chemins d'accès sont, du plus outillé au plus brut : **bot** Telegram, **Kiosque**
(apps web), **client** Python `electricore-client`, **API** REST, **DuckDB** en lecture
seule. Un besoin qui revient à l'identique mérite de monter d'un cran (une app Kiosque
plutôt qu'un script refait à chaque fois).

## Consommations et index

### Consommation par mois civil d'un PDL au périmètre

- **Statut** : calculé pour le **C5** ; **à construire pour le C4** (la chaîne part du
  C15 : `flux_c12` ne l'alimente pas, R17 n'est pas ingéré). Grain `(RSC, mois civil)`,
  bornes tronquées aux entrées/sorties.
- **Où** : méta-périodes (`pipeline_energie` → `pipeline_facturation`), à partir de
  `chronologie_releves`.
- **Accès** : `GET /facturation/meta-periodes?mois=…` (JSONL,
  [contrat](../contrat-meta-periodes.md)) · client `ElectricoreClient.meta_periodes()` ·
  Kiosque `exports`, onglet Facturation · build `contexte_du_mois`.
- **Pièges** :
    - **un mois par appel** : trois ans = 36 appels ;
    - le filtre porte sur la RSC, pas le PDL : sans `rsc`, l'appel rend tout le mois, à
      filtrer sur le champ `pdl` ; un PDL peut avoir **deux lignes** dans un mois
      (changement de RSC) — sommer par PDL ;
    - seuls `energie_base_kwh`, `energie_hp_kwh`, `energie_hc_kwh` sont exposés — pas
      les quatre cadrans saisonniers ;
    - le verdict `qualite` (réelle / estimée / incalculable) et le
      `statut_communication` disent si le chiffre est fiable — à reporter dans la réponse ;
    - l'énergie des compteurs **non communicants** n'est pas encore calculée (#322).

### Consommation sur une période non calendaire (année scolaire, budgétaire…)

- **Statut** : calculé par assemblage — pas d'endpoint dédié.
- **Où** : somme des mois civils de la fiche précédente (septembre → août pour une
  année scolaire).
- **Pièges** : la profondeur. Si l'instance ou le PDL ne couvre pas toute la période,
  il faut [obtenir l'historique](sources-enedis.md#obtenir-un-historique-manquant) :
  R64 (index quotidiens) atterrit dans `releves` ; R67 arrive en **périodes de relevé**
  dans `flux_r67`, à proratiser aux jours pour reconstituer des mois.

### Index à une date, historique de relevés

- **Statut** : lu. Le mart `releves` dédoublonne **à l'intérieur** de chaque source ;
  l'arbitrage entre sources (C15 > R64 > R151) est fait par `chronologie_releves`.
- **Où** : mart `releves` = union C15 + R64 + R151 ; R15 en est exclu
  ([conventions de dates](../conventions-dates-enedis.md) § Exclusions) mais reste
  lisible via `flux_r15`.
- **Accès** : `GET /releves?prm=…&source=…&debut=…&fin=…` (bornes inclusives),
  `/releves.xlsx`, `/releves.arrow` · client `ElectricoreArrowClient.releves()` ·
  Kiosque `exports`, onglet Relevés · loaders `releves()`, `chronologie_releves()`.
- **Pièges** : `GET /releves` peut rendre **plusieurs sources pour le même jour** —
  filtrer par `source` ou passer par `chronologie_releves` ; R151 à +1 jour ; R64 est le
  seul flux à horodatage naïf (supposé Paris) ; index en kWh entiers ; `nature_index`
  réel / estimé / corrigé.

### Courbe de charge, puissance maximale atteinte

- **Statut** : à construire. R63 (courbe) et R66 (Pmax) ne sont pas ingérés.
- **Profondeur** : 24 mois pour la courbe, 36 pour la Pmax, côté Enedis.
- **Où** : demande M023 sur le portail SGE, fichier traité hors ElectriCore —
  [Obtenir les données Enedis](sources-enedis.md).
- **Pièges** : la courbe n'existe que si sa collecte était active (pas par défaut en
  C5) ; un moyennage trop large masque des pointes (dépassements de puissance mal
  estimés lors d'une optimisation de puissance souscrite).

### Volumes d'autoconsommation collective

- **Statut** : lu (C5), à construire (C4).
- **Où** : `flux_r15_acc` pour les C5 (énergies mensuelles par classe de mesure, pas des
  index) ; `flux_f12_detail` pour les C4 ; R17 (relevés C4) n'est pas ingéré.
- **Accès** : `GET /flux/r15_acc` et exports `.xlsx` / `.arrow` · loader `r15_acc()`.
- **Pièges** : le F15 ne distingue pas autoproduit et alloproduit, le F12 si ; les
  volumes suivent les périodes de relevé, pas le mois civil.

## Contrat et périmètre

### Périmètre à une date, entrées et sorties

- **Statut** : calculé (spans `[début, fin)` par RSC).
- **Où** : `presence_perimetre`, `pdls_actifs_a` (`core/pipelines/perimetre.py`), sur
  `flux_c15`.
- **Accès** : `GET /perimetre/pdls.csv?jour=…` (aussi la liste à déposer pour une
  demande M023) · `POST /perimetre/sorties`, client `ElectricoreClient.sorties()` ·
  `/flux/c15/entrees.xlsx`, `/flux/c15/sorties.xlsx` · bot `/perimetre pdls|entrees|sorties`.
- **Pièges** : un changement d'occupant sans résiliation est invisible ; une RES et une
  MES le même jour signalent un changement d'occupant ; « au périmètre Enedis » ≠
  « client chez nous » (l'ERP fait autorité).

### Situation contractuelle d'un PDL (FTA, puissance, calendrier)

- **Statut** : lu (C15, C12), calculé (situation propagée jour par jour).
- **Où** : `flux_c15` (C5), `flux_c12` (C2-C4, quatre puissances), mart `spine_contrat`.
- **Accès** : `GET /flux/c15?prm=…`, `GET /flux/c12?prm=…` · loaders `c15()`, `c12()`,
  `spine_contrat()`.
- **Pièges** : `flux_c12` n'alimente pas encore la chaîne de facturation (C4 à
  construire) ; le C15 donne l'identifiant du calendrier distributeur, pas le détail des
  plages heures creuses ; une photo de tout le périmètre à une date passe par un C68
  (non ingéré, demande M023).

### Affaires SGE en cours, résolution d'une affaire en RSC

- **Statut** : lu (jalons X12/X13), calculé (en-cours, ancienneté).
- **Où** : `flux_affaires`, `affaires_ouvertes`.
- **Accès** : `GET /perimetre/affaires` · bot `/perimetre affaires` ·
  `POST /facturation/rsc` ([contrat](../contrat-rsc.md)) · client `resoudre_rsc()`.
- **Pièges** : lecture seule, ElectriCore n'agit jamais sur le SGE.

### Compteurs non communicants

- **Statut** : calculé (verdict par méta-période), énergie à construire (#322).
- **Où** : `statut_communication` des méta-périodes ; niveau d'ouverture des services
  dans `flux_c15`.
- **Accès** : `GET /facturation/meta-periodes` · `GET /flux/c15`.

### Identité du client (titulaire, SIRET, NAF, payeur, groupement)

- **Statut** : ailleurs. Le C15 ne porte pas le titulaire.
- **Où** : l'ERP ou le logiciel de facturation du fournisseur ; formulaire de
  souscription.

## Montants et taxes

### Décomposer un budget ou une facture

Un prix d'électricité se découpe en trois blocs, et chacun a son détenteur :

| Bloc | Composantes | Statut | Où |
|---|---|---|---|
| **Acheminement** (Enedis) | TURPE fixe, TURPE variable, comptage, prestations | calculé / lu | méta-périodes (`turpe_fixe_eur`, `turpe_variable_eur`) ; facturé Enedis réel dans `flux_f15_detail` (C5) et `flux_f12_detail` (C4) ; prestations via `GET /facturation/prestations` |
| **Taxes** | CTA, accise, TVA | calculé (CTA) / ailleurs (accise facturée, TVA) | `cta_eur` des méta-périodes ; accise et TVA dans l'ERP — ElectriCore donne le taux d'accise (`taux_accise_eur_mwh`) |
| **Fourniture** (fournisseur) | énergie, abonnement fournisseur, offres | ailleurs | grille tarifaire et factures du fournisseur |

- **Pièges** : le recalcul ElectriCore (mois civil) et le facturé Enedis (F15 au
  moisniversaire) ne tombent pas sur les mêmes périodes ; le rapprochement TURPE
  calculé ↔ F15 n'existe pas encore (#468) ; les méta-périodes ne couvrent pas encore le
  C4. Pour un budget passé, la référence est ce qui a été **facturé** au client : c'est
  l'ERP du fournisseur qui fait foi.

### TURPE

- **Statut** : calculé (fixe et variable, barèmes versionnés `turpe_rules.csv`).
- **Accès** : méta-périodes · calculateur `POST /facturation/turpe-variable`, l'ERP
  fournissant l'assiette ([contrat](../contrat-turpe-variable.md)) ·
  [TURPE en standalone](../turpe-usage-standalone.md).
- **Pièges** : C4 — formules présentes ([TURPE fixe C4](../turpe-fixe-c4-btsup36kva.md))
  mais pas encore alimentées par `flux_c12` ; dépassements (CMDPS) non alimentés.

### Déclaration CTA trimestrielle

- **Statut** : calculé, sans état, sur le parc entier
  ([ADR-0058](../adr/0058-declaration-taxe-nouvelle-architecture.md)).
- **Accès** : `GET /taxes/cta/declaration.xlsx?trimestre=AAAA-TN` (ou `.arrow`) · bot
  `/taxes cta` · Kiosque `declaration-cta` · client `declaration_cta_xlsx()`.
- **Pièges** : le trimestre doit être terminé (sinon 422) ; terminé ≠ frais, des flux
  peuvent encore arriver ; l'assiette est le TURPE fixe **recalculé**, pas le facturé ;
  une régularisation (ACC, rattrapage) passée hors ElectriCore n'y figure pas.

### Déclaration d'accise

- **Statut** : ailleurs pour l'assiette (le **facturé**, donc l'ERP) ; calculé pour le
  taux en vigueur (`accise_rules.csv`).
- **Accès** : `GET /taxes/millesimes` (fraîcheur des barèmes) · chemin legacy couplé à
  Odoo : `/taxes/accise/rapport.xlsx`, bot `/taxes accise`.
- **Pièges** : catégories d'accise (ménages, PME, puissance) à construire (#226) ;
  vérifier le millésime des barèmes avant toute déclaration.

### Provision d'un contrat lissé

- **Statut** : calculé, en kWh, à partir des mesures facturantes R67 (démarrage de
  contrat) ([ADR-0048](../adr/0048-estimation-provision-lisses-coeur-kwh-cold-start-r67.md)).
- **Accès** : `GET /provision/estimation?pdl=…` · bot `/provision <pdl>` · client
  `provision_estimation()`.
- **Pièges** : le R67 se commande à la main (M023) ; la valorisation et la provision
  retenue sont dans l'ERP.

## Flux et complétude

### Un flux brut, tel qu'Enedis l'a livré

- **Statut** : lu.
- **Accès** : `GET /flux/{table}` (paginé), `/flux/{table}.xlsx`, `/flux/{table}.arrow`
  · client `ElectricoreArrowClient.flux()` · bot `/flux export <table>` · Kiosque
  `exports`, onglet Flux bruts · loader `flux()`.
- **Pièges** : le Kiosque ne propose pas encore toutes les tables (`c12`, `r15_acc`,
  `r67`, `affaires` manquent).

### Facturé Enedis (F12, F15) pour la comptabilité

- **Statut** : lu.
- **Où** : `flux_f15_detail` (C5, une ligne par élément valorisé), `flux_f12_detail`
  (C4).
- **Pièges** : F15 arrive en retard et daté dans le passé ; il est au moisniversaire.

### Une archive Enedis manque-t-elle ?

- **Statut** : calculé.
- **Où** : vue `audit_sequences` (trous dans les numéros de séquence), DuckDB direct
  seulement — [ingestion](../ingestion.md).
- **Pièges** : R64 et R67 sont hors champ (flux à la demande) ; un trou est « à
  vérifier », pas forcément une perte.

## Pour aller plus loin

Comment c'est construit : [ingestion des flux](../ingestion.md),
[contrats d'intégration](../contrat-meta-periodes.md),
[index des ADRs](../maintenir/index-adr.md).

[Retour à Trouver la donnée](index.md).
