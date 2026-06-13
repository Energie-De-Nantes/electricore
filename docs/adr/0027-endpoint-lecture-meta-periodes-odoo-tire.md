# Endpoint de lecture des méta-périodes : Odoo tire d'electricore

## Contexte

Jusqu'ici, la facturation circulait d'electricore **vers** Odoo : les calculs étaient
rapprochés aux lignes de facture Odoo (`sale.order`, champs `x_*`), puis un opérateur
poussait les quantités depuis un notebook (`OdooWriter`, sous validation humaine —
[ADR-0012](0012-api-read-only-odoo.md)).

La migration de l'addon `souscriptions_odoo` vers Odoo 19 (module réécrit
`souscription.souscription` / `souscription.periode`) **inverse le sens du flux** :
Odoo devient le système d'écriture et **tire** ses quantités d'electricore par HTTP GET,
puis construit/upsert ses `souscription.periode`. Côté `souscriptions_odoo`, ce contrat
est posé par ses ADR-0001 (Odoo = écriture, electricore = lecture), ADR-0002 (deux
sources de vérité : electricore = physique/réseau, Odoo = comptable) et ADR-0003 (la
migration Odoo 19 désigne cet endpoint comme **chemin critique** : tant qu'il n'existe
pas, la bascule métier ne peut pas se faire).

electricore **calcule déjà** la donnée demandée : `ContexteMensuel.facturation_mensuelle`,
une *méta-période mensuelle* par `(ref_situation_contractuelle, mois)`
([CONTEXT.md](../../electricore/core/CONTEXT.md), [ADR-0026](0026-facturation-calendaire-pas-moisniversaire.md)).
Il reste à l'**exposer**.

## Décision

**Ajouter un endpoint de lecture `GET /facturation/meta-periodes`** qui expose les
méta-périodes mensuelles sous un modèle **electricore-natif** (pas un miroir des modèles
`souscription.*`), en JSON enveloppé. Décisions structurantes :

- **Une seule opération, en lecture.** GET filtrable (`mois`, `rsc`) et paginé. **Pas de
  `POST recompute`** : le calcul est déjà *à la volée* depuis DuckDB (`contexte_du_mois`),
  donc toujours frais ; les corrections de flux remontent par l'**ingestion** (les
  endpoints `/ingestion/*` existants), pas par un déclencheur de calcul. **Pas de
  matérialisation** : à <1000 RSC, le pipeline Polars complet par requête est négligeable.

- **Router ERP-agnostique.** L'endpoint expose `core/` directement et **n'importe pas
  `integrations/odoo`** ([ADR-0016](0016-core-erp-agnostique.md)) — contrairement aux
  endpoints `/facturation/*` historiques qui lisent les lignes Odoo. Il ne sait rien
  d'Odoo : c'est un feed neutre, réutilisable par un autre ERP.

- **Charge utile = quantités physiques + réglementé selon qui possède l'assiette.**
  énergies par *cadran facturé* (`base`/`hp`/`hc`), `nb_jours`, `puissance_moyenne_kva`,
  `formule_tarifaire_acheminement`. Pour le réglementé, une **règle simple** : *electricore
  livre le **montant** € quand il possède l'assiette, le **taux** quand Odoo la possède.*
  → `turpe_fixe_eur`, `turpe_variable_eur` (assiette réseau/mesurée) et `cta_eur` (assiette
  = `turpe_fixe`, possédée par electricore) en **€ final** ; `taux_accise_eur_mwh` en
  **taux** (l'assiette accise = le *facturé*, possédé par Odoo). Les **prix fournisseur**
  (énergie, abonnement) ne sont **jamais** dans la charge utile : Odoo applique ses grilles
  datées (« référencer, pas recopier »).

- **Accise : electricore livre le taux, Odoo calcule le montant.** L'assiette accise est le
  *facturé* (= provisions pour un lissé, quantité qui vit côté Odoo ; taux par **catégorie**
  de consommateur). electricore reste l'**autorité du taux**
  (`taux_accise_eur_mwh`, taux standard en v1 — [ADR-0024](0024-trois-registres-de-savoir.md))
  et **n'expose aucun montant d'accise** dans le contrat : pas de figure « physique » à
  réconcilier côté Odoo, et aucune dépendance à une quantité Odoo (un `accise_eur` correct
  exigerait le facturé, qu'electricore ne possède pas sur ce chemin pur). L'accise
  **physique** (énergie mesurée × taux standard) reste *calculable* pour l'analytique
  interne, hors de ce contrat. Voir l'entrée *Accise physique vs Accise de déclaration* de
  [CONTEXT.md](../../electricore/core/CONTEXT.md).

- **Périmètre C5.** La méta-période actuelle ne porte qu'une puissance et `base`/`hp`/`hc` —
  elle ne représente pas un site C4 (4 puissances, 4 cadrans réseau). Le détail 4 cadrans
  et les 4 puissances **existent déjà** un cran plus haut (`PeriodeEnergie`,
  `PeriodeAbonnement` — le TURPE C4 est correctement calculé). Les exposer le jour où un
  C4 entre au périmètre est un **ajout de colonnes versionné**, pas une réécriture.

- **Feed non destructif.** Le retour est un *flux source*, pas une synchro avec
  suppression : l'upsert Odoo est **insert-or-update sur les brouillons uniquement,
  jamais delete**, et ne touche jamais une période validée/éditée à la main. electricore
  **ne peut pas** effacer de donnée Odoo (read-only, [ADR-0012](0012-api-read-only-odoo.md)) ;
  la garantie est donc portée par l'upsert Odoo, et electricore l'**outille** en
  publiant un `source_hash` déterministe par ligne (skip-si-inchangé + détection de dérive
  sous verrou).

- **Évolution additive + `contract_version`.** Le contrat grandit par ajout de colonnes
  optionnelles ; un lecteur tolérant survit à tout changement additif. Un entier
  `contract_version` dans l'enveloppe permet à l'addon d'asserter la compat ; on ne passe
  à un versionnage d'URL (`/v2/…`) que sur rupture réelle.

Le **write-back vers `sale.order`** (notebook + `OdooWriter`) **sort du flux standard de
facturation** : il reste autorisé pour l'enrichissement ad-hoc et les écritures validées
à la main, mais n'est plus une étape du chemin de facturation.

## Raison

electricore reste l'autorité du **physique/réseau** (quantités mesurées, coûts réseau,
taux réglementés) ; Odoo reste l'autorité du **comptable** (prix fournisseur, provisions,
ce qui est facturé et déclaré). L'endpoint matérialise cette frontière sans la franchir :
il expose ce qu'electricore *sait*, et laisse à Odoo ce qu'Odoo *possède*. Le sens « pull »
(plutôt que « push ») met le système d'écriture aux commandes de ses propres écritures —
aucun service automatique d'electricore ne modifie Odoo.

## Conséquences

- **C'est le long pole de la migration Odoo 19** : tant que l'endpoint n'est pas debout,
  `souscriptions_odoo` reste sur Odoo 17. À livrer en priorité.
- **Deux figures d'accise coexistent** et c'est correct : *physique* (mesurée, pour
  l'analytique et la référence de régularisation) et *de déclaration* (facturée, pour
  `/taxes/accise/*`). Ne pas laisser l'une se faire passer pour l'autre.
- **Bord de l'idempotence** : `(debut, fin)` font partie de la clé d'upsert Odoo, mais une
  correction de date contractuelle (entrée/sortie) **déplace** ces bornes — l'ancienne
  ligne devient orpheline. L'upsert Odoo doit réconcilier (delete-orphan sur RSC+mois, ou
  clé `(RSC, mois_annee)` pour les périodes tronquées). À signaler dans le doc de contrat.
- **Découvertes ouvertes** : (D1) comptage réglementaire exact de l'assiette accise ;
  (D2) catégories de taux accise — dérivables du flux Enedis, ou saisies côté Odoo ?
  Tracées en issues `needs-info`.

### Alternatives écartées

- **`POST recompute` ciblé `(RSC, mois)`** (demandé dans le handoff initial) : sans valeur
  tant que le calcul est à la volée et sans cache — un GET recalcule déjà. N'aurait de sens
  qu'avec un store matérialisé, lui-même injustifié à cette volumétrie. Réintroduisible
  trivialement en fine couche au-dessus de l'ingestion si un besoin réel émerge.
- **Exposer les assiettes accise / CTA** (plutôt que les montants) : confond « assiette »
  et « quantité ». La CTA est assise sur `turpe_fixe` (qu'electricore possède → montant
  final). L'accise est assise sur le *facturé*, qui pour un lissé est une quantité Odoo →
  electricore ne peut pas en faire un montant, il livre le taux.
- **Continuer le write-back depuis electricore** : recouple electricore au schéma Odoo
  ([ADR-0016](0016-core-erp-agnostique.md)) et fait d'electricore un écrivain comptable —
  exactement ce que la frontière des deux sources de vérité refuse.
