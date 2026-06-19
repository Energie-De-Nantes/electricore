---
status: accepted
---

# Relevés utilisés imbriqués dans les méta-périodes : trace d'index légale tirée par Odoo

## Contexte

[ADR-0027](0027-endpoint-lecture-meta-periodes-odoo-tire.md) a posé
`GET /facturation/meta-periodes` : Odoo **tire** d'electricore (cœur read-only,
[ADR-0012](0012-api-read-only-odoo.md)), payload physique enveloppé en JSON, `source_hash`
par ligne + `contract_version`. Ce contrat exposait les **quantités** (énergie par cadran,
TURPE, CTA, taux accise, verdicts jumeaux) mais **pas les relevés d'index eux-mêmes** —
[#282](https://github.com/Energie-De-Nantes/electricore/issues/282) écrivait même
explicitement « pas de snapshot des index bruts » côté Odoo.

Or deux exigences imposent qu'Odoo **dispose** des relevés bornants :

1. **Légale** — les index doivent figurer sur la facture (cf. *Traçabilité des index*,
   [`core/CONTEXT.md`](../../electricore/core/CONTEXT.md)).
2. **Opérationnelle** — ils doivent **persister entre le pull et la génération de la
   facture**, et s'**afficher en espace usager**.

L'artefact existe déjà : le frame `releves_utilises` du bundle `ContexteMensuel`
(*Relevés utilisés*, [#233](https://github.com/Energie-De-Nantes/electricore/issues/233)) —
le sous-ensemble bornant réellement consommé par le calcul d'énergie, avec `releve_id`,
`nature_index` et registres réels. Il n'était **ni exposé ni persisté**. Grill du 19/06/2026.

## Décision

- **Bloc imbriqué.** Chaque méta-période de `/facturation/meta-periodes` porte un tableau
  `releves_utilises` (les relevés bornant ses sous-périodes du mois). Évolution **additive**,
  **`contract_version` bumpée**. Endpoint JSON-only → l'imbrication est triviale.

- **Nested-only, pas de second endpoint.** Le grain méta-période = une `souscription.periode`
  Odoo ; les relevés sont ses enfants. Un consommateur purement audit lit déjà `/releves`
  (le *modèle de relevés canonique*, tous les relevés). On n'ajoute pas de
  `/releves-utilises` séparé (qui forcerait Odoo à deux pulls + un join).

- **Objet relevé = `{ releve_id, date_releve, nature_index, registres index réels }`.**
  Registres **réels** uniquement (jamais de cadran synthétisé) ; `nature_index` = mention
  légale ; `releve_id` = handle de **reprise** ([#191](https://github.com/Energie-De-Nantes/electricore/issues/191)).
  `source`/`ordre_index` ne sont **pas** exposés (subsumés par `releve_id`). Périmètre C5
  (`index_base_kwh` / `index_hp_kwh` / `index_hc_kwh`) ; le 4-cadran C4 sera **additif**.

- **Invariant « vide ssi incalculable ».** `releves_utilises` non vide **⟺**
  `qualite ∈ {réelle, estimée}` ; `qualite = incalculable` **⟹** `[]`. Plein-ou-rien,
  miroir d'[ADR-0033](0033-qualite-periode-remplace-data-complete-coverage.md) /
  [ADR-0036](0036-statut-communication-routage-energie-grain-meta.md) : un mois incalculable
  n'est pas facturé en réel, donc ne porte aucune paire d'index à imprimer ; la cause est déjà
  signalée par `qualite` + `statut_communication`. Le **salvage** des sous-périodes valides
  d'un mois mixte (MCT mid-mois) est **différé à
  [#322](https://github.com/Energie-De-Nantes/electricore/issues/322)** — l'exposer plus tard
  sera un changement additif, pas une régression.

- **`source_hash` étendu** au tableau imbriqué (canonicalisation déterministe incluant
  `releve_id`, `date_releve`, `nature_index`, registres). Toute dérive d'un index **imprimé**,
  de sa nature ou de son identité flippe le déclencheur — **même à delta kWh constant** (reset
  compteur, correction ±k aux deux bornes, swap de relevé). C'est ce qui rend
  `source_hash` = *déclencheur* + `releve_id` = *reprise* fidèle à la promesse de
  régularisation.

- **`releve_id` reformaté en hash court** au seam dbt : `mint_releve_id` passe de la
  concaténation verbeuse (`source|pdl|timestamp+tz|discriminant`) à
  `substr(md5(<mêmes composantes>), 1, 16)`. Les composantes d'identité (source, pdl, date,
  discriminant) sont **inchangées** → stabilité `CORR ≡ BRUT` et reprise intactes ; reste une
  clé métier dérivée ([ADR-0028](0028-identite-releve-cle-metier-priorite-sources.md)). Fait
  **maintenant** car `releve_id` n'est encore exposé dans aucun contrat (coût aval nul, hors
  régénération golden + parité).

- **Stockage Odoo (délégué) — révise [ADR-0027](0027-endpoint-lecture-meta-periodes-odoo-tire.md)
  / #282.** Odoo stocke une **copie de travail** des relevés sur un sous-modèle de
  `souscription.periode` (persiste pull→facture, alimente l'espace usager), rafraîchie à chaque
  pull et gardée par `source_hash`. Le **gel légal immuable reste `account.move` au posting** —
  inchangé. Le cœur reste **sans état** ([ADR-0028](0028-identite-releve-cle-metier-priorite-sources.md))
  : il ne persiste rien ; la persistance vit dans le système de référence (ADR-0027), pas dans
  la lib.

## Raison

Le besoin légal et l'espace usager imposent qu'Odoo **détienne** les relevés bornants entre
le pull et la facturation ; le cœur ne peut pas les garder (sans état) ni les ré-fournir à la
demande sans risque de dérive mid-vol. Le grain méta-période est le grain naturel de stockage
(une période Odoo) : imbriquer évite un second flux à corréler. La rejouabilité reste
**recalcul + dérive** : `source_hash` (étendu) signale qu'une source a bougé, `releve_id`
(stable) retrouve exactement le relevé — la **détection appartient à Odoo** (re-pull + compare),
la lib n'introspecte pas ce qu'elle a émis.

## Considered options

- **Endpoint séparé `/releves-utilises`** — rejeté : deux pulls + join pour un modèle qui
  stocke les relevés en enfants de la période ; l'audit pur a déjà `/releves`.
- **`source_hash` sur agrégats seuls** — rejeté : aveugle à une dérive d'**index absolu** à
  delta constant, alors que l'index absolu est imprimé sur la facture.
- **Tableau présent-partiel pour les mois incalculables** — rejeté : un relevé orphelin est
  ambigu et non facturable, et casse l'invariant ; le salvage est le territoire de #322.
- **`releve_id` en concaténation (statu quo)** — rejeté : verbeux (timestamp+tz), pénible à
  stocker/afficher ; le hash préserve déterminisme et stabilité sans la verbosité.

## Conséquences

- `api/services/meta_periodes_service.py` : slicer `releves_utilises` par
  `(ref_situation_contractuelle, fenêtre [debut, fin])` et l'attacher ; étendre `COLONNES_CONTRAT`
  et la canonicalisation `source_hash` ; **bump `CONTRAT_VERSION`**.
- dbt : `mint_releve_id` → hash ; **régénérer golden + test de parité
  [#291](https://github.com/Energie-De-Nantes/electricore/issues/291)** ; les consommateurs
  internes de `releve_id` (bundle) suivent sans changement de sémantique.
- `docs/contrat-meta-periodes.md` étendu (bloc `releves_utilises` + invariant).
- [#282](https://github.com/Energie-De-Nantes/electricore/issues/282) **amendé** : ce bloc
  devient l'endpoint à livrer ; « Odoo stocke une copie de travail des index » remplace « pas de
  snapshot des index bruts ».
- `souscriptions_odoo` (hors périmètre electricore) : sous-modèle enfant de `souscription.periode`
  + rendu espace usager — tracé côté addon.

## Statut

Accepté (grill 19/06/2026). Étend [ADR-0027](0027-endpoint-lecture-meta-periodes-odoo-tire.md),
révise sa clause « pas de snapshot des index » côté Odoo. Glossaire :
[`core/CONTEXT.md`](../../electricore/core/CONTEXT.md) (*Traçabilité des index*, *Relevés
utilisés*). Implémentation : issues à créer (`/to-issues`).
