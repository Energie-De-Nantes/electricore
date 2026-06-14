# Modèle de relevés canonique : dbt assemble, le cœur arbitre

## Contexte

Les relevés d'index viennent de quatre sources hétérogènes : R151, R15, R64, et les
relevés *avant*/*après* portés par les événements C15. Jusqu'ici, leur assemblage était
**éclaté en trois endroits** :

- deux unions au *query-time* dans les loaders DuckDB (`releves` = R151 + R15,
  `releves_harmonises` = R151 + R64) ;
- une extraction des avant/après C15 **en cœur** (`extraire_releves_evenements`) puis une
  union avec les relevés périodiques (`reconstituer_chronologie_releves`, devenue le module
  *Chronologie des relevés*, [#180](https://github.com/Energie-De-Nantes/electricore/issues/180)).

Triple comptabilité. En dbt, les sources périodiques sont déjà « 1 ligne = 1 relevé » et,
depuis [ADR-0028](0028-identite-releve-cle-metier-priorite-sources.md), minent `releve_id`
à la source — **mais C15 reste l'exception** : 1 ligne = 1 *événement*, avant/après pivotés
en colonnes, relevés extraits en cœur. L'identité est donc mintée à deux endroits, ce qui
est incohérent pour la traçabilité ([#232](https://github.com/Energie-De-Nantes/electricore/issues/232)/[#233](https://github.com/Energie-De-Nantes/electricore/issues/233)).

Par ailleurs, les relevés périodiques (R151/R64) ne portent pas la `ref_situation_contractuelle` ;
l'attribution contractuelle se faisait **incidemment** via un `join_asof` (tolérance 4 h) sur
les événements *FACTURATION* — un construit de facturation, pas la source contractuelle.

## Décision

Un **modèle de relevés canonique** matérialisé en dbt (`releves`), et une frontière nette
dbt / cœur : **dbt assemble et nettoie une donnée fidèle à la source ; le cœur prend les
décisions de facturation.**

**dbt — `releves`** (la ligne de temps des relevés *consommée* par l'aval) :

1. **Union** des quatre sources, avant/après C15 **dépivotés en lignes** (1 ligne = 1 relevé
   pour tout le monde).
2. **Harmonisation** des dates (conventions Enedis, R151 +1 j — [ADR-0003](0003-r151-date-harmonisation.md)).
3. **Mint uniforme**, pour **toutes** les sources C15 comprise, de `releve_id` (clé métier),
   `nature`, `id_releve` (provenance), `occurrence_id` (forensique). Supersède le
   « dérivée de l'événement […] en core pour c15 » d'ADR-0028.
4. **Enrichissement contractuel piloté par C15** : forward-fill de `ref_situation_contractuelle`
   et de la FTA depuis les relevés C15 (la source contractuelle), en remplacement du
   `join_asof` incident sur les événements FACTURATION.
5. **Dedup même-source** (re-livraison) : clé `(source, pdl, date, discriminant)`, on garde la
   **livraison la plus récente** (la *nature* suit le survivant — un `CORR` reçu après un `BRUT`
   gagne). Cf. ADR-0028 « garde la livraison la plus récente ».

→ Un ledger **multi-source, propre, enrichi**, portant la colonne `source`.

**Cœur — la *Chronologie des relevés* (amincie)** :

6. **Arbitrage de priorité des sources** (`C15 > R64 > R151`, ADR-0028) → 1 ligne par
   `(ref_situation_contractuelle, date_releve, ordre_index)`.
7. **Sélection des bornes** de facturation (1ᵉʳ du mois + événements contractuels, dépendant
   de l'`horizon`).
8. **Flag des relevés manquants**.
9. `impacte_energie` et la détection de changement de l'*Historique* **restent en cœur** et
   lisent `flux_c15` (avant/après conservés).

`flux_c15` reste **fidèle** : il garde avant/après (fidélité de l'événement *et* substrat de
`impacte_energie`). `flux_r151`/`flux_r64`/`flux_r15` restent (linéarisation, API `/flux`,
forensique ; peuvent devenir des *vues*).

## Raison

- **Une seule frontière, lisible.** Ce qui dépend de l'`horizon` (bornes, manquants) ne peut
  pas précéder dans une table statique → reste en cœur. Le reste — sourcing fidèle, nettoyage,
  enrichissement déterministe — descend en dbt.
- **L'enrichissement RSC/FTA piloté par C15 est principiel**, pas un raccourci : la RSC vient
  des événements qui la *définissent* (MES/MCT/RES), de façon déterministe et découplée de la
  facturation. C15 est la source contractuelle ; attacher la situation à un relevé « compteur »
  est un enrichissement *fait → dimension* idiomatique en dbt.
- **Mint uniforme en dbt = identité cohérente** pour le cas dominant (R151/R64) *et* pour C15,
  condition de la traçabilité (#232/#233). `releve_id` n'est pas encore en production ni tiré
  par Odoo → en changer la forme C15 **maintenant est gratuit** ; plus tard, c'est une migration.
- **`flux_c15` garde avant/après** parce que ce sont les relevés *de l'événement* (Enedis les
  livre avec lui) et le substrat de la détection de changement. Les en sortir forcerait soit un
  dédoublement de logique en SQL, soit un re-join, sans gain — la donnée doit de toute façon
  vivre dans la lignée c15 pour alimenter `releves`.
- **La redondance `flux_*` / `releves` est bénigne** : `raw_json` (landing) est la source de
  vérité unique ; tout le reste est une dérivation rejouable.

## Conséquences

- `releves` / `releves_harmonises` des loaders DuckDB et `extraire_releves_evenements` +
  l'union en cœur **disparaissent** (triple comptabilité résolue).
- La *Chronologie des relevés* (#180, mergée) **rétrécit** par refactor sur `main` ; #232
  (PR #238, draft) et #233 (PR #239) sont **redirigés** — le `releve_id` C15 passe en dbt.
- **ADR-0028 à amender** : la phrase « dérivée de l'événement + marqueur avant/après en core
  pour c15 » devient « mintée au seam dbt pour toutes les sources ». Identité, priorité et
  nature canonique restent inchangées.
- L'**arbitrage de priorité** reste une *politique de consommation* en cœur ; le ledger dbt
  garde **toutes** les sources (audit / provenance).
- Le **cycle de vie** des relevés (correction tardive, annulation, transition estimé→réel)
  **n'est pas** traité ici : MVP « dernière livraison gagne ». Discovery dédiée
  [#240](https://github.com/Energie-De-Nantes/electricore/issues/240), reliée à l'épique
  régularisation [#191](https://github.com/Energie-De-Nantes/electricore/issues/191).

## Alternatives écartées

- **Tout pousser en dbt, arbitrage et bornes compris.** Les bornes dépendent de l'`horizon`
  (runtime) : impossible dans une table statique. Pousser l'arbitrage seul n'achète pas la
  « pureté » recherchée et disperse la politique de priorité hors du cœur.
- **Sortir avant/après de `flux_c15` (drop + lien relationnel).** Casse la fidélité de
  l'événement et `impacte_energie` ; la donnée doit de toute façon vivre dans la lignée c15
  pour bâtir `releves` ; aucun consommateur ne navigue *événement → relevé* (la trace vit dans
  le journal #233). Gain de stockage ~nul (colonnes colonnaires majoritairement nulles).
- **Calculer `impacte_energie` en dbt.** Soit on n'y déplace que `index_change` (la logique de
  changement se retrouve éclatée SQL + Polars, cohésion perdue), soit on déplace tout
  l'enrichissement *Historique* (évide `pipeline_historique`, logique branchue mal placée en
  SQL, casse les property tests [#198](https://github.com/Energie-De-Nantes/electricore/issues/198)).
- **RSC via `join_asof` sur les événements FACTURATION (statu quo).** Fait dépendre
  l'attribution contractuelle d'un construit de *facturation* ; le forward-fill piloté par C15
  est plus correct et découplé.
- **Dedup cross-source en dbt.** L'arbitrage de priorité est une politique de consommation ; le
  garder en cœur préserve le ledger multi-source pour l'audit (ADR-0028).
