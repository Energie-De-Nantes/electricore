# Relations de la spine référencées par clé naturelle (`releve_id`) : normaliser `chronologie_releves`, surrogate/FK différés

## Statut

accepted — **précise et prolonge** [ADR-0041](0041-chronologie-contrat-spine-relationnelle-dbt.md) : celui-ci a posé la spine CTI (épine + situation forward-fillée + relations rattachées par source) mais a laissé **implicite** *quelle* colonne va dans la spine et *comment* une relation s'y rattache physiquement. **Réalise l'intention** d'[ADR-0029](0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md) (`releves` = modèle canonique, source de vérité unique) et d'[ADR-0038](0038-releves-utilises-imbriques-meta-periodes.md) (`releve_id` = clé métier). **Préserve** [ADR-0023](0023-periodisations-separees-abonnement-energie.md) (périodisations séparées).

## Contexte

ADR-0041 a fait de la *Chronologie du contrat* une **spine CTI** assemblée en dbt. Deux
points restaient ouverts :

- **(a)** quelle colonne appartient à la spine, vs à une relation ?
- **(b)** comment la relation *relevés* s'y rattache physiquement ?

Aujourd'hui le mart `chronologie_releves` **recopie** le payload d'index (`index_*_kwh`,
`nature_index`, `id_calendrier_distributeur`) **alors qu'il porte déjà `releve_id`** — une
duplication qui contredit la *source de vérité unique* d'ADR-0029/0038.

Intuition de départ (Virgile) : poser une **vraie FK interne / surrogate entier** pour la
perf de jointure (« sinon on pète l'opti »). C'est l'inconnue porteuse → **spike**.

**Finding du spike** (DuckDB 1.5.3 / dbt-duckdb 1.10.1, base dev = **441 867 relevés**) :
DuckDB **applique** la FK mais **seulement inline au `CREATE TABLE`** (`ALTER ADD FOREIGN
KEY` non implémenté → post-hook mort) ; dbt-duckdb expose `foreign_key = ENFORCED` mais
**uniquement via les *model contracts*** (schéma typé colonne par colonne sur les **deux**
marts = **taxe YAML** réelle). La jointure `INT64` vs `VARCHAR(16)` est **2,17× plus rapide
mais 4,8 ms contre 10,4 ms** à l'échelle du parc → **immatériel**. ⟹ la perf ne justifie
**ni** surrogate **ni** FK ici ; le gain de la normalisation est **de modèle** (source de
vérité unique), pas de vitesse. La prémisse « vraie FK sinon on pète l'opti » est renversée.

Le diagramme [`chronologie-spine-modele`](../chronologie-spine-modele.png)
([source](../chronologie-spine-modele.excalidraw)) argumente l'ensemble : substrat →
périodisations homogènes, règle d'intégration, dimension/fait/vue, et « pourquoi N frises ».

## Décision

**1. Règle d'intégration d'une colonne (rend explicite le CTI d'ADR-0041).**
Une colonne entre dans la spine **⟺ c'est un attribut de *situation*** : état contractuel à
sémantique **forward-fill** (step-function), nécessaire à des points qui **ne sont pas** son
événement-source (bornes FACTURATION, relevés périodiques). Tout le reste — le *payload*
propre à un fait (index, nature, calendrier ; demain le détail d'une affaire) — est une
**relation**, **référencée par clé**, pas inlinée. « Utile à ≥1 pipeline » **n'est pas** le
critère : les `flux_*` sont déjà filtrés aux colonnes utiles → ce critère inlinerait *tout*
→ STI (frame large nullable), écartée par 0041. Le partage par ≥2 pipelines est *conséquence*
de la nature « état », pas critère.

**2. Normaliser `chronologie_releves`.**
L'amincir à : identité + **situation** + **`releve_id` (référence)** + attributs de slot
calculés (`ordre_index`, `source`, `releve_manquant`) ; **retirer** le payload d'index
recopié (`index_*_kwh`, `nature_index`, `id_calendrier_distributeur`). Une **vue dbt**
(`materialized: view`) en **star-join** `chronologie_releves ⨝ releves ON releve_id`
ré-attache les index → `releves` = **source de vérité unique**. Le cœur (`pipeline_energie`)
lit la **vue** ; son contrat `ChronologieReleves` est **inchangé**.
*Nuance load-bearing* : la **situation (FTA / niveau / RSC) reste dans le mart mince** — sa
provenance est **native C15 OU ffill-spine** (≠ `releves[releve_id]`, nul pour les
périodiques) ; **seul le payload d'index pur** (fonction 1:1 de `releve_id`) migre vers la
vue.

**3. Clés.**
`releve_id` (clé métier, ADR-0038) est **la référence interne** *et* la clé **portable**
(Odoo, rejouabilité inter-instances). Le **surrogate entier + FK enforced sont différés** —
à rouvrir *seulement si* le parc ×100, *ou* si l'intégrité enforced est voulue pour
elle-même (la taxe *contracts* + le gain perf nul ne le justifient pas aujourd'hui).

**4. Nommage.**
*« Chronologie du périmètre »* = le **concept** parapluie (spine + ses relations ; *du
contrat* / *du point* = des **grains de coupe**, pas des marts). On **garde** le mart
`spine_contrat` (son grain de forward-fill *est* le contrat/RSC). On **dés-abrège** les
loaders : `spine()` → `spine_contrat()`, `chronologie()` → `chronologie_releves()`.

## Alternatives écartées

- **Surrogate entier + FK enforced maintenant** — perf immatérielle à 442k lignes, FK
  seulement via la taxe *contracts* (schéma typé sur 2 marts) ; rouvrir à ×100.
- **Garder la recopie du payload d'index (statu quo)** — contredit la source de vérité
  unique 0029/0038 ; corrigeable à coût quasi nul (le `releve_id` est déjà là).
- **Inliner les relevés comme lignes de spine, ou une base large avec colonnes FK au
  grain-fait** — le fan-out avant/après (2 relevés par événement C15) veut la référence au
  grain *slot* (côté « plusieurs ») ; et l'appariement périodique↔borne est **calculé**
  (equi-join jour), pas une clé native de l'événement.
- **Renommer le mart `spine_contrat` → `chronologie_*`** — « spine » nomme la *forme*
  (l'épine de colonnes communes), le grain est le contrat, « périmètre » est le concept
  parapluie (pas un mart).
- **STI (frame large nullable)** — déjà écartée par ADR-0041 ; rappelée car « utile à ≥1
  pipeline » y mène tout droit.

## Conséquences

- `releves` devient la **source de vérité unique** des valeurs d'index (fin de la
  duplication ; aligne 0029/0038).
- Le **patron de relation généralise** : une nouvelle relation (affaires/jalons) s'attache
  pareil — référence par clé, payload dans sa table, projection = vue.
- `chronologie_releves` **reste un mart matérialisé** (l'appariement equi-join jour +
  priorité `QUALIFY` C15>R64>R151 n'est pas trivial) mais **plus mince** ; l'énergie lit une
  **vue** (un join de plus, ~10 ms à 442k → négligeable).
- La **règle d'intégration** devient le critère normatif cité dès qu'une colonne nouvelle se
  présente (relais : affaires, accise…).
- **Périodisations séparées préservées (ADR-0023)** : les frises *abonnement* / *énergie*
  (et *NC*… demain) ne fusionnent pas — leurs jeux de coupes se **croisent** (`puissance` ∈
  abo-seul, `index/calendrier` ∈ énergie-seul, `fta` ∈ commun, aucun n'emboîte l'autre) et
  l'énergie est **contrainte par la donnée** (pas d'index = incalculable). Impossible dans
  les **deux** sens, l'union aussi. **Une spine, N frises.** (Diagramme.)
- **Suites (issues)** : (1) amincir `chronologie_releves` + vue + énergie lit la vue + tests
  de parité ; (2) nommage / diagramme / glossaire (rename loaders, *« Chronologie du
  périmètre »*). Le surrogate/FK ne fait **pas** d'issue — différé, tracé ici avec son
  déclencheur (×100 ou intégrité voulue).
