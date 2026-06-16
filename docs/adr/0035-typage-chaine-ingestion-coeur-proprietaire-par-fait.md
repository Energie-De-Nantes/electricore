# Typage de la chaîne ingestion↔cœur : un propriétaire par fait, parité vérifiée aux frontières

## Contexte

La discovery [#147](https://github.com/Energie-De-Nantes/electricore/issues/147) (candidat 4 de la
revue d'architecture du 11/06/2026, calque de #55) instruit la **triplication du savoir de typage** :
le vocabulaire de colonnes (les 7 cadrans `index_*_kwh`) et leurs types sont ré-écrits à la main
dans les modèles dbt source (`flux_r151.sql`, `flux_r64.sql`, `flux_c15.sql`), dans le mart
`releves.sql` (× 4 branches d'union), dans les re-CAST du loader (`core/loaders/duckdb/sql.py`,
~386 l.) et une dernière fois dans les schémas Pandera (`core/models/`). Une colonne nouvelle =
3-4 éditions synchronisées à la main, avec dérive silencieuse possible (Polars lazy → erreur tardive
au `collect`).

Le paysage a bougé depuis la rédaction de #147 :
- **[ADR-0029](0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md)** a descendu l'assemblage
  des relevés (union, harmonisation R151 J+1, dedup, forward-fill RSC/FTA) dans dbt ; le loader
  `releves()` est déjà un quasi-`SELECT *`.
- **[ADR-0034](0034-index-kwh-entiers-floor-au-boundary-dbt.md)** descend la normalisation d'unité
  (Wh→kWh `floor`) dans les modèles de linéarisation dbt.

**Pièce à conviction.** ADR-0034 documente précisément le mode d'échec que #147 prédit : quand
`releves()` est devenu `SELECT *`, la conversion Wh→kWh qui ne vivait que dans le loader a été
**silencieusement perdue** → index R151/R64 ~1000× trop grands, énergie/TURPE/accise corrompus.
Une déclaration redondante a dérivé entre deux couches parce qu'**aucun garde-fou ne siégeait à la
frontière dbt↔cœur**.

Deux « source unique » tentantes ont été instruites puis écartées (voir *Raison*) : générer dbt
depuis Pandera (consumer-driven contract), et un registre de types logiques émettant SQL + Polars.

## Décision

### 1. Un propriétaire par fait (délimitation), recouvrement nul

| Fait | Propriétaire unique |
|------|--------------------|
| Vocabulaire de colonnes (liste des cadrans, constructeurs `index_*_kwh`) | `core/models/cadrans.py` — **importé** par Pandera, **injecté** en dbt via macro/var |
| Type physique + forme (1 ligne/relevé, cadrans pivotés) + unité (kWh entier) | **dbt** — le modèle de linéarisation (ADR-0034) |
| Dtype Polars + invariants métier (`isin`, présence croisée, nullabilité-règle) | **Pandera**, au seam du pipeline |
| (rien) | **Le loader** ne connaît aucun type → `SELECT *` |

Les re-CAST de `sql.py` et les transforms de rattrapage (`transform_wh_to_kwh`, ancrages tz devenus
redondants post-0029/0034) sont supprimés. Le loader cesse d'être une 3ᵉ déclaration.

### 2. La parité est *prouvée*, pas *copiée*

Le type reste écrit dans deux langages (SQL côté dbt, dtype Polars côté Pandera) — c'est inévitable,
les deux couches tournent dans des moteurs différents. Au lieu de générer l'un depuis l'autre, un
**test de frontière** compare le schéma réellement émis par dbt au schéma Pandera attendu :

```python
def test_dbt_releves_respecte_le_contrat_pandera():
    emis = releves().limit(0).collect().schema          # ce que dbt produit
    attendu = RelevéIndex  # dtypes via une table de correspondance SQL↔Polars
    assert _types_compatibles(emis, attendu)
```

La **table de correspondance SQL↔Polars** (~8 lignes : `bigint↔Int64`, `timestamptz↔Datetime(tz)`…)
est le **seul** endroit du système qui connaît les deux langages. Changer un type d'un côté sans
l'autre → le test rougit immédiatement. Le test est un *fil de détente*, pas un oracle : il dit
*qu'*on a divergé, jamais *qui* a raison — c'est la table de propriété ci-dessus qui tranche.

### 3. Rejet du codegen

Ni « Pandera génère le `schema.yml` dbt », ni « registre de types logiques → 2 émetteurs ». Coût
itemisé : générateur + émetteur (~150 l.) + check de fraîcheur CI + contrats dbt *enforced* +
**régime scindé** (Pandera ne modélise que les seams de consommation, pas `flux_f15`/`flux_affaires`/
staging → la moitié des contrats générés, l'autre à la main) + on a **de toute façon** besoin du test
de seam (le YAML généré ne garantit pas que le SQL hand-written s'y conforme). Tout ça pour un gain —
« changer un type à un seul endroit » — sur un événement quasi nul (les types d'index sont
quasi-statiques ; c'est la *liste* de cadrans qui bouge, déjà couverte par `cadrans.py` dans les deux
approches).

### 4. Contrats dbt : documentation + tests de clés, **pas** enforced

`schema.yml` reste hand-éditable (descriptions, `not_null`/`unique` sur les clés). Le **test de parité
Python EST le contrat de type** à la frontière dbt↔cœur. Les golden dbt restent le garde-fou intra-dbt.

### 5. Portée : contrat au seam, pas par table

Un contrat (schéma Pandera + test de parité) à **chaque frontière dbt → pipeline-cœur**, pas à chaque
table dbt. La **nullabilité** reste un axe par couche (dbt `not_null` + Pandera `nullable`), hors du
test de parité (un schéma Polars n'encode pas la nullabilité).

## Raison

- **DRY s'applique aux *décisions*, pas aux *projections*.** En ELT chaque couche tourne dans un
  moteur distinct et échoue différemment ; on ne supprime pas les couches, on supprime la
  re-*décision* et on pose un garde-fou à chaque frontière (défense en profondeur). Re-nommer une
  colonne dans le SQL qui la *produit* n'est pas de la duplication — c'est du code, attrapé par le
  golden de cette couche.
- **Le codegen ne supprime pas le bug, il le déplace** : un mapping faux régénère le faux partout. Le
  test de parité l'attrape là où il naît, au seam.
- **`flux_*` est un asset partagé** (API `/flux`, notebooks, pipeline affaires), pas l'entrée privée
  du cœur. Un consumer-driven contract couplerait la table à un seul consommateur ; le test de parité
  donne au cœur sa garantie sans revendiquer la propriété de la table.

## Conséquences

- `cadrans.py` devient le générateur effectif (macro dbt `pivot_cadrans`, import Pandera) — le fan-out
  cadran passe de ~7 copies à 1.
- `sql.py` re-CASTs supprimés, loaders → `SELECT *` ; `transforms.py` réduit au strict non-exprimable
  en dbt (à inventorier).
- Nouveau helper de test de parité + table de correspondance SQL↔Polars partagée.
- **Lacune de registre identifiée** (finding #147) : `affaires_ouvertes` consomme `flux_affaires`
  sans contrat Pandera ; F15→facturation à auditer. Suite = ajouter un contrat **aux seams qui en
  manquent**, pas un modèle par flux.
- Filet de refonte : empreinte canonique `/releves` + golden dbt ([ADR-0021](0021-bascule-dbt-production.md))
  restent le garde-fou, comme recommandé par la discovery.
- Découpage en tracer bullets (issues de suite de #147) — aucun code de production dans la discovery.

## Statut

Accepté (discovery #147). Cadre les instances déjà décidées ([ADR-0028](0028-identite-releve-cle-metier-priorite-sources.md),
[ADR-0029](0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md),
[ADR-0034](0034-index-kwh-entiers-floor-au-boundary-dbt.md)) et clôt le candidat 4 de la revue
d'architecture (ADR-0021).
