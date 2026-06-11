# Rôles des dossiers — loaders, pipelines, builds, writers, integrations, api

## Contexte

La codebase a accrété un vocabulaire (`core/loaders/`, `core/pipelines/`, `core/writers/`, `core/orchestrations/`, `integrations/odoo/`, `api/routers/`, `api/services/`) sans définition stricte des rôles. Plusieurs dérives observables :

- [`pipeline_cta`](../../electricore/core/pipelines/cta.py) prend deux sources hétérogènes en entrée (`df_facturation` Enedis + `df_pdl` Odoo) et fait une jointure — c'est une orchestration déguisée en pipeline.
- [`integrations/odoo/taxes.py`](../../electricore/integrations/odoo/taxes.py) mélange dans un même fichier les *sources* Odoo (`accise_par_contrat`, `cta_par_contrat`) et les *assembleurs* de livrables (`rapport_accise`, `rapport_cta`, `feuilles_rapport_*`). Une partie de cette logique est ERP-agnostique et devrait vivre en `core/` selon [ADR-0016](0016-core-erp-agnostique.md).
- [`pipeline_facturation`](../../electricore/core/pipelines/facturation.py) et [`pipeline_accise`](../../electricore/core/pipelines/accise.py) appellent `.collect()` en interne, contre la pratique courante (Polars, dbplyr, Ibis) qui place la matérialisation au boundary.
- [`core/orchestrations/contexte_mensuel.py`](../../electricore/core/orchestrations/contexte_mensuel.py) est nommé « orchestration » alors qu'il assemble un livrable (`ContexteMensuel` dataclass). Dans la convention industrielle (Airflow, Dagster jobs, Prefect, dlt), « orchestration » désigne le *scheduling* d'un DAG, pas l'assemblage d'un livrable.

Sans cadre explicite, chaque refactor perpétue ou aggrave l'ambiguïté. Le moment de figer la convention est avant d'attaquer la refonte Accise/CTA (issue à venir), qui crée de nouveaux modules et touche aux cinq rôles à la fois.

## Décision

**Cinq rôles dans `core/`, plus `integrations/<erp>/` côté ERP et `api/` côté transport.** Vocabulaire et règles d'import figés ci-dessous.

### Vocabulaire et responsabilités

| Dossier | Rôle | Entrée | Sortie | I/O | Peut importer |
|---|---|---|---|---|---|
| `core/models/` | Schémas Pandera (contrats de forme) | — | — | non | (rien) |
| `core/loaders/` | **Sources internes** (DuckDB, fichiers, configs) | identifiants (chemins, filtres) | `LazyFrame[Schema core]` ou query builders | oui (DuckDB, fichiers) | `core/models/` |
| `core/pipelines/` | **Transformations pures** (suite d'opérations) | N frames Pandera-typés | 1 frame Pandera-typé | **non** | `core/models/` |
| `core/builds/` | **Producteurs de livrables** (bundles dataclass, side-effects) | frames fournis par caller OU appels à `core/loaders/` | dataclass (`RapportTaxe`, `ContexteMensuel`) ou side-effect via writer | possible (via loaders) | `core/models/`, `core/pipelines/`, `core/loaders/`, `core/writers/` |
| `core/writers/` | **Sinks internes** (DuckDB, fichiers) | `DataFrame` | side-effect | oui | `core/models/` |
| `integrations/<erp>/` | **Sources et sinks ERP** | requêtes ERP | `LazyFrame[Schema core]` (lecture) / side-effect (écriture) | oui (ERP) | `core/models/`, `core/loaders/` (rare) |
| `api/routers/` | **Transport HTTP** (FastAPI, query params, sérialisation) | requête HTTP | réponse HTTP | non (délègue) | tout `core/`, `integrations/<erp>/`, `api/services/` |
| `api/services/` | **Wire-up** non-trivial ou stateful entre sources et builds | requête métier | DataFrame, bundle, ou statut | oui (compose sources + builds) | tout `core/`, `integrations/<erp>/` |

### Règles d'import (interdictions)

1. **`core/pipelines/` n'importe que `core/models/`.** Aucune I/O, aucune source, aucun assemblage de livrable.
2. **`core/builds/` n'importe jamais `integrations/<erp>/`.** Si un build a besoin d'une source ERP, le caller (typiquement `api/services/` ou `api/routers/`) la fournit en argument. Cette interdiction préserve [ADR-0016](0016-core-erp-agnostique.md) : `core/` reste ERP-agnostique.
3. **`integrations/<erp>/` n'orchestre pas de logique métier.** Son rôle est *uniquement* d'exposer des sources et sinks conformes aux schémas `core/models/`. Un fichier comme `integrations/odoo/taxes.py` qui assemble un livrable est une violation — l'assemblage descend en `core/builds/`, l'I/O Odoo reste en `integrations/odoo/sources.py` (ou équivalent).
4. **`core/loaders/` et `core/writers/` ne contiennent pas de logique métier.** Lecture/écriture seulement.
5. **`.collect()` au boundary, pas dans les pipelines.** Un `pipeline_*` retourne `LazyFrame[Schema]`. La matérialisation est de la responsabilité du build ou du caller. Aligne avec Polars / dbplyr / Ibis. (Migration progressive : les `pipeline_*` existants qui collectent restent valides en transition ; les nouveaux suivent la règle.)

### Comment trancher entre `pipeline_*` et un build

> *Pipeline = suite d'opérations pures qui produit un frame composable. Build = produit un livrable final consommable (bundle dataclass ou side-effect).*

Critères pratiques :

- Sortie = `LazyFrame` ou `DataFrame` unique consommé par d'autres pipelines → **pipeline**.
- Sortie = `@dataclass(frozen=True, slots=True)` portant plusieurs frames → **build**.
- Side-effect (écriture DB/fichier, push ERP) → **build**.
- N entrées hétérogènes (multi-sources) est *autorisé* dans un pipeline tant que le **domaine métier reste cohérent** (ex : la CTA d'un PDL, qu'on enrichisse l'`order_name` Odoo ou non, c'est toujours « la CTA »). L'arité d'entrée n'est pas un critère discriminant — aucun framework majeur (Polars, Dagster, Beam, dbt) ne le promeut au rang de catégorie.

### Intention différée : `core/ports/` le jour d'un 2ème ERP

Aujourd'hui un seul ERP (Odoo) est branché. La règle 2 (« `core/builds/` n'importe jamais `integrations/<erp>/` ») suffit à préserver l'agnosticité : les sources ERP sont injectées par le caller.

Le jour où un 2ème ERP (Haulogy, autre) apparaît, introduire **`core/ports/`** avec des `typing.Protocol` pour formaliser les sources/sinks ERP attendus, et faire des adaptateurs `integrations/<erp>/` des implémentations explicites de ces protocoles. Pattern ports & adapters / hexagonal. **Pas de code Protocol aujourd'hui** : ce serait de la machinerie sans seam réel à protéger (cf. principe « one adapter = hypothetical seam ; two adapters = real seam »).

### Caveat sur le mot « orchestration »

Dans l'industrie data (Airflow, Dagster *jobs*, Prefect *flows*, dlt), **« orchestration » désigne le scheduling et l'exécution d'un DAG**, pas l'assemblage d'un livrable. Le rôle que nous appelions historiquement « orchestration » (`core/orchestrations/contexte_mensuel.py`) est plus proche du concept Dagster d'`asset` ou d'un *build artifact*.

Choix de vocabulaire retenu : **`core/builds/`**. Neutre (pas de collision avec Dagster `asset` ou Airflow scheduling), lisible (`builds.rapport_taxe.rapport_accise(...)`), réserve le mot « orchestration » pour le jour où un vrai scheduler est ajouté.

Migration : `core/orchestrations/` → `core/builds/` en PR mécanique séparée (renommage + imports). Le seul fichier concerné aujourd'hui est `contexte_mensuel.py`.

## Raison

Trois facteurs cumulés :

1. **Le vocabulaire actuel ment.** `pipeline_cta` n'est pas un pipeline (jointure multi-sources hétérogènes), `integrations/odoo/taxes.py` n'est pas qu'une intégration (assemble des livrables), `core/orchestrations/` n'est pas une orchestration au sens industriel. Lire ces noms égare les nouveaux contributeurs (humains et LLM) et masque la dette.

2. **Le seam le plus rentable est le boundary build/pipeline.** Sans règle, on hésite à chaque nouvelle fonction : « pipeline ou orchestration ? `.collect()` ou pas ? ». Avec la règle (`pipeline` = LF→LF composable, `build` = livrable matérialisé, collect au boundary), chaque nouveau fichier trouve sa place sans discussion. Ancré dans la pratique : Dagster `ops`/`assets`, dbt `ephemeral`/`table`, dlt `resource`/`pipeline`.

3. **L'AI-navigability y gagne.** Un agent (humain ou LLM) qui lit un fichier de `core/pipelines/` sait par construction qu'il ne trouvera ni I/O, ni Odoo, ni `RapportXxx`. Un fichier de `integrations/odoo/` est un adapter de source, pas un livrable. Les attentes sont stables, la navigation est rapide.

### Alternatives écartées

- **Garder `core/orchestrations/`** — choix possible (zéro renommage), mais perpétue le mismatch avec la convention Dagster/Airflow. Le jour où on ajoute un scheduler (Dagster, Prefect), le nom devient ambigu. Renommer maintenant pendant que le seul fichier concerné est `contexte_mensuel.py` est ~10 minutes de mécanique.

- **Renommer `core/orchestrations/` → `core/assets/`** (Dagster-aligned) — explicite mais évoque un *catalogue de données* qu'on n'a pas. `core/builds/` est plus neutre et n'engage pas plus que ce qu'on a.

- **Discriminer pipelines par arité d'entrée (1→1 vs N→1)** — invention locale, aucun framework majeur ne le fait. Beam, Dagster, dbt, Polars utilisent le même primitif pour 1→1 et N→1. Critère écarté.

- **Autoriser `core/builds/` à importer `integrations/<erp>/`** — plus simple à court terme (chaque build appelle directement son ERP), mais casse [ADR-0016](0016-core-erp-agnostique.md) : `core/` ne serait plus ERP-agnostique. La règle « caller injecte les sources ERP » paie une légère ergonomie pour préserver l'agnosticité, sans coût de Protocol tant qu'on n'a qu'un ERP.

- **Introduire `core/ports/` avec Protocols dès maintenant** — over-engineering tant qu'il n'y a qu'un seul ERP (un adapter = pas de seam réel). À introduire le jour du 2ème ERP. L'intention est inscrite dans cet ADR pour ne pas être oubliée.

## Conséquences

### Drift identifiée à corriger

PRs de cleanup (séparées de l'ADR pour rester reviewables) :

1. **Renommage `core/orchestrations/` → `core/builds/`** (PR mécanique). Touche `contexte_mensuel.py` et les ~5 imports le référençant (`integrations/odoo/taxes.py`, `integrations/odoo/facturation.py`, tests, notebooks).

2. **Refonte Accise/CTA** (PR de design, dans la foulée) :
   - `integrations/odoo/taxes.py` → supprimé. Sources ERP extraites en `integrations/odoo/sources.py` (ou intégrées à `helpers.py`).
   - `core/builds/rapport_taxe.py` (nouveau) : `RapportTaxe` `@dataclass(frozen=True, slots=True)`, primitives `agreger_par_taux` / `agreger_resume`, helper `feuilles_rapport_taxe`, assembleurs `rapport_accise(lignes_lf, trim)` / `rapport_cta(facturation_df, pdl_df, trim)`.
   - `pipeline_cta` actuel reste un pipeline (jointure tolérée — même domaine métier « taxe d'un PDL »).
   - `api/services/taxes_service.py` (nouveau) : wire-up sources Odoo + DuckDB + builds.
   - `api/routers/taxes.py` : transport pur, délègue à `taxes_service`.
   - Migration `RapportAccise`/`RapportCta` `NamedTuple` → `@dataclass(frozen=True, slots=True)` (drift résiduelle [ADR-0018](0018-classes-justifiees-par-l-etat.md) réglée en passant).

3. **`.collect()` sorti des `pipeline_*` existants** (PR plus large, hors scope C1). À planifier après stabilisation des builds.

### Garde-fou exécutable

Un test d'architecture (`tests/architecture/test_imports_par_role.py`, à créer) parse les imports de chaque module et échoue sur :

- un fichier de `core/pipelines/**` qui importe autre chose que `core/models/`, `polars`, `pandera`, stdlib ;
- un fichier de `core/builds/**` qui importe `electricore.integrations.**` ;
- un fichier de `integrations/<erp>/**` qui produit un dataclass de livrable (heuristique : retourne un `@dataclass` du module `core/builds/`).

Pattern hérité de [ADR-0018](0018-classes-justifiees-par-l-etat.md#garde-fou-ex%C3%A9cutable) : la régression est attrapée par CI au lieu d'attendre la prochaine revue.

### Mise à jour CONTEXT.md

La section *Concepts pipeline* de [`electricore/core/CONTEXT.md`](../../electricore/core/CONTEXT.md) est étendue avec les définitions de **Loader**, **Pipeline**, **Build**, **Writer**, et la précision sur le caveat « orchestration ». L'entrée *Rapport* est mise à jour (`pipeline d'orchestration` → `build`).

## Limites à connaître avant un changement d'échelle

- **`core/builds/` peut importer `core/loaders/` (DuckDB).** Un build qui charge ses propres données depuis DuckDB est testable mais nécessite une base DuckDB pour les tests d'intégration. Si ce couplage devient gênant, scinder le build : une partie pure (frames en argument) + un helper qui charge depuis loaders.

- **La règle « caller injecte les sources ERP » exige discipline côté `api/services/`.** Un service qui se met à dépendre directement de plusieurs ERP devient un autre type de dette. Le jour où ça arrive, c'est le signal d'introduire `core/ports/`.

- **`pipeline_cta` reste un *pipeline* malgré sa jointure de 2 sources hétérogènes.** Choix conscient pour ne pas casser les imports et notebooks existants. La justification (« même domaine métier ») est valable mais subjective ; si une autre fonction frôle cette ambiguïté, statuer cas par cas.

- **Le test d'architecture détecte les imports interdits, pas les violations sémantiques** (ex : un fichier de `core/pipelines/` qui ferait du calcul I/O via une lib qui ne s'appelle pas `electricore.loaders`). Couverture imparfaite mais utile.

## Addendum (juin 2026, issue #146)

**`electricore/config/` est le socle transverse de la table ci-dessus** : stdlib-only
(chargement `.env`, résolution `chemin_base_duckdb()`, config Odoo, CSV de taux),
importable par tous les rôles — `core/` compris, sans violer [ADR-0016](0016-core-erp-agnostique.md)
(aucune dépendance ERP ni lib externe). Premier usage : `core/loaders/duckdb/`,
`api/services/`, le runner ETL et les tools délèguent la résolution du chemin de la
base DuckDB à `chemin_base_duckdb()` au lieu de porter chacun leur propre défaut.
