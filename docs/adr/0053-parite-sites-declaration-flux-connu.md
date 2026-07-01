# ADR-0053 — Parité des sites de déclaration d'un flux connu

## Statut

Accepté (#535, #536, #537, #538 — grilling du 01/07).

## Contexte

Un **flux connu** (glossaire, [`ingestion/CONTEXT.md`](../../electricore/ingestion/CONTEXT.md))
se déclare dans plusieurs sites répartis sur quatre modules :

1. **Configuration de flux** (`electricore/ingestion/config/flux.yaml`) — mouvement SFTP.
2. **Mapping raw → modèles** du runner (`MODELES_PAR_RAW`, `electricore/ingestion/runner.py`) —
   quelles tables `flux_*` matérialise chaque `raw_<flux>`.
3. **Sources dbt** (`electricore/ingestion/dbt/models/sources.yml`) — déclaration des tables
   brutes que dbt peut lire.
4. **Modèles dbt** (`electricore/ingestion/dbt/models/flux/*.sql`) — la linéarisation elle-même.
5. **Registre des descripteurs loaders** (`electricore/core/loaders/duckdb/registry.py`) +
   factories/exports (`helpers.py`, `__init__.py`) — l'accès requêtable côté cœur.
6. **Colonnes de date métier** (`COLONNE_DATE_METIER`,
   `electricore/api/services/duckdb_service.py`) — la fraîcheur exposée par les stats de table.
7. **Libellés bot** (`DESCRIPTIONS`, `electricore/bot/handlers/flux.py`) — le menu `/flux`.

Ces sites ont **dérivé en silence** : c12 et affaires sont devenus ingérables par le runner
CLI (site 1-4) sans être déclenchables via API/bot (site listant les modes, #535) ni requêtables
via le registre loaders (#536) ni visibles en fraîcheur/libellé (#537). Aucune de ces omissions
n'a fait échouer un test — chaque site tolère silencieusement l'absence d'un flux (une liste plus
courte reste une liste valide).

## Décision

**Pas de registre central.** Trois murs structurels empêchent de fusionner ces sites en une
seule source de vérité exécutable partout :

- **dbt parse son YAML avant tout runtime Python** — `sources.yml` doit exister en fichier,
  y compris pour un `dbt build` standalone (hors du process API/runner). Un registre généré à
  la volée par du code Python ne peut pas remplacer ce fichier : dbt le lit avant que ce code
  ne s'exécute.
- **`core/` doit rester installable sans l'extra `[ingestion]`** ([ADR-0016](0016-core-erp-agnostique.md)).
  Les descripteurs loaders (et leurs validateurs Pandera) ne peuvent pas migrer dans un module
  partagé avec le runner (qui importe `dlt`) sans inverser cette stratification.
- **Le bot ne consomme que l'API** (topologie testée, `tests/architecture/test_bot_topology.py`)
  — il ne peut pas lire `flux.yaml` ni le registre loaders directement, seulement dériver d'un
  endpoint.

Un registre central violerait l'un de ces trois murs. La protection retenue est donc un
**test de parité** (`tests/architecture/test_parite_flux.py`) qui croise tous les sites listés
ci-dessus et échoue si l'un d'eux diverge des autres — chaque exception (un site qui ne peut
structurellement pas couvrir un flux, ex. `releves` hors du registre loaders car mart et non
flux brut) y est déclarée et commentée en clair, plutôt que silencieusement absente.

Complément : `flux_connus()` (#535, `electricore/ingestion/config/__init__.py`) est la
**dérivation légère** que les sites capables de la consommer (runner CLI, API) utilisent au lieu
de recopier la liste à la main. Le test de parité protège les sites qui, pour les raisons
ci-dessus, ne peuvent pas dériver (dbt sources.yml, modèles, registre loaders, libellés bot).

### Alternative écartée : génération de `sources.yml`

Générer `sources.yml` depuis `flux.yaml` (codegen) a été considéré et écarté : cela ajoute un
artefact committé (le fichier généré) et nécessiterait de toute façon un test de non-dérive
entre l'artefact et sa source — la même protection que le test de parité direct, avec une pièce
mobile de plus (un script de génération à maintenir, un risque d'artefact non régénéré oublié
en commit). Le test de parité obtient la même garantie sans ce détour.

## Conséquences

- Ajouter un flux exige toujours de toucher les sites énumérés ci-dessus — ce n'est **pas**
  éliminé, seulement **vérifié**. Le test de parité échoue vite (au lieu d'une dérive silencieuse
  découverte des mois plus tard par un opérateur, comme celle qui a motivé cette série).
- Retirer un flux d'un seul site (mutation locale, ex. l'oublier du registre loaders) fait
  échouer le test — verrou de non-régression explicite.
- Les exceptions légitimes (grain différent par site, ex. `flux_r15_acc` qui n'a pas de
  descripteur loader dédié — le registre est keyé par *flux ingéré*, pas par *modèle dbt* ; ou
  `releves`/`spine_contrat` qui sont des marts dérivés, hors du périmètre des *flux connus*)
  sont des exceptions **déclarées** dans le test, pas des trous silencieux.
