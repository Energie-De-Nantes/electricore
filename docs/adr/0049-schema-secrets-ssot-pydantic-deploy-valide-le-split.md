# 0049 — Schéma des secrets en SSOT pydantic : le bash deploy valide la politique de split, pas le schéma

- Status: proposed
- Date: 2026-06-29

## Context

La configuration d'instance est scindée en deux (ADR-0044) : `config.env` clair (versionné)
et `secrets.env` chiffré SOPS+age. À l'install/reconfigure, ces fichiers sont validés par du
bash (`deploy/lib/validate.sh` + `deploy/lib/env_validate.sh`) : ~190 lignes qui
ré-implémentent un parseur dotenv (awk), l'extraction des trousseaux étiquetés (`grep|sed`
sur `AES__TROUSSEAU__<label>__KEY`, `API__TROUSSEAU__<consommateur>__KEY`) et les contraintes
de format (clé AES hex 32/64, clé API ≥32, schéma d'URL SFTP).

Or le moteur valide DÉJÀ ces mêmes variables au runtime via le registre pydantic-settings
(ADR-0024/0025/0046, `electricore/config/runtime.py`) : un `BaseSettings` par domaine,
accessors mis en cache, `valider(*accessors)` en fail-fast par point d'entrée. Le schéma des
variables d'env — le *contrat* entre deploy et runtime — est donc écrit **deux fois**, dans
deux langages, et dérive indépendamment. Voir le diagramme
[0049-collapse.excalidraw](0049-collapse.excalidraw).

Deux faits ont tranché le « où poser la coupe » :

1. **Décompte de forme.** Sur ~1400 lignes de `deploy/`, ~60 % est du wrapping de commandes
   système (apt/docker/ufw/age/sops/git — `shell-op` + `crypto-shell`) que Python ne ferait
   qu'emballer en subprocess, et ~18 % du templating. Seuls ~41 % sont de la logique,
   concentrée dans les validateurs. Réécrire tout `deploy/` en Python déplacerait la frontière
   shell sans la supprimer, en ajoutant un runtime Python hors-conteneur à amorcer sur une box
   nue (le bootstrap reste `curl | sudo bash`). Le seul îlot où Python *concentre* la
   complexité (test de suppression positif) est la validation — et c'est précisément l'îlot où
   le runtime Python est DÉJÀ présent (le moteur).

2. **Le seul chemin de validation véridique est le vrai conteneur.** Les secrets n'atteignent
   l'application que via `sops exec-env` (entrypoint, ADR-0044), **verbatim** (guillemets
   compris — cf. la régression rc12 `ODOO__` dé-quote, #454). Un preflight Python qui lirait
   `secrets.env` avec la sémantique python-dotenv pourrait donc PASSER là où le conteneur réel
   ÉCHOUE. Les étapes d'install existantes valident déjà le contenu contre la vraie image :
   étape 11 (health API) et surtout étape 12 (test ingestion → `valider(sftp, aes, duckdb)`,
   `electricore/ingestion/runner.py:306`). Une 2ᵉ surface serait redondante ET moins fiable.

Note : les validateurs pydantic actuels vérifient la *présence* (et l'hex des clés AES,
paresseusement dans `PaireCles.octets()`) mais PAS le format complet (longueur 32/64, len API
≥32, schéma d'URL). Faire de pydantic la vraie SSOT implique donc d'y *déplacer* ces
contraintes — ce qui durcit aussi le fail-fast runtime, gratuitement.

## Decision

Le **schéma** des variables d'env (présence + format) est la SSOT du registre pydantic
(`electricore/config/runtime.py`). On y déplace les contraintes de format manquantes comme
`field_validators` : clé/IV AES hex 32/64, clé API ≥32, schéma d'URL SFTP.

Le **bash de deploy** ne valide plus que la **politique de split** — propre au déploiement,
sans contenu de schéma :

- `validate_config_env` : présence des substitutions compose (`INSTANCE_SLUG`,
  `ELECTRICORE_VERSION`, `BACKUPS_PATH`) + **anti-leak** (aucun nom de secret en clair dans
  `config.env`) ;
- validation des arguments CLI (`validate_slug`, `validate_domain`).

La **validation du contenu de `secrets.env`** s'appuie sur le vrai conteneur via
`sops exec-env` → `valider(...)` (étapes 11-12 existantes), jamais sur un preflight Python.

On **supprime** les validateurs bash de secrets devenus redondants (`validate_secrets_plaintext`,
`validate_secrets_env`, et les primitives de format `validate_aes_key`/`aes_iv`/`api_key`/
`url`/`email`), ainsi que la branche d'install legacy `.env` déjà morte (`--deploy-repo`
obligatoire depuis le cutover ADR-0044 ; `validate_env_file` n'était même plus définie).

Le **bootstrap et l'orchestration shell-op** (`install.sh`, `harden.sh`,
`lib/{os,system,user,dns,stack,secrets,…}`) **restent en bash** : ils tournent sur une box nue
avant tout runtime Python et ne font qu'emballer des commandes système.

## Consequences

- **Plus facile** : un seul schéma à maintenir ; le format est vérifié au deploy ET au runtime
  depuis la même définition ; la surface de test des validateurs passe en pytest
  (`tests/unit/test_runtime.py`), où l'on est fluent ; ~90 lignes de bash supprimées ; le
  harnais `deploy/tests/unit.sh` rétrécit à son irréductible (slug/domain/os/parse_args/render/
  crypto).
- **Plus dur / vigilance** : durcir pydantic peut en théorie rejeter une conf laxiste — mais
  *safe by construction* (toute conf prod vivante a déjà passé le gate bash à l'install).
  L'échec d'un secret mal formé surface désormais à l'étape 12 (test ingestion) plutôt qu'à
  l'étape 9 ; on enrichit le hint d'échec pour remonter le message `ConfigurationManquante` du
  conteneur.
- **Ferme** la question récurrente « réécrire `deploy/` en Python ? » : non, sauf l'îlot
  validation, pour les raisons de forme ci-dessus.
- **Suit** : implémentation TDD (field_validators → suppression bash → élagage tests). Étend
  ADR-0025 (validation par point d'entrée) et ADR-0046 (schéma d'env) ; s'inscrit dans
  ADR-0044 (secrets-as-code).

## Alternatives considered

- **Preflight Python séparé** (`python -m electricore.config.preflight` appelé par install.sh)
  — rejeté : 2ᵉ surface de validation qui peut diverger du vrai conteneur (classe rc12), et
  redondante avec les étapes 11-12.
- **Réécrire tout `deploy/` en Python** (bootstrap bash minimal → Python) — rejeté : ~60 % du
  code est du wrapping de commandes système que Python emballe sans le supprimer, au prix d'un
  runtime à amorcer sur box nue. Déplace la frontière, ne la concentre pas.
- **Garder le format en bash, le relocaliser dans un validateur Python deploy-only** — rejeté :
  déplace le doublon bash→python au lieu de l'effacer.
- **Présence seulement (jeter les contraintes de format)** — rejeté : perd des garde-fous qui
  ont attrapé de vraies casses prod (clé AES mal formée, URL sans schéma).
