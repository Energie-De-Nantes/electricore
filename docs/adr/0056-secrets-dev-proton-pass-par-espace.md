# 0056 — Secrets de développement : Proton Pass par espace, injectés dans pydantic

- Status: accepted
- Date: 2026-07-10

## Context

La prod a une SSOT de secrets claire (ADR-0044) : `providers/<slug>/secrets.env` chiffré
SOPS+age, déchiffré au runtime par `sops exec-env`, avec le schéma des variables porté par le
registre pydantic-settings (ADR-0046/0049). Le **dev n'a aucun équivalent** — chaque repo
improvise :

- **electricore** : `.env` clair à la racine (chargé par `FICHIER_ENV`, `runtime.py`) ;
- **souscriptions_migration** : `os.environ[...]` brut, credentials **exportés à la main**
  dans le shell (`export SOUSCRIPTIONS_PROD_PASSWORD=…` → historique shell, cf. `COOKBOOK.md`) ;
- **souscriptions_odoo** : `docker/.env` + `ir.config_parameter`.

Trois idiomes, un secret qui transite par l'historique shell, aucun store commun. Le point de
partition qui structure déjà la prod — le **slug d'espace** (`edn` aujourd'hui, `enargia`
bientôt, plus un VPS perso `infra`) — n'existe pas côté dev, alors qu'electricore devient
multi-tenant : le même repo tournera contre plusieurs instances client aux credentials
distincts.

Fait qui débloque la décision : **Proton Pass ships désormais un CLI officiel scriptable**
(`pass-cli`, Proton AG, 2025-11-25, GPLv3), clone fonctionnel de `op run` de 1Password — refs
`pass://vault/item/field`, injection dans un subprocess via `pass-cli run --env-file`, agent
tokens scopables par vault. Feature payante (Pass Plus/Family/Pro).

**Spike (empirique, à retirer avant `accepted`).** Un seul inconnu bloquant restait : une vraie
ref `pass://` injecte-t-elle réellement la valeur de bout en bout ? Deux sous-questions déjà
tranchées à l'usage : (a) l'auth non-interactive **fonctionne sans re-login** — `pass-cli`
verrouillé derrière kwallet résout son token sans prompt (observé) ; (b) le pass-through docker
est reporté (souscriptions_odoo est en dernier). Reste à valider l'injection d'un secret réel
sur un item Proton créé pour le test — go/no-go de la bascule. **Finding (2026-07-10, PoC
souscriptions_migration) : GO.** Vault `cli-edn` + item custom `souscriptions` (8 champs prod/dev)
créés via `pass-cli item create custom --from-template`. `pr uv run … python -c 'settings()'`
résout les refs `pass://cli-edn/souscriptions/*` de bout en bout : pydantic reçoit les vraies
valeurs, masquées à l'affichage par `<concealed by Proton Pass>` — le masking *prouve*
l'injection (pass-cli ne masque que ce qu'il a résolu ; une ref non résolue s'afficherait
littérale). Auth kwallet silencieuse confirmée, zéro re-login.

## Decision

**Proton Pass est le store des secrets de DEV ; SOPS+age reste strictement la prod.** Frontière
nette, pas d'unification : les valeurs dev et prod sont différentes par construction (autres
instances, autres clés), donc rien à réconcilier.

**Le slug d'espace est la clé de partition, dev et prod.** Côté dev : **un vault Proton par
espace, préfixé `cli-`** (`cli-edn`, `cli-enargia`, `cli-infra`). Le préfixe distingue les vaults
**consommés par CLI** (injectés dans l'env via pass-cli) des vaults d'usage web quotidien, et les
**groupe en bloc** dans la liste Proton (tri par `cli-`), séparés des vaults perso mélangés. Ce
n'est pas « dev vs prod » (trompeur) mais un *mode de consommation*. Le slug prod SOPS reste nu
(`providers/edn/`). Miroir de `providers/<slug>/` en prod.

**Format de ref unifié** : `pass://<espace>/<projet>/<VARIABLE_ENV_VERBATIM>`.
- vault = espace ; item = projet (`electricore`, `souscriptions`) ; field = le **nom de variable
  d'env exact, casse comprise** (`__` préservé). Le champ Proton **==** la variable : un seul
  identifiant, **zéro transformation** — pas de fold de casse, donc l'import bulk `sops decrypt`
  → `pass-cli item update --field <ligne>` est une identité (la ligne dotenv EST l'arg).
- `pass://` n'a que 3 niveaux et l'espace en consomme un : la hiérarchie pydantic ne peut donc
  pas se déplier en segments `/`, elle vit intégralement dans le field.
  Ex : `API__TROUSSEAU__operator__KEY` → `pass://cli-edn/electricore/API__TROUSSEAU__operator__KEY`.
- Le `.env.pass` a donc **la même chaîne des deux côtés** : gauche = variable, droite =
  `<espace>/<projet>/<variable>`. Les repos à `env_prefix` le **retirent** pour éviter une ref
  redondante (souscriptions : var `PROD__URL`, pas `SOUSCRIPTIONS_PROD__URL`).

**Injection** : fonction fish `pr` + fichiers `.env.pass` de refs (gitignorés, perso — ils
pointent vers les noms de vault personnels). Repo mono-espace → `.env.pass`, `pr uv run …`.
Repo multi-espace → `.env.pass.<slug>`, choix **explicite** `pr <slug> uv run …`, et `pr`
**refuse** de tourner sans slug si le repo est multi-espace (garde-fou anti-« migration contre
le mauvais tenant »). Isolation subprocess : le secret vit le temps de la commande, jamais dans
le shell interactif.

**pydantic-settings sur tout le parc, SAUF `souscriptions_odoo`.** electricore l'est déjà ;
`souscriptions_migration` migre de `os.environ` brut vers un `BaseSettings` nested
(`env_nested_delimiter="__"`). `souscriptions_odoo` reste en `ir.config_parameter` : c'est un
module Odoo, le framework possède déjà la config, on ne force pas pydantic dans son runtime.

**Auth non-interactive** : token `pass-cli` au repos protégé par kwallet ; pour l'automatisation,
un **agent token scopé au vault = espace** (isolation par client : le token `cli-enargia` ne lit
pas `cli-edn`).

## Consequences

- **Plus facile** : un seul idiome de config (pydantic) ; plus de secret dans l'historique
  shell ; rotation/gestion via GUI Proton (mobile inclus) au lieu du rituel SOPS
  decrypt→edit→reencrypt→shred ; isolation token par espace ; le `.env.pass` d'electricore est
  la copie 1:1 en refs du gabarit `deploy/providers/example/secrets.env.example` existant.
- **Plus dur / vigilance** : dépendance à un **tier payant** Proton (bloqueur d'adoption — plan
  B ci-dessous) ; **deux systèmes** à connaître (Proton dev / SOPS prod), assumé car la
  frontière est nette ; injection par env **non isolée same-user** (`/proc/<pid>/environ` —
  modèle de menace OK en dev perso, pas sur machine partagée) ; `souscriptions_migration` à
  migrer vers pydantic ; pass-through docker de `souscriptions_odoo` à valider.
- **Suit** : ordre de bascule à risque croissant — (1) `souscriptions_migration` (plus gros
  gain, migre pydantic + `.env.pass` d'un coup), (2) electricore (`.env` racine → `.env.pass`,
  `runtime.py` inchangé), (3) `souscriptions_odoo` (docker, pass-through compose). Générateur
  `.env.pass` depuis le schéma pydantic à partir de 3 espaces (YAGNI avant). Étend ADR-0044
  (secrets-as-code) et ADR-0046/0049 (schéma pydantic SSOT) au périmètre dev.

## Alternatives considered

- **Étendre SOPS+age au dev** (un `secrets.dev.env` chiffré par repo, `sops exec-env` au lieu de
  `pr`) — rejeté malgré **zéro nouvel outil et gratuit** : le rituel decrypt/edit/reencrypt/shred
  à chaque rotation d'un secret dev qu'on touche souvent est plus coûteux au quotidien que la
  GUI Proton. Reste le filet si le tier payant saute (bascule `pr` → wrapper `sops exec-env`
  sans réécrire le reste).
- **1Password CLI (`op run`)** — iso-fonctionnel (`op://`, `.env` de refs, injection subprocess).
  Plan B direct si l'abo Proton gêne ; écarté par défaut car Proton est déjà l'écosystème en place.
- **direnv** (chargement auto sur `cd`) — rejeté : exporte les secrets résolus dans le shell
  interactif, donc dans l'env de TOUTES les commandes suivantes ; casse l'isolation subprocess
  que `pr` préserve.
- **Un vault unique pour tous les espaces** (item = `<projet>__<slug>`) — rejeté : perd
  l'isolation par agent token scopé au vault, qui est précisément le levier multi-client.
- **Forcer pydantic dans `souscriptions_odoo`** — rejeté : se battre contre le système de config
  natif d'Odoo (`ir.config_parameter`) pour 2 variables.
