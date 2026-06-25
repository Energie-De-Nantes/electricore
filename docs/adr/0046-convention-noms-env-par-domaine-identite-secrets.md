# Convention de nommage env `<DOMAINE>__<CHAMP>` + modèle d'identité/accès des secrets

## Statut

Accepté (grill `/grill-with-docs` 25/06/2026). Prolonge [ADR-0044](0044-secrets-as-code-sops-age.md)
(secrets-as-code SOPS+age), [ADR-0025](0025-registre-runtime-pydantic-settings.md) (registre runtime
pydantic-settings), [ADR-0037](0037-trousseau-cles-aes-n-cles-selection-par-essai.md) /
[ADR-0040](0040-schema-dechiffrement-aes-iv-prefixe.md) (trousseau AES). **Exécute la clôture de
[#190](https://github.com/Energie-De-Nantes/electricore/issues/190)** (suppression du sélecteur
`ODOO_ENV` / test-prod). **À livrer dans la MÊME release** que le mécanisme secrets-as-code ([#422](https://github.com/Energie-De-Nantes/electricore/pull/422),
déjà sur `main`, non déployé) — un seul cutover, zéro migration (cf. *Conséquences*).

## Contexte

ADR-0044 a introduit secrets-as-code (le `.env` en clair → `config.env` clair + `secrets.env` chiffré)
mais n'a **pas** harmonisé les noms de variables ni modélisé finement les **identités d'accès** au-delà
des destinataires SOPS. Deux problèmes restent ouverts.

**1. Le nommage est un zoo.** Deux conventions coexistent dans `runtime.py` :
- **nested `__`** (séparateur imbriqué pydantic) : `SFTP__URL`, `AES__TROUSSEAU__<label>__KEY` ;
- **flat `_`** (alias plat) : `API_KEY`, `TELEGRAM_BOT_TOKEN`, `ODOO_<ENV>_*`, `DUCKDB_PATH`,
  `INSTANCE_SLUG`, `BACKUPS_PATH`…

Cette incohérence est une source de bugs avérée (préfixe `ODOO__` vs `ODOO_PROD_` confondus pendant le
scaffolding #422). Le nom de la variable n'est **pas prédictible** depuis le domaine.

**2. Les surfaces d'accès ne sont pas toutes isolées.** L'isolation est nette presque partout (age et SSH
sont **par slug** — une box ne déchiffre que son provider), **sauf** le consommateur de l'API :
`API_KEY` + `API_KEYS` est un **sac anonyme** de chaînes acceptées — impossible de savoir *qui* a utilisé
quelle clé, impossible de révoquer **un** consommateur sans tout roter.

Question instruite au grill : décomposer les clés/accès **par slug** et **par sous-cas d'identité**
(admin, box/serveur, consommateur API) pour isoler les surfaces, et **harmoniser tous les noms**.

## Décision

### 1. Convention `<DOMAINE>__<CHAMP>`

Les **six domaines runtime** (`duckdb`, `sftp`, `aes`, `odoo`, `api`, `bot`) exposent leurs variables sous
`<DOMAINE>__<CHAMP>` — séparateur `__` (= `env_nested_delimiter` pydantic), `<CHAMP>` en `UPPER_SNAKE`. La
promesse : **le nom du domaine prédit le préfixe** (le domaine `runtime.bot()` ⇒ préfixe `BOT__`). `SFTP` et
`AES` sont **déjà conformes** ; la convention se résume à « faire ressembler tout le monde à SFTP/AES ».

### 2. Sous-motif trousseau `<DOMAINE>__TROUSSEAU__<label>__<CHAMP>`

Un **trousseau** = un sac **étiqueté** de credentials d'un même type, à **révocation par label**. Le
`<label>` est arbitraire, choisi par l'opérateur, injecté **sans énumération** (`sops exec-env`, ADR-0044
§2). Déjà le cas pour AES (`AES__TROUSSEAU__<label>__KEY[__IV]`) ; généralisé à l'API (§4). Le trousseau
devient un **motif réutilisable** du registre, pas une bizarrerie AES.

### 3. Carve-out : les variables d'infra / substitution compose restent **plates**

`INSTANCE_SLUG`, `ELECTRICORE_VERSION`, `BACKUPS_PATH` **ne sont pas des domaines runtime** : ce sont des
variables d'identité d'instance et de **substitution `${...}` docker compose** (résolues côté hôte). Elles
restent **plates**, en **clair**, dans `config.env`. La convention `__` ne vise **que** les domaines de
config runtime — pas la couche déploiement/compose.

### 4. Consommateur API = trousseau étiqueté (isolation de la surface faible)

`API_KEY` / `API_KEYS` (sac anonyme) → `API__TROUSSEAU__<consommateur>__KEY`. L'authentification associe la
clé entrante à son **label** → **révocation ciblée** d'un consommateur **+ attribution** (le label remonte
dans les logs ; `APIKeyInfo` le porte). Le bare `API_KEY`/`API_KEYS` est **retiré** (trousseau-only, comme
AES). « client user » et « api user » sont le **même** principal — un porteur de clé qui appelle l'API REST
(`X-API-Key`) : on le nomme **consommateur API**. Décisif : le nom de variable est **côté serveur
uniquement** → les clients (`electricore-client`, `bot/client.py`, notebooks) **ne changent pas** (ils
envoient une chaîne bearer, ils ignorent le nom de la var serveur).

### 5. Odoo : un seul bloc, suppression du sélecteur test/prod (exécute #190)

[#190](https://github.com/Energie-De-Nantes/electricore/issues/190) (clos *completed* le 18/06) tranche la
direction : plus de nouveau code d'écriture Odoo, Odoo **pull** à terme, `OdooWriter` pourrait disparaître.
Le sélecteur `ODOO_ENV` + blocs `ODOO_TEST_*` / `ODOO_PROD_*` n'existait que pour **répéter une écriture sur
la base de test avant prod** — sa raison d'être disparaît. On **supprime** : `ODOO_ENV`, `_SelecteurOdoo`,
l'injection de préfixe `f"ODOO_{env}_"`, le paramètre `env`. Un **seul** bloc `ODOO__{URL,DB,USERNAME,PASSWORD}`
(lecture, [ADR-0012](0012-api-read-only-odoo.md)). Plus de répétition test→prod (**assumé** : les
injections notebooks visent l'unique Odoo configuré, sans filet). Les commentaires « gelée dans l'attente de
#190 » ([runtime.py](../../electricore/config/runtime.py), [ADR-0025](0025-registre-runtime-pydantic-settings.md))
sont **caducs** → corrigés.

### 6. Bot : préfixe `BOT__` (le domaine, pas le fournisseur)

`TELEGRAM_*` → `BOT__*` : le préfixe suit le **domaine runtime** (`bot()`, contrat `valider(bot)` d'ADR-0025),
pas le fournisseur. Telegram est un **détail d'implémentation**. `BOT__TOKEN`, `BOT__ALLOWED_USERS`,
`BOT__NOTIFY_CHAT_ID`. (YAGNI sur un second fournisseur de bot ; le « domaine = préfixe » prime.)

### 7. Frontière config/secret affinée

Règle : **`secrets.env` = matériel qui confère une capacité ; `config.env` = identité / politique / routage.**
Un user-id ou un chat-id seul ne confère **rien** sans le `BOT__TOKEN`. **Exception privacy** :
`BOT__ALLOWED_USERS` (user-ids Telegram des opérateurs) part en **`secrets.env`** malgré la règle —
`config.env` est en **clair dans git**, y publier les identifiants des opérateurs est une fuite (RGPD).
`BOT__NOTIFY_CHAT_ID` (routage seul, peu sensible) reste en **`config.env`**.

### 8. Modèle d'identité admin : deux autorités, escrow de déchiffrement

L'**admin** = un **rôle humain** portant **deux autorités isolées par type de credential** :
- **autorité de déchiffrement** = la clé age privée (déchiffre tout provider) ;
- **autorité de publication** = le write git du dépôt de déploiement (pousse du ciphertext, édite `.sops.yaml`).

Une fuite de l'une **ne donne pas** l'autre (ciphertext poussé sans clé age = inerte ; clé age sans write =
ne publie rien). L'autorité de **déchiffrement se dédouble** : une clé age **opérationnelle** (édition au
quotidien) **+** une clé age **escrow hors-ligne**, **toutes deux destinataires de CHAQUE règle** `.sops.yaml`.
L'escrow ne sort jamais sauf reprise sur sinistre → supprime le point unique de défaillance « perte de la
clé admin ⇒ plus rien de re-chiffrable » (les box ne déchiffrent que leur propre provider ; re-keyer exige
un destinataire admin vivant).

Inchangés (déjà isolés au grill ADR-0044) : **box/serveur** = par slug, deux identités à credentials
distincts (age déchiffre son seul provider + SSH deploy RO) ; **CI** = jamais destinataire.

## Table de renommage

| Domaine | Aujourd'hui | Nouveau | Fichier | Secret |
|---|---|---|---|---|
| sftp | `SFTP__URL` | `SFTP__URL` *(déjà conforme)* | secrets.env | 🔒 |
| aes | `AES__TROUSSEAU__<l>__KEY/IV` | *inchangé* | secrets.env | 🔒 |
| duckdb | `DUCKDB_PATH` | `DUCKDB__PATH` | config.env | — |
| api | `API_KEY` + `API_KEYS` | `API__TROUSSEAU__<consommateur>__KEY` | secrets.env | 🔒 |
| api | `API_TITLE`/`API_VERSION`/`API_DESCRIPTION` | `API__TITLE`/`API__VERSION`/`API__DESCRIPTION` | config.env | — |
| bot | `TELEGRAM_BOT_TOKEN` | `BOT__TOKEN` | secrets.env | 🔒 |
| bot | `TELEGRAM_ALLOWED_USERS` | `BOT__ALLOWED_USERS` | secrets.env | 🔒 *(privacy)* |
| bot | `TELEGRAM_NOTIFY_CHAT_ID` | `BOT__NOTIFY_CHAT_ID` | config.env | — |
| odoo | `ODOO_ENV` | **supprimé** | — | — |
| odoo | `ODOO_<ENV>_{URL,DB,USERNAME,PASSWORD}` | `ODOO__{URL,DB,USERNAME,PASSWORD}` | secrets.env | 🔒 |
| *instance* | `INSTANCE_SLUG` | `INSTANCE_SLUG` *(plat, carve-out)* | config.env | — |
| *deploy* | `ELECTRICORE_VERSION` | `ELECTRICORE_VERSION` *(plat)* | config.env | — |
| *deploy* | `BACKUPS_PATH` | `BACKUPS_PATH` *(plat)* | config.env | — |

## Alternatives écartées

- **Rename complet incluant Odoo en respellant `ODOO__TEST__*`/`ODOO__PROD__*`** — écarté : on respellait
  une structure que #190 vient de condamner. On **collapse** plutôt (§5).
- **Convention sans rename (going-forward only)** — écarté par l'opérateur : on veut un nommage homogène, et
  **rien n'est déployé** ⇒ la fenêtre du cutover SOPS rend le rename **gratuit** (un seul ré-écriture d'env).
- **API : garder le sac anonyme `API_KEYS`** — écarté : ni révocation ciblée ni attribution ; le trousseau
  étiqueté est le **point de convergence** isolation + harmonisation.
- **Préfixe `TELEGRAM__`** — écarté : casse « nom du domaine = préfixe » ; YAGNI multi-fournisseurs (§6).
- **Clé admin unique sauvegardée hors-ligne** — écarté : la sauvegarde serait l'**unique** chemin de reprise
  et l'utiliser ramène la clé racine en ligne ; l'escrow **distinct** (destinataire permanent) l'évite (§8).
- **Clés admin par humain** — **différé** (opérateur solo) ; `.sops.yaml` les accueillera gratuitement quand
  il y aura plusieurs administrateurs.

## Conséquences

- **`runtime.py`** : aliases respellés ; domaine **Odoo simplifié** (suppression `_SelecteurOdoo`, du
  paramètre `env`, de l'injection de préfixe) ; domaine **Api → trousseau** (dict `label → clé`, calqué sur
  `Aes`) + helper `consommateur_pour(cle)` ; **Bot → `BOT__`**.
- **`api/security.py`** : `get_api_key` peut renvoyer le **label** du consommateur (attribution) ;
  `APIKeyInfo` porte le label.
- **`deploy/`** (déjà sur `main` via #422) : `providers/example/{secrets,config}.env.example`, le garde-fou
  `validate_config_env`, et `env_validate.sh` (API → trousseau) respellés aux **noms finaux**.
- **`electricore-secrets`** (dépôt privé séparé, hors PR) : `secrets.env.template` + `config.env` aux noms
  finaux — runbook opérateur.
- **Migration : NULLE.** Rien n'est déployé ; la **1re** adoption SOPS de la box EDN se fait directement sur
  les noms finaux. **À livrer dans la même release que #422.**
- **Docs** : `CLAUDE.md` (ajout `API__TROUSSEAU__`), `docs/configuration.md`, `ADR-0025` (retrait « gelée
  #190 »), `ADR-0037`/`0040` (rotation : éditer `secrets.env`).
- **Tests** : **test de parité de frontière** (les noms lus par `runtime` == noms écrits dans
  l'exemple/template) ; trousseau API testé comme AES (déchiffrement, révocation par label).

## Glossaire (en contexte — pas de `CONTEXT.md`, cf. ADR-0044)

Termes **ops/infra**, pas du vocabulaire métier electricore → définis ici, pas dans un `CONTEXT.md`.

- **Trousseau** — sac **étiqueté** de credentials d'un même type (`<DOMAINE>__TROUSSEAU__<label>__<CHAMP>`),
  à **révocation par label**. AES (clés Enedis), API (clés des consommateurs).
- **Consommateur API** — principal portant une **clé API** et appelant l'API REST (`X-API-Key`). Unifie
  « client user » et « api user » (un seul principal).
- **Autorité de déchiffrement** (clé age) vs **autorité de publication** (write git du dépôt) — les **deux**
  autorités de l'admin, isolées **par type de credential**.
- **Clé escrow** — clé age admin **hors-ligne**, destinataire **permanent** de chaque règle, réservée à la
  **reprise sur sinistre** (jamais utilisée en exploitation).
