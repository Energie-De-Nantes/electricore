# Secrets-as-code : chiffrement SOPS + age, déchiffrement en image, identité par instance

## Statut

Accepté (grill 25/06/2026, `/grill-with-docs`). S'appuie sur [ADR-0011](0011-deploiement-vps-docker.md)
(stack docker compose), [ADR-0017](0017-layout-deploiement-srv-slug.md) (layout `/srv/<slug>/`),
[ADR-0031](0031-durcissement-ssh-vps-utilisateur-ops.md) (durcissement), [ADR-0015](0015-deploiement-multi-instance.md)
(multi-instance), [ADR-0024](0024-trois-registres-de-savoir.md)/[ADR-0025](0025-registre-runtime-pydantic-settings.md)
(registre runtime). **Révise la procédure de rotation** d'[ADR-0037](0037-trousseau-cles-aes-n-cles-selection-par-essai.md)
et [ADR-0040](0040-schema-dechiffrement-aes-iv-prefixe.md) (« éditer le `.env` » → éditer le `secrets.env`
chiffré). Porté par la chaîne d'issues *secrets-as-code* (à créer via `/to-issues`).

> **Vue d'ensemble visuelle** — diagramme [`secrets-as-code-sops`](../secrets-as-code-sops.png)
> ([source Excalidraw](../secrets-as-code-sops.excalidraw)) : où vit quoi (admin / dépôt privé /
> box VPS), le cycle de vie d'un secret, les sécurités en place, et l'ouverture sur la suite.

## Contexte

Aujourd'hui les secrets vivent en clair dans `/srv/<slug>/.env` ([ADR-0017](0017-layout-deploiement-srv-slug.md)) :
source unique, montée dans les conteneurs (`env_file:`) et lue par `docker compose --env-file` pour ses
substitutions `${...}`. Ce fichier est **édité à la main** à l'install, **jamais versionné**, et n'existe
**que** sur le VPS — perdre la box, c'est perdre les secrets ; aucun historique, aucune revue, aucune
récupération. La cible est un déploiement **multi-instance** (un VPS par fournisseur à terme,
[ADR-0015](0015-deploiement-multi-instance.md)).

**Modèle de menace, cadré honnêtement.** Sur une box unique, la clé de déchiffrement vit sur la **même
box** que la donnée : SOPS **seul** ne protège donc **pas** du vol à froid (snapshot hébergeur, backup qui
fuite) — la clé est dans le snapshot aussi. Le gain **immédiat** de SOPS n'est pas la confidentialité au
repos, c'est :

1. **secrets-as-code** — secrets versionnés, revus en diff, récupérables (le `.env` à la main disparaît) ;
2. **isolation cryptographique multi-cible** — un dépôt, N fournisseurs, une box ne déchiffre que les siens ;
3. **substrat** — la couche sur laquelle la vraie protection au repos (LUKS + Tang/NBDE, *hors scope*) se
   branchera plus tard : la clé age deviendra ce que Tang garde.

La portée de cet ADR est **« remplacer le `.env` »**. Le chiffrement au repos du disque / des backups
(LUKS, Tang, `age` sur les dumps), l'isolation `raw.db`/`serve.db` et le durcissement DuckDB de l'API sont
des concerns **distincts**, suivis séparément (cf. *Alternatives écartées*).

## Décision

### 1. SOPS + age, deux fichiers (secret / config séparés)

Le `.env` se scinde en deux, parce que `docker compose` résout ses substitutions `${...}` (tags d'image,
chemins de volume) **côté hôte, avant tout conteneur** — il ne peut pas lire du ciphertext :

- **`config.env`** (clair, versionné) : vars de substitution compose (`ELECTRICORE_VERSION`,
  `BACKUPS_PATH`) + config non-secrète (`INSTANCE_SLUG`, `ODOO_ENV`, métadonnées API).
- **`secrets.env`** (chiffré SOPS) : **uniquement** des credentials — `SFTP__URL` (mot de passe embarqué),
  trousseau `AES__TROUSSEAU__*`, `API_KEY` / `API_KEYS`, `TELEGRAM_BOT_TOKEN`, bloc de connexion `ODOO_*`.
  SOPS chiffre les **valeurs** et laisse les **noms de champs** en clair → diffs Git lisibles.

> **`ELECTRICORE_VERSION` est un *paramètre de déploiement*, pas un secret ni un état GitOps strict**
> ([#460](https://github.com/Energie-De-Nantes/electricore/issues/460)). La valeur pinée dans le `config.env`
> du dépôt est une **baseline optionnelle** : `install.sh --version <tag>` l'**override localement** sur la box
> (réécrit `ELECTRICORE_VERSION` dans le `config.env` tiré, après le pull, sans toucher au dépôt). Motivation :
> en dev intense, bumper l'image ne doit pas exiger une PR sur le dépôt secrets pour quelque chose qui n'est
> pas un secret. Sans `--version`, la box redéploie la version pinée du dépôt (comportement GitOps par défaut).
> Le tag effectivement lancé est surfacé dans le récap d'install (`Image: ghcr.io/.../electricore:<tag>`).

### 2. Déchiffrement en image, à l'entrypoint (`sops exec-env`)

L'image electricore embarque `sops` + `age` (binaires statiques, ~15 Mo). Un entrypoint déchiffre
`secrets.env` **dans l'environnement du process** (`sops exec-env`) puis `exec` la commande
(uvicorn / supercronic) — **jamais de fichier en clair**. Décisif : cette voie gère les **noms de
variables dynamiques** du trousseau AES (`AES__TROUSSEAU__<label>__KEY`, `<label>` choisi par l'opérateur
à chaque rotation, [ADR-0037](0037-trousseau-cles-aes-n-cles-selection-par-essai.md)) **sans
énumération**. Toute approche qui énumère les secrets un par un (passthrough compose `environment:`) est
**éliminée** par là : elle ne sait pas exprimer un nom de var inconnu d'avance.

### 3. Fail-fast, pas de double chemin

Un conteneur sans clé age **ni** fichier chiffré ⇒ l'entrypoint **échoue bruyamment** (« secrets SOPS
requis, aucune clé age »), il ne **retombe pas** silencieusement sur un run sans secrets (qui serait une
API sans `API_KEY`, une ingestion sans SFTP/AES — une mauvaise config masquée). Le mécanisme de déchiffrement
reste celui du §2 ; seul son **repli** est un échec dur, pas un passthrough. Le dev **non-conteneur**
(`uv run`) lit `.env` directement via pydantic-settings et **n'est pas concerné**. Échappatoire conteneur
dev/test **explicite et documentée** (`ELECTRICORE_DECRYPT=off`, ou clé de test montée).
`runtime.valider()` reste en **défense en profondeur**, derrière l'entrypoint.

### 4. L'identité vit par instance, générée sur la box

Chaque box génère **deux** paires à l'install : une paire **age** (déchiffrement) et une paire **SSH**
(lecture du dépôt de déploiement, cf. §5). Les deux clés **privées naissent sur la box et ne la quittent
jamais** (`/srv/<slug>/`, `600`, montées RO dans les conteneurs). L'opérateur enregistre les deux clés
**publiques** : la age pub comme **destinataire `.sops.yaml`**, la SSH pub comme **deploy key en lecture
seule** du dépôt privé. Mort de la box ⇒ on **forge une identité neuve** + `sops updatekeys` — **pas de
récupération de clé** (forger est déjà bon marché). Coût **assumé** : onboarding **en deux temps** (la age
pub doit être destinataire avant que la box puisse déchiffrer), donc l'install « stack qui tourne à la fin »
d'aujourd'hui se scinde.

### 5. Le ciphertext arrive par auto-pull (GitOps)

La box **clone/pull** le dépôt de déploiement **privé** via sa deploy key RO et y lit
`providers/<slug>/{config.env,secrets.env}`. La deploy key est de **faible valeur** : elle ne donne accès
qu'à du **ciphertext**, inutile sans la clé age — toute la sécurité repose sur la clé age, jamais sur le
transport. Cadence : **on-demand** (étape `reconfigure` / commande dédiée) en v1 ; le cron auto-pull est
**différé** (ne pas pull une livraison de secrets cassée sans surveillance).

### 6. Frontière de dépôt : mécanisme public, secrets privés

electricore (public, AGPL) ne porte **que le mécanisme + le scaffolding** : entrypoint, Dockerfile
(`sops`/`age`), câblage compose, génération des clés dans `install.sh` (**via l'image**, zéro dép hôte),
`add-provider.sh`, et un **exemple clairement marqué** au format multi-cible `providers/<slug>/`
(`.sops.yaml.example`, `secrets.env.example`, `config.env.example`, destinataires **factices**). Les
**vrais** secrets, le **vrai** `.sops.yaml` et les clés publiques vivent dans un **dépôt de déploiement
privé séparé** (jamais dans electricore). Destinataires `.sops.yaml` = **admin (toujours, dans chaque
règle)** + **chaque box** ; **la CI n'est pas destinataire** (elle build des images publiques sans secret,
garanties par le scan d'image — elle ne déchiffre jamais). L'isolation entre instances est
**cryptographique** (la clé d'une box n'est pas destinataire d'une autre), pas une convention de dossiers.

### 7. Forme multi-cible adoptée maintenant (là où c'est gratuit)

Le scaffolding est structuré `providers/<slug>/` **dès une seule instance** : les primitives d'isolation
(destinataires par cible dans `.sops.yaml`) sont gratuites à adopter et évitent un **refactor artificiel**
plus tard. Seule la **machinerie de flotte coûteuse** (un dépôt privé par fournisseur, CI de flotte) est
différée jusqu'à ce que l'isolation devienne une exigence contractuelle.

### 8. Bascule forcée à la prochaine release

Pas de double chemin **opérateur** maintenu : la release qui introduit SOPS fait migrer l'instance vivante
(EDN) dans une **fenêtre planifiée**. Migration, par box : `age-keygen` + deploy key sur la box ; scinder
le `.env` vivant en `config.env` + `secrets.env` ; chiffrer `secrets.env` **sur la machine admin** (l'unique
manipulation de secrets en clair, **jamais committée**) ; pousser ; enregistrer les deux pubs ; `reconfigure`
→ la box pull + déchiffre ; **vérifier** ; **puis** supprimer le `.env` en clair.

### 9. Tests : clé de test committée + fixture ciphertext

Une paire age de **test committée** (`tests/fixtures/`) + un `secrets.env` fixture **chiffré à valeurs
factices** → test **déterministe** du déchiffrement bout-en-bout contre un **vrai** `sops`/`age` ; fail-fast
testé en **omettant** la clé. Conséquence à absorber : la clé privée de test **fait sonner gitleaks**
(détection de secrets, Palier 0) → une **entrée d'allowlist** explicite (elle ne protège rien de réel). Les
tests shell de `deploy/` suivent le **motif fake-binary** déjà en place (`deploy/tests/fixtures/fake_lib`).

## Alternatives écartées

- **OpenBao / Vault** (coffre central, secrets dynamiques, OIDC) — **différé**, pas rejeté : surdimensionné
  pour ~6 secrets et une instance (un coffre HA pour si peu est un anti-pattern asso). SOPS+age est le 90/10 ;
  la trajectoire connue est *SOPS-age → OpenBao transit + identité OIDC* quand la flotte le justifie.
- **Déchiffrement côté hôte** (tmpfs `env_file`, ou `sops exec-env` enveloppant `docker compose`) — écarté :
  le passthrough compose ne sait pas exprimer les **noms dynamiques** du trousseau AES (§2) ; la variante
  tmpfs laisse du clair en RAM et déporte la logique dans le shell `deploy/`. L'image porte déjà l'identité
  runtime ; l'entrypoint en image est self-contained et portable.
- **Admin forge la clé de la box / la conserve (chiffrée-admin) dans le dépôt** — écarté : la clé privée
  transiterait (machine admin → box) ou vivrait à un endroit de plus, alors que forger une identité neuve à
  la mort de la box est **déjà** bon marché (`add-provider` + `updatekeys`) — la récupération de clé n'a pas
  de valeur. (Choix opérateur au grill : la box est seule maître de sa clé privée.)
- **Migration opt-in / double chemin permanent** — écarté : un passthrough silencieux masque une box prod
  qui tournerait sans secrets, et c'est exactement le double chemin qu'on ne veut pas maintenir (§3, §8).
- **Chiffrement natif DuckDB, `age` sur les backups, isolation `raw.db`/`serve.db`, durcissement DuckDB de
  l'API, LUKS/Tang** — **hors scope** de « remplacer le `.env` » : concerns distincts (vol à froid, surface
  API), suivis comme follow-ups indépendants.

## Conséquences

- **Image electricore** : +`sops`+`age` (~15 Mo statiques) + `entrypoint.sh` (déchiffre-ou-échoue). Le dev
  non-conteneur reste intact.
- **`docker-compose.yml`** : `api` + `ingestion-scheduler` montent la clé age (RO) + le `secrets.env` chiffré
  + `SOPS_AGE_KEY_FILE` ; `env_file:` passe de `.env` à `config.env` ; `caddy` inchangé (aucun secret).
  Le **bot** tourne dans le process API ([ADR-0025](0025-registre-runtime-pydantic-settings.md)) → son token
  est dans `secrets.env`, déchiffré par l'entrypoint de l'API.
- **`deploy/install.sh` + `deploy/lib/`** : génération des deux paires (via l'image), onboarding en deux
  temps, deploy key + clone du dépôt privé, `env_validate` adapté au split. `substitute_env` écrit
  désormais `config.env`.
- **`runtime.py` / app Python : inchangés** — l'env-système peuplé par l'entrypoint gagne déjà sur `.env`
  (précédence native pydantic-settings) ; le `config.env` arrive via `env_file:` ; l'absence du `.env`
  racine dans le conteneur est un no-op.
- **Procédure de rotation** ([ADR-0037](0037-trousseau-cles-aes-n-cles-selection-par-essai.md) §migration,
  [ADR-0040](0040-schema-dechiffrement-aes-iv-prefixe.md) §migration, `CLAUDE.md`, `docs/configuration.md`) :
  « éditer le `.env` » → `sops providers/<slug>/secrets.env` (édition chiffrée in-place) → commit → pull →
  restart ; retrait d'une box / rotation admin → `sops updatekeys`. **Distinction cruciale** : `updatekeys`
  ne rote que l'**enveloppe** ; si un secret a pu fuiter, le changer **aussi à la source**.
- **Nouveau dépôt privé de déploiement** à créer et peupler (runbook opérateur, **hors PR**).

## Glossaire

Pas de `CONTEXT.md` deploy créé : SOPS / age / destinataire sont des outils **généraux**, pas du vocabulaire
métier electricore (les `CONTEXT.md` existants sont des glossaires métier). Les rares termes propres au
projet (`config.env` vs `secrets.env`, dépôt de déploiement, identité par instance) sont définis **en
contexte** dans cet ADR. À réévaluer si un glossaire ops émerge.
