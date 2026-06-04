# Déploiement VPS

Guide opérationnel pour déployer ElectriCore sur un VPS via la stack Docker
([`deploy/docker/`](../deploy/docker/)).

## Sommaire

1. [Prérequis](#prérequis)
2. [Installation initiale](#installation-initiale)
3. [Provisionner une nouvelle instance](#provisionner-une-nouvelle-instance)
4. [Accès distant depuis un notebook Python](#accès-distant-depuis-un-notebook-python)
5. [Mode SFTP distant vs fichiers collocés](#mode-sftp-distant-vs-fichiers-collocés)
6. [Rotation des clés AES](#rotation-des-clés-aes)
7. [Sauvegarde et restauration](#sauvegarde-et-restauration)
8. [Mise à jour de version](#mise-à-jour-de-version)
9. [Fenêtre ETL et concurrence DuckDB](#fenêtre-etl-et-concurrence-duckdb)
10. [Dépannage](#dépannage)

---

## Prérequis

- **VPS Linux** (Ubuntu 22.04+ / Debian 12+ recommandé), 2 vCPU et 4 Go RAM minimum.
- **Docker Engine ≥ 24** et **docker compose plugin** (la commande `docker compose`, pas `docker-compose`).
- **Un nom de domaine** pointant (A/AAAA record) vers l'IP publique du VPS.
- **Ports 80 et 443** ouverts dans le pare-feu (pour ACME HTTP-01 et HTTPS).
- **Accès au SFTP Enedis** :
  - Soit identifiants SFTP distants (mode A).
  - Soit le dépôt Enedis collocé sur le VPS, accessible en lecture seule sur le système de fichiers (mode B — recommandé si possible : pas de re-téléchargement, pas de transfert réseau supplémentaire des données sensibles).
- **Les clés AES Enedis** (clé + IV en hex) — toujours requises, y compris en mode collocé.

## Installation initiale

### Layout de déploiement

Sur le VPS, créer le dossier de déploiement et reproduire la structure suivante (pas besoin de cloner le dépôt — on n'utilise que l'image GHCR et 4 fichiers de configuration) :

```
/opt/electricore/
├── .env                          ← secrets (source unique, ../../.env depuis le compose)
└── deploy/docker/
    ├── docker-compose.yml
    ├── Caddyfile
    └── crontab
```

### 1. Préparer le dossier et récupérer les fichiers de config

```bash
mkdir -p /opt/electricore/deploy/docker
cd /opt/electricore/deploy/docker

# Télécharger les fichiers (depuis le tag souhaité, ex: v1.4.0)
BASE=https://raw.githubusercontent.com/Energie-De-Nantes/electricore/v1.4.0/deploy/docker
curl -fsSL -o docker-compose.yml "$BASE/docker-compose.yml"
curl -fsSL -o Caddyfile          "$BASE/Caddyfile.example"
curl -fsSL -o crontab            "$BASE/crontab.example"
curl -fsSL -o /opt/electricore/.env "$BASE/.env.example"
```

(Variante : si tu as cloné le dépôt par confort, tu pars de `<repo>/deploy/docker/`, et le `.env` racine du repo est utilisé tel quel.)

### 2. Remplir le fichier `.env`

Éditer `/opt/electricore/.env` :

- `INSTANCE_SLUG` : identifiant court de l'instance (ex : `edn`, `enargia`). Voir [ADR-0015](adr/0015-deploiement-multi-instance.md). Apparaît dans `/health`, dans le titre `/docs`, et préfixe le nom des backups DuckDB.
- `ELECTRICORE_VERSION` : tag de l'image GHCR à déployer (ex : `1.4.0`).
- `API_KEY` : générer une clé sécurisée (`python -c "import secrets; print(secrets.token_urlsafe(32))"`).
- `SFTP__URL` : URL SFTP distante ou `file:///var/enedis/` selon le mode.
- `AES__CURRENT__KEY` / `AES__CURRENT__IV` : clés Enedis en hexadécimal.
- `TELEGRAM_BOT_TOKEN` et `TELEGRAM_ALLOWED_USERS` si vous activez le bot.
- `ODOO_*` si vous utilisez les endpoints `/taxes/*` et `/facturation/check/odoo`.

Le compose lit ce fichier via `env_file: ../../.env` (deux niveaux au-dessus de `deploy/docker/`).

### 3. Configurer Caddy (TLS)

Le fichier `Caddyfile` téléchargé est l'exemple Let's Encrypt. Remplacer `electricore.exemple.fr` par votre domaine et `votre-email@example.com` par votre email.

### 4. Ajuster le crontab (planning ETL)

Par défaut : ETL à 02:00, sauvegarde à 03:30 Europe/Paris. Modifier si nécessaire.

### 5. Démarrer la stack

```bash
cd /opt/electricore/deploy/docker
docker compose up -d
```

Au premier démarrage, Caddy demande un certificat TLS à Let's Encrypt (compter ~30s).

### 6. Vérifier le bon fonctionnement

```bash
# Santé de l'API (publique, sans authentification)
curl https://votredomaine.fr/health

# Liste des tables (avec authentification)
curl -H "X-API-Key:VOTRE_CLE" https://votredomaine.fr/

# Logs en direct
docker compose logs -f api
docker compose logs -f etl-scheduler
```

Le payload `/health` doit ressembler à :

```json
{
  "status": "ok",
  "api_version": "1.4.0",
  "database": {
    "accessible": true,
    "last_write": "2026-06-01T02:14:33+00:00",
    "tables": {"flux_c15": 1234567, "flux_r151": 9876543, ...}
  },
  "bot": {"running": true},
  "authentication": {"api_keys_configured": true, "method": "X-API-Key header"}
}
```

Avant le premier ETL, `database.accessible` peut être `false` (fichier inexistant) — c'est normal. Lancer un premier import manuellement :

```bash
docker compose exec etl-scheduler curl -X POST \
    -H "X-API-Key:$(grep ^API_KEY= .env | cut -d= -f2)" \
    -H "Content-Type: application/json" \
    -d '{"mode":"test"}' \
    http://api:8001/etl/run
```

## Provisionner une nouvelle instance

ElectriCore est déployé en **multi-instance** : un VPS dédié par fournisseur (EDN, Enargia, …), chacun avec sa propre stack, sa base DuckDB, ses clés AES, sa config Odoo, sa source SFTP, son bot Telegram, et son sous-domaine sur `electricore.fr`. Le rationale et les alternatives écartées sont consignés dans [ADR-0015](adr/0015-deploiement-multi-instance.md).

Cette section décrit la procédure complète pour monter `<slug>.electricore.fr` de zéro. Elle suit la même trame que l'[Installation initiale](#installation-initiale), en insistant sur les points spécifiques au multi-instance.

### 1. Choisir le slug

Le slug est l'identifiant court de l'instance, en minuscules sans accent (ex : `edn`, `enargia`). Il sert à la fois de sous-domaine, de valeur `INSTANCE_SLUG`, et de préfixe de backup. Une fois choisi, il est difficile à changer sans casser les références DNS et les noms de fichiers de sauvegarde — choisir soigneusement.

### 2. Provisionner le VPS

Spécifications minimales : 2 vCPU, 4 Go RAM, 40 Go SSD, Ubuntu 22.04+ ou Debian 12+. Installer Docker Engine ≥ 24 et le plugin Compose. Ouvrir les ports 80 et 443 au pare-feu.

### 3. Créer le A-record DNS

Dans la zone `electricore.fr`, ajouter un enregistrement :

```
<slug>.electricore.fr.   A   <IP-VPS>
```

Vérifier la propagation avant de continuer :

```bash
dig +short <slug>.electricore.fr
```

Pas de wildcard (`*.electricore.fr`) — chaque VPS gère son propre certificat via HTTP-01 (cf. ADR-0015).

### 4. Récupérer les fichiers de configuration

Suivre l'étape [§1 de l'installation initiale](#1-préparer-le-dossier-et-récupérer-les-fichiers-de-config) (téléchargement de `docker-compose.yml`, `Caddyfile`, `crontab`, `.env.example`).

### 5. Configurer le `.env` (variables par catégorie)

Le `.env` est **la source de vérité de l'identité de l'instance**. Toutes les variables ci-dessous sont **spécifiques par instance** — ne jamais partager un `.env` entre deux instances.

**Identité de l'instance**

| Variable | Exemple | Notes |
|---|---|---|
| `INSTANCE_SLUG` | `edn` | Identifiant court, minuscule. Doit matcher le sous-domaine. |
| `ELECTRICORE_VERSION` | `1.6.0` | Tag de l'image GHCR. Pin explicite — chaque instance contrôle sa version. |

**API**

| Variable | Notes |
|---|---|
| `API_KEY` | Générer une clé dédiée (`python -c "import secrets; print(secrets.token_urlsafe(32))"`). Ne pas réutiliser une clé d'une autre instance. |
| `API_KEYS` | Optionnel, clés multiples séparées par virgule. |

**Odoo**

| Variable | Notes |
|---|---|
| `ODOO_ENV` | `prod` (ou `test` pour une instance d'essai). |
| `ODOO_PROD_URL` | URL de l'instance Odoo du fournisseur (ex : `https://edn.odoo.com`). |
| `ODOO_PROD_DB` | Nom de la base Odoo. Vérifier que ce nom apparaîtra dans le log de boot (cf. §8). |
| `ODOO_PROD_USERNAME` | Compte utilisateur Odoo dédié à ElectriCore. |
| `ODOO_PROD_PASSWORD` | Mot de passe ou clé API Odoo. |

**SFTP Enedis**

| Variable | Notes |
|---|---|
| `SFTP__URL` | `sftp://utilisateur:motdepasse@hote:22/chemin` ou `file:///var/enedis/` en mode collocé. Chaque fournisseur a son propre compte SFTP Enedis. |

**Clés AES Enedis**

| Variable | Notes |
|---|---|
| `AES__CURRENT__KEY` | Clé en hexadécimal, fournie par Enedis au fournisseur. **Distincte par fournisseur.** |
| `AES__CURRENT__IV` | IV en hexadécimal. |
| `AES__PREVIOUS__*` | Optionnel pendant ~4 semaines après une rotation (cf. [Rotation des clés AES](#rotation-des-clés-aes)). |

**Bot Telegram**

| Variable | Notes |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Créer un bot dédié via [@BotFather](https://t.me/BotFather) (commande `/newbot`). Convention de nommage : `<slug>_electricore_bot`. |
| `TELEGRAM_ALLOWED_USERS` | IDs Telegram autorisés (allowlist propre à l'équipe du fournisseur). |

### 6. Configurer le Caddyfile

Dans le `Caddyfile`, substituer :

- `electricore.exemple.fr` → `<slug>.electricore.fr`
- `votre-email@example.com` → email valide (notifications Let's Encrypt).

### 7. Ajuster le crontab

Vérifier l'horaire ETL (`0 2 * * *` par défaut, 02:00 Europe/Paris). Si plusieurs instances tournent sur le même VPS Enedis source (rare), décaler les fenêtres pour éviter les pics simultanés.

### 8. Démarrer la stack et vérifier

```bash
cd /opt/electricore/deploy/docker
docker compose up -d
docker compose logs -f api
```

**Checklist de vérification post-déploiement** :

- [ ] Les logs au boot de l'API contiennent une ligne `Instance=<slug>, odoo_db=<db>, sftp=<host>` — vérifier que le slug, la DB Odoo et le SFTP correspondent bien à l'instance attendue (garde-fou contre un `.env` copié à tort depuis une autre instance).
- [ ] `curl https://<slug>.electricore.fr/health` retourne `{"status": "ok", "instance": "<slug>", ...}`.
- [ ] `https://<slug>.electricore.fr/docs` s'ouvre et affiche un titre incluant le slug (ex : `ElectriCore API — EDN`).
- [ ] Le certificat Let's Encrypt est valide (badge cadenas dans le navigateur, pas d'avertissement).
- [ ] Le bot Telegram répond à `/start` pour un user listé dans `TELEGRAM_ALLOWED_USERS`.
- [ ] Premier ETL test :
  ```bash
  docker compose exec etl-scheduler curl -X POST \
      -H "X-API-Key:$(grep ^API_KEY= .env | cut -d= -f2)" \
      -H "Content-Type: application/json" \
      -d '{"mode":"test"}' \
      http://api:8001/etl/run
  ```
  Vérifier dans les logs `etl-scheduler` que les fichiers sont déchiffrés (clés AES correctes) et ingérés.
- [ ] Premier backup nocturne : après la fenêtre 03:30, vérifier `docker compose exec etl-scheduler ls -lh /backups/` — le fichier produit doit être préfixé par le slug (ex : `backup_edn_<date>.duckdb`).

### 9. Documenter et archiver

- Noter les coordonnées de l'instance (slug, IP VPS, contacts du fournisseur, registrar DNS) dans un endroit sûr.
- Sauvegarder le `.env` dans un gestionnaire de secrets (1Password, Bitwarden, Vaultwarden auto-hébergé…).
- Ajouter le sous-domaine à votre monitoring distant (ping `/health` régulier).

---

## Accès distant depuis un notebook Python

Depuis la v1.5, l'API expose les résultats des pipelines opérationnels en flux **Arrow IPC**, consommables sans rapatrier la base DuckDB. Un notebook local peut ainsi piloter le calcul côté serveur tout en restant maître de la chaîne d'écriture vers Odoo (cf. [ADR-0012](adr/0012-api-read-only-odoo.md)).

### Endpoints Arrow IPC

| Endpoint | Sortie | Paramètre |
|---|---|---|
| `GET /facturation/arrow` | `lignes_facture_rapprochees` (rapprochement Odoo ↔ Enedis du mois) | `mois=YYYY-MM-DD` (défaut : dernier mois disponible) |
| `GET /taxes/accise/arrow` | Détail Accise TICFE (table par PDL × mois × trimestre) | `trimestre=YYYY-TX` (sans : tout) |
| `GET /taxes/cta/arrow` | Détail CTA mensuel (PDL × mois, avec `cta_eur`, `taux_cta_pct`, `turpe_fixe_eur`) | idem |

Les endpoints `xlsx` existants restent inchangés (générés depuis la même source).

### Client Python

Le package `electricore` expose une classe `ElectricoreClient` synchrone basée sur `httpx`. Installation côté notebook :

```bash
uv add electricore --extra viz   # core + libs notebooks (marimo, altair, plotly…)
```

Usage :

```python
from electricore.client import ElectricoreClient

client = ElectricoreClient(
    url="https://electricore.votredomaine.fr",
    api_key="votre_cle_api",
)

# Lignes de facture rapprochées du dernier mois
df = client.facturation()                       # mois=None → dernier mois

# Détail Accise du Q1 2025
df_accise = client.accise(trimestre="2025-T1")

# Détail CTA du Q1 2025
df_cta = client.cta(trimestre="2025-T1")
```

Les méthodes retournent un `polars.DataFrame` typé (types préservés via Arrow IPC). Le pipeline tourne **côté serveur** — le notebook orchestre, analyse, et écrit le cas échéant vers Odoo via `OdooWriter`.

### TLS local (cert auto-signé)

Pour tester en local (`https://electricore.localhost` avec Caddy en `tls internal`), passer un client `httpx` qui désactive la vérif TLS :

```python
import httpx
from electricore.client import ElectricoreClient

http = httpx.Client(verify=False, timeout=httpx.Timeout(30.0, read=120.0))
client = ElectricoreClient(url="https://electricore.localhost", api_key="…", http_client=http)
```

## Mode SFTP distant vs fichiers collocés

### Mode A — SFTP distant (par défaut)

Le scheduler ETL se connecte au serveur SFTP Enedis pour télécharger les fichiers chiffrés, puis les déchiffre et les ingère.

`.env` :

```
SFTP__URL=sftp://utilisateur:motdepasse@hote.enedis.fr:22/exports
```

Rien à changer dans `docker-compose.yml`.

### Mode B — Fichiers collocés sur le VPS

Si le serveur SFTP Enedis tourne sur **le même VPS**, on évite un téléchargement inutile et un transfert réseau supplémentaire de données sensibles : l'API lit directement les fichiers chiffrés depuis le système de fichiers lors de chaque déclenchement ETL.

`.env` :

```
SFTP__URL=file:///var/enedis/
```

Dans `docker-compose.yml`, décommenter le bind-mount du service **`api`** (c'est le conteneur qui exécute le sous-processus du pipeline lorsque l'on appelle `/etl/run`) :

```yaml
services:
  api:
    volumes:
      - duckdb_data:/data
      - /var/enedis:/var/enedis:ro     # ← cette ligne
```

> **⚠️ Important** : les fichiers Enedis restent chiffrés en AES sur disque (même en mode collocé). Les clés `AES__*` sont toujours obligatoires.
>
> Le service `etl-scheduler` n'a **pas** besoin de ce bind-mount : il se contente d'appeler `POST /etl/run` via HTTP, c'est `api` qui exécute le pipeline.

Puis :

```bash
docker compose up -d api
```

## Rotation des clés AES

Enedis effectue périodiquement une rotation de ses clés AES. Le format à deux clés (`current` + `previous`) permet de couvrir la période de transition pendant laquelle d'anciens fichiers (avec l'ancienne clé) et de nouveaux fichiers (avec la nouvelle) coexistent sur le SFTP.

### Procédure

1. Recevoir la nouvelle clé Enedis.
2. Dans `.env`, déplacer les valeurs actuelles vers `AES__PREVIOUS__*` et mettre la nouvelle clé dans `AES__CURRENT__*` :

   ```
   AES__CURRENT__KEY=nouvelle_cle_hex
   AES__CURRENT__IV=nouvel_iv_hex
   AES__PREVIOUS__KEY=ancienne_cle_hex
   AES__PREVIOUS__IV=ancien_iv_hex
   ```

3. Recharger le scheduler :

   ```bash
   docker compose restart etl-scheduler
   ```

4. Pendant ~4 semaines (jusqu'à ce que plus aucun ancien fichier ne traîne sur le SFTP), garder les deux clés. Les logs `[current]` / `[previous]` indiquent quelle clé a déchiffré quel fichier.

5. Après la transition, supprimer `AES__PREVIOUS__*` de `.env` et `docker compose restart etl-scheduler`.

## Sauvegarde et restauration

### Sauvegarde automatique

Le scheduler crée un snapshot complet de la base chaque nuit à 03:30 (Europe/Paris) — voir [`deploy/docker/crontab.example`](../deploy/docker/crontab.example) et [`deploy/docker/backup_duckdb.sh`](../deploy/docker/backup_duckdb.sh).

- Format : `EXPORT DATABASE` (SQL + parquet), compressé en `tar.gz`.
- Emplacement : volume Docker `duckdb_backups`, monté dans `/backups` du conteneur scheduler.
- Rétention : 14 snapshots les plus récents (configurable via `RETAIN_DAYS`).

Liste des sauvegardes :

```bash
docker compose exec etl-scheduler ls -lh /backups/
```

### Copie offsite (recommandée)

Le snapshot reste sur le VPS — ajouter une étape pour le pousser ailleurs. Exemple avec [rclone](https://rclone.org/) (à configurer séparément) dans `crontab` :

```
45 3 * * * rclone copy /backups remote:electricore-backups --max-age 24h
```

### Restauration

```bash
# 1. Copier un snapshot hors du volume
docker compose cp etl-scheduler:/backups/snapshot_20260601T013000Z.tar.gz ./

# 2. Décompresser
tar -xzf snapshot_20260601T013000Z.tar.gz

# 3. Restaurer dans une nouvelle base
duckdb restored.duckdb "IMPORT DATABASE 'snapshot_20260601T013000Z/'"

# 4. Stopper la stack et remplacer la base courante
docker compose down
docker run --rm -v electricore_duckdb_data:/data -v $PWD:/host alpine \
    cp /host/restored.duckdb /data/flux_enedis_pipeline.duckdb
docker compose up -d
```

## Mise à jour de version

### Mise à jour standard (mineure / correctifs)

```bash
cd /opt/electricore/deploy/docker
git pull                # récupérer les changements de la stack (compose.yml, etc.)
docker compose pull     # tirer la nouvelle image GHCR
docker compose up -d    # recréer les conteneurs avec la nouvelle image
docker compose logs -f api
```

### Épingler une version précise

Dans `.env` :

```
ELECTRICORE_VERSION=1.4.0
```

Puis `docker compose up -d`. Utile pour les déploiements bloqués sur une version testée.

### Rollback

```bash
# Repasser à la version précédente
sed -i 's/ELECTRICORE_VERSION=.*/ELECTRICORE_VERSION=1.3.5/' .env
docker compose up -d
```

> ⚠️ Une rétrogradation **majeure** peut nécessiter une restauration de la base si le schéma a évolué — vérifier le CHANGELOG.

## Fenêtre ETL et concurrence DuckDB

DuckDB autorise plusieurs lecteurs en parallèle, mais le writer (ici le scheduler ETL) prend un verrou exclusif sur le fichier pendant l'écriture. Concrètement :

- L'API reste accessible pendant la lecture (`SELECT`) tant que l'ETL n'écrit pas.
- Si une requête API arrive **pendant** un `pipeline.run()`, l'API tente jusqu'à 3 fois (1s d'écart) d'ouvrir une connexion read-only avant de renvoyer une erreur. La plupart des écritures DLT sont courtes (checkpoints), donc le retry suffit.
- Si une requête tombe pile sur un long checkpoint, le client recevra un `500` — relancer après 30 s. C'est pour cela qu'on planifie l'ETL à 02:00, en dehors des heures de pointe.

Pour ajuster la fenêtre ETL, modifier l'horaire dans `deploy/docker/crontab` (`0 2 * * *` = 2h00 dans le fuseau du conteneur, qui est `Europe/Paris`).

## Dépannage

### `/health` retourne `database.accessible: false`

- Si `error` mentionne `Fichier DuckDB introuvable` : aucun ETL n'a encore été lancé. Lancer manuellement (voir [installation §6](#6-vérifier-le-bon-fonctionnement)).
- Si `error` mentionne `Could not set lock` : l'ETL est en cours d'écriture. Réessayer dans quelques secondes.
- Sinon : examiner les logs `docker compose logs api`.

### `etl-scheduler` redémarre en boucle

```bash
docker compose logs etl-scheduler
```

Causes typiques :

- `API_KEY` absente ou ne correspond pas à `API_KEYS`.
- `api` n'est pas encore *healthy* (le scheduler attend `condition: service_healthy`).
- Erreur de syntaxe dans `crontab` (chaque ligne doit être une expression cron valide).

### Certificat Caddy bloqué

```bash
docker compose logs caddy | grep -i acme
```

- Vérifier que les ports 80 et 443 sont bien exposés et atteignables depuis l'extérieur (pas de pare-feu cloud bloquant).
- Vérifier que le domaine résout vers l'IP du VPS (`dig +short votredomaine.fr`).
- Pendant les tests, activer le `acme_ca` staging dans `Caddyfile` pour éviter les rate-limits Let's Encrypt.

### L'ETL échoue avec `Échec déchiffrement avec X clé(s)`

Mauvaise clé AES ou fichier corrompu. Comparer les valeurs `AES__*` dans `.env` avec celles fournies par Enedis. En période de rotation, s'assurer que `AES__PREVIOUS__*` est toujours configurée.

### Le bot Telegram ne répond pas

- Vérifier que `bot.running: true` dans `/health`.
- Vérifier que votre ID Telegram est listé dans `TELEGRAM_ALLOWED_USERS`.
- Examiner les logs : `docker compose logs api | grep -i telegram`.
