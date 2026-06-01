# Déploiement VPS

Guide opérationnel pour déployer ElectriCore sur un VPS via la stack Docker
([`deploy/docker/`](../deploy/docker/)).

## Sommaire

1. [Prérequis](#prérequis)
2. [Installation initiale](#installation-initiale)
3. [Mode SFTP distant vs fichiers collocés](#mode-sftp-distant-vs-fichiers-collocés)
4. [Rotation des clés AES](#rotation-des-clés-aes)
5. [Sauvegarde et restauration](#sauvegarde-et-restauration)
6. [Mise à jour de version](#mise-à-jour-de-version)
7. [Fenêtre ETL et concurrence DuckDB](#fenêtre-etl-et-concurrence-duckdb)
8. [Dépannage](#dépannage)

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

### 1. Cloner le dépôt

```bash
cd /opt
git clone https://github.com/Energie-De-Nantes/electricore.git
cd electricore/deploy/docker
```

### 2. Créer le fichier `.env`

```bash
cp .env.example .env
```

Éditer `.env` (voir [`.env.example`](../deploy/docker/.env.example) pour la liste complète des variables) :

- `API_KEY` : générer une clé sécurisée (`python -c "import secrets; print(secrets.token_urlsafe(32))"`).
- `SFTP__URL` : URL SFTP distante ou `file:///var/enedis/` selon le mode.
- `AES__CURRENT__KEY` / `AES__CURRENT__IV` : clés Enedis en hexadécimal.
- `TELEGRAM_BOT_TOKEN` et `TELEGRAM_ALLOWED_USERS` si vous activez le bot.
- `ODOO_*` si vous utilisez les endpoints `/taxes/*` et `/facturation/check/odoo`.

### 3. Configurer Caddy (TLS)

```bash
cp Caddyfile.example Caddyfile
```

Remplacer `electricore.exemple.fr` par votre domaine et `votre-email@example.com` par votre email pour les notifications Let's Encrypt.

### 4. Préparer le crontab (planning ETL)

```bash
cp crontab.example crontab
```

Ajuster les horaires si nécessaire (par défaut : ETL à 02:00, sauvegarde à 03:30 Europe/Paris).

### 5. Démarrer la stack

```bash
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
