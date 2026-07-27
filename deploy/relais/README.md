# Relais de flux Enedis déchiffrés → SFTP partenaire (#637, #657)

Outil autonome, pose-et-oublie, qui n'importe aucune surface mouvante de
l'ingestion (runner, curseur, état, DuckDB — voir
`electricore/ingestion/relais/pipeline.py`). Depuis #657, c'est un **composant
de l'installeur unique** (`deploy/install.sh --relais`) : socle commun partagé
avec la stack (OS, Docker, UFW, user de service, durcissement, identité de
box + secrets SOPS mutualisés) + un mini-compose dédié, versionné et
déployé **indépendamment** de la stack.

## Installation (`install.sh --relais`)

Sur une machine avec ou sans la stack ElectriCore déjà installée :

```bash
sudo bash install.sh --slug <slug> --relais --deploy-repo <url-dépôt-déploiement>
```

Contrairement au chemin stack (sans `--relais`) : **pas de `--domain`**, pas de
contrôle DNS, pas de Caddy — le relais n'expose rien. Le `--deploy-repo` reste
obligatoire : le composant relais a lui aussi besoin de l'identité de la box
(clé age + deploy key SSH, ADR-0044) et du trousseau AES, **mutualisé** avec la
stack (une rotation de clé Enedis atteint les deux d'un coup ; une clé périmée
casse l'ingestion bruyamment plutôt que de laisser le relais tourner à vide).

Ce que pose l'installeur, dans l'ordre :

1. Socle commun (idempotent, partagé avec la stack) : paquets, Docker, UFW,
   user système `<slug>`, durcissement VPS (préflight sshd #656 compris).
2. Identité de la box + pull du dépôt de déploiement privé (`providers/<slug>/`)
   — mêmes étapes que la stack, mêmes secrets.
3. **Clé SSH partenaire** (voir section dédiée ci-dessous) — l'installeur
   **refuse** si elle est absente ou trop permissive ; il ne la génère ni ne la
   copie jamais.
4. Mini-compose `compose-relais.yml` + unités systemd
   (`electricore-relais.service` + `.timer`), timer activé
   (`systemctl enable --now electricore-relais.timer`).

Le premier run réel se produit au premier déclenchement du timer
(`OnBootSec=5min`), **jamais pendant l'installation** — contrairement à la
stack, il n'y a pas de test d'ingestion synchrone ici (l'amorçage, voir plus
bas, est un geste distinct et conscient).

**Config non-secrète du relais** (`providers/<slug>/config.env`, versionnée,
clair) :

```bash
RELAIS_VERSION=1.2.0                                    # tag GHCR, DÉCOUPLÉ d'ELECTRICORE_VERSION
RELAIS__SOURCE_URL=file:///srv/<slug>/flux-deposit       # ou sftp://... (auth par clé, jamais mot de passe en dur)
RELAIS__PARTNER_URL=sftp://relais@partenaire.example/in
RELAIS__FLUX=C15,R151,R15,F12,F15                        # phase 1 : liste explicite, vide = tous
```

**Secrets** (`providers/<slug>/secrets.env`, chiffré SOPS) : le trousseau
`AES__TROUSSEAU__*` est **mutualisé** avec la stack (même fichier, monté ro
dans les deux conteneurs) — rien à dupliquer.

### Répertoire de dépôt des flux (mode `file://`)

Si `RELAIS__SOURCE_URL` pointe un dossier local plutôt qu'un SFTP distant, ce
dossier doit être **lisible par le conteneur** (uid 1000, cf. Dockerfile) — la
ligne de montage correspondante est présente, commentée, dans
`compose-relais.yml` (à décommenter et adapter). Vérifier les droits du
dossier hôte avant le premier run.

## Clé SSH partenaire

Le conteneur relais monte en lecture seule une clé SSH **PRIVÉE**, dédiée à ce
seul usage (jamais de réutilisation d'une clé d'ingestion existante), au nom
par défaut attendu par paramiko (`id_ed25519`) dans le `HOME` du user
conteneur (`electricore`, uid 1000, home `/app` — cf.
`deploy/docker/Dockerfile`) : le code du relais ne passe aucun `key_filename`,
fsspec/paramiko ne cherche que les noms standards.

Chemin de convention, **hors dépôt de déploiement** (pas un secret versionné
SOPS — comme `age.key` / `ssh_deploy_key`, elle ne quitte jamais la box) :

```
/srv/<slug>/relais_ssh_key
```

L'installeur **vérifie** présence + droits (600) et **refuse** avec un message
de remédiation sinon — il **ne copie ni ne génère jamais** de clé privée :
seule la clé **publique**, installée chez le partenaire (Haulogy), fait foi.

```bash
scp ma_cle_relais_ed25519 root@<box>:/srv/<slug>/relais_ssh_key
ssh root@<box> chmod 600 /srv/<slug>/relais_ssh_key
```

La vérification d'empreinte du host key du partenaire reste en TOFU (politique
fsspec) — l'empreinte est confirmée hors-bande.

## Reconfigure (bump `RELAIS_VERSION`)

Éditer `RELAIS_VERSION` dans `providers/<slug>/config.env` (dépôt de
déploiement), commit, push, puis relancer la **même** commande :

```bash
sudo bash install.sh --slug <slug> --relais --deploy-repo <url>
```

Seul le composant relais est reconfiguré : la stack (si présente sur la même
box) et le journal DuckDB du relais (volume nommé `relais_data`, jamais
recréé par un changement de tag) ne sont pas touchés. Réciproquement, un bump
d'`ELECTRICORE_VERSION` (stack) ne change rien au relais.

## Vérifier

```bash
systemctl status electricore-relais.timer
journalctl -u electricore-relais.service -f
```

## Run manuel (hors timer)

```bash
sudo -u <slug> docker compose --env-file /srv/<slug>/config.env \
    -f /srv/<slug>/deploy/relais/compose-relais.yml run --rm relais
```

## Amorçage (#643)

Marque l'historique existant comme livré **sans** le pousser au partenaire —
acte **UNIQUE**, à faire une fois avant le premier run réel (sinon le premier
passage du timer pousserait tout l'historique). Refuse s'il y a déjà des
livraisons dans le journal (`--force` sinon). **Jamais exécuté par
l'installeur** : le récapitulatif final de `install.sh --relais` imprime la
commande prête à copier-coller.

```bash
sudo -u <slug> docker compose --env-file /srv/<slug>/config.env \
    -f /srv/<slug>/deploy/relais/compose-relais.yml run --rm relais \
    python -m electricore.ingestion.relais seed --avant 2026-06-01
```

## Complétude

Requête ad hoc (zips reçus jamais relayés), depuis le conteneur :

```bash
sudo -u <slug> docker compose --env-file /srv/<slug>/config.env \
    -f /srv/<slug>/deploy/relais/compose-relais.yml run --rm relais \
    python -c "
from electricore.ingestion.relais.pipeline import zips_non_relayes
print(zips_non_relayes('sftp://user:pass@source.example/flux', '/data-relais/relais.duckdb'))
"
```

## Vue d'audit (#646)

`journal.relais_audit_sequences` (matérialisée en fin de chaque passage du
timer) audite la nomenclature des zips VUS par le relais (trous de séquence,
queue invérifiable, noms non reconnus) — même macro que l'audit côté
ingestion (#645), zéro règle dupliquée. Vue **passive** : aucune alerte,
consultation à la demande (rapprochement partenaire) :

```bash
sudo -u <slug> docker compose --env-file /srv/<slug>/config.env \
    -f /srv/<slug>/deploy/relais/compose-relais.yml run --rm relais \
    python -c "
import duckdb
con = duckdb.connect('/data-relais/relais.duckdb')
print(con.execute('select * from journal.relais_audit_sequences').fetchall())
"
```

## Alerte mail (`OnFailure=`, #659)

L'alerte mail sur échec du relais (`electricore-relais-alerte.service` +
`.sh`, msmtp, voir l'historique #659) a été conçue pour le chemin **bare-metal**
(elle lit `/etc/electricore-relais/relais.env`, qui n'existe pas dans le
chemin conteneurisé — la config vit désormais dans
`providers/<slug>/config.env`). Le composant relais conteneurisé (#657)
**n'active pas** cette alerte automatiquement : c'est un suivi naturel, non
couvert par cette réécriture. L'escalade de base reste garantie sans elle —
un run « aveugle » (0 push réussi, ≥ 1 échec) fait passer
`electricore-relais.service` en `failed` (visible par tout monitoring
`systemctl --failed` / `journalctl`), que l'alerte mail soit câblée ou non.

## Générale : l'état ne survit pas (PRD #658)

> **L'état d'une générale ne survit jamais à la générale.**

Pendant une répétition (générale, PRD #658) sur le VPS partenaire, isoler le
journal DuckDB (volume `relais_data` de `compose-relais.yml`) dans un volume
**jetable** distinct, détruit en fin de générale. Sans cette destruction, tout
zip marqué « livré » au faux partenaire de la répétition le resterait **à
vie** dans un état qui n'aurait jamais dû exister — et le vrai partenaire ne
le recevrait alors jamais.

## Conteneurisé maintenant : le renversement de l'argumentaire

L'ancienne notice de ce fichier expliquait « pourquoi systemd et pas docker
compose » : `crontab.example` (voir [../docker/](../docker/)) appelle l'API du
stack principal, le relais n'a ni l'un ni l'autre comme dépendance (design
#637), et un timer systemd autonome garde cette indépendance visible au niveau
déploiement — l'argument concluait alors à l'**absence de conteneur**.

Depuis #657, l'argumentaire est **renversé** : l'indépendance visée n'a
jamais été l'absence de conteneur, mais celle du **versionnage et du cycle de
vie** — `RELAIS_VERSION` découplé d'`ELECTRICORE_VERSION`, reconfigure du
relais sans toucher la stack, et réciproquement. Le conteneur (même image
ghcr, même entrypoint SOPS que la stack) apporte cette indépendance-là
**en plus** de la simplicité d'exploitation (un seul installeur, un seul
mécanisme de secrets, un seul socle durci) — sans rien perdre : le timer
systemd reste le déclencheur (`OnBootSec=5min`, `OnUnitActiveSec=15min`,
balayage réconciliant), il invoque simplement `docker compose … run --rm`
plutôt que `python -m …` directement.
