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
5. Hook d'alerte mail OnFailure= (`electricore-relais-alerte.service` +
   `/usr/local/bin/electricore-relais-alerte.sh`) — voir section dédiée
   ci-dessous. Posé et **référencé** par `OnFailure=` sur
   `electricore-relais.service`, mais **pas activé** lui-même (il ne se
   déclenche que sur échec, jamais via un timer). Le paquet `msmtp` est
   installé par le composant ; seul son `msmtprc` (secret) reste à poser à la
   main.

Le premier run réel se produit au premier déclenchement du timer
(`OnBootSec=5min`), **jamais pendant l'installation** — contrairement à la
stack, il n'y a pas de test d'ingestion synchrone ici (l'amorçage, voir plus
bas, est un geste distinct et conscient).

**Config non-secrète du relais** (`providers/<slug>/config.env`, versionnée,
clair) :

```bash
RELAIS_VERSION=1.2.0                                    # tag GHCR, DÉCOUPLÉ d'ELECTRICORE_VERSION
RELAIS__SOURCE_URL=file:///flux/enedis                   # ou sftp://... (auth par clé, jamais mot de passe en dur)
FLUX_DEPOSIT_DIR=/flux/enedis                            # dossier réel du dépôt en mode file:// (monté ro tel quel)
RELAIS__PARTNER_URL=sftp://relais@partenaire.example/in
RELAIS__FLUX=C15,R151,R15,F12,F15                        # phase 1 : liste explicite, vide = tous
# CSV, alerte OnFailure= (#659/#668, voir plus bas). Commentaire sur sa PROPRE ligne,
# contrairement aux clés ci-dessus : cette valeur est lue par systemd (EnvironmentFile=),
# qui ne coupe PAS les commentaires de fin de ligne — il les avalerait dans les adresses.
RELAIS_ALERTE_MAILS=alertes-ops@example.fr
# Alerte mail : params SMTP NON-secrets (routage, #674) — le token vit dans
# secrets.env (ALERTE__SMTP__PASSWORD), voir section « msmtprc » plus bas.
ALERTE__SMTP__HOST=smtp.example.fr
ALERTE__SMTP__PORT=587
ALERTE__SMTP__FROM=alertes@example.fr
ALERTE__SMTP__USER=alertes@example.fr
```

**Secrets** (`providers/<slug>/secrets.env`, chiffré SOPS) : le trousseau
`AES__TROUSSEAU__*` est **mutualisé** avec la stack (même fichier, monté ro
dans les deux conteneurs) — rien à dupliquer. `ALERTE__SMTP__PASSWORD` (token
SMTP de l'alerte, #674) est **propre au relais**, voir section « msmtprc »
plus bas.

### Répertoire de dépôt des flux (mode `file://`)

Si `RELAIS__SOURCE_URL` pointe un dossier local plutôt qu'un SFTP distant, ce
dossier est monté ro **au même chemin** dans le conteneur — l'URL vaut telle
quelle des deux côtés, aucun montage à décommenter. `FLUX_DEPOSIT_DIR`
(config.env) désigne le dossier réel — Enargia : `/flux/enedis`, dont
l'arborescence par flux (`C15/`, `R151/`, …) convient telle quelle : le relais
liste en **récursif** (`**/*.zip`) et filtre sur le **nom** des zips, pas sur
les dossiers. Une garde à l'install refuse un `file://` sans `FLUX_DEPOSIT_DIR`
cohérent (sinon le conteneur serait aveugle à sa source). L'installeur **crée**
le dossier s'il manque (lisible par le conteneur, uid 1000) ; un dossier
**existant** (le dépôt de production où Enedis dépose) n'est **jamais
modifié** — c'est à l'opérateur d'ouvrir la lecture à l'uid 1000 (groupe/ACL
sur toute l'arborescence), l'installeur avertit si elle manque. Sur une box à
source SFTP distante, le montage est inerte (dossier vide).

Côté **destination** (partenaire), le relais reproduit désormais ce même
rangement par flux (#686, demande Haulogy) : chaque fichier extrait atterrit
dans `<RELAIS__PARTNER_URL>/<CODE_FLUX>/` (`C15/`, `R151/`, `R15/`, `F12/`,
`F15/`, …), dossier créé au besoin. Câblé en dur — pas de knob d'arborescence
ni de template dans l'URL, `RELAIS__PARTNER_URL` reste une racine simple.

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
# Box durcie (défaut) — root SSH coupé (ADR-0031), on passe par ops :
scp ma_cle_relais_ed25519 ops@<box>:/tmp/relais_ssh_key
ssh ops@<box> 'sudo install -m 600 /tmp/relais_ssh_key /srv/<slug>/relais_ssh_key && sudo rm /tmp/relais_ssh_key'

# Box non durcie (--no-harden) :
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

Sous-commande dédiée (`relais completude`, #690 — remplace l'ancien `python -c`
à rallonge), depuis le conteneur. **Défaut** : l'écart restreint aux flux
**dus** au partenaire (`RELAIS__FLUX`, arbitrage revue #688 — la vraie
question opérateur) ; sortie : un zip par ligne (lisible à l'œil, consommable
en pipe) puis une ligne de compte, exit **0** dans tous les cas — consultation
passive, même philosophie que la vue d'audit ci-dessous, l'escalade du relais
reste `relais_aveugle()` :

```bash
sudo -u <slug> docker compose --env-file /srv/<slug>/config.env \
    -f /srv/<slug>/deploy/relais/compose-relais.yml run --rm relais \
    python -m electricore.ingestion.relais completude
```

`--tous` : l'écart brut, tous flux confondus (comportement historique de
`zips_non_relayes`) — sans le filtre par défaut, l'écart inclut les flux
jamais dus au partenaire (X13, LTE01, R63…), du bruit qui a failli masquer un
vrai trou de 47 zips à la générale (#683) :

```bash
sudo -u <slug> docker compose --env-file /srv/<slug>/config.env \
    -f /srv/<slug>/deploy/relais/compose-relais.yml run --rm relais \
    python -m electricore.ingestion.relais completude --tous
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
con = duckdb.connect('/data/relais.duckdb')
print(con.execute('select * from journal.relais_audit_sequences').fetchall())
"
```

## Alerte mail (`OnFailure=`, #659, câblée sur ce layout par #668, msmtprc rendu #674)

Quand `electricore-relais.service` échoue (run aveugle — 0 push réussi, ≥ 1
échec, cf. `electricore/ingestion/relais/pipeline.py`), `OnFailure=` déclenche
`electricore-relais-alerte.service` : un mail part vers `RELAIS_ALERTE_MAILS`
sans que personne n'ait à regarder `systemctl --failed`. Le hook est un simple
script shell + [msmtp](https://marlam.de/msmtp/), délibérément sans Python —
le scénario où l'alerte est la plus nécessaire est précisément celui où le
conteneur, Docker ou SOPS est en panne : ce hook tourne **host-level**, hors
du conteneur du relais.

`install.sh --relais` **pose et branche tout ça automatiquement**, comme le
reste du composant (`install_relais_alerte_units`, `deploy/lib/relais.sh`) :

- `electricore-relais-alerte.service` est rendue par slug — son
  `EnvironmentFile=` pointe sur `/srv/<slug>/config.env` (le layout #657, où
  `RELAIS_ALERTE_MAILS` est déjà prévu), **pas** sur l'ancien
  `/etc/electricore-relais/relais.env` du chemin bare-metal, qui n'existe pas
  ici.
- `/usr/local/bin/electricore-relais-alerte.sh` est posé (le script est rendu
  par `render_relais_alerte_script`, pas fetché à part : l'installeur ne
  télécharge que `deploy/lib/*.sh`).
- `OnFailure=electricore-relais-alerte.service` est ajouté à l'unité
  `electricore-relais.service` **rendue** (`render_relais_service`).
- L'unité d'alerte elle-même n'est **jamais activée** (pas de
  `enable --now` : elle n'a pas de section `[Install]`, elle ne se déclenche
  que via `OnFailure=`).
- Le paquet `msmtp`, le répertoire `/etc/electricore-relais` (700) **et**
  `/etc/electricore-relais/msmtprc` (600) sont posés par l'installeur —
  **zéro étape manuelle** (#674, voir section dédiée ci-dessous).
- Idempotent, comme le reste du composant : un `install.sh --relais` relancé
  régénère les trois fichiers sans effet de bord.
- Le **déclenchement** se prouve sans attendre un vrai échec :
  `./deploy/tests/e2e/multipass.sh verify-relais onfailure <slug>` force un
  échec du service (compose écarté, stub msmtp) et vérifie que l'alerte tire —
  la mécanique seulement ; le mail réel se vérifie à la générale (#661).

### `msmtprc` : rendu, token SMTP dans `secrets.env` (#674)

Jusqu'à #674, `/etc/electricore-relais/msmtprc` (dont le **token SMTP**)
était le **dernier secret hors secrets-as-code** de tout le composant relais —
posé à la main sur la box, en contradiction avec [ADR-0044](../../docs/adr/0044-secrets-as-code-sops-age.md).
Ce n'est plus le cas :

- Le token SMTP vit dans `providers/<slug>/secrets.env` (chiffré SOPS+age,
  champ `ALERTE__SMTP__PASSWORD`) — jamais en clair sur la box, jamais dans
  un dépôt ni dans l'image.
- Les paramètres NON secrets (routage) vivent dans `providers/<slug>/config.env`
  (clair, versionné) : `ALERTE__SMTP__HOST`, `ALERTE__SMTP__PORT`,
  `ALERTE__SMTP__FROM`, `ALERTE__SMTP__USER` (convention `<DOMAINE>__<CHAMP>`,
  [ADR-0046](../../docs/adr/0046-convention-noms-env-par-domaine-identite-secrets.md)
  §7 : secret = capacité, config = routage — même split que `BOT__TOKEN` vs
  `BOT__NOTIFY_CHAT_ID`).
- `install.sh --relais` lit ces quatre valeurs dans `config.env` et **rend**
  `/etc/electricore-relais/msmtprc` (600) avec un `passwordeval` : à chaque
  envoi, msmtp exécute un `sops decrypt` **hôte** sur
  `providers/<slug>/secrets.env` (`SOPS_AGE_KEY_FILE=/srv/<slug>/age.key` —
  sops, age et la clé de la box sont déjà là depuis l'onboarding, l'unité
  d'alerte tourne root) et en extrait `ALERTE__SMTP__PASSWORD` — le token
  n'existe **jamais** en clair sur la box, seulement en extraction **éphémère**
  dans le pipe de `passwordeval` (`render_relais_alerte_msmtprc`,
  `deploy/lib/relais.sh`).
- **Garde-fou au reconfigure** : si `RELAIS_ALERTE_MAILS` est posé (alerte
  voulue) mais `ALERTE__SMTP__{HOST,FROM,USER}` incomplets, `validate_config_env`
  **refuse la config avant toute pose** — un msmtprc existant qui marche n'est
  jamais écrasé par un rendu invalide (msmtp refuse un fichier à directive
  vide, l'alerte mourrait en silence ; arbitrage revue #675). Compléter
  `providers/<slug>/config.env`, pousser, relancer.
- **Mode dégradé assumé** : si ce sops hôte échoue (clé age absente/invalide),
  le pipe remonte son code non-zéro ; si sops réussit mais que
  `ALERTE__SMTP__PASSWORD` manque de `secrets.env`, le `| grep .` final échoue
  sur l'extraction vide (sinon msmtp tenterait une auth avec mot de passe vide
  — erreur confuse, loin de la cause). Dans les deux cas `passwordeval`
  échoue — msmtp logue bruyamment sur stderr, capté par
  `journalctl -u electricore-relais-alerte.service`, et **n'envoie pas** de
  mail. C'est une **perte assumée** par rapport au principe « l'alerte survit
  à tout » de #659 (le hook lui-même reste délibérément sans Python, hors du
  conteneur) : un SOPS cassé empêche maintenant l'envoi, alors qu'un
  `msmtprc` posé à la main survivait à une panne SOPS. L'escalade systemd
  reste intacte dans tous les cas — `electricore-relais-alerte.service`
  passe `failed`, visible par `systemctl --failed` / le monitoring, jamais un
  crash silencieux.
- **Rotation** du token = comme tout secret : `sops providers/<slug>/secrets.env`
  (éditer `ALERTE__SMTP__PASSWORD`), commit, push, puis `reconfigure` sur la
  box (`sudo bash install.sh --slug <slug> --relais --deploy-repo <url>`) —
  `msmtprc` est régénéré, le nouveau token est actif au prochain envoi.
- **Surface actée** (arbitrage revue #675) : comme tout champ de `secrets.env`,
  le token SMTP entre dans l'env des conteneurs qui font `sops exec-env`
  (API, bot…) alors que seul msmtp **hôte** en a besoin — cohérent avec
  l'injection en bloc d'[ADR-0044](../../docs/adr/0044-secrets-as-code-sops-age.md),
  token peu sensible et facile à roter ; un scoping par consommateur serait un
  chantier à part, non justifié pour ce seul champ.

Une fois posé, tester la chaîne (le token doit être résolu par `passwordeval`
et le mail doit arriver — **validation réservée à une box réelle**, ex.
Enargia, pas exécutable depuis un sandbox sans réseau) :

```bash
sudo systemctl start electricore-relais-alerte.service   # → le mail doit arriver
```

Vérifier la syntaxe des deux unités (`systemd-analyze verify` — l'e2e
multipass et le runner bash unitaire, `deploy/tests/unit.sh`, le font déjà) :

```bash
sudo systemd-analyze verify /etc/systemd/system/electricore-relais.service
sudo systemd-analyze verify /etc/systemd/system/electricore-relais-alerte.service
```

Le vrai critère d'acceptation — un échec forcé du relais déclenche
effectivement un mail réel — se vérifie à la générale (PRD #658, #661), pas
ici : cette section garantit seulement que le câblage est posé et syntaxiquement
valide, pas qu'un mail a réellement transité par un vrai serveur SMTP.

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
