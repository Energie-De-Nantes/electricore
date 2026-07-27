# Relais de flux Enedis déchiffrés → SFTP partenaire (#637)

Unité de déploiement **séparée** de la stack docker compose de l'ingestion
([../docker/](../docker/)) : le relais est un outil autonome, pose-et-oublie,
qui n'importe aucune surface mouvante de l'ingestion (runner, curseur, état,
DuckDB — voir `electricore/ingestion/relais/pipeline.py`). Il tourne via un
timer systemd dédié, **pas** le service inotify existant de l'ingestion et pas
`crontab.example` (qui, lui, appelle l'API du stack principal).

## Installation (poste avec accès à la fois à la source déchiffrable et à la cible partenaire)

```bash
# 1. Environnement Python avec les extras [ingestion] (dlt, PyCryptodome, fsspec) et [dbt]
#    (dbt-core + dbt-duckdb, #646 : étage Transform embarqué — vue journal.relais_audit_sequences,
#    même versions figées que le projet dbt principal, aucun pin dupliqué)
uv sync --extra ingestion --extra dbt   # ou pip install "electricore[ingestion,dbt]"

# 2. Config (RELAIS__* + AES__TROUSSEAU__* — même format que l'ingestion, voir CLAUDE.md)
sudo mkdir -p /etc/electricore-relais
sudo tee /etc/electricore-relais/relais.env <<'ENV'
RELAIS__SOURCE_URL=sftp://user:pass@source.example/flux
RELAIS__PARTNER_URL=sftp://relais@partenaire.example/in
RELAIS__DESTINATION_DB=/opt/electricore-relais/relais.duckdb
RELAIS__FLUX=C15,R151,R15,F12,F15   # phase 1 : liste explicite, vide = tous
AES__TROUSSEAU__aes256_2026__KEY=...
# CSV, destinataires de l'alerte OnFailure= (#659, voir plus bas). Commentaire sur sa
# PROPRE ligne : systemd n'ignore un « # » qu'en début de ligne — en fin de ligne il entre dans la valeur.
RELAIS_ALERTE_MAILS=alertes-ops@example.fr
ENV
sudo chmod 600 /etc/electricore-relais/relais.env

# 3. Alerte mail (#659) : installer msmtp, écrire /etc/electricore-relais/msmtprc
#    (token SMTP Proton — contenu complet dans la section « Alerte mail
#    (OnFailure=, #659) » plus bas de ce README), puis chmod 600 + poser le script.
sudo apt install -y msmtp
sudo $EDITOR /etc/electricore-relais/msmtprc   # coller le contenu de la section plus bas
sudo chmod 600 /etc/electricore-relais/msmtprc
sudo cp electricore-relais-alerte.sh /usr/local/bin/
sudo chmod +x /usr/local/bin/electricore-relais-alerte.sh

# 4. Auth SFTP partenaire : clé SSH ed25519 DÉDIÉE (jamais de mot de passe en dur),
#    générée pour ce seul usage — pas de réutilisation d'une clé d'ingestion existante.

# 5. Amorçage (#643) : marque l'historique existant comme livré SANS le pousser au
#    partenaire — acte UNIQUE, à faire une fois avant d'activer le timer (sinon le
#    premier run pousserait tout l'historique). Refuse s'il y a déjà des livraisons.
sudo -u electricore-relais env $(cat /etc/electricore-relais/relais.env | xargs) \
  python -m electricore.ingestion.relais seed --avant 2026-06-01

# 6. Units (le service d'alerte n'est PAS activé : déclenché uniquement par
#    OnFailure=, jamais par le timer ni par enable --now)
sudo cp electricore-relais.service electricore-relais-alerte.service electricore-relais.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now electricore-relais.timer
```

## Vérifier

```bash
systemctl status electricore-relais.timer
journalctl -u electricore-relais.service -f
```

## Complétude

Requête ad hoc (zips reçus jamais relayés) :

```python
from electricore.ingestion.relais.pipeline import zips_non_relayes
zips_non_relayes("sftp://user:pass@source.example/flux", "/opt/electricore-relais/relais.duckdb")
```

## Vue d'audit (#646)

`journal.relais_audit_sequences` (matérialisée en fin de chaque passage du timer) audite
la nomenclature des zips VUS par le relais (trous de séquence, queue invérifiable, noms non
reconnus) — même macro que l'audit côté ingestion (#645), zéro règle dupliquée. Vue
**passive** : aucune alerte, consultation à la demande (rapprochement Haulogy) :

```bash
uv run python -c "
import duckdb
con = duckdb.connect('/opt/electricore-relais/relais.duckdb')
print(con.execute('select * from journal.relais_audit_sequences').fetchall())
"
```

## Alerte mail (`OnFailure=`, #659)

Quand `electricore-relais.service` échoue (run aveugle, #643 — voir
`electricore/ingestion/relais/pipeline.py`), `OnFailure=` déclenche
`electricore-relais-alerte.service` : un mail part vers `RELAIS_ALERTE_MAILS`
sans que personne n'ait à regarder `systemctl status`. Le hook est un simple
script shell + [msmtp](https://marlam.de/msmtp/), délibérément sans Python — le
scénario où l'alerte est la plus nécessaire est celui où le venv du relais est
cassé.

### Installer msmtp

```bash
sudo apt install -y msmtp   # paquet système, aucune dépendance Python
```

### Configurer `/etc/electricore-relais/msmtprc`

Le SMTP submission de Proton (`smtp.protonmail.ch:587`, STARTTLS) authentifie par
**token SMTP**, pas par le mot de passe du compte — **exclusif aux adresses du
domaine custom `electricore.fr`** (les adresses @proton.me / @pm.me n'ont pas
cette fonctionnalité).

Prérequis one-shot déjà fait (27/07/2026, à refaire seulement si le domaine change
de main) : `electricore.fr` rattaché à Proton Mail — TXT de vérification, MX Proton
(les MX OVH retirés, l'offre « redirect » n'avait aucun compte), SPF
`v=spf1 include:_spf.protonmail.ch -all`, 3 CNAME DKIM. Ensuite :

1. Proton → Settings → SMTP submission (sous le domaine custom `electricore.fr`)
2. Générer un token pour l'adresse d'envoi (ex. `alertes@electricore.fr`)
3. Coller ce token comme `password` ci-dessous — **jamais** le mot de passe du compte

```
defaults
tls on
tls_starttls on

account electricore-relais
host smtp.protonmail.ch
port 587
auth on
from alertes@electricore.fr
user alertes@electricore.fr
password <token SMTP Proton>

account default : electricore-relais
```

```bash
sudo chmod 600 /etc/electricore-relais/msmtprc
```

Aucun secret dans le repo : le token ne vit que dans ce fichier local en 600.

### Destinataires

Une seule variable, dans le **même** fichier d'env que le relais
(`/etc/electricore-relais/relais.env`) — le domaine runtime pydantic du relais
est en `extra="ignore"` (voir `electricore/config/runtime.py`), donc
`RELAIS_ALERTE_MAILS` cohabite avec `RELAIS__*` sans aucune modification de
code Python :

```bash
RELAIS_ALERTE_MAILS=alice@example.fr,bob@example.fr
```

Modifier la liste = éditer cette seule ligne, sans toucher aux unités ni au script.

### Tester la chaîne

```bash
sudo cp electricore-relais-alerte.sh /usr/local/bin/
sudo chmod +x /usr/local/bin/electricore-relais-alerte.sh
sudo cp electricore-relais-alerte.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start electricore-relais-alerte.service   # → le mail doit arriver
```

## Générale : l'état ne survit pas (PRD #658)

> **L'état d'une générale ne survit jamais à la générale.**

Pendant une répétition (générale, PRD #658) sur le VPS partenaire, on pointe
`RELAIS__DESTINATION_DB` dans un répertoire dédié **jetable** — ça isole aussi
l'état dlt, `pipelines_dir` étant épinglé sur son parent (voir
`electricore/ingestion/relais/pipeline.py`). En fin de générale, ce répertoire
est **détruit**. Sans cette destruction, tout zip marqué « livré » au faux
partenaire de la répétition le resterait **à vie** dans un état qui n'aurait
jamais dû exister — et le vrai partenaire ne le recevrait alors jamais.

## Pourquoi systemd et pas docker compose / crontab.example

`crontab.example` (voir [../docker/](../docker/)) appelle l'API du stack principal — il
suppose l'ingestion et sa DuckDB. Le relais n'a ni l'un ni l'autre comme dépendance
(design #637) : un timer systemd autonome garde cette indépendance visible au niveau
déploiement, pas seulement au niveau code.
