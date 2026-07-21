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
ENV
sudo chmod 600 /etc/electricore-relais/relais.env

# 3. Auth SFTP partenaire : clé SSH ed25519 DÉDIÉE (jamais de mot de passe en dur),
#    générée pour ce seul usage — pas de réutilisation d'une clé d'ingestion existante.

# 4. Amorçage (#643) : marque l'historique existant comme livré SANS le pousser au
#    partenaire — acte UNIQUE, à faire une fois avant d'activer le timer (sinon le
#    premier run pousserait tout l'historique). Refuse s'il y a déjà des livraisons.
sudo -u electricore-relais env $(cat /etc/electricore-relais/relais.env | xargs) \
  python -m electricore.ingestion.relais seed --avant 2026-06-01

# 5. Units
sudo cp electricore-relais.service electricore-relais.timer /etc/systemd/system/
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

## Pourquoi systemd et pas docker compose / crontab.example

`crontab.example` (voir [../docker/](../docker/)) appelle l'API du stack principal — il
suppose l'ingestion et sa DuckDB. Le relais n'a ni l'un ni l'autre comme dépendance
(design #637) : un timer systemd autonome garde cette indépendance visible au niveau
déploiement, pas seulement au niveau code.
