#!/bin/sh
# Sauvegarde de la base DuckDB ElectriCore.
# Exécuté par supercronic dans le conteneur ingestion-scheduler.
#
# Variables d'environnement attendues :
#   DUCKDB__PATH   — chemin de la base (par défaut /data/flux_enedis_pipeline.duckdb, ADR-0046)
#   BACKUP_DIR     — répertoire des snapshots (par défaut /backups)
#   RETAIN_DAYS    — nombre de snapshots à conserver (par défaut 14)
#   INSTANCE_SLUG  — identifiant de l'instance (ADR-0015). Si défini, les snapshots
#                    sont nommés snapshot_<slug>_<TS>.tar.gz pour éviter les
#                    collisions cross-instance lorsque les backups sont poussés
#                    vers un store central. Sinon : snapshot_<TS>.tar.gz.

set -eu

DUCKDB_PATH="${DUCKDB__PATH:-/data/flux_enedis_pipeline.duckdb}"
BACKUP_DIR="${BACKUP_DIR:-/backups}"
RETAIN_DAYS="${RETAIN_DAYS:-14}"
INSTANCE_SLUG="${INSTANCE_SLUG:-}"

if [ ! -f "$DUCKDB_PATH" ]; then
    echo "[backup_duckdb] Aucune base à sauvegarder ($DUCKDB_PATH absent), rien à faire." >&2
    exit 0
fi

TS=$(date -u +%Y%m%dT%H%M%SZ)
if [ -n "$INSTANCE_SLUG" ]; then
    NAME="snapshot_${INSTANCE_SLUG}_${TS}"
else
    NAME="snapshot_${TS}"
fi
SNAPSHOT_DIR="${BACKUP_DIR}/${NAME}"
ARCHIVE="${BACKUP_DIR}/${NAME}.tar.gz"

mkdir -p "$BACKUP_DIR"

# EXPORT DATABASE produit un dossier SQL + parquet relisible depuis n'importe
# quelle version DuckDB ≥ celle utilisée pour exporter.
echo "[backup_duckdb] Snapshot vers $SNAPSHOT_DIR"
duckdb -readonly "$DUCKDB_PATH" "EXPORT DATABASE '$SNAPSHOT_DIR' (FORMAT PARQUET)"

echo "[backup_duckdb] Compression vers $ARCHIVE"
tar -C "$BACKUP_DIR" -czf "$ARCHIVE" "$NAME"
rm -rf "$SNAPSHOT_DIR"

# Rétention : on garde les N plus récents
KEEP=$((RETAIN_DAYS + 1))
ls -1t "$BACKUP_DIR"/snapshot_*.tar.gz 2>/dev/null | tail -n +"$KEEP" | xargs -r rm -f

SIZE=$(stat -c '%s' "$ARCHIVE" 2>/dev/null || stat -f '%z' "$ARCHIVE")
echo "[backup_duckdb] Snapshot OK ($ARCHIVE, ${SIZE} octets)"
