#!/bin/sh
# Sauvegarde de la base DuckDB ElectriCore.
# Exécuté par supercronic dans le conteneur etl-scheduler.
#
# Variables d'environnement attendues :
#   DUCKDB_PATH  — chemin de la base (par défaut /data/flux_enedis_pipeline.duckdb)
#   BACKUP_DIR   — répertoire des snapshots (par défaut /backups)
#   RETAIN_DAYS  — nombre de snapshots à conserver (par défaut 14)

set -eu

DUCKDB_PATH="${DUCKDB_PATH:-/data/flux_enedis_pipeline.duckdb}"
BACKUP_DIR="${BACKUP_DIR:-/backups}"
RETAIN_DAYS="${RETAIN_DAYS:-14}"

if [ ! -f "$DUCKDB_PATH" ]; then
    echo "[backup_duckdb] Aucune base à sauvegarder ($DUCKDB_PATH absent), rien à faire." >&2
    exit 0
fi

TS=$(date -u +%Y%m%dT%H%M%SZ)
SNAPSHOT_DIR="${BACKUP_DIR}/snapshot_${TS}"
ARCHIVE="${BACKUP_DIR}/snapshot_${TS}.tar.gz"

mkdir -p "$BACKUP_DIR"

# EXPORT DATABASE produit un dossier SQL + parquet relisible depuis n'importe
# quelle version DuckDB ≥ celle utilisée pour exporter.
echo "[backup_duckdb] Snapshot vers $SNAPSHOT_DIR"
duckdb -readonly "$DUCKDB_PATH" "EXPORT DATABASE '$SNAPSHOT_DIR' (FORMAT PARQUET)"

echo "[backup_duckdb] Compression vers $ARCHIVE"
tar -C "$BACKUP_DIR" -czf "$ARCHIVE" "snapshot_${TS}"
rm -rf "$SNAPSHOT_DIR"

# Rétention : on garde les N plus récents
KEEP=$((RETAIN_DAYS + 1))
ls -1t "$BACKUP_DIR"/snapshot_*.tar.gz 2>/dev/null | tail -n +"$KEEP" | xargs -r rm -f

SIZE=$(stat -c '%s' "$ARCHIVE" 2>/dev/null || stat -f '%z' "$ARCHIVE")
echo "[backup_duckdb] Snapshot OK ($ARCHIVE, ${SIZE} octets)"
