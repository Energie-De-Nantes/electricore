# 🔧 Outils de debug de l'ingestion

Ce dossier contient les outils de diagnostic et de debug pour l'ingestion (dlt + dbt).

## Scripts disponibles

### `check_incremental_state.py`
Affiche les curseurs incrémentaux (`modification_date`) du pipeline, par namespace de
source. Signale la fragmentation des curseurs sur plusieurs namespaces (curseurs posés
par un backfill mono-flux invisibles d'un `ingestion all`).
```bash
uv run python -m electricore.ingestion.tools.check_incremental_state
```

### `diagnostic_flux.py`
Diagnostic SFTP read-only : pour chaque flux de `config/flux.yaml`, compte les fichiers
présents sur le SFTP et leur fenêtre de dates — sans rien lander ni déchiffrer.
Répond à « Enedis a-t-il déposé des R64 cette semaine ? ».
```bash
uv run python -m electricore.ingestion.tools.diagnostic_flux
```

### `reset_incremental_state.py`
Réinitialise les curseurs incrémentaux DLT (état pipeline local **et** tables
`_dlt_*` dans DuckDB). Préférer le mode `resync` du runner, qui enchaîne reset +
re-téléchargement.

## Usage typique

1. **Que livre Enedis en ce moment ?** → `diagnostic_flux.py`
2. **Qu'est-ce qui a déjà été landé (curseurs) ?** → `check_incremental_state.py`
3. **Test rapide d'un flux** → `uv run python -m electricore.ingestion <flux> --max-files 1`
4. **Resync complet** → `uv run python -m electricore.ingestion resync` (confirmation requise côté bot)
