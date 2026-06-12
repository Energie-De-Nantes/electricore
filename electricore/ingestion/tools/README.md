# 🔧 Outils de debug de l'ingestion

Ce dossier contient les outils de diagnostic et de debug pour l'ingestion (dlt + dbt).

## Scripts disponibles

### `check_incremental_state.py`
Vérifie l'état incrémental détaillé de chaque resource du pipeline.
```bash
uv run python -m electricore.ingestion.tools.check_incremental_state
```

### `diagnostic_flux.py`
Diagnostic méthodique de tous les flux configurés :
- Vérification de la présence de fichiers SFTP
- État du pipeline et des tables
- Résumé complet par flux
```bash
uv run python -m electricore.ingestion.tools.diagnostic_flux
```

### `debug_single_flux.py`
Test d'un seul flux spécifique avec limitation de fichiers.
```bash
uv run python -m electricore.ingestion.tools.debug_single_flux R151
```

### `reset_incremental_state.py`
Réinitialise les curseurs incrémentaux DLT (état pipeline local **et** tables
`_dlt_*` dans DuckDB). Préférer le mode `resync` du runner, qui enchaîne reset +
re-téléchargement.

### `comparaison_bases.py`
Compare deux bases DuckDB table par table, par empreintes canoniques — outil de
validation de la bascule legacy → dbt ([ADR-0020](../../../docs/adr/0020-linearisation-en-dbt.md)).
Verdict du 11/06/2026 : parité totale 7/7 tables sur le corpus SFTP complet.
```bash
uv run python -m electricore.ingestion.tools.comparaison_bases \
    <db_legacy> <schema_legacy> <db_dbt> <schema_dbt>
```

## Usage typique

1. **Problème d'incrémental** → `check_incremental_state.py`
2. **Flux manquants** → `diagnostic_flux.py`
3. **Test rapide d'un flux** → `debug_single_flux.py [FLUX]`
4. **Resync complet** → `uv run python -m electricore.ingestion resync` (confirmation requise côté bot)
