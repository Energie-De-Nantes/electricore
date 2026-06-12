# 🔧 Outils de debug de l'ingestion

Ce dossier contient les outils de diagnostic et de debug pour l'ingestion (dlt + dbt).

## Scripts disponibles

### `check_incremental_state.py`
Vérifie l'état incrémental détaillé de chaque resource du pipeline.
```bash
poetry run python tools/check_incremental_state.py
```

### `diagnostic_flux.py` 
Diagnostic méthodique de tous les flux configurés :
- Vérification de la présence de fichiers SFTP
- État du pipeline et des tables
- Résumé complet par flux
```bash
poetry run python tools/diagnostic_flux.py
```

### `test_single_flux.py`
Test d'un seul flux spécifique avec limitation de fichiers.
```bash
# Tester R151
poetry run python tools/test_single_flux.py R151

# Tester R64 CSV
poetry run python tools/test_single_flux.py R64
```

## Usage typique

1. **Problème d'incrémental** → `check_incremental_state.py`
2. **Flux manquants** → `diagnostic_flux.py` 
3. **Test rapide** → `test_single_flux.py [FLUX]`