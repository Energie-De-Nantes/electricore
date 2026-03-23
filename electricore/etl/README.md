# ETL ElectriCore

Pipeline ETL pour les flux énergétiques Enedis : SFTP → DuckDB via DLT.

## Structure

```
electricore/etl/
├── transformers/
│   ├── crypto.py           # Déchiffrement AES (chaîne de clés, rotation)
│   ├── archive.py          # Extraction ZIP
│   └── parsers.py          # Parsing XML/CSV/JSON
├── sources/
│   └── sftp_enedis.py      # Source DLT SFTP multi-flux
├── tools/
│   └── reset_incremental_state.py  # Reset curseurs DLT
├── config/
│   ├── flux.yaml           # Configuration des flux
│   └── settings.py
├── pipeline_production.py  # Point d'entrée
└── .dlt/
    ├── config.toml
    └── secrets.toml        # Clés AES + URL SFTP (non commité)
```

## Utilisation

```bash
# Depuis electricore/etl/
uv run --extra etl python pipeline_production.py test    # 2 fichiers, ~3s
uv run --extra etl python pipeline_production.py r151   # R151 complet
uv run --extra etl python pipeline_production.py all    # Tous les flux
uv run --extra etl python pipeline_production.py reset  # Reset complet (supprime données + état)
```

## Configuration (`secrets.toml`)

```toml
[sftp]
url = "sftp://user:pass@host/path/"

# Format recommandé — supporte la rotation de clés
[aes.current]
key = "hex_encoded_key"
iv  = "hex_encoded_iv"

[aes.previous]           # optionnel, garder ~4 semaines après rotation
key = "ancienne_clé_hex"
iv  = "ancien_iv_hex"

# Format hérité (toujours supporté)
# [aes]
# key = "..."
# iv  = "..."
```

## Flux supportés

| Flux | Description | Tables |
|------|-------------|--------|
| **C15** | Événements contractuels | `flux_c15` |
| **F12** | Facturation distributeur | `flux_f12_detail` |
| **F15** | Facturation détaillée | `flux_f15_detail` |
| **R15** | Relevés index | `flux_r15`, `flux_r15_acc` |
| **R151** | Relevés périodiques Linky | `flux_r151` |
| **R64** | Timeseries JSON | `flux_r64` |

## Rotation des clés AES

Enedis effectue des rotations de clés périodiquement. Procédure :

1. Obtenir la nouvelle clé Enedis
2. Dans `secrets.toml` : déplacer `[aes]` → `[aes.previous]`, créer `[aes.current]`
3. Relancer le pipeline — les fichiers anciens déchiffrent avec `previous`, les nouveaux avec `current`
4. Après ~4 semaines : supprimer `[aes.previous]`

## Reset de l'état incrémental

DLT stocke l'état dans deux endroits (fichier local + DuckDB). Pour réinitialiser :

```bash
# Reset complet (supprime données et état, repart de zéro)
uv run --extra etl python pipeline_production.py reset

# Reset des curseurs uniquement (conserve les données)
uv run --extra etl python tools/reset_incremental_state.py --clear

# Recul à une date précise (retraite les fichiers depuis cette date)
uv run --extra etl python tools/reset_incremental_state.py 2026-03-17
```

## Vérification de l'état

```bash
uv run --extra etl dlt pipeline flux_enedis_pipeline info
```
