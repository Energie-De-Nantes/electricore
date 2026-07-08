# Ingestion ElectriCore

> Architecture complète, schéma et recettes : [docs/ingestion.md](../../docs/ingestion.md) — ADR : [0020](../../docs/adr/0020-linearisation-en-dbt.md), [0021](../../docs/adr/0021-bascule-production-dbt.md)

Ingestion des flux énergétiques Enedis : SFTP → DuckDB via dlt (landing brut) + dbt (linéarisation).

## Structure

```
electricore/ingestion/
├── transformers/
│   ├── crypto.py           # Déchiffrement AES (chaîne de clés, rotation)
│   ├── archive.py          # Extraction ZIP
│   └── chaine.py           # Discipline « attraper → compter → continuer » (ADR-0037)
├── parsing/
│   └── xml.py               # Conversion XML → dict générique (landing brut, ADR-0020)
├── sources/
│   ├── sftp_enedis_brut.py # Source DLT « brut » (landing JSON, ADR-0020) — production
│   └── sftp_enedis.py      # Brique SFTP partagée (listing incrémental, mouvement)
├── tools/
│   ├── reset_incremental_state.py   # Reset curseurs DLT
│   ├── check_incremental_state.py   # Inspecte les curseurs incrémentaux par namespace
│   └── diagnostic_flux.py           # Diagnostic SFTP read-only (que livre Enedis ?)
├── config/
│   └── flux.yaml           # Configuration de mouvement des flux
├── raw_landing.py       # Landing brut : document Enedis intégral en colonne JSON
├── runner.py            # Ingestion (landing brut → dbt build)
├── __main__.py          # CLI : python -m electricore.ingestion
├── CONTEXT.md            # Vocabulaire spécifique à l'ingestion
└── dbt/                    # Modèles dbt (staging + flux_*)

# Secrets (trousseau AES, URL SFTP) : variables d'environnement / .env à la racine
# du dépôt (le support .dlt/secrets.toml a été retiré, #141).
```

## Utilisation

```bash
# Depuis la racine du repo
uv run --extra ingestion --extra dbt python -m electricore.ingestion test    # 2 fichiers/flux
uv run --extra ingestion --extra dbt python -m electricore.ingestion r151    # un seul flux
uv run --extra ingestion --extra dbt python -m electricore.ingestion all     # tous les flux
uv run --extra ingestion --extra dbt python -m electricore.ingestion rebuild # dbt seul, zéro réseau
uv run --extra ingestion --extra dbt python -m electricore.ingestion resync  # re-télécharge tout (brut perdu)
```

## Configuration (variables d'environnement)

Les secrets vivent en variables d'environnement (`.env` à la racine ou env système ;
le support `.dlt/secrets.toml` a été retiré, #141).

```bash
SFTP__URL=sftp://user:pass@host/path/

# Trousseau de clés AES (ADR-0037) : un <label> parlant par clé, sélection par essai.
# Garder les anciennes clés préserve l'accès aux archives passées.
# __IV optionnel (ADR-0040) : absent ⇒ schéma IV-préfixé (AES-256) ; présent ⇒ IV-fixe (AES-128).
AES__TROUSSEAU__aes256_2026__KEY=cle_hex_64   # AES-256 (32 octets), SANS __IV (IV-préfixé)
AES__TROUSSEAU__aes128_2024__KEY=cle_hex_32   # AES-128 historique (16 octets)
AES__TROUSSEAU__aes128_2024__IV=iv_hex_32     # IV-fixe
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
| **R67** | Mesures facturantes (ponctuel, ADR-0047) | `flux_r67` |
| **C12** | Spine contractuelle C4 (>36 kVA, ADR-0051) | `flux_c12` |
| **X12/X13** | Affaires SGE (initiées/reçues) | `flux_affaires` |

## Rotation des clés AES (trousseau N-clés, ADR-0037)

Enedis rote ses clés périodiquement (et en change la longueur). Procédure :

1. Obtenir la nouvelle clé Enedis
2. L'ajouter au trousseau sous un nouveau label : `AES__TROUSSEAU__<nouveau>__{KEY,IV}`
3. Relancer le pipeline — chaque fichier déchiffre par essai avec la clé qui marche
4. Retirer une vieille clé seulement quand plus aucune archive chiffrée avec elle n'est (re)téléchargeable

L'échec n'est plus silencieux : un flux sans aucun déchiffrement réussi fait passer le
job à `failed` (escalade per-flux) → la surveillance bot alerte.

## Reset de l'état incrémental

DLT stocke l'état dans deux endroits (fichier local + DuckDB). Pour réinitialiser :

```bash
# Reset complet (supprime données et état, repart de zéro)
uv run --extra ingestion --extra dbt python -m electricore.ingestion resync

# Reset des curseurs uniquement (conserve les données)
uv run --extra ingestion python -m electricore.ingestion.tools.reset_incremental_state --clear

# Recul à une date précise (retraite les fichiers depuis cette date)
uv run --extra ingestion python -m electricore.ingestion.tools.reset_incremental_state 2026-03-17
```

## Vérification de l'état

```bash
uv run --extra ingestion dlt pipeline flux_enedis_pipeline info
```
