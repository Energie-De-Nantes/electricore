#!/usr/bin/env python3
"""
Réinitialise les curseurs incrémentaux DLT.

DLT stocke l'état dans DEUX endroits :
  1. Fichier local  : ~/.dlt/pipelines/{pipeline}/state.json  (prioritaire au démarrage)
  2. DuckDB         : flux_enedis._dlt_pipeline_state         (restauré au lancement)

Ce script met à jour les deux de manière cohérente.

Usage (depuis electricore/etl/) :
    # Reset complet (efface tous les curseurs → retraite tout depuis le début)
    uv run --extra etl python tools/reset_incremental_state.py --clear

    # Reset à une date (retraite les fichiers depuis cette date)
    uv run --extra etl python tools/reset_incremental_state.py 2026-03-17
    uv run --extra etl python tools/reset_incremental_state.py          # défaut : 2026-03-17
"""

import base64
import json
import sys
import uuid
import zlib
from pathlib import Path

import duckdb

DB_PATH = Path("flux_enedis_pipeline.duckdb")
DATASET = "flux_enedis"
RESET_DATE_DEFAULT = "2026-03-17T00:00:00+00:00"

# Préfixe interne DLT dans les valeurs incrémentales (Unicode private use area U+F027)
# Présent dans la version DuckDB du state, absent dans le fichier local JSON
_DLT_VALUE_PREFIX = "\uf027"


def _decode_duckdb_state(state_encoded: str) -> dict:
    """Décode le state DuckDB DLT : base64 → zlib → JSON."""
    raw = base64.b64decode(state_encoded + "==")
    return json.loads(zlib.decompress(raw))


def _encode_duckdb_state(state: dict) -> str:
    """Encode le state DuckDB DLT : JSON → zlib → base64."""
    raw = json.dumps(state, ensure_ascii=False).encode()
    return base64.b64encode(zlib.compress(raw)).decode().rstrip("=")


def _clear_resources(state: dict) -> list[tuple[str, str]]:
    """
    Efface tous les curseurs incrémentaux (last_value → None).
    Retourne la liste des resources modifiées avec leur ancienne valeur.
    """
    modified = []
    for source_state in state.get("sources", {}).values():
        for res_name, res_state in source_state.get("resources", {}).items():
            for inc in res_state.get("incremental", {}).values():
                old_value = inc.get("last_value")
                if old_value is not None:
                    old_date = str(old_value).lstrip(_DLT_VALUE_PREFIX)
                    inc["last_value"] = None
                    inc["unique_hashes"] = []
                    modified.append((res_name, old_date))
    return modified


def _reset_resources(state: dict, reset_to: str, value_prefix: str = "") -> list[tuple[str, str]]:
    """
    Modifie en place les curseurs incrémentaux du dict state.
    Retourne la liste des resources modifiées avec leur ancienne valeur.
    """
    modified = []
    for source_state in state.get("sources", {}).values():
        for res_name, res_state in source_state.get("resources", {}).items():
            for inc in res_state.get("incremental", {}).values():
                old_value = inc.get("last_value", "") or ""
                old_date = str(old_value).lstrip(_DLT_VALUE_PREFIX)
                if old_date > reset_to:
                    inc["last_value"] = value_prefix + reset_to
                    inc["unique_hashes"] = []
                    modified.append((res_name, old_date))
    return modified


def _load_duckdb_state(con) -> tuple:
    row = con.execute(f"""
        SELECT version, engine_version, pipeline_name, state, _dlt_load_id
        FROM {DATASET}._dlt_pipeline_state
        ORDER BY created_at DESC LIMIT 1
    """).fetchone()
    if not row:
        print("❌ Aucun état dans _dlt_pipeline_state")
        con.close()
        sys.exit(1)
    return row


def _write_duckdb_state(con, version: int, engine_version, pipeline_name: str, state: dict, load_id: str) -> int:
    new_version = version + 1
    con.execute(f"""
        INSERT INTO {DATASET}._dlt_pipeline_state
            (version, engine_version, pipeline_name, state, created_at, version_hash, _dlt_load_id, _dlt_id)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, ?, ?)
    """, [new_version, engine_version, pipeline_name, _encode_duckdb_state(state), load_id, str(uuid.uuid4())])
    return new_version


def clear_all_state() -> None:
    """Efface tous les curseurs incrémentaux (local + DuckDB)."""
    if not DB_PATH.exists():
        print(f"❌ Base DuckDB introuvable : {DB_PATH}")
        print("   Exécuter depuis le répertoire electricore/etl/")
        sys.exit(1)

    con = duckdb.connect(str(DB_PATH))
    version, engine_version, pipeline_name, state_encoded, load_id = _load_duckdb_state(con)

    print(f"📋 Pipeline : {pipeline_name}  |  version DuckDB actuelle : {version}")
    print("🗑️  Mode : effacement complet de tous les curseurs")
    print()

    # ── 1. Fichier local ──────────────────────────────────────────────────────
    local_state_path = Path.home() / ".dlt" / "pipelines" / pipeline_name / "state.json"
    if local_state_path.exists():
        local_state = json.loads(local_state_path.read_text())
        modified_local = _clear_resources(local_state)
        if modified_local:
            for res_name, old_date in modified_local:
                print(f"   🗑️  [local] {res_name}: {old_date}  →  None")
            local_state_path.write_text(json.dumps(local_state, indent=2, ensure_ascii=False))
            print(f"   💾 {local_state_path}")
        else:
            print("   ℹ️  Fichier local : déjà vide")
    else:
        print(f"   ℹ️  Pas de fichier local : {local_state_path}")
        modified_local = []

    print()

    # ── 2. DuckDB ─────────────────────────────────────────────────────────────
    db_state = _decode_duckdb_state(state_encoded)
    modified_db = _clear_resources(db_state)
    if modified_db:
        for res_name, old_date in modified_db:
            print(f"   🗑️  [duckdb] {res_name}: {old_date}  →  None")
        new_version = _write_duckdb_state(con, version, engine_version, pipeline_name, db_state, load_id)
        print(f"   💾 DuckDB version {version} → {new_version}")
    else:
        print("   ℹ️  DuckDB : déjà vide")

    con.close()

    total = len(modified_local) + len(modified_db)
    if total:
        print(f"\n✅ {total} curseur(s) effacé(s)")
    else:
        print("\nℹ️  Aucune modification nécessaire.")

    print("\n→ Relancer le pipeline :")
    print("   uv run --extra etl python pipeline_production.py all")


def reset_incremental_state(reset_to: str) -> None:
    if not DB_PATH.exists():
        print(f"❌ Base DuckDB introuvable : {DB_PATH}")
        print("   Exécuter depuis le répertoire electricore/etl/")
        sys.exit(1)

    con = duckdb.connect(str(DB_PATH))
    version, engine_version, pipeline_name, state_encoded, load_id = _load_duckdb_state(con)

    print(f"📋 Pipeline : {pipeline_name}  |  version DuckDB actuelle : {version}")
    print(f"📅 Reset vers : {reset_to}")
    print()

    # ── 1. Fichier local (prioritaire pour DLT) ──────────────────────────────
    local_state_path = Path.home() / ".dlt" / "pipelines" / pipeline_name / "state.json"

    if local_state_path.exists():
        local_state = json.loads(local_state_path.read_text())
        modified_local = _reset_resources(local_state, reset_to, value_prefix="")
        if modified_local:
            for res_name, old_date in modified_local:
                print(f"   ✅ [local] {res_name}: {old_date}  →  {reset_to}")
            local_state_path.write_text(json.dumps(local_state, indent=2, ensure_ascii=False))
            print(f"   💾 {local_state_path}")
        else:
            print("   ℹ️  Fichier local : aucun curseur à modifier")
    else:
        print(f"   ⚠️  Pas de fichier local : {local_state_path}")
        modified_local = []

    print()

    # ── 2. DuckDB (autorité après sync) ──────────────────────────────────────
    db_state = _decode_duckdb_state(state_encoded)
    modified_db = _reset_resources(db_state, reset_to, value_prefix=_DLT_VALUE_PREFIX)
    if modified_db:
        for res_name, old_date in modified_db:
            print(f"   ✅ [duckdb] {res_name}: {old_date}  →  {reset_to}")
        new_version = _write_duckdb_state(con, version, engine_version, pipeline_name, db_state, load_id)
        print(f"   💾 DuckDB version {version} → {new_version}")
    else:
        print("   ℹ️  DuckDB : aucun curseur à modifier")

    con.close()

    total = len(modified_local) + len(modified_db)
    if total:
        print(f"\n✅ {total} resource(s) réinitialisée(s) au {reset_to}")
    else:
        print("\nℹ️  Aucune modification nécessaire.")

    print("\n→ Relancer le pipeline :")
    print("   uv run --extra etl python pipeline_production.py all")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--clear":
        clear_all_state()
    else:
        reset_to = sys.argv[1] if len(sys.argv) > 1 else RESET_DATE_DEFAULT
        if len(reset_to) == 10:  # YYYY-MM-DD → ISO complet
            reset_to = f"{reset_to}T00:00:00+00:00"
        reset_incremental_state(reset_to)
