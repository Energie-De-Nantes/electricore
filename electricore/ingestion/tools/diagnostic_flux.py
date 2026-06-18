#!/usr/bin/env python3
"""Diagnostic SFTP par flux : que livre Enedis en ce moment ?

Liste (read-only, sans rien lander ni déchiffrer) le bucket SFTP du registre runtime
pour chaque glob de `config/flux.yaml` : nombre de fichiers et fenêtre de dates de
modification. Répond à « Enedis a-t-il déposé des R64 cette semaine ? » sans lancer
le pipeline.

Pour l'état des CURSEURS incrémentaux (ce qui a déjà été landé), voir
`check_incremental_state.py`.

Usage : uv run python -m electricore.ingestion.tools.diagnostic_flux
"""

from pathlib import Path

import yaml
from dlt.sources.filesystem import filesystem

from electricore.config import runtime
from electricore.ingestion.sources.sftp_enedis import mask_password_in_url

# `config/flux.yaml` ancré sur le module (indépendant du CWD), comme le runner.
_CONFIG = Path(__file__).resolve().parents[1] / "config" / "flux.yaml"


def diagnostic_flux() -> None:
    """Compte les fichiers SFTP et leur fenêtre de dates, flux par flux."""
    flux_config = yaml.safe_load(_CONFIG.read_text())
    sftp_url = runtime.sftp().url

    print("=" * 80)
    print("🔍 DIAGNOSTIC SFTP PAR FLUX")
    print("=" * 80)
    print(f"🌐 SFTP : {mask_password_in_url(sftp_url)}\n")

    for flux_name, config in flux_config.items():
        pattern = config["file_pattern"]
        print(f"📁 {flux_name}  (glob : {pattern})")
        try:
            dates = [item["modification_date"] for item in filesystem(bucket_url=sftp_url, file_glob=pattern)]
        except Exception as e:  # noqa: BLE001 — diagnostic : on rapporte l'erreur, on ne plante pas
            print(f"   ❌ erreur : {e}\n")
            continue
        if not dates:
            print("   ❌ aucun fichier\n")
            continue
        print(f"   ✅ {len(dates)} fichier(s)  |  du {min(dates)} au {max(dates)}\n")


if __name__ == "__main__":
    diagnostic_flux()
