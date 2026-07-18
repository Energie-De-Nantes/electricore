"""Entrypoint du relais : `python -m electricore.ingestion.relais` (#637).

Outil autonome pose-et-oublie : un run = un balayage réconciliant complet de
la source (voir `pipeline.py`). Pensé pour un timer périodique (systemd,
`deploy/relais/`), pas pour de l'inotify — la continuité tient par le
re-listing, pas par la détection d'événements.
"""

import logging
import sys

from electricore.config import runtime
from electricore.ingestion.relais.pipeline import executer

logging.disable(logging.CRITICAL)


def main() -> None:
    runtime.valider(runtime.relais, runtime.aes)
    try:
        info = executer()
    except Exception as e:  # noqa: BLE001 — un run qui échoue au niveau pipeline (pas par-zip) doit sortir en erreur (systemd le rejouera)
        print(f"❌ Relais : échec du run : {e}", flush=True)
        sys.exit(1)
    print(f"✅ Relais : {info}", flush=True)


if __name__ == "__main__":
    main()
