"""Entrypoint du relais : `python -m electricore.ingestion.relais` (#637, #643).

Outil autonome pose-et-oublie : un run = un balayage réconciliant complet de
la source (voir `pipeline.py`). Pensé pour un timer périodique (systemd,
`deploy/relais/`), pas pour de l'inotify — la continuité tient par le
re-listing, pas par la détection d'événements.

Sous-commande `seed --avant <date> [--force]` : amorçage explicite, un acte
UNIQUE distinct du run périodique (voir `pipeline.py::seed_avant`).

Pas de `logging.disable` (retiré, #643) : les `logger.warning` d'échec de push
(`pipeline.py::_pousser`, via `etape_chaine`) doivent rester visibles dans
`journalctl` — un relais qui avale ses propres warnings retenterait en
silence pour toujours, le reproche exact fait à inotify dans #637. Le niveau
par défaut de `logging` (WARNING sur stderr, sans handler configuré) suffit ;
resserrer le logger `dlt` si trop bavard n'a pas été nécessaire à l'usage.
"""

import argparse
import sys

from electricore.config import runtime
from electricore.ingestion.relais.pipeline import executer, seed_avant


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m electricore.ingestion.relais")
    sous = parser.add_subparsers(dest="commande")

    seed = sous.add_parser(
        "seed",
        help="Amorçage (#643) : marque les zips antérieurs à --avant comme livrés SANS les pousser.",
    )
    seed.add_argument("--avant", required=True, help="date ISO (YYYY-MM-DD) : borne exclusive")
    seed.add_argument(
        "--force",
        action="store_true",
        help="outrepasse le refus si le journal contient déjà des livraisons",
    )

    return parser.parse_args(argv)


def _run_seed(args: argparse.Namespace) -> None:
    try:
        info = seed_avant(args.avant, force=args.force)
    except RuntimeError as e:  # garde-fou métier (#643) : message déjà explicite
        print(f"❌ {e}", flush=True)
        sys.exit(1)
    except Exception as e:  # noqa: BLE001 — échec pipeline (pas par-zip) : sortie en erreur
        print(f"❌ Relais seed : échec : {e}", flush=True)
        sys.exit(1)
    print(f"✅ Amorçage : {info}", flush=True)


def _run_relais() -> None:
    try:
        info, stats = executer()
    except Exception as e:  # noqa: BLE001 — un run qui échoue au niveau pipeline (pas par-zip) doit sortir en erreur (systemd le rejouera)
        print(f"❌ Relais : échec du run : {e}", flush=True)
        sys.exit(1)

    resume = f"{stats.candidats} candidat(s), {stats.pousses} poussé(s), {stats.echecs_push} échec(s)"
    if stats.relais_aveugle():
        # Escalade s'arrêtant au processus (#643) : sortie non-zéro → systemd marque
        # l'unité failed. Sans ça, un relais où TOUS les push échouent afficherait ✅
        # et retenterait en silence pour toujours (le reproche fait à inotify, #637).
        print(f"❌ Relais aveugle : {resume}", flush=True)
        sys.exit(1)
    print(f"✅ Relais : {resume} — {info}", flush=True)


def main() -> None:
    args = _parse_args(sys.argv[1:])
    runtime.valider(runtime.relais, runtime.aes)
    if args.commande == "seed":
        _run_seed(args)
    else:
        _run_relais()


if __name__ == "__main__":
    main()
