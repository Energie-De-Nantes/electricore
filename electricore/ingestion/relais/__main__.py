"""Entrypoint du relais : `python -m electricore.ingestion.relais` (#637, #643).

Outil autonome pose-et-oublie : un run = un balayage réconciliant complet de
la source (voir `pipeline.py`). Pensé pour un timer périodique (systemd,
`deploy/relais/`), pas pour de l'inotify — la continuité tient par le
re-listing, pas par la détection d'événements.

Sous-commande `seed --avant <date> [--force]` : amorçage explicite, un acte
UNIQUE distinct du run périodique (voir `pipeline.py::seed_avant`).

Sous-commande `completude [--tous]` (#690) : remplace le `python -c` à
rallonge qui vivait dans le README opérateur (`deploy/relais/README.md`) —
zips reçus jamais relayés (`pipeline.py::zips_non_relayes`, #637/#683).
Défaut : écart restreint aux flux **dus** au partenaire
(`runtime.relais().flux_filtres()`, arbitrage revue #688) — sans ce filtre,
le bruit hors liste (X13, LTE01, R63…) a failli masquer un vrai trou de 47
zips à la générale. `--tous` : écart brut, comportement historique de
`zips_non_relayes`. Consultation **passive** : sortie toujours 0, même s'il
manque des zips — l'escalade du relais reste `relais_aveugle()` (run normal),
jamais cette sous-commande.

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
from electricore.ingestion.relais.pipeline import executer, seed_avant, zips_non_relayes


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

    completude = sous.add_parser(
        "completude",
        help="Complétude (#690) : zips reçus jamais relayés — restreint aux flux dus par défaut.",
    )
    completude.add_argument(
        "--tous",
        action="store_true",
        help="écart brut, tous flux confondus (comportement historique) — "
        "défaut : restreint aux flux dus au partenaire (RELAIS__FLUX)",
    )

    return parser.parse_args(argv)


def _run_seed(args: argparse.Namespace) -> None:
    try:
        _, n_amorces = seed_avant(args.avant, force=args.force)
    except RuntimeError as e:  # garde-fou métier (#643) : message déjà explicite
        print(f"❌ {e}", flush=True)
        sys.exit(1)
    except Exception as e:  # noqa: BLE001 — échec pipeline (pas par-zip) : sortie en erreur
        print(f"❌ Relais seed : échec : {e}", flush=True)
        sys.exit(1)
    # Pas de `— {info}` dlt ici (#684) : le seed est un geste interactif, le pavé LoadInfo
    # noyait le chiffre — `_run_relais` (timer → journalctl) garde le sien, lui.
    print(f"✅ Amorçage : {n_amorces} zip(s) marqués livrés (antérieurs à {args.avant})", flush=True)


def _run_completude(args: argparse.Namespace) -> None:
    """Consultation passive (#690) : un zip par ligne (lisible à l'œil, consommable en
    pipe) puis une ligne de compte — jamais de `sys.exit` non-zéro ici, quel que soit le
    nombre de manquants (l'escalade du relais reste `relais_aveugle()`, pas cet outil)."""
    cfg = runtime.relais()
    flux_filtres = None if args.tous else cfg.flux_filtres()
    manquants = zips_non_relayes(cfg.source_url, cfg.destination_db, flux_filtres=flux_filtres)
    for zip_name in manquants:
        print(zip_name, flush=True)
    print(f"{len(manquants)} zip(s) manquant(s)", flush=True)


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
    elif args.commande == "completude":
        _run_completude(args)
    else:
        _run_relais()


if __name__ == "__main__":
    main()
