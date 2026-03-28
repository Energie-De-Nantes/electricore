"""
Pipeline de production avec l'architecture refactorisée.
Traite tous les flux configurés avec des options flexibles.
"""

# Charger .env avant que DLT lise ses secrets depuis os.environ.
# DLT résout nativement les env vars avec la convention SECTION__CLÉ :
#   SFTP__URL  →  dlt.secrets['sftp']['url']
#   AES__KEY   →  dlt.secrets['aes']['key']  etc.
import os as _os
from pathlib import Path as _Path
for _c in [_Path(".env"), _Path(__file__).parents[3] / ".env"]:
    if _c.exists():
        with open(_c) as _f:
            for _l in _f:
                _l = _l.strip()
                if _l and not _l.startswith("#") and "=" in _l:
                    _k, _, _v = _l.partition("=")
                    _os.environ.setdefault(_k.strip(), _v.strip())
        break

import logging
import sys

import dlt
import yaml
from pathlib import Path
from sources.sftp_enedis import flux_enedis

# DLT écrit sur stderr — on laisse faire, le bot ne lit que stdout
logging.disable(logging.CRITICAL)


def _out(msg: str) -> None:
    """Écrit sur stdout (séparé du bruit DLT sur stderr)."""
    print(msg, flush=True)


def run_production_pipeline(
    flux_selection=None,
    max_files=None,
    destination="duckdb",
    dataset_name="flux_enedis",
    refresh=None,
):
    """
    Lance le pipeline de production avec l'architecture modulaire refactorisée.

    Args:
        flux_selection: Liste des flux à traiter (ex: ['R151', 'C15']) ou None pour tous
        max_files: Limitation du nombre de fichiers par resource (pour tests)
        destination: Destination DLT (duckdb, postgres, etc.)
        dataset_name: Nom du dataset de destination
    """

    # Charger la configuration des flux
    config_path = Path(__file__).parent / "config/flux.yaml"
    if not config_path.exists():
        raise FileNotFoundError("Configuration flux.yaml non trouvée")

    with open(config_path, 'r', encoding='utf-8') as f:
        all_flux_config = yaml.safe_load(f)

    # Filtrer les flux si spécifié
    if flux_selection:
        flux_config = {k: v for k, v in all_flux_config.items() if k in flux_selection}
    else:
        flux_config = all_flux_config

    if not flux_config:
        _out("Aucun flux à traiter")
        return

    flux_list = ", ".join(flux_config.keys())
    suffix = f" | {max_files} fichiers max" if max_files else ""
    _out(f"Pipeline {dataset_name} | flux: {flux_list}{suffix}")

    # Créer le pipeline
    pipeline = dlt.pipeline(
        pipeline_name="flux_enedis_pipeline",
        destination=destination,
        dataset_name=dataset_name
    )

    source = flux_enedis(flux_config, max_files=max_files)

    try:
        # Pipeline complet: Extract + Normalize + Load
        load_info = pipeline.run(source, refresh=refresh)

        trace = pipeline.last_trace
        total_rows = 0

        if trace and trace.last_normalize_info:
            table_metrics = trace.last_normalize_info.row_counts
            for table_name, row_count in table_metrics.items():
                if not table_name.startswith("_dlt"):
                    total_rows += row_count

        if total_rows == 0:
            _out("Aucune nouvelle donnée")
        else:
            if trace and trace.last_normalize_info:
                for table_name, row_count in sorted(table_metrics.items()):
                    if not table_name.startswith("_dlt"):
                        _out(f"  {table_name:<25} : {row_count:>8,} lignes")
            _out(f"  {'Total':<25} : {total_rows:>8,} lignes chargées")

    except Exception as e:
        _out(f"Erreur pipeline: {e}")
        raise


def main():
    """Point d'entrée principal avec différents modes"""

    import sys

    if len(sys.argv) > 1:
        mode = sys.argv[1]

        if mode == "test":
            _out("MODE TEST RAPIDE")
            run_production_pipeline(
                flux_selection=['R151', 'C15', 'F15', 'R64'],
                max_files=2,
                dataset_name="flux_enedis_test"
            )

        elif mode == "r151":
            _out("MODE R151 COMPLET")
            run_production_pipeline(
                flux_selection=['R151'],
                dataset_name="flux_enedis_r151"
            )

        elif mode == "all":
            _out("MODE PRODUCTION COMPLÈTE")
            run_production_pipeline(
                dataset_name="flux_enedis"
            )

        elif mode == "reset":
            _out("MODE RESET COMPLET (données supprimées)")
            run_production_pipeline(
                dataset_name="flux_enedis",
                refresh="drop_sources"
            )

        else:
            _out(f"Mode inconnu: {mode}")
            _out("Usage: python pipeline_production.py [test|r151|all|reset]")
            sys.exit(1)
    else:
        _out("MODE PAR DÉFAUT: TEST RAPIDE")
        run_production_pipeline(
            flux_selection=['R151'],
            max_files=2,
            dataset_name="flux_enedis_default"
        )


if __name__ == "__main__":
    main()
