#!/usr/bin/env python3
"""
Test d'un seul flux pour comprendre le problème.
"""

from pathlib import Path

import dlt
import yaml

from electricore.etl.sources.sftp_enedis import flux_enedis


def test_single_flux(flux_name: str):
    """Teste un seul flux spécifique."""

    print("=" * 80)
    print(f"🧪 TEST DU FLUX {flux_name}")
    print("=" * 80)

    # Charger la configuration
    config_path = Path("config/flux.yaml")
    with open(config_path, encoding="utf-8") as f:
        all_flux_config = yaml.safe_load(f)

    if flux_name not in all_flux_config:
        print(f"❌ Flux {flux_name} non trouvé dans la configuration")
        return

    # Prendre seulement le flux demandé
    flux_config = {flux_name: all_flux_config[flux_name]}

    print(f"📋 Configuration {flux_name}:")
    print(f"   Pattern: {flux_config[flux_name]['zip_pattern']}")

    # Créer un nouveau pipeline pour éviter les conflits d'état
    pipeline = dlt.pipeline(
        pipeline_name=f"test_{flux_name.lower()}",
        destination="duckdb",
        dataset_name=f"test_{flux_name.lower()}",
        full_refresh=True,  # Force le traitement complet
    )

    print(f"🎯 Pipeline: test_{flux_name.lower()} → test_{flux_name.lower()}")

    # Créer la source avec max_files=1 pour test rapide
    print("📦 Création de la source...")
    source = flux_enedis(flux_config, max_files=1)

    # Lister les resources créées
    print("\n📄 Resources créées:")
    for resource in source.resources.values():
        print(f"   - {resource.name}")

    print("\n🚀 Exécution...")
    print("-" * 50)

    try:
        load_info = pipeline.run(source)

        print()
        print("=" * 80)
        print(f"✅ TEST {flux_name} TERMINÉ")
        print("=" * 80)

        # Afficher les résultats
        print(f"📦 Load info: {load_info}")

        # Vérifier les tables créées
        import duckdb

        db_name = f"test_{flux_name.lower()}.duckdb"
        conn = duckdb.connect(db_name)

        # Chercher le schema (avec timestamp)
        schemas = conn.execute(
            "SELECT DISTINCT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'test_%'"
        ).fetchall()
        if schemas:
            schema = schemas[0][0]
            tables = conn.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name NOT LIKE '_dlt%'"
            ).fetchall()

            print(f"\n📊 Tables créées dans {schema}:")
            for table in tables:
                count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table[0]}").fetchone()[0]
                print(f"   - {table[0]}: {count} lignes")
        else:
            print("❌ Aucun schema créé")

    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        flux_name = sys.argv[1].upper()
    else:
        # Tester R151 par défaut (ancien fichier)
        flux_name = "R151"

    test_single_flux(flux_name)
