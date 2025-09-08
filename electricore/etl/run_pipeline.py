#!/usr/bin/env python3
"""
Point d'entrée simple pour exécuter le pipeline ETL Enedis.
"""

if __name__ == "__main__":
    from pipelines.flux_enedis import main
    main()