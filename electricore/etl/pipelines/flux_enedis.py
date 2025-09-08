"""
Pipeline principal pour les flux Enedis.
Point d'entrée pour l'exécution et la vérification des résultats.
"""

import dlt
import duckdb
from sources.sftp_enedis import sftp_flux_enedis_multi


def run_sftp_multi_pipeline(flux_config: dict):
    """
    Exécute le pipeline SFTP multi-ressources.
    
    Args:
        flux_config: Configuration des flux depuis config/settings.py
    
    Returns:
        bool: True si succès, False sinon
    """
    print("🚀 Démarrage du pipeline SFTP + AES + DLT (Multi-ressources)")
    print("=" * 70)
    
    # Créer le pipeline DLT
    pipeline = dlt.pipeline(
        pipeline_name="flux_enedis",
        destination="duckdb",
        dataset_name="enedis_multi",
    )
    
    try:
        # Exécuter le chargement
        load_info = pipeline.run(sftp_flux_enedis_multi(flux_config))
        
        print("✅ Pipeline SFTP multi-ressources exécuté avec succès !")
        print(f"📊 Résultats du chargement:")
        print(load_info)
        
        # Vérifier les résultats
        verify_multi_results()
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution du pipeline SFTP multi: {e}")
        return False


def verify_multi_results():
    """
    Vérifie et affiche les résultats du pipeline SFTP multi-ressources.
    """
    print("\\n📈 Vérification des résultats SFTP multi dans DuckDB...")
    
    try:
        conn = duckdb.connect('flux_enedis.duckdb')
        
        # Lister les tables créées
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'enedis_multi'"
        ).fetchall()
        
        if not tables:
            print("❌ Aucune table trouvée dans le schéma enedis_multi")
            return
        
        print(f"📋 Tables créées: {[t[0] for t in tables]}")
        
        # Statistiques par table
        total_records = 0
        for table_name, in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM enedis_multi.{table_name}").fetchone()[0]
                total_records += count
                
                # Statistiques par ZIP source pour les tables de flux
                if table_name.startswith('flux_'):
                    zip_stats = conn.execute(f"""
                        SELECT _source_zip, COUNT(*) as records 
                        FROM enedis_multi.{table_name} 
                        WHERE _source_zip IS NOT NULL
                        GROUP BY _source_zip 
                        ORDER BY records DESC 
                        LIMIT 3
                    """).fetchall()
                    
                    if zip_stats:
                        print("   📦 Top 3 ZIP sources :")
                        for zip_name, count in zip_stats:
                            print(f"      {zip_name}: {count:,} records")
                
            except Exception as e:
                print(f"   ❌ Erreur pour table {table_name}: {e}")
        
        print(f"\\n🎯 Total: {total_records:,} enregistrements chargés")
        conn.close()
        
    except Exception as e:
        print(f"❌ Erreur lors de la vérification SFTP multi: {e}")


def main():
    """
    Point d'entrée principal avec vérification de configuration.
    """
    print("🔧 Pipeline ETL SFTP multi-ressources avec déchiffrement AES")
    print("=" * 70)
    
    # Vérifier la configuration
    try:
        secrets = dlt.secrets
        sftp_config = secrets['sftp']
        aes_config = secrets['aes']
        print("✅ Configuration SFTP et AES trouvée")
        print(f"   SFTP URL configuré: {sftp_config['url'][:20]}...")
        print(f"   Clé AES configurée: {len(aes_config['key'])} caractères")
    except Exception as e:
        print(f"❌ Configuration manquante: {e}")
        print("📝 Configurez .dlt/secrets.toml avec les sections [sftp] et [aes]")
        return False
    
    # Charger la configuration des flux
    from config.settings import FLUX_CONFIG
    
    # Exécuter le pipeline
    success = run_sftp_multi_pipeline(FLUX_CONFIG)
    
    if success:
        print("\\n🎉 Pipeline SFTP multi-ressources terminé avec succès !")
    else:
        print("\\n💥 Pipeline SFTP multi-ressources terminé avec des erreurs")
    
    return success


if __name__ == "__main__":
    main()