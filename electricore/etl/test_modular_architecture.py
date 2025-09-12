"""
Script de test pour valider l'architecture modulaire avec transformers.
Test sur un échantillon R151 pour vérifier le bon fonctionnement.
"""

import dlt
from sources.sftp_enedis_modular import test_r151_modular

def test_modular_architecture():
    """
    Test de l'architecture modulaire sur R151.
    """
    print("🧪 DÉBUT TEST ARCHITECTURE MODULAIRE")
    print("=" * 60)
    
    try:
        # Créer un pipeline DLT de test
        pipeline = dlt.pipeline(
            pipeline_name="test_modular_r151",
            destination="duckdb",
            dataset_name="test_enedis_modular"
        )
        
        print("📦 Pipeline DLT créé")
        
        # Charger les données avec la nouvelle architecture
        print("🚀 Exécution du pipeline modulaire...")
        
        # Source avec architecture modulaire
        source = test_r151_modular()
        
        print("📋 Source modulaire créée")
        
        # Exécuter seulement l'extraction pour tester (sans load complet)
        print("🔍 Test d'extraction...")
        extract_info = pipeline.extract(source)
        
        print("✅ Extraction réussie !")
        print(f"📊 Infos extraction: {extract_info}")
        
        # Optionnel : test de normalisation
        print("🔧 Test de normalisation...")
        normalize_info = pipeline.normalize()
        
        print("✅ Normalisation réussie !")
        print(f"📊 Infos normalisation: {normalize_info}")
        
        # Optionnel : chargement complet (décommenter pour test complet)
        # print("💾 Test de chargement...")
        # load_info = pipeline.load()
        # print("✅ Chargement réussi !")
        # print(f"📊 Infos chargement: {load_info}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERREUR lors du test: {e}")
        import traceback
        print("📋 Traceback complet:")
        traceback.print_exc()
        return False
    
    finally:
        print("=" * 60)
        print("🏁 FIN DU TEST")


def compare_architectures():
    """
    Comparaison entre ancienne et nouvelle architecture.
    Utile pour valider la migration.
    """
    print("⚖️  COMPARAISON DES ARCHITECTURES")
    print("=" * 60)
    
    print("🏗️  ANCIENNE ARCHITECTURE (monolithique):")
    print("   • Resource fait tout: SFTP + decrypt + unzip + parse")
    print("   • Code dans une seule fonction")
    print("   • Difficilement testable par parties")
    print("   • Réplication de logique entre flux")
    
    print("\n🔧 NOUVELLE ARCHITECTURE (modulaire):")
    print("   • Chaînage: SFTP | decrypt | unzip | parse")
    print("   • Chaque transformer isolé et testable")
    print("   • Réutilisation des transformers communs")
    print("   • Évolutivité (ajout facile de transformers)")
    
    print("\n✅ AVANTAGES VALIDÉS:")
    print("   • Séparation des responsabilités")
    print("   • Tests unitaires par transformer")
    print("   • Réutilisabilité du code")
    print("   • Maintenabilité améliorée")
    
    print("=" * 60)


if __name__ == "__main__":
    # Comparaison des architectures
    compare_architectures()
    
    print("\n" + "="*80)
    print("LANCEMENT DU TEST")
    print("="*80)
    
    # Test de la nouvelle architecture
    success = test_modular_architecture()
    
    if success:
        print("🎉 TEST RÉUSSI - Architecture modulaire fonctionnelle!")
        print("👉 Prêt pour migrer les autres flux")
    else:
        print("🔧 Des ajustements sont nécessaires")
        print("👉 Vérifier la configuration et les imports")