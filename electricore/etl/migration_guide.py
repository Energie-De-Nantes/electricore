"""
Guide et outils pour migrer vers l'architecture modulaire.
Comparaisons et utilitaires de migration.
"""

import dlt
from typing import Dict, Any


def compare_pipeline_outputs(
    old_source_func,
    new_source_func, 
    flux_config: Dict[str, Any],
    max_records: int = 100
):
    """
    Compare les sorties des deux architectures pour valider la migration.
    
    Args:
        old_source_func: Fonction de l'ancienne source
        new_source_func: Fonction de la nouvelle source  
        flux_config: Configuration des flux
        max_records: Nombre max d'enregistrements à comparer
    """
    print("⚖️  COMPARAISON DES ARCHITECTURES")
    print("=" * 60)
    
    try:
        # Pipeline pour ancienne architecture
        old_pipeline = dlt.pipeline(
            pipeline_name="compare_old",
            destination="duckdb",
            dataset_name="compare_old"
        )
        
        # Pipeline pour nouvelle architecture  
        new_pipeline = dlt.pipeline(
            pipeline_name="compare_new",
            destination="duckdb", 
            dataset_name="compare_new"
        )
        
        print("🔍 Extraction avec ancienne architecture...")
        old_source = old_source_func(flux_config)
        old_extract = old_pipeline.extract(old_source)
        
        print("🔍 Extraction avec nouvelle architecture...")
        new_source = new_source_func(flux_config)
        new_extract = new_pipeline.extract(new_source)
        
        print(f"📊 Ancienne: {old_extract}")
        print(f"📊 Nouvelle: {new_extract}")
        
        # Ici on pourrait ajouter des comparaisons plus détaillées
        # des données extraites si nécessaire
        
        print("✅ Comparaison terminée - vérifiez les résultats")
        
    except Exception as e:
        print(f"❌ Erreur lors de la comparaison: {e}")
        import traceback
        traceback.print_exc()


def migration_checklist():
    """
    Checklist pour la migration vers l'architecture modulaire.
    """
    print("📋 CHECKLIST DE MIGRATION")
    print("=" * 50)
    
    checklist = [
        "✅ Modules créés (sources/base, transformers/*)",
        "✅ Tests unitaires des transformers écrits",
        "🔲 Test complet sur flux simple (R151)",
        "🔲 Comparaison ancienne vs nouvelle architecture",
        "🔲 Migration de tous les flux",
        "🔲 Tests d'intégration complets",
        "🔲 Vérification des performances",
        "🔲 Mise à jour de la documentation",
        "🔲 Formation équipe sur nouvelle architecture",
        "🔲 Déploiement progressif en production"
    ]
    
    for item in checklist:
        print(f"  {item}")
    
    print("=" * 50)
    
    return checklist


def architecture_benefits():
    """
    Résumé des bénéfices de la nouvelle architecture.
    """
    print("🎯 BÉNÉFICES DE L'ARCHITECTURE MODULAIRE")
    print("=" * 50)
    
    benefits = {
        "🧪 Testabilité": [
            "Chaque transformer testable isolément",
            "Tests unitaires plus faciles à écrire",
            "Mocking simplifié pour les tests"
        ],
        "🔄 Réutilisabilité": [
            "Transformers communs entre flux",
            "Décryptage AES réutilisé partout", 
            "Extraction ZIP généralisée",
            "Parsing XML/CSV configurable"
        ],
        "🛠️  Maintenabilité": [
            "Séparation claire des responsabilités",
            "Code plus modulaire et organisé",
            "Ajout de nouveaux flux simplifié",
            "Debug plus facile (erreurs isolées)"
        ],
        "⚡ Performance": [
            "Clés AES chargées une seule fois",
            "Optimisations Polars préservées",
            "Possibilité d'ajouter @dlt.defer",
            "Parallélisation granulaire"
        ],
        "🚀 Évolutivité": [
            "Ajout facile de nouveaux transformers",
            "Enrichissement avec APIs externes",
            "Validation Pandera intégrable",
            "Chaînage conditionnel possible"
        ]
    }
    
    for category, items in benefits.items():
        print(f"\n{category}:")
        for item in items:
            print(f"  • {item}")
    
    print("\n" + "=" * 50)
    
    return benefits


def next_steps():
    """
    Prochaines étapes recommandées.
    """
    print("👉 PROCHAINES ÉTAPES RECOMMANDÉES")
    print("=" * 50)
    
    steps = [
        {
            "step": "1. Tests immédiats",
            "actions": [
                "Tester le script test_modular_architecture.py",
                "Vérifier que l'extraction fonctionne",
                "Comparer quelques enregistrements avec l'ancienne version"
            ]
        },
        {
            "step": "2. Tests unitaires",
            "actions": [
                "Créer tests pour chaque transformer",
                "Mocker les dépendances (SFTP, AES)",
                "Valider les transformations de données"
            ]
        },
        {
            "step": "3. Migration progressive",
            "actions": [
                "Commencer par un flux simple (R151)",
                "Valider en parallèle avec l'ancienne version", 
                "Migrer flux par flux avec validation"
            ]
        },
        {
            "step": "4. Optimisations",
            "actions": [
                "Ajouter @dlt.defer sur transformers lourds",
                "Mesurer les performances vs ancienne version",
                "Optimiser les goulots d'étranglement identifiés"
            ]
        }
    ]
    
    for i, step_info in enumerate(steps, 1):
        print(f"\n{step_info['step']}:")
        for action in step_info['actions']:
            print(f"  • {action}")
    
    print("\n" + "=" * 50)
    
    return steps


if __name__ == "__main__":
    # Afficher tous les guides
    migration_checklist()
    print("\n")
    architecture_benefits()
    print("\n") 
    next_steps()
    
    print("\n🎉 CONCLUSION:")
    print("L'architecture modulaire apporte des bénéfices significatifs")
    print("pour la maintenabilité et l'évolutivité de votre ETL Enedis.")
    print("La migration progressive est recommandée pour minimiser les risques.")