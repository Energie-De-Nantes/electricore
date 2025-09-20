#!/usr/bin/env python3
"""
Script de test pour la stratégie de validation Pandera.

Ce script teste les différents modes de validation :
- SCHEMA_ONLY : Validation schéma uniquement (mode par défaut)
- SCHEMA_AND_DATA : Validation complète des données
- OFF : Pas de validation (via désactivation)
"""
import pytest

import os
import polars as pl
from datetime import datetime, timezone

# Import du pipeline typé
from electricore.core.pipelines_polars.facturation_polars import pipeline_facturation_polars

def create_sample_abonnements() -> pl.LazyFrame:
    """Crée des données d'abonnement valides pour les tests."""
    lf = pl.LazyFrame({
        "ref_situation_contractuelle": ["SC001", "SC001", "SC002"],
        "pdl": ["PDL001", "PDL001", "PDL002"],
        "mois_annee": ["janvier 2025", "janvier 2025", "janvier 2025"],
        "debut_lisible": ["1 janvier 2025", "1 janvier 2025", "1 janvier 2025"],
        "fin_lisible": ["31 janvier 2025", "31 janvier 2025", "31 janvier 2025"],
        "debut": [
            datetime(2025, 1, 1),
            datetime(2025, 1, 15),
            datetime(2025, 1, 1),
        ],
        "fin": [
            datetime(2025, 1, 15),
            datetime(2025, 1, 31),
            datetime(2025, 1, 31),
        ],
        "puissance_souscrite": [6.0, 6.0, 9.0],
        "formule_tarifaire_acheminement": ["BASE", "BASE", "HP_HC"],
        "nb_jours": [14, 16, 31],
        "turpe_fixe": [15.0, 17.0, 25.0],
        "turpe_fixe_journalier": [1.0, 1.1, 0.8],
        "data_complete": [True, True, True],
        "nb_sous_periodes": [1, 1, 1],
        "coverage_abo": [1.0, 1.0, 1.0],
        "has_changement": [False, False, False],
    })

    # Convertir les types pour être conformes
    return lf.with_columns([
        pl.col("debut").dt.replace_time_zone("Europe/Paris"),
        pl.col("fin").dt.replace_time_zone("Europe/Paris"),
        pl.col("nb_jours").cast(pl.Int32),
        pl.col("nb_sous_periodes").cast(pl.Int32),
    ])

def create_sample_energies() -> pl.LazyFrame:
    """Crée des données d'énergie valides pour les tests."""
    # Créer les données avec types corrects
    lf = pl.LazyFrame({
        "ref_situation_contractuelle": ["SC001", "SC001", "SC002"],
        "pdl": ["PDL001", "PDL001", "PDL002"],
        "mois_annee": ["janvier 2025", "janvier 2025", "janvier 2025"],
        "debut": [
            datetime(2025, 1, 1),
            datetime(2025, 1, 15),
            datetime(2025, 1, 1),
        ],
        "fin": [
            datetime(2025, 1, 15),
            datetime(2025, 1, 31),
            datetime(2025, 1, 31),
        ],
        "nb_jours": [14, 16, 31],
        "base_energie": [150.0, 180.0, None],
        "hp_energie": [None, None, 200.0],
        "hc_energie": [None, None, 120.0],
        "turpe_variable": [5.0, 6.0, 8.0],
        "data_complete": [True, True, True],
        "nb_sous_periodes": [1, 1, 1],
        "coverage_energie": [1.0, 1.0, 1.0],
        "has_changement": [False, False, False],
        # Ajouter les colonnes manquantes
        "source_avant": ["R151", "R151", "R151"],
        "source_apres": ["R151", "R151", "R151"],
        "periode_irreguliere": [False, False, False],
        # Ajouts optionnels
        "debut_lisible": ["1 janvier 2025", "15 janvier 2025", "1 janvier 2025"],
        "fin_lisible": ["15 janvier 2025", "31 janvier 2025", "31 janvier 2025"],
    })

    # Convertir les types pour être conformes
    return lf.with_columns([
        pl.col("debut").dt.replace_time_zone("Europe/Paris"),
        pl.col("fin").dt.replace_time_zone("Europe/Paris"),
        pl.col("nb_jours").cast(pl.Int32),
        pl.col("nb_sous_periodes").cast(pl.Int32),
    ])

@pytest.mark.skip(reason="Script de test manuel, pas un test unitaire")
def test_validation_mode(mode: str):
    """Teste un mode de validation spécifique."""
    print(f"\n🧪 Test du mode: {mode}")
    print("=" * 50)

    # Configurer le mode
    os.environ["PANDERA_VALIDATION_DEPTH"] = mode

    # Créer les données de test
    abonnements = create_sample_abonnements()
    energies = create_sample_energies()

    try:
        # Exécuter le pipeline
        print(f"Exécution du pipeline avec mode {mode}...")
        result = pipeline_facturation_polars(abonnements, energies)

        print(f"✅ Succès ! {len(result)} méta-périodes générées")
        print(f"Colonnes: {list(result.columns)}")
        print(f"Premier échantillon:\n{result.head(1)}")

    except Exception as e:
        print(f"❌ Erreur: {e}")

    finally:
        # Nettoyer la variable d'environnement
        if "PANDERA_VALIDATION_DEPTH" in os.environ:
            del os.environ["PANDERA_VALIDATION_DEPTH"]

def main():
    """Fonction principale de test."""
    print("🚀 Test de la stratégie de validation Pandera")
    print("=" * 60)

    # Tester les différents modes
    modes = ["SCHEMA_ONLY", "SCHEMA_AND_DATA"]

    for mode in modes:
        test_validation_mode(mode)

    print(f"\n✅ Tests terminés !")

if __name__ == "__main__":
    main()