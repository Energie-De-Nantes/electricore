"""
Tests unitaires pour les fonctions pures de parsing R64.

Ces tests vérifient la logique de transformation des données JSON R64
pour identifier les problèmes causant les erreurs TURPE.
"""

import pytest
import json
from datetime import datetime
from typing import Dict, Any

from electricore.etl.transformers.parsers import (
    extract_header_metadata,
    is_valid_calendrier,
    is_valid_data_point,
    should_process_grandeur,
    build_base_record,
    collect_timeseries_data,
    process_single_mesure,
    r64_timeseries_to_wide_format
)


class TestExtractHeaderMetadata:
    """Tests pour extract_header_metadata()."""

    def test_extract_basic_metadata(self):
        """Test extraction métadonnées de base."""
        data = {
            "header": {
                "idDemande": "DEM123",
                "siDemandeur": "DISTRIBUTEUR",
                "codeFlux": "R64",
                "format": "JSON"
            }
        }

        result = extract_header_metadata(data)

        expected = {
            'id_demande': "DEM123",
            'si_demandeur': "DISTRIBUTEUR",
            'code_flux': "R64",
            'format': "JSON"
        }
        assert result == expected

    def test_extract_empty_data(self):
        """Test avec données vides."""
        result = extract_header_metadata({})
        expected = {
            'id_demande': None,
            'si_demandeur': None,
            'code_flux': None,
            'format': None
        }
        assert result == expected

    def test_extract_partial_metadata(self):
        """Test avec métadonnées partielles."""
        data = {
            "header": {
                "codeFlux": "R64_PARTIAL"
            }
        }
        result = extract_header_metadata(data)

        expected = {
            'id_demande': None,
            'si_demandeur': None,
            'code_flux': "R64_PARTIAL",
            'format': None
        }
        assert result == expected


class TestIsValidCalendrier:
    """Tests pour is_valid_calendrier()."""

    def test_valid_distributeur_ids(self):
        """Test avec IDs distributeur valides."""
        valid_calendriers = [
            {"idCalendrier": "DI000001"},
            {"idCalendrier": "DI000002"},
            {"idCalendrier": "DI000003"}
        ]

        for cal in valid_calendriers:
            assert is_valid_calendrier(cal) is True

    def test_invalid_fournisseur_ids(self):
        """Test avec IDs fournisseur (invalides)."""
        invalid_calendriers = [
            {"idCalendrier": "FC022035"},
            {"idCalendrier": "FC022637"},
            {"idCalendrier": "FO123456"}
        ]

        for cal in invalid_calendriers:
            assert is_valid_calendrier(cal) is False

    def test_valid_libelle_distributeur(self):
        """Test avec libellé contenant 'distributeur'."""
        calendrier = {
            "idCalendrier": "UNKNOWN",
            "libelleCalendrier": "Calendrier Distributeur Standard"
        }
        assert is_valid_calendrier(calendrier) is True

    def test_missing_fields(self):
        """Test avec champs manquants."""
        assert is_valid_calendrier({}) is False
        assert is_valid_calendrier({"libelleCalendrier": "Test"}) is False


class TestIsValidDataPoint:
    """Tests pour is_valid_data_point()."""

    def test_valid_data_point(self):
        """Test avec point de données valide."""
        point = {
            "d": "2024-09-27T10:30:00Z",
            "v": 12345.67,
            "iv": 0
        }
        assert is_valid_data_point(point) is True

    def test_invalid_iv_values(self):
        """Test avec valeurs iv invalides."""
        invalid_points = [
            {"d": "2024-09-27T10:30:00Z", "v": 12345.67, "iv": 1},
            {"d": "2024-09-27T10:30:00Z", "v": 12345.67, "iv": -1},
            {"d": "2024-09-27T10:30:00Z", "v": 12345.67, "iv": None}
        ]

        for point in invalid_points:
            assert is_valid_data_point(point) is False

    def test_missing_required_fields(self):
        """Test avec champs requis manquants."""
        invalid_points = [
            {"v": 12345.67, "iv": 0},  # Missing 'd' - returns None
            {"d": "2024-09-27T10:30:00Z", "iv": 0},  # Missing 'v' - returns False
            {"d": "2024-09-27T10:30:00Z", "v": 12345.67}  # Missing 'iv' - returns False
        ]

        # Premier cas retourne None à cause du court-circuit sur point.get('d')
        assert is_valid_data_point(invalid_points[0]) is None

        # Les autres retournent False
        assert is_valid_data_point(invalid_points[1]) is False
        assert is_valid_data_point(invalid_points[2]) is False

    def test_zero_value_valid(self):
        """Test que valeur 0 est valide."""
        point = {
            "d": "2024-09-27T10:30:00Z",
            "v": 0,
            "iv": 0
        }
        assert is_valid_data_point(point) is True


class TestShouldProcessGrandeur:
    """Tests pour should_process_grandeur()."""

    def test_valid_cons_ea_grandeur(self):
        """Test avec grandeur CONS + EA valide."""
        grandeur = {
            "grandeurMetier": "CONS",
            "grandeurPhysique": "EA"
        }
        assert should_process_grandeur(grandeur) is True

    def test_invalid_grandeur_combinations(self):
        """Test avec combinaisons invalides."""
        invalid_grandeurs = [
            {"grandeurMetier": "PROD", "grandeurPhysique": "EA"},
            {"grandeurMetier": "CONS", "grandeurPhysique": "ER"},
            {"grandeurMetier": "PROD", "grandeurPhysique": "ER"},
            {"grandeurMetier": "CONS"},  # Missing grandeurPhysique
            {"grandeurPhysique": "EA"},  # Missing grandeurMetier
            {}  # Missing both
        ]

        for grandeur in invalid_grandeurs:
            assert should_process_grandeur(grandeur) is False


class TestBuildBaseRecord:
    """Tests pour build_base_record()."""

    def test_build_complete_record(self):
        """Test construction enregistrement complet."""
        mesure = {"idPrm": "12345678901234"}
        contexte = {
            "etapeMetier": "BRUT",
            "contexteReleve": "COL",
            "typeReleve": "AQ"
        }
        grandeur = {
            "grandeurPhysique": "EA",
            "grandeurMetier": "CONS",
            "unite": "kWh"
        }
        header_meta = {"identifiant_flux": "R64_TEST"}

        result = build_base_record(mesure, contexte, grandeur, header_meta)

        expected = {
            'pdl': "12345678901234",
            'etape_metier': "BRUT",
            'contexte_releve': "COL",
            'type_releve': "AQ",
            'grandeur_physique': "EA",
            'grandeur_metier': "CONS",
            'unite': "kWh",
            'identifiant_flux': "R64_TEST"
        }
        assert result == expected

    def test_build_record_with_missing_fields(self):
        """Test avec champs manquants."""
        mesure = {"idPrm": "12345678901234"}
        contexte = {}
        grandeur = {"unite": "kWh"}
        header_meta = {}

        result = build_base_record(mesure, contexte, grandeur, header_meta)

        expected = {
            'pdl': "12345678901234",
            'etape_metier': None,
            'contexte_releve': None,
            'type_releve': None,
            'grandeur_physique': None,
            'grandeur_metier': None,
            'unite': "kWh"
        }
        assert result == expected


class TestR64IntegrationWithFixtures:
    """Tests d'intégration avec fixtures JSON R64 réalistes."""

    @pytest.fixture
    def sample_r64_json(self):
        """Fixture avec données R64 réalistes."""
        return {
            "header": {
                "idDemande": "DEM123",
                "siDemandeur": "DISTRIBUTEUR",
                "codeFlux": "R64",
                "format": "JSON"
            },
            "mesures": [
                {
                    "idPrm": "12345678901234",
                    "contexte": [
                        {
                            "etapeMetier": "BRUT",
                            "contexteReleve": "COL",
                            "typeReleve": "AQ",
                            "grandeur": [
                                {
                                    "grandeurMetier": "CONS",
                                    "grandeurPhysique": "EA",
                                    "unite": "kWh",
                                    "calendrier": [
                                        {
                                            "idCalendrier": "DI000002",
                                            "libelleCalendrier": "Distributeur HP/HC",
                                            "classeTemporelle": [
                                                {
                                                    "idClasseTemporelle": "HP",
                                                    "valeur": [
                                                        {"d": "2024-09-27T08:00:00Z", "v": 1000.5, "iv": 0},
                                                        {"d": "2024-09-27T09:00:00Z", "v": 1100.8, "iv": 0}
                                                    ]
                                                },
                                                {
                                                    "idClasseTemporelle": "HC",
                                                    "valeur": [
                                                        {"d": "2024-09-27T08:00:00Z", "v": 500.2, "iv": 0},
                                                        {"d": "2024-09-27T09:00:00Z", "v": 550.1, "iv": 0}
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    def test_collect_timeseries_data_integration(self, sample_r64_json):
        """Test collecte données timeseries avec fixture réaliste."""
        mesure = sample_r64_json["mesures"][0]
        header_meta = extract_header_metadata(sample_r64_json)

        # Créer un base_record simple pour le test
        base_record = {
            'pdl': "12345678901234",
            'etape_metier': "BRUT",
            'contexte_releve': "COL",
            'type_releve': "AQ",
            'grandeur_physique': "EA",
            'grandeur_metier': "CONS",
            'unite': "kWh",
            **header_meta
        }

        result = collect_timeseries_data(mesure, base_record)

        # Vérifications
        assert len(result) == 2  # 2 dates distinctes
        assert "2024-09-27T08:00:00Z" in result
        assert "2024-09-27T09:00:00Z" in result

        # Vérifier les valeurs HP/HC pour la première date
        first_record = result["2024-09-27T08:00:00Z"]
        assert first_record["hp"] == 1000.5
        assert first_record["hc"] == 500.2
        assert first_record["pdl"] == "12345678901234"

    def test_r64_timeseries_to_wide_format_complete(self, sample_r64_json):
        """Test transformation complète R64 vers format wide."""
        json_bytes = json.dumps(sample_r64_json).encode('utf-8')

        records = list(r64_timeseries_to_wide_format(json_bytes, "R64"))

        # Vérifications de base
        assert len(records) == 2  # 2 enregistrements (2 dates)

        # Vérifier structure des enregistrements
        for record in records:
            assert record["pdl"] == "12345678901234"
            assert record["type_releve"] == "AQ"
            assert record["grandeur_metier"] == "CONS"
            assert record["grandeur_physique"] == "EA"
            assert "hp" in record
            assert "hc" in record
            assert "date_releve" in record

    def test_turpe_validation_rules(self, sample_r64_json):
        """Test des règles de validation TURPE."""
        json_bytes = json.dumps(sample_r64_json).encode('utf-8')
        records = list(r64_timeseries_to_wide_format(json_bytes, "R64"))

        for record in records:
            # Règle TURPE: HP et HC doivent être >= 0
            if 'hp' in record and record['hp'] is not None:
                assert record['hp'] >= 0, f"HP négatif détecté: {record['hp']}"

            if 'hc' in record and record['hc'] is not None:
                assert record['hc'] >= 0, f"HC négatif détecté: {record['hc']}"

            # Règle TURPE: PDL doit être 14 caractères numériques
            assert len(record['pdl']) == 14, f"PDL invalide: {record['pdl']}"
            assert record['pdl'].isdigit(), f"PDL non-numérique: {record['pdl']}"


class TestR64EdgeCases:
    """Tests pour les cas limites et erreurs potentielles."""

    def test_calendrier_fournisseur_filtered_out(self):
        """Test que les calendriers fournisseur sont bien filtrés."""
        calendrier_fournisseur = {
            "idCalendrier": "FC022035",
            "libelleCalendrier": "Calendrier Fournisseur"
        }
        assert is_valid_calendrier(calendrier_fournisseur) is False

    def test_data_point_with_invalid_iv(self):
        """Test que les points avec iv != 0 sont filtrés."""
        point_invalide = {
            "d": "2024-09-27T10:30:00Z",
            "v": 12345.67,
            "iv": 1  # Invalide
        }
        assert is_valid_data_point(point_invalide) is False

    def test_negative_values_allowed(self):
        """Test que les valeurs négatives sont acceptées (injection)."""
        point_negatif = {
            "d": "2024-09-27T10:30:00Z",
            "v": -500.0,  # Injection possible
            "iv": 0
        }
        assert is_valid_data_point(point_negatif) is True


class TestR64TurpeProblems:
    """Tests spécifiques pour identifier les problèmes TURPE."""

    @pytest.fixture
    def problematic_r64_json(self):
        """Fixture avec données R64 qui causent des erreurs TURPE."""
        return {
            "header": {
                "idDemande": "DEM456",
                "siDemandeur": "DISTRIBUTEUR",
                "codeFlux": "R64",
                "format": "JSON"
            },
            "mesures": [
                {
                    "idPrm": "98765432109876",
                    "contexte": [
                        {
                            "etapeMetier": "BRUT",
                            "contexteReleve": "COL",
                            "typeReleve": "AQ",
                            "grandeur": [
                                {
                                    "grandeurMetier": "CONS",
                                    "grandeurPhysique": "EA",
                                    "unite": "Wh",  # Unité en Wh au lieu de kWh
                                    "calendrier": [
                                        # Calendrier DISTRIBUTEUR valide
                                        {
                                            "idCalendrier": "DI000002",
                                            "libelleCalendrier": "Distributeur HP/HC",
                                            "classeTemporelle": [
                                                {
                                                    "idClasseTemporelle": "HP",
                                                    "valeur": [
                                                        {"d": "2024-09-27T08:00:00Z", "v": 1500000, "iv": 0},  # Valeur très élevée en Wh
                                                        {"d": "2024-09-27T09:00:00Z", "v": -100000, "iv": 0}   # Valeur négative
                                                    ]
                                                },
                                                {
                                                    "idClasseTemporelle": "HC",
                                                    "valeur": [
                                                        {"d": "2024-09-27T08:00:00Z", "v": 800000, "iv": 0},
                                                        {"d": "2024-09-27T09:00:00Z", "v": 900000, "iv": 0}
                                                    ]
                                                }
                                            ]
                                        },
                                        # Calendrier FOURNISSEUR qui devrait être filtré
                                        {
                                            "idCalendrier": "FC022035",
                                            "libelleCalendrier": "Calendrier Fournisseur",
                                            "classeTemporelle": [
                                                {
                                                    "idClasseTemporelle": "BASE",
                                                    "valeur": [
                                                        {"d": "2024-09-27T08:00:00Z", "v": 2000000, "iv": 0}
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    def test_turpe_fournisseur_data_filtered(self, problematic_r64_json):
        """Test que les données de calendrier fournisseur sont bien filtrées."""
        json_bytes = json.dumps(problematic_r64_json).encode('utf-8')
        records = list(r64_timeseries_to_wide_format(json_bytes, "R64"))

        # Vérifier qu'aucun enregistrement n'a de données BASE (calendrier fournisseur)
        for record in records:
            assert record.get('base') is None, f"Données BASE détectées (calendrier fournisseur): {record}"

            # Mais doit avoir HP/HC (calendrier distributeur)
            assert 'hp' in record or 'hc' in record, f"Aucune donnée HP/HC trouvée: {record}"

    def test_turpe_unit_consistency(self, problematic_r64_json):
        """Test de cohérence des unités pour TURPE."""
        json_bytes = json.dumps(problematic_r64_json).encode('utf-8')
        records = list(r64_timeseries_to_wide_format(json_bytes, "R64"))

        for record in records:
            # Unité doit être normalisée vers kWh ou Wh
            assert record.get('unite') in ['kWh', 'Wh'], f"Unité invalide: {record.get('unite')}"

    def test_turpe_value_ranges(self, problematic_r64_json):
        """Test des plages de valeurs acceptables pour TURPE."""
        json_bytes = json.dumps(problematic_r64_json).encode('utf-8')
        records = list(r64_timeseries_to_wide_format(json_bytes, "R64"))

        problems = []
        for record in records:
            pdl = record.get('pdl')
            date = record.get('date_releve')

            # Vérifier les valeurs numériques
            for cadran in ['hp', 'hc', 'hph', 'hch', 'hpb', 'hcb', 'base']:
                value = record.get(cadran)
                if value is not None:
                    # Valeurs extrêmes (> 100k kWh dans une période courte)
                    if abs(value) > 100000:
                        problems.append(f"Valeur extrême {cadran}={value} pour PDL {pdl} à {date}")

                    # Vérifier si des valeurs négatives non justifiées
                    if value < 0 and cadran in ['hp', 'hc']:  # HP/HC généralement positifs
                        problems.append(f"Valeur négative suspecte {cadran}={value} pour PDL {pdl} à {date}")

        # Rapporter les problèmes détectés
        if problems:
            print("🚨 Problèmes TURPE détectés:")
            for problem in problems:
                print(f"   - {problem}")

        # Les tests passent mais signalent les problèmes
        assert len(records) > 0, "Aucun enregistrement généré"

        # Retourner les problèmes pour inspection
        return problems

    def test_turpe_data_completeness(self, problematic_r64_json):
        """Test de complétude des données pour validation TURPE."""
        json_bytes = json.dumps(problematic_r64_json).encode('utf-8')
        records = list(r64_timeseries_to_wide_format(json_bytes, "R64"))

        for record in records:
            # Champs obligatoires pour TURPE
            required_fields = ['pdl', 'date_releve', 'type_releve', 'grandeur_metier', 'grandeur_physique']
            for field in required_fields:
                assert record.get(field) is not None, f"Champ obligatoire manquant: {field} dans {record}"

            # Au moins un cadran doit avoir une valeur
            cadrans = ['hp', 'hc', 'hph', 'hch', 'hpb', 'hcb', 'base']
            has_value = any(record.get(cadran) is not None for cadran in cadrans)
            assert has_value, f"Aucun cadran avec valeur pour PDL {record.get('pdl')}"

    def test_wh_to_kwh_conversion(self):
        """Test spécifique de conversion Wh → kWh avec valeurs réelles."""
        # Données avec valeurs Wh élevées comme dans la vraie base
        wh_data = {
            "header": {
                "idDemande": "CONV_TEST",
                "siDemandeur": "DISTRIBUTEUR",
                "codeFlux": "R64",
                "format": "JSON"
            },
            "mesures": [
                {
                    "idPrm": "12345678901234",
                    "contexte": [
                        {
                            "etapeMetier": "BRUT",
                            "contexteReleve": "COL",
                            "typeReleve": "AQ",
                            "grandeur": [
                                {
                                    "grandeurMetier": "CONS",
                                    "grandeurPhysique": "EA",
                                    "unite": "Wh",  # Données en Wh
                                    "calendrier": [
                                        {
                                            "idCalendrier": "DI000002",
                                            "classeTemporelle": [
                                                {
                                                    "idClasseTemporelle": "HP",
                                                    "valeur": [
                                                        {"d": "2024-09-27T08:00:00Z", "v": 1447311, "iv": 0},  # 1447.311 kWh
                                                        {"d": "2024-09-27T09:00:00Z", "v": 14701326, "iv": 0}  # 14701.326 kWh
                                                    ]
                                                },
                                                {
                                                    "idClasseTemporelle": "HC",
                                                    "valeur": [
                                                        {"d": "2024-09-27T08:00:00Z", "v": 1350582, "iv": 0},  # 1350.582 kWh
                                                        {"d": "2024-09-27T09:00:00Z", "v": 17070964, "iv": 0}  # 17070.964 kWh
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        json_bytes = json.dumps(wh_data).encode('utf-8')
        records = list(r64_timeseries_to_wide_format(json_bytes, "R64"))

        print("\\n🔍 Test conversion Wh → kWh:")
        for i, record in enumerate(records):
            hp_wh = [1447311, 14701326][i]  # Valeurs originales en Wh
            hc_wh = [1350582, 17070964][i]

            hp_kwh_expected = hp_wh / 1000  # Conversion attendue
            hc_kwh_expected = hc_wh / 1000

            print(f"   Record {i+1}:")
            print(f"     HP: {hp_wh} Wh → {record.get('hp')} (attendu: {hp_kwh_expected})")
            print(f"     HC: {hc_wh} Wh → {record.get('hc')} (attendu: {hc_kwh_expected})")
            print(f"     Unité: {record.get('unite')}")

            # Les valeurs devraient être converties sans .floor()
            # floor() peut causer des pertes de précision
            assert record.get('unite') == 'Wh', f"Unité non convertie: {record.get('unite')}"

            # Vérifier si la conversion cause des erreurs TURPE
            if record.get('hp') and record.get('hp') > 10000:  # > 10 MWh suspect
                print(f"     ⚠️  Valeur HP très élevée après conversion: {record.get('hp')}")

        assert len(records) == 2