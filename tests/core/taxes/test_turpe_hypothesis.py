"""
Tests property-based avec Hypothesis pour les fonctions pures du module TURPE.

Ce module démontre l'usage d'Hypothesis pour tester des propriétés invariantes
des fonctions de calcul TURPE variable, complémentant les tests classiques.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from hypothesis import given, assume, strategies as st
from hypothesis.extra import pandas as pd_st

from electricore.core.taxes.turpe import (
    joindre_avec_regles,
    valider_regles_presentes, 
    filtrer_validite_temporelle,
    calculer_contributions_turpe,
    aggreger_par_periode_originale,
    calculer_turpe_variable
)

PARIS_TZ = ZoneInfo("Europe/Paris")


# ==================================================================================
# STRATÉGIES DE BASE POUR LES DONNÉES TURPE
# ==================================================================================

@st.composite
def fta_strategy(draw):
    """Génère des FTA réalistes selon la nomenclature Enedis."""
    segments = ["BTINF", "BTSUPU", "HTA5", "HTA8"] 
    options = ["", "CU4", "MU4", "CUST", "MUDT"]
    
    segment = draw(st.sampled_from(segments))
    option = draw(st.sampled_from(options))
    
    return f"{segment}{option}".rstrip()


@st.composite  
def dates_paris_strategy(draw, min_year=2020, max_year=2030):
    """Génère des dates avec timezone Europe/Paris."""
    dt = draw(st.datetimes(
        min_value=datetime(min_year, 1, 1),
        max_value=datetime(max_year, 12, 31)
    ))
    return dt.replace(tzinfo=PARIS_TZ)


@st.composite
def regles_turpe_strategy(draw, ftas=None):
    """Génère des règles TURPE cohérentes avec contraintes métier."""
    if ftas is None:
        ftas = draw(st.lists(fta_strategy(), min_size=1, max_size=5, unique=True))
    
    # Dates de validité cohérentes
    start_date = draw(dates_paris_strategy(min_year=2020, max_year=2025))
    
    end_date = draw(st.one_of(
        st.none(),  # Pas de fin (règle permanente)
        dates_paris_strategy(min_year=2025, max_year=2030).filter(
            lambda d: d > start_date
        )
    ))
    
    # Générer tarifs cohérents selon le type de compteur
    def generer_tarifs(fta):
        if "CU4" in fta or "MU4" in fta:  # 4 cadrans
            return {
                'HPH': draw(st.floats(min_value=1.0, max_value=10.0)),
                'HCH': draw(st.floats(min_value=0.5, max_value=8.0)), 
                'HPB': draw(st.floats(min_value=0.8, max_value=5.0)),
                'HCB': draw(st.floats(min_value=0.3, max_value=3.0)),
                'HP': 0.0, 'HC': 0.0, 'BASE': 0.0
            }
        elif "CUST" in fta:  # Simple
            return {
                'BASE': draw(st.floats(min_value=2.0, max_value=6.0)),
                'HPH': 0.0, 'HCH': 0.0, 'HPB': 0.0, 'HCB': 0.0,
                'HP': 0.0, 'HC': 0.0
            }
        else:  # Bi-horaire par défaut
            return {
                'HP': draw(st.floats(min_value=1.0, max_value=8.0)),
                'HC': draw(st.floats(min_value=0.5, max_value=5.0)),
                'HPH': 0.0, 'HCH': 0.0, 'HPB': 0.0, 'HCB': 0.0, 'BASE': 0.0
            }
    
    # Construire DataFrame
    data = []
    for fta in ftas:
        tarifs = generer_tarifs(fta)
        data.append({
            'Formule_Tarifaire_Acheminement': fta,
            'start': start_date,
            'end': end_date,
            'cg': draw(st.floats(min_value=10.0, max_value=25.0)),
            'cc': draw(st.floats(min_value=15.0, max_value=30.0)),
            'b': draw(st.floats(min_value=5.0, max_value=15.0)),
            **tarifs
        })
    
    return pd.DataFrame(data)


def generer_energies_coherentes(draw, fta):
    """Génère des valeurs d'énergie cohérentes avec le type de compteur."""
    if "CU4" in fta or "MU4" in fta:  # 4 cadrans
        total_energie = draw(st.floats(min_value=100, max_value=5000))
        part_hiver = draw(st.floats(min_value=0.4, max_value=0.8))
        part_hp = draw(st.floats(min_value=0.2, max_value=0.5))
        
        energie_hiver = total_energie * part_hiver
        energie_ete = total_energie * (1 - part_hiver)
        
        return {
            'HPH_energie': energie_hiver * part_hp,
            'HCH_energie': energie_hiver * (1 - part_hp),
            'HPB_energie': energie_ete * part_hp, 
            'HCB_energie': energie_ete * (1 - part_hp),
            'HP_energie': 0.0, 'HC_energie': 0.0, 'BASE_energie': 0.0
        }
    elif "CUST" in fta:  # Base simple
        return {
            'BASE_energie': draw(st.floats(min_value=100, max_value=3000)),
            'HPH_energie': 0.0, 'HCH_energie': 0.0, 'HPB_energie': 0.0, 
            'HCB_energie': 0.0, 'HP_energie': 0.0, 'HC_energie': 0.0
        }
    else:  # Bi-horaire
        total_energie = draw(st.floats(min_value=100, max_value=4000))
        part_hp = draw(st.floats(min_value=0.2, max_value=0.5))
        
        return {
            'HP_energie': total_energie * part_hp,
            'HC_energie': total_energie * (1 - part_hp),
            'HPH_energie': 0.0, 'HCH_energie': 0.0, 'HPB_energie': 0.0,
            'HCB_energie': 0.0, 'BASE_energie': 0.0
        }


@st.composite
def periodes_energie_strategy(draw, ftas=None, nb_periodes=None, date_base=None):
    """Génère des périodes d'énergie avec énergies cohérentes."""
    if nb_periodes is None:
        nb_periodes = draw(st.integers(min_value=1, max_value=20))

    if ftas is None:
        ftas = draw(st.lists(fta_strategy(), min_size=1, max_size=5, unique=True))

    # Périodes chronologiques - utiliser date_base si fournie pour cohérence temporelle
    if date_base is None:
        start_date = draw(dates_paris_strategy(min_year=2023, max_year=2024))
    else:
        # Générer dates dans une plage restreinte pour garantir la validité temporelle
        # Décalage maximum de 90 jours pour éviter de dépasser la validité des règles
        delta_days = draw(st.integers(min_value=0, max_value=90))
        start_date = date_base + timedelta(days=delta_days)
    
    periodes = []
    current_date = start_date
    
    for i in range(nb_periodes):
        nb_jours = draw(st.integers(min_value=1, max_value=31))
        date_fin = current_date + timedelta(days=nb_jours)
        fta = draw(st.sampled_from(ftas))
        
        energies = generer_energies_coherentes(draw, fta)
        
        periode = {
            'pdl': f"PDL{i:03d}",
            'debut': current_date,
            'fin': date_fin,
            'nb_jours': nb_jours,
            'Formule_Tarifaire_Acheminement': fta,
            **energies
        }
        periodes.append(periode)
        current_date = date_fin
    
    return pd.DataFrame(periodes)


# ==================================================================================
# TESTS PROPERTY-BASED POUR LES FONCTIONS PURES
# ==================================================================================

class TestJoindreAvecRegles:
    """Tests property-based pour joindre_avec_regles."""
    
    @given(
        regles=regles_turpe_strategy(),
        periodes=periodes_energie_strategy()
    )
    def test_preserve_toutes_periodes(self, regles, periodes):
        """La jointure doit préserver toutes les périodes (left join)."""
        result = joindre_avec_regles(regles, periodes)
        
        # Propriété : nombre de lignes >= périodes originales
        assert len(result) >= len(periodes)
        
        # Propriété : index original préservé
        assert '_index_original' in result.columns
        assert set(result['_index_original']) == set(range(len(periodes)))
    
    @given(
        regles=regles_turpe_strategy(),
        periodes=periodes_energie_strategy()
    )
    def test_ajoute_colonnes_regles(self, regles, periodes):
        """La jointure doit ajouter les colonnes des règles."""
        result = joindre_avec_regles(regles, periodes)
        
        # Propriété : colonnes des règles ajoutées
        colonnes_regles = ['start', 'end', 'cg', 'cc', 'b', 
                          'HPH', 'HCH', 'HPB', 'HCB', 'HP', 'HC', 'BASE']
        for col in colonnes_regles:
            assert col in result.columns
    
    @given(
        ftas=st.lists(fta_strategy(), min_size=2, max_size=4, unique=True),
        data=st.data()
    )
    def test_jointure_avec_ftas_communes(self, ftas, data):
        """Test avec FTA communes garanties entre règles et périodes."""
        regles = data.draw(regles_turpe_strategy(ftas=ftas))
        periodes = data.draw(periodes_energie_strategy(ftas=ftas))
        
        result = joindre_avec_regles(regles, periodes)
        
        # Propriété : pas de règles manquantes (start non null)
        assert not result['start'].isna().any()


class TestValiderReglesPresentes:
    """Tests property-based pour valider_regles_presentes."""
    
    @given(data=st.data())
    def test_passe_si_regles_completes(self, data):
        """La validation doit passer si toutes les règles sont présentes."""
        # Créer DataFrame avec règles complètes
        nb_regles = data.draw(st.integers(min_value=1, max_value=5))
        ftas = data.draw(st.lists(st.text(min_size=1), min_size=nb_regles, max_size=nb_regles))
        starts = data.draw(st.lists(dates_paris_strategy(), min_size=nb_regles, max_size=nb_regles))
        
        df = pd.DataFrame({
            'Formule_Tarifaire_Acheminement': ftas,
            'start': starts
        })
        
        # Aucun start NaN -> doit passer
        result = valider_regles_presentes(df)
        assert result is df  # Retourne le DataFrame original
    
    @given(
        ftas=st.lists(fta_strategy(), min_size=2, max_size=5, unique=True),
        data=st.data()
    )
    def test_echoue_si_regles_manquantes(self, ftas, data):
        """La validation doit échouer si des règles manquent."""
        # Créer DataFrame avec start NaN pour certaines FTA
        df_data = []
        for fta in ftas:
            has_rule = data.draw(st.booleans())
            df_data.append({
                'Formule_Tarifaire_Acheminement': fta,
                'start': data.draw(dates_paris_strategy()) if has_rule else pd.NaT
            })
        
        df = pd.DataFrame(df_data)
        
        if df['start'].isna().any():
            with pytest.raises(ValueError, match="Règles.*manquantes"):
                valider_regles_presentes(df)
        else:
            # Si pas de NaN, doit passer
            result = valider_regles_presentes(df)
            assert result is df


class TestFiltrerValiditeTemporelle:
    """Tests property-based pour filtrer_validite_temporelle."""
    
    @given(
        dates_debut=st.lists(dates_paris_strategy(), min_size=1, max_size=10),
        data=st.data()
    )
    def test_filtre_selon_validite_temporelle(self, dates_debut, data):
        """Le filtre doit ne garder que les périodes dans la plage de validité."""
        # Créer une plage de validité
        start_validite = data.draw(dates_paris_strategy())
        end_validite = data.draw(st.one_of(
            st.none(),
            dates_paris_strategy().filter(lambda d: d > start_validite)
        ))
        
        # DataFrame avec différentes dates
        df_data = []
        for date_debut in dates_debut:
            df_data.append({
                'debut': date_debut,
                'start': start_validite,
                'end': end_validite
            })
        
        df = pd.DataFrame(df_data)
        
        # Appliquer le filtre
        try:
            result = filtrer_validite_temporelle(df)
            
            # Propriétés : toutes les dates sont dans la plage
            end_effective = end_validite if end_validite else pd.Timestamp("2100-01-01").tz_localize(PARIS_TZ)
            
            assert all(result['debut'] >= result['start'])
            assert all(result['debut'] < end_effective)
            
        except ValueError:
            # Si aucune période valide, c'est normal
            pass
    
    @given(data=st.data())
    def test_remplit_end_si_null(self, data):
        """Le filtre doit remplir end avec 2100-01-01 si null."""
        date_debut = data.draw(dates_paris_strategy())
        start_validite = data.draw(dates_paris_strategy())
        
        df = pd.DataFrame({
            'debut': [date_debut],
            'start': [start_validite], 
            'end': [None]  # end null
        })
        
        try:
            result = filtrer_validite_temporelle(df)
            # end doit être rempli avec 2100-01-01
            expected_end = pd.Timestamp("2100-01-01").tz_localize(PARIS_TZ)
            assert (result['end'].iloc[0] == expected_end) if not result.empty else True
        except ValueError:
            # Si aucune période valide, c'est normal
            pass


class TestCalculerContributionsTurpe:
    """Tests property-based pour calculer_contributions_turpe."""
    
    @given(data=st.data())  
    def test_contribution_positive_avec_energies_positives(self, data):
        """Avec énergies et tarifs positifs, contribution doit être positive."""
        cadrans = ["HPH", "HCH", "HPB", "HCB", "HP", "HC", "BASE"]
        
        # Générer DataFrame avec énergies et tarifs positifs
        df_data = {}
        contribution_attendue = 0.0
        
        for cadran in cadrans:
            energie = data.draw(st.floats(min_value=0, max_value=1000))
            tarif = data.draw(st.floats(min_value=0, max_value=10))
            
            df_data[f"{cadran}_energie"] = [energie]
            df_data[cadran] = [tarif]
            
            contribution_attendue += (energie * tarif / 100)
        
        df = pd.DataFrame(df_data)
        result = calculer_contributions_turpe(df)
        
        # Propriétés
        assert 'contribution_totale' in result.columns
        assert result['contribution_totale'].iloc[0] >= 0
        
        # Vérifier calcul approximatif (éviter erreurs d'arrondi)
        assert abs(result['contribution_totale'].iloc[0] - contribution_attendue) < 0.01
    
    @given(
        energies=st.lists(st.floats(min_value=0, max_value=1000), min_size=1, max_size=5),
        data=st.data()
    )
    def test_contribution_nulle_si_tarifs_nuls(self, energies, data):
        """Si tous les tarifs sont nuls, contribution doit être nulle."""
        cadrans = ["HPH", "HCH", "HPB", "HCB", "HP", "HC", "BASE"]
        
        df_data = {}
        for cadran in cadrans:
            df_data[f"{cadran}_energie"] = energies
            df_data[cadran] = [0.0] * len(energies)  # Tarifs nuls
        
        df = pd.DataFrame(df_data)
        result = calculer_contributions_turpe(df)
        
        # Propriété : contribution nulle
        assert all(result['contribution_totale'] == 0.0)


class TestAgregerParPeriodeOriginale:
    """Tests property-based pour aggreger_par_periode_originale."""
    
    @given(
        nb_periodes=st.integers(min_value=1, max_value=10),
        data=st.data()
    )
    def test_preserve_index_original(self, nb_periodes, data):
        """L'agrégation doit préserver l'index des périodes originales."""
        # Créer périodes originales
        periodes_orig = pd.DataFrame({
            'pdl': [f"PDL{i}" for i in range(nb_periodes)]
        })
        
        # Créer données avec index original et contributions
        df_data = []
        for i in range(nb_periodes):
            # Chaque période peut avoir plusieurs lignes (ex: différentes règles)
            nb_lignes = data.draw(st.integers(min_value=1, max_value=3))
            for j in range(nb_lignes):
                df_data.append({
                    '_index_original': i,
                    'contribution_totale': data.draw(st.floats(min_value=0, max_value=100))
                })
        
        df = pd.DataFrame(df_data)
        
        # Agrégation
        aggregateur = aggreger_par_periode_originale(periodes_orig)
        result = aggregateur(df)
        
        # Propriétés
        assert isinstance(result, pd.Series)
        assert result.name == 'turpe_variable'
        assert len(result) == nb_periodes
        assert all(result >= 0)  # Sommes positives
    
    @given(
        contributions=st.lists(st.floats(min_value=0, max_value=100), min_size=2, max_size=5)
    )
    def test_somme_contributions_correctement(self, contributions):
        """L'agrégation doit sommer correctement les contributions."""
        # Une période originale avec plusieurs contributions
        periodes_orig = pd.DataFrame({'pdl': ['PDL001']})
        
        df_data = [{
            '_index_original': 0,
            'contribution_totale': contrib
        } for contrib in contributions]
        
        df = pd.DataFrame(df_data)
        
        # Agrégation
        aggregateur = aggreger_par_periode_originale(periodes_orig)
        result = aggregateur(df)
        
        # Propriété : somme correcte (arrondi à 2 décimales)
        expected_sum = round(sum(contributions), 2)
        assert abs(result.iloc[0] - expected_sum) < 0.01


class TestCalculerTurpeVariableComplete:
    """Tests property-based pour la fonction complète calculer_turpe_variable."""
    
    @given(
        ftas=st.lists(fta_strategy(), min_size=1, max_size=3, unique=True),
        data=st.data()
    )
    def test_pipeline_complet_avec_ftas_communes(self, ftas, data):
        """Test du pipeline complet avec FTA communes garanties."""
        regles = data.draw(regles_turpe_strategy(ftas=ftas))
        
        # Utiliser les dates des règles pour générer des périodes compatibles
        date_base_regle = regles['start'].iloc[0]
        periodes = data.draw(periodes_energie_strategy(ftas=ftas, date_base=date_base_regle))
        
        result = calculer_turpe_variable(regles, periodes)
        
        # Propriétés invariantes
        assert isinstance(result, pd.Series)
        assert result.name == 'turpe_variable'
        assert len(result) == len(periodes)
        assert all(result >= 0)  # Jamais négatif
        assert not result.isna().any()  # Pas de NaN
        assert result.dtype == float  # Type correct
    
    @given(regles=regles_turpe_strategy())
    def test_periodes_vides(self, regles):
        """Avec périodes vides, doit retourner Series vide."""
        periodes_vides = pd.DataFrame()
        result = calculer_turpe_variable(regles, periodes_vides)
        
        assert isinstance(result, pd.Series)
        assert len(result) == 0
        assert result.dtype == float
        assert result.name == 'turpe_variable'
    
    @given(periodes=periodes_energie_strategy())
    def test_erreur_si_colonne_fta_manquante(self, periodes):
        """Doit lever erreur si colonne FTA manquante."""
        regles = pd.DataFrame({'Formule_Tarifaire_Acheminement': ['TEST']})
        periodes_sans_fta = periodes.drop(columns=['Formule_Tarifaire_Acheminement'], errors='ignore')
        
        with pytest.raises(ValueError, match="Formule_Tarifaire_Acheminement.*requise"):
            calculer_turpe_variable(regles, periodes_sans_fta)