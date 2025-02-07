import pytest
import pandas as pd
from electricore.inputs.from_electriflux import situation_périmetre

def test_situation_périmetre_filtrage():
    """Test que la fonction filtre correctement les événements après la date T."""
    data = pd.DataFrame({
        "pdl": ["A", "B", "A", "C"],
        "Ref_Situation_Contractuelle": ["R1", "R2", "R3", "R4"],
        "Date_Evenement": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"])
    })
    
    date_t = pd.Timestamp("2024-02-15")
    result = situation_périmetre(date_t, data)

    assert len(result) == 2, "Le DataFrame doit contenir uniquement les événements <= 2024-02-15"
    assert "C" not in result["pdl"].values, "L'événement du 2024-04-01 (PDL C) ne doit pas être présent"

def test_situation_périmetre_selection_plus_recent():
    """Test que la fonction garde uniquement l'événement le plus récent par PDL."""
    data = pd.DataFrame({
        "pdl": ["A", "A", "B", "B"],
        "Ref_Situation_Contractuelle": ["R1", "R1", "R2", "R2"],
        "Date_Evenement": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-02-01", "2024-03-01"])
    })

    date_t = pd.Timestamp("2024-02-15")
    result = situation_périmetre(date_t, data)

    assert len(result) == 2, "Seule la dernière situation contractuelle par PDL doit être conservée"
    assert (result["Date_Evenement"] == "2024-02-01").any(), "Seul le derniers événement de chaque pdl doit être conservé"

def test_situation_périmetre_vide():
    """Test que la fonction retourne un DataFrame vide si aucune ligne ne correspond à la date."""
    data = pd.DataFrame({
        "pdl": ["A", "B"],
        "Ref_Situation_Contractuelle": ["R1", "R2"],
        "Date_Evenement": pd.to_datetime(["2024-05-01", "2024-06-01"])  # Dates après la date T
    })

    date_t = pd.Timestamp("2024-02-15")
    result = situation_périmetre(date_t, data)

    assert result.empty, "Le DataFrame retourné doit être vide si aucune situation contractuelle ne correspond"

def test_situation_périmetre_un_seul_pdl():
    """Test que la fonction fonctionne même si un seul PDL est présent."""
    data = pd.DataFrame({
        "pdl": ["A", "A", "A"],
        "Ref_Situation_Contractuelle": ["R1", "R1", "R1"],
        "Date_Evenement": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"])
    })

    date_t = pd.Timestamp("2024-02-15")
    result = situation_périmetre(date_t, data)

    assert len(result) == 1, "Il doit rester une seule ligne après la suppression des doublons"
    assert result.iloc[0]["Date_Evenement"] == pd.Timestamp("2024-02-01"), "La ligne la plus récente avant T doit être conservée"
