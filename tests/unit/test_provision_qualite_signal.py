"""Qualité + couverture + signal alertable de l'estimation de provision (#489, ADR-0048).

- Flag qualité depuis le mix R/E/C (réelle si majorité R, estimée sinon ; régul C signalée).
- Flag couverture < 12 mois.
- Signal alertable exposé (couverture insuffisante OU qualité estimée) — la lib expose, l'aval
  alerte (ADR-0037). Aucune logique de notification dans le cœur.
- Cas non-communicant / non-Linky (R67 surtout estimé, bimestriel) : pas de traitement
  spécial, le mix de natures le fait remonter « estimée » + signal.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from electricore.core.models.cadrans import CADRANS, col_energie
from electricore.core.pipelines.provision import effondrer_profil

_COLS_ENERGIE = [col_energie(c) for c in CADRANS]
GOLDEN = Path(__file__).parents[1] / "fixtures" / "flux" / "golden" / "flux_r67.json"


def _profil(lignes: list[dict], pdl: str = "P1") -> pl.LazyFrame:
    data: dict[str, list] = {
        "pdl": [pdl] * len(lignes),
        "debut": [ligne["debut"] for ligne in lignes],
        "fin": [ligne["fin"] for ligne in lignes],
        "code_nature": [ligne["code_nature"] for ligne in lignes],
    }
    for col in _COLS_ENERGIE:
        data[col] = [ligne.get(col, 100 if col == col_energie("base") else None) for ligne in lignes]
    frame = pl.DataFrame(data)
    for col in _COLS_ENERGIE:
        frame = frame.with_columns(pl.col(col).cast(pl.Int64))
    return frame.lazy()


# =============================================================================
# Flag qualité depuis le mix R/E/C
# =============================================================================


def test_qualite_reelle_si_majorite_r():
    lignes = [
        {"debut": date(2024, m, 1), "fin": date(2024, m, 28), "code_nature": ("R" if m <= 8 else "E")}
        for m in range(1, 13)
    ]  # 8 R / 4 E → majorité R
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    row = out.to_dicts()[0]
    assert row["qualite"] == "réelle"
    assert row["presence_regularisation"] is False


def test_qualite_estimee_si_pas_majorite_r():
    lignes = [
        {"debut": date(2024, m, 1), "fin": date(2024, m, 28), "code_nature": ("R" if m <= 4 else "E")}
        for m in range(1, 13)
    ]  # 4 R / 8 E → pas majorité R
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    row = out.to_dicts()[0]
    assert row["qualite"] == "estimée"


def test_presence_regularisation_signale_le_c():
    lignes = [
        {"debut": date(2024, 1, 1), "fin": date(2024, 2, 1), "code_nature": "R"},
        {"debut": date(2024, 2, 1), "fin": date(2024, 3, 1), "code_nature": "C"},  # régul
    ]
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    row = out.to_dicts()[0]
    assert row["presence_regularisation"] is True


def test_golden_100pct_r_remonte_reelle():
    """Golden : un PDL 100 % R → qualité réelle (acceptance #489)."""
    rows = json.loads(GOLDEN.read_text())
    frame = pl.DataFrame(rows).with_columns(
        pl.col("debut").str.to_date(), pl.col("fin").str.to_date(), pl.col("energie_base_kwh").cast(pl.Int64)
    )
    for col in _COLS_ENERGIE:
        if col not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Int64).alias(col))
        else:
            frame = frame.with_columns(pl.col(col).cast(pl.Int64))
    # 99054135750249 : toutes natures R dans le golden.
    out = effondrer_profil(frame.lazy(), as_of=date(2026, 6, 27)).collect()
    par_pdl = {r["pdl"]: r for r in out.to_dicts()}
    assert par_pdl["99054135750249"]["qualite"] == "réelle"
    # 99224295862625 : mix C/E/R → estimée + régul signalée.
    assert par_pdl["99224295862625"]["qualite"] == "estimée"
    assert par_pdl["99224295862625"]["presence_regularisation"] is True


# =============================================================================
# Flag couverture < 12 mois
# =============================================================================


def test_couverture_insuffisante_sous_12_mois():
    lignes = [
        {"debut": date(2024, m, 1), "fin": date(2024, m, 28), "code_nature": "R"} for m in range(1, 7)
    ]  # 6 mois seulement
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    row = out.to_dicts()[0]
    assert row["couverture_suffisante"] is False
    assert row["couverture_mois"] < 12


def test_couverture_suffisante_a_12_mois():
    lignes = [{"debut": date(2024, m, 1), "fin": date(2024, m + 1, 1), "code_nature": "R"} for m in range(1, 12)]
    lignes.append({"debut": date(2024, 12, 1), "fin": date(2025, 1, 1), "code_nature": "R"})
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    row = out.to_dicts()[0]
    assert row["couverture_suffisante"] is True


# =============================================================================
# Signal alertable (la lib expose ; l'aval alerte)
# =============================================================================


def test_signal_alertable_si_couverture_insuffisante():
    lignes = [{"debut": date(2024, m, 1), "fin": date(2024, m, 28), "code_nature": "R"} for m in range(1, 5)]
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    assert out.to_dicts()[0]["signal_alertable"] is True


def test_signal_alertable_si_qualite_estimee_meme_couverture_pleine():
    lignes = [{"debut": date(2024, m, 1), "fin": date(2024, m + 1, 1), "code_nature": "E"} for m in range(1, 12)]
    lignes.append({"debut": date(2024, 12, 1), "fin": date(2025, 1, 1), "code_nature": "E"})
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    row = out.to_dicts()[0]
    assert row["couverture_suffisante"] is True
    assert row["qualite"] == "estimée"
    assert row["signal_alertable"] is True


def test_pas_de_signal_si_couverture_pleine_et_reelle():
    lignes = [{"debut": date(2024, m, 1), "fin": date(2024, m + 1, 1), "code_nature": "R"} for m in range(1, 12)]
    lignes.append({"debut": date(2024, 12, 1), "fin": date(2025, 1, 1), "code_nature": "R"})
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    assert out.to_dicts()[0]["signal_alertable"] is False


def test_cas_non_communicant_bimestriel_estime_remonte_estimee_et_signal():
    """Non-Linky : R67 surtout estimé, bimestriel — pas de traitement spécial.

    Le mix de natures (majorité E) le fait remonter « estimée » + signal alertable, et la
    cadence bimestrielle réduit la couverture → double cause de signal (acceptance #489)."""
    # 6 relevés bimestriels sur l'année, surtout estimés (typique non-communicant).
    bornes = [(date(2024, m, 1), date(2024, m + 2, 1) if m + 2 <= 12 else date(2025, 1, 1)) for m in range(1, 12, 2)]
    lignes = [
        {"debut": debut, "fin": fin, "code_nature": ("R" if i == 0 else "E")} for i, (debut, fin) in enumerate(bornes)
    ]
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    row = out.to_dicts()[0]
    assert row["qualite"] == "estimée"  # mix surtout E, sans cas particulier
    assert row["signal_alertable"] is True
