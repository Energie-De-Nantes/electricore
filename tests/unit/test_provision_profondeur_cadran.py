"""Profondeur cadran WIDE de l'estimation de provision (#488, ADR-0048).

Sortie WIDE comme flux_r67 : `energie_base_kwh` = total ; hp/hc et 4 cadrans peuplés
SEULEMENT si toute la fenêtre les porte (sinon null, base porte le total) ; profondeur
déclarée en méta ; invariant `base = hp+hc = Σ4` sur une fenêtre cohérente.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from electricore.core.models.cadrans import CADRANS, col_energie
from electricore.core.models.provision import EstimationProvision
from electricore.core.pipelines.provision import effondrer_profil

_COLS_ENERGIE = [col_energie(c) for c in CADRANS]


def _profil(lignes: list[dict], pdl: str = "P1") -> pl.LazyFrame:
    """Forge un profil cadran : chaque `ligne` = dict {debut, fin, code_nature, <energie_*>}."""
    data: dict[str, list] = {
        "pdl": [pdl] * len(lignes),
        "debut": [ligne["debut"] for ligne in lignes],
        "fin": [ligne["fin"] for ligne in lignes],
        "code_nature": [ligne.get("code_nature", "R") for ligne in lignes],
    }
    for col in _COLS_ENERGIE:
        data[col] = [ligne.get(col) for ligne in lignes]
    frame = pl.DataFrame(data)
    for col in _COLS_ENERGIE:
        frame = frame.with_columns(pl.col(col).cast(pl.Int64))
    return frame.lazy()


def _douze_mois(payload, debut_an=2024):
    """12 périodes mensuelles 2024 portant `payload(m)` (dict d'énergie par cadran)."""
    return [
        {"debut": date(debut_an, m, 1), "fin": date(debut_an, m, 28), "code_nature": "R", **payload(m)}
        for m in range(1, 13)
    ]


# =============================================================================
# Profondeur 4 cadrans cohérente → invariant base = hp+hc = Σ4
# =============================================================================


def test_fenetre_4_cadrans_coherente_invariant_base_hp_hc_sigma4():
    lignes = _douze_mois(
        lambda m: {
            col_energie("hph"): 10,
            col_energie("hpb"): 20,
            col_energie("hch"): 5,
            col_energie("hcb"): 15,
        }
    )
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    EstimationProvision.validate(out)
    row = out.to_dicts()[0]

    assert row["profondeur_cadran"] == "4_cadrans"
    # Σ4 par cadran sur 12 mois.
    assert row[col_energie("hph")] == 120
    assert row[col_energie("hpb")] == 240
    assert row[col_energie("hch")] == 60
    assert row[col_energie("hcb")] == 180
    # hp/hc dérivés de la relation de synthèse (hp = hph+hpb, hc = hch+hcb).
    assert row[col_energie("hp")] == 360
    assert row[col_energie("hc")] == 240
    # Invariant : base = hp + hc = Σ4.
    assert row[col_energie("base")] == 600
    assert row[col_energie("hp")] + row[col_energie("hc")] == row[col_energie("base")]
    sigma4 = sum(row[col_energie(c)] for c in ("hph", "hpb", "hch", "hcb"))
    assert sigma4 == row[col_energie("base")]
    # Mensuel /12 plate cohérent aussi.
    assert row["energie_base_mensuel_kwh"] == pytest.approx(50.0)
    assert row["energie_hp_mensuel_kwh"] == pytest.approx(30.0)


# =============================================================================
# Profondeur hp/hc cohérente (sans les 4 sous-cadrans)
# =============================================================================


def test_fenetre_hp_hc_coherente_sans_4_cadrans():
    lignes = _douze_mois(lambda m: {col_energie("hp"): 30, col_energie("hc"): 20})
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    EstimationProvision.validate(out)
    row = out.to_dicts()[0]

    assert row["profondeur_cadran"] == "hp_hc"
    assert row[col_energie("hp")] == 360
    assert row[col_energie("hc")] == 240
    assert row[col_energie("base")] == 600  # base = hp + hc
    # Les 4 sous-cadrans ne sont pas portés → null.
    for c in ("hph", "hpb", "hch", "hcb"):
        assert row[col_energie(c)] is None


# =============================================================================
# Cas historique grille-mixé : base-only + 4-cadrans → repli propre sur base
# =============================================================================


def test_fenetre_grille_mixee_retombe_sur_base():
    """Mélange dans la fenêtre : 6 mois base-only + 6 mois 4-cadrans → profondeur base,
    base = total reconstruit, cadrans fins null (la fenêtre n'est pas cohérente)."""
    lignes_base = [
        {"debut": date(2024, m, 1), "fin": date(2024, m, 28), "code_nature": "R", col_energie("base"): 100}
        for m in range(1, 7)
    ]
    lignes_4 = [
        {
            "debut": date(2024, m, 1),
            "fin": date(2024, m, 28),
            "code_nature": "R",
            col_energie("hph"): 25,
            col_energie("hpb"): 25,
            col_energie("hch"): 25,
            col_energie("hcb"): 25,
        }
        for m in range(7, 13)
    ]
    out = effondrer_profil(_profil(lignes_base + lignes_4), as_of=date(2025, 1, 1)).collect()
    EstimationProvision.validate(out)
    row = out.to_dicts()[0]

    assert row["profondeur_cadran"] == "base"
    # base = total reconstruit : 6×100 (base) + 6×100 (Σ4 par période) = 1200.
    assert row[col_energie("base")] == 1200
    # Aucun cadran fin populé (fenêtre incohérente).
    for c in ("hp", "hc", "hph", "hpb", "hch", "hcb"):
        assert row[col_energie(c)] is None


def test_une_seule_periode_base_only_dans_la_fenetre_4_cadrans_casse_la_coherence():
    """Un seul mois base-only au milieu de 4-cadrans → la profondeur retombe sur base."""
    lignes = _douze_mois(
        lambda m: (
            {col_energie("base"): 50}
            if m == 6
            else {
                col_energie("hph"): 10,
                col_energie("hpb"): 10,
                col_energie("hch"): 10,
                col_energie("hcb"): 10,
            }
        )
    )
    out = effondrer_profil(_profil(lignes), as_of=date(2025, 1, 1)).collect()
    row = out.to_dicts()[0]
    assert row["profondeur_cadran"] == "base"
    assert row[col_energie("hph")] is None
    # base = total : 11 mois × 40 (Σ4) + 1 mois × 50 = 490.
    assert row[col_energie("base")] == 490
