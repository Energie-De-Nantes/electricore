"""Estimation de provision — pipeline + build cold-start R67 (ADR-0048, #487).

Spine BASE bout-en-bout : profil cadran R67 → effondrement 12 mois glissants (somme nette
R+E+C, négatifs préservés) → `/12` plate → métadonnées de couverture. Testé sur le golden
R67 (`tests/fixtures/flux/golden/flux_r67.json`, base-only) et sur des fixtures forgées.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from electricore.core.builds.rapport_provision import estimer
from electricore.core.models.cadrans import CADRANS, col_energie
from electricore.core.models.provision import EstimationProvision
from electricore.core.pipelines.provision import (
    effondrer_profil,
    pipeline_estimation_provision_r67,
    profil_cadran_depuis_r67,
)

GOLDEN = Path(__file__).parents[1] / "fixtures" / "flux" / "golden" / "flux_r67.json"

# Colonnes d'énergie WIDE attendues en sortie du loader r67() (dbt émet les 7, null si absent).
_COLS_ENERGIE = [col_energie(c) for c in CADRANS]


def charger_golden_r67() -> pl.LazyFrame:
    """Charge le golden R67 au format que renvoie le loader `r67()` (WIDE, Int64, Date).

    Le golden JSON ne sérialise que les colonnes non-nulles (base-only) → on complète les
    cadrans fins manquants en `Int64` null pour matcher la forme du mart `flux_r67`.
    """
    rows = json.loads(GOLDEN.read_text())
    frame = pl.DataFrame(rows)
    # debut/fin sérialisés en str ISO → Date ; energie en Int64.
    frame = frame.with_columns(
        pl.col("debut").str.to_date(),
        pl.col("fin").str.to_date(),
        pl.col("energie_base_kwh").cast(pl.Int64),
    )
    for col in _COLS_ENERGIE:
        if col not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Int64).alias(col))
        else:
            frame = frame.with_columns(pl.col(col).cast(pl.Int64))
    return frame.lazy()


# =============================================================================
# Étage 1 — profil cadran (adaptateur cold-start R67)
# =============================================================================


def test_profil_cadran_projette_la_forme_partagee():
    profil = profil_cadran_depuis_r67(charger_golden_r67()).collect()
    assert profil.columns == ["pdl", "debut", "fin", "code_nature", *_COLS_ENERGIE]
    # base toujours présent (total), pas de routage par calculer_periodes_energie.
    assert profil["energie_base_kwh"].dtype == pl.Int64


# =============================================================================
# Étage 2 — effondrement 12 mois glissants + /12 plate
# =============================================================================


def _profil_base(pdl="P1", lignes=None) -> pl.LazyFrame:
    """Profil cadran base-only forgé : `lignes` = liste de (debut, fin, nature, base_kwh)."""
    lignes = lignes or []
    data = {
        "pdl": [pdl] * len(lignes),
        "debut": [d for d, *_ in lignes],
        "fin": [f for _, f, *_ in lignes],
        "code_nature": [n for _, _, n, _ in lignes],
        "energie_base_kwh": [b for *_, b in lignes],
    }
    frame = pl.DataFrame(data)
    for col in _COLS_ENERGIE:
        if col != "energie_base_kwh":
            frame = frame.with_columns(pl.lit(None, dtype=pl.Int64).alias(col))
    return frame.with_columns(pl.col("energie_base_kwh").cast(pl.Int64)).lazy()


def test_effondrement_somme_nette_sur_12_mois_glissants():
    """Tracer : 13 mois de base, as_of fixé → seuls les 12 mois glissants sont sommés."""
    lignes = [
        (date(2024, m, 1), date(2024, m, 28), "R", 100)
        for m in range(1, 13)  # jan..dec 2024
    ]
    # Une période hors fenêtre (trop vieille) : ne doit pas compter.
    lignes.insert(0, (date(2023, 6, 1), date(2023, 6, 28), "R", 9999))
    out = effondrer_profil(_profil_base(lignes=lignes), as_of=date(2025, 1, 1)).collect()
    ((row,),) = (out.to_dicts(),)
    # debut dans [2024-01-01, 2025-01-01) → 12 périodes × 100 = 1200 ; la vieille exclue.
    assert row["energie_base_kwh"] == 1200
    # /12 plate.
    assert row["energie_base_mensuel_kwh"] == pytest.approx(100.0)


def test_effondrement_preserve_les_negatifs_de_regul():
    """Régul `C` négative préservée (ADR-0047) — pas de clipping."""
    lignes = [
        (date(2024, 1, 1), date(2024, 2, 1), "R", 100),
        (date(2024, 2, 1), date(2024, 3, 1), "C", -40),  # régul négative
    ]
    out = effondrer_profil(_profil_base(lignes=lignes), as_of=date(2025, 1, 1)).collect()
    assert out["energie_base_kwh"][0] == 60  # 100 - 40, net


def test_metadonnees_couverture_fenetre_et_mois():
    lignes = [
        (date(2024, 1, 1), date(2024, 7, 1), "R", 100),
        (date(2024, 7, 1), date(2025, 1, 1), "R", 100),
    ]
    out = effondrer_profil(_profil_base(lignes=lignes), as_of=date(2025, 1, 1)).collect()
    row = out.to_dicts()[0]
    assert row["couverture_debut"] == date(2024, 1, 1)
    assert row["couverture_fin"] == date(2025, 1, 1)
    assert row["couverture_mois"] == pytest.approx(12.0, abs=0.1)
    assert row["couverture_suffisante"] is True


def test_couverture_mois_mesure_la_densite_pas_le_span():
    """Note de revue PR #490 : historique creux → couverture = densité, pas l'étendue.

    Deux périodes de ~2 mois aux extrémités d'une fenêtre de 12 mois, séparées par un grand
    trou. Le span global `[min debut, max fin)` vaut ~12 mois, mais la donnée réelle ne couvre
    que ~4 mois → `couverture_mois` doit lire la densité (Σ durées de période, sans les trous,
    périodes non recouvrantes ADR-0047) et signaler l'insuffisance.
    """
    lignes = [
        (date(2024, 1, 1), date(2024, 3, 1), "R", 100),  # 60 j
        (date(2024, 11, 1), date(2025, 1, 1), "R", 100),  # 61 j ; trou mars→nov
    ]
    out = effondrer_profil(_profil_base(lignes=lignes), as_of=date(2025, 1, 1)).collect()
    row = out.to_dicts()[0]
    # Étendue de fenêtre inchangée (affichage).
    assert row["couverture_debut"] == date(2024, 1, 1)
    assert row["couverture_fin"] == date(2025, 1, 1)
    # Densité ≈ 4 mois (121 j), PAS les ~12 mois du span global.
    assert row["couverture_mois"] == pytest.approx(121 / 30.4375, abs=0.1)
    assert row["couverture_mois"] < 6
    assert row["couverture_suffisante"] is False


# =============================================================================
# Pipeline + build — golden R67 (base-only)
# =============================================================================


def test_pipeline_sortie_validee_par_pandera_sur_golden():
    """La sortie complète passe la frontière `EstimationProvision`."""
    out = pipeline_estimation_provision_r67(charger_golden_r67(), as_of=date(2026, 6, 27)).collect()
    EstimationProvision.validate(out)
    # Golden = 4 PDLs ; seuls ceux avec des périodes dans la fenêtre ressortent.
    assert out.height >= 1
    assert set(out.columns) >= {"energie_base_kwh", "energie_base_mensuel_kwh", "couverture_mois"}


def test_build_estimer_par_pdl_sur_golden():
    """Build autonome par PDL : un PDL du golden → 0/1 ligne validée."""
    # 99054135750249 : 9 périodes R/D, base populated, fin jusqu'à 2026-06.
    rapport = estimer(charger_golden_r67(), pdl="99054135750249", as_of=date(2026, 6, 27))
    assert rapport.pdl == "99054135750249"
    assert rapport.as_of == date(2026, 6, 27)
    assert rapport.trouve
    assert rapport.estimation.height == 1
    EstimationProvision.validate(rapport.estimation)
    row = rapport.estimation.to_dicts()[0]
    assert row["energie_base_kwh"] is not None
    assert row["energie_base_mensuel_kwh"] == pytest.approx(row["energie_base_kwh"] / 12)


def test_build_pdl_absent_renvoie_vide():
    rapport = estimer(charger_golden_r67(), pdl="00000000000000", as_of=date(2026, 6, 27))
    assert not rapport.trouve
    assert rapport.estimation.height == 0
