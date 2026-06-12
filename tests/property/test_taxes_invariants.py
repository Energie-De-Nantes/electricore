"""
Property tests des invariants de taxes (issue #195).

Cibles : `ajouter_taux_en_vigueur` (mécanique commune), `ajouter_accise` et
`ajouter_cta` (adaptateurs) — le cœur « taux × assiette » des pipelines de
taxes. L'agrégation amont de `pipeline_accise`/`pipeline_cta` (group_by Odoo,
jointure mapping) reste couverte par les tests unitaires : ses entrées Odoo
brutes n'ont pas de schéma Pandera à dériver.

Invariants :
- montant = assiette × taux en vigueur (au même arrondi que le pipeline) ;
- taux nul → montant nul ;
- fenêtres de validité : le taux appliqué est celui dont le start est le
  dernier ≤ date ; date antérieure au premier start → erreur explicite ;
- préservation des lignes : la jointure ne perd ni ne duplique aucune ligne.
"""

import datetime as dt
from zoneinfo import ZoneInfo

import hypothesis.strategies as st
import polars as pl
import pytest
from hypothesis import given

from electricore.core.models.accise_mensuel import AcciseMensuel
from electricore.core.models.periode_meta import PeriodeMeta
from electricore.core.pipelines.accise import ajouter_accise, load_accise_rules
from electricore.core.pipelines.cta import ajouter_cta, load_cta_rules
from electricore.core.pipelines.taux import ajouter_taux_en_vigueur
from tests.property.strategies import strategie_depuis_schema

# Vraies règles versionnées — fixtures de module (hypothesis interdit les fixtures function-scoped)
REGLES_ACCISE = load_accise_rules().collect().sort("start")
REGLES_CTA = load_cta_rules().collect().sort("start")


def _historique(starts_taux: list[tuple[dt.datetime, float]], taux_col: str) -> pl.LazyFrame:
    """Historique de taux synthétique au schéma attendu (start datetime Paris + taux Float64)."""
    starts, taux = zip(*starts_taux, strict=True)
    return (
        pl.DataFrame({"start": list(starts), taux_col: list(taux)})
        .with_columns(
            pl.col("start").cast(pl.Datetime("us")).dt.replace_time_zone("Europe/Paris"),
            pl.col(taux_col).cast(pl.Float64),
        )
        .lazy()
    )


def _taux_attendu(regles: pl.DataFrame, taux_col: str, date) -> float:
    """Oracle : dernier taux dont le start est ≤ date."""
    return regles.filter(pl.col("start") <= date)[taux_col][-1]


# =============================================================================
# ajouter_taux_en_vigueur — mécanique commune
# =============================================================================

_DATES_2024_2030 = st.datetimes(
    dt.datetime(2024, 6, 1),
    dt.datetime(2030, 12, 31),
    timezones=st.just(ZoneInfo("Europe/Paris")),
    allow_imaginary=False,
)


@pytest.mark.hypothesis
@given(
    df=strategie_depuis_schema(
        PeriodeMeta,
        colonnes=["pdl", "debut"],
        surcharges={"debut": _DATES_2024_2030},
    )
)
def test_taux_en_vigueur_respecte_les_fenetres(df: pl.DataFrame):
    """Chaque ligne reçoit exactement le taux dont la fenêtre contient sa date."""
    bascule = dt.datetime(2027, 1, 1)
    historique = _historique([(dt.datetime(2024, 1, 1), 10.0), (bascule, 20.0)], "taux_test")

    result = ajouter_taux_en_vigueur(df.lazy(), historique, date_col="debut", taux_col="taux_test").collect()

    attendu = (
        result["debut"]
        .dt.replace_time_zone(None)
        .map_elements(lambda d: 10.0 if d < bascule else 20.0, return_dtype=pl.Float64)
    )
    assert result["taux_test"].to_list() == attendu.to_list()


@pytest.mark.hypothesis
@given(
    df=strategie_depuis_schema(
        PeriodeMeta,
        colonnes=["pdl", "debut"],
        surcharges={"debut": _DATES_2024_2030},
    )
)
def test_taux_en_vigueur_preserve_les_lignes(df: pl.DataFrame):
    """La jointure asof ne perd ni ne duplique aucune ligne."""
    avec_index = df.with_row_index("ligne")
    historique = _historique([(dt.datetime(2024, 1, 1), 10.0)], "taux_test")

    result = ajouter_taux_en_vigueur(avec_index.lazy(), historique, date_col="debut", taux_col="taux_test").collect()

    assert sorted(result["ligne"].to_list()) == list(range(df.height))


def test_taux_en_vigueur_refuse_les_dates_orphelines():
    """Une date antérieure au premier start n'a pas de taux : erreur explicite, pas de silence."""
    df = pl.LazyFrame({"debut": [dt.datetime(2023, 12, 31)]}).with_columns(
        pl.col("debut").cast(pl.Datetime("us")).dt.replace_time_zone("Europe/Paris")
    )
    historique = _historique([(dt.datetime(2024, 1, 1), 10.0)], "taux_test")

    with pytest.raises(ValueError, match="sans taux en vigueur"):
        ajouter_taux_en_vigueur(df, historique, date_col="debut", taux_col="taux_test").collect()


# =============================================================================
# Accise — montant = énergie (MWh) × taux en vigueur
# =============================================================================

# Mois valides postérieurs au premier start des vraies règles (2024-01-02)
_MOIS_VALIDES = (
    st.tuples(st.integers(2024, 2030), st.integers(1, 12))
    .filter(lambda am: am > (2024, 1))
    .map(lambda am: f"{am[0]}-{am[1]:02d}")
)


def _consommations(max_size: int = 10) -> st.SearchStrategy[pl.DataFrame]:
    return strategie_depuis_schema(
        AcciseMensuel,
        colonnes=["pdl", "mois_annee", "energie_kwh"],
        surcharges={"mois_annee": _MOIS_VALIDES},
        max_size=max_size,
    )


@pytest.mark.hypothesis
@given(consommations=_consommations())
def test_accise_montant_egale_assiette_fois_taux(consommations: pl.DataFrame):
    """`accise_eur` vaut énergie_mwh × taux en vigueur du mois, au même arrondi que le pipeline."""
    result = ajouter_accise(consommations.lazy(), REGLES_ACCISE.lazy()).collect()

    for ligne in result.iter_rows(named=True):
        date_mois = dt.datetime.strptime(ligne["mois_annee"], "%Y-%m").replace(tzinfo=dt.UTC)
        taux_attendu = _taux_attendu(
            REGLES_ACCISE.with_columns(pl.col("start").dt.convert_time_zone("UTC")), "taux_accise_eur_mwh", date_mois
        )
        assert ligne["taux_accise_eur_mwh"] == taux_attendu, f"taux hors fenêtre pour {ligne['mois_annee']}"
        assert ligne["accise_eur"] == round(ligne["energie_kwh"] / 1000 * taux_attendu, 2)


@pytest.mark.hypothesis
@given(consommations=_consommations())
def test_accise_taux_nul_montant_nul(consommations: pl.DataFrame):
    historique_nul = _historique([(dt.datetime(2020, 1, 1), 0.0)], "taux_accise_eur_mwh")

    result = ajouter_accise(consommations.lazy(), historique_nul).collect()

    assert (result["accise_eur"] == 0.0).all()
    assert result["accise_eur"].null_count() == 0


@pytest.mark.hypothesis
@given(consommations=_consommations())
def test_accise_preserve_les_lignes(consommations: pl.DataFrame):
    avec_index = consommations.with_row_index("ligne")

    result = ajouter_accise(avec_index.lazy(), REGLES_ACCISE.lazy()).collect()

    assert sorted(result["ligne"].to_list()) == list(range(consommations.height))


# =============================================================================
# CTA — montant = TURPE fixe × taux (%) en vigueur
# =============================================================================


def _facturation_mensuelle(max_size: int = 10) -> st.SearchStrategy[pl.DataFrame]:
    return strategie_depuis_schema(
        PeriodeMeta,
        colonnes=["pdl", "debut", "turpe_fixe_eur"],
        surcharges={"debut": _DATES_2024_2030},
        max_size=max_size,
    )


@pytest.mark.hypothesis
@given(facturation=_facturation_mensuelle())
def test_cta_montant_egale_assiette_fois_taux(facturation: pl.DataFrame):
    """`cta_eur` vaut turpe_fixe_eur × taux_cta_pct / 100 — null si l'assiette est nulle (schéma nullable)."""
    result = ajouter_cta(facturation.lazy(), REGLES_CTA.lazy()).collect()

    attendu = (pl.col("turpe_fixe_eur") * pl.col("taux_cta_pct") / 100).round(2)
    assert result["cta_eur"].equals(result.select(attendu.alias("cta_eur"))["cta_eur"])
    for ligne in result.iter_rows(named=True):
        taux_attendu = _taux_attendu(REGLES_CTA, "taux_cta_pct", ligne["debut"])
        assert ligne["taux_cta_pct"] == taux_attendu, f"taux hors fenêtre pour {ligne['debut']}"


@pytest.mark.hypothesis
@given(facturation=_facturation_mensuelle())
def test_cta_taux_nul_montant_nul(facturation: pl.DataFrame):
    historique_nul = _historique([(dt.datetime(2020, 1, 1), 0.0)], "taux_cta_pct")

    result = ajouter_cta(facturation.lazy(), historique_nul).collect()

    non_nuls = result.filter(pl.col("turpe_fixe_eur").is_not_null())
    assert (non_nuls["cta_eur"] == 0.0).all()


@pytest.mark.hypothesis
@given(facturation=_facturation_mensuelle())
def test_cta_preserve_les_lignes(facturation: pl.DataFrame):
    avec_index = facturation.with_row_index("ligne")

    result = ajouter_cta(avec_index.lazy(), REGLES_CTA.lazy()).collect()

    assert sorted(result["ligne"].to_list()) == list(range(facturation.height))
