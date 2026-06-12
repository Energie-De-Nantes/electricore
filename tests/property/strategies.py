"""
Socle property-based testing : stratégies dérivées des schémas Pandera (issue #194).

pandera-polars ne sait pas synthétiser de données (`Model.strategy()` lève
`NotImplementedError` — suivi upstream : https://github.com/unionai-oss/pandera/issues/1661,
dormant depuis 2024). En attendant, ce module dérive des colonnes
`polars.testing.parametric` depuis un `DataFrameModel` via `to_schema()` :
noms, dtypes, nullabilité et bornes simples (ge/le). Source de vérité unique :
les schémas Pandera — aucune redéfinition manuelle du savoir de typage.

Garde-fou (anti-pattern 2025, cf. tests/README.md) : les stratégies restent
*plates* — colonnes indépendantes, contraintes simples. Les checks dataframe
multi-colonnes (ex: debut < fin) ne sont PAS satisfaits par génération ;
dès qu'un test exige une cohérence métier séquentielle, c'est le territoire
des fixtures + snapshots.
"""

import datetime as dt
from zoneinfo import ZoneInfo

import hypothesis.strategies as st
import polars as pl
from polars.testing.parametric import column, dataframes

# Bornes par défaut : garder l'arithmétique des pipelines stable (pas d'overflow,
# pas de perte de précision float qui rendrait les invariants indécidables)
_FLOAT_MIN, _FLOAT_MAX = -1e6, 1e6
_INT_MIN, _INT_MAX = -10_000, 10_000
_DATE_MIN, _DATE_MAX = dt.datetime(2020, 1, 1), dt.datetime(2030, 12, 31)


def _bornes_depuis_checks(checks) -> dict:
    """Extrait les bornes simples (ge/le/in_range) et regex des checks Pandera."""
    bornes = {}
    for check in checks:
        stats = getattr(check, "statistics", None) or {}
        if "min_value" in stats and stats["min_value"] is not None:
            bornes["min_value"] = stats["min_value"]
        if "max_value" in stats and stats["max_value"] is not None:
            bornes["max_value"] = stats["max_value"]
        if "pattern" in stats and stats["pattern"] is not None:
            bornes["pattern"] = stats["pattern"]
    return bornes


def _strategie_valeurs(dtype: pl.DataType, checks) -> st.SearchStrategy:
    """Stratégie de valeurs pour un dtype Polars, bornée par les checks Pandera."""
    bornes = _bornes_depuis_checks(checks)

    if dtype == pl.Float64:
        return st.floats(
            min_value=float(bornes.get("min_value", _FLOAT_MIN)),
            max_value=float(bornes.get("max_value", _FLOAT_MAX)),
            allow_nan=False,
            allow_infinity=False,
            width=64,
        )
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64):
        return st.integers(
            min_value=int(bornes.get("min_value", _INT_MIN)),
            max_value=int(bornes.get("max_value", _INT_MAX)),
        )
    if dtype == pl.Boolean:
        return st.booleans()
    if isinstance(dtype, pl.Datetime):
        timezones = st.just(ZoneInfo(dtype.time_zone)) if dtype.time_zone else st.none()
        return st.datetimes(
            min_value=_DATE_MIN,
            max_value=_DATE_MAX,
            timezones=timezones,
            allow_imaginary=False,  # pas d'heures inexistantes (trou DST)
        )
    if dtype == pl.Date:
        return st.dates(min_value=_DATE_MIN.date(), max_value=_DATE_MAX.date())
    if dtype == pl.Utf8:
        if "pattern" in bornes:
            # fullmatch : sans lui, `$` tolère un '\n' final que str_matches refuse
            return st.from_regex(bornes["pattern"], fullmatch=True)
        return st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", min_size=1, max_size=14)

    raise NotImplementedError(f"Dtype non supporté par le socle property-based : {dtype}")


def strategie_depuis_schema(
    modele,
    colonnes: list[str] | None = None,
    surcharges: dict[str, st.SearchStrategy] | None = None,
    min_size: int = 1,
    max_size: int = 10,
    lazy: bool = False,
) -> st.SearchStrategy:
    """
    Dérive une stratégie `polars.testing.parametric.dataframes` d'un DataFrameModel Pandera.

    Args:
        modele: classe DataFrameModel Pandera (ex: PeriodeAbonnement)
        colonnes: sous-ensemble de colonnes à générer (défaut : toutes celles du schéma)
        surcharges: stratégies de valeurs par colonne, prioritaires sur la dérivation —
            réservées aux contraintes propres au test (ex: FTA existant dans les règles),
            jamais à redéclarer ce que le schéma sait déjà
        min_size / max_size: nombre de lignes générées
        lazy: produire des LazyFrames au lieu de DataFrames

    Returns:
        Stratégie hypothesis générant des DataFrames conformes au schéma
        (dtypes, nullabilité, bornes simples ge/le, regex str_matches)
    """
    schema = modele.to_schema()
    surcharges = surcharges or {}
    noms = colonnes if colonnes is not None else list(schema.columns)

    cols = []
    for nom in noms:
        col_pa = schema.columns[nom]
        dtype = col_pa.dtype.type
        strategie = surcharges[nom] if nom in surcharges else _strategie_valeurs(dtype, col_pa.checks)
        cols.append(column(name=nom, dtype=dtype, strategy=strategie, allow_null=col_pa.nullable))

    return dataframes(cols=cols, min_size=min_size, max_size=max_size, lazy=lazy, allow_chunks=False)
