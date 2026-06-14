"""`RelevéIndex` porte l'identité métier du relevé (#232, ADR-0028).

Le modèle expose la clé métier `releve_id`, la provenance native `id_releve`
(NULL pour R151/R64), et la nature canonique `nature_index` (réel/estimé/corrigé).
Ces champs sont optionnels (nullable) : les loaders réels les fournissent toujours,
mais les fixtures légères des pipelines peuvent les omettre.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from electricore.core.models.releve_index import RelevéIndex

PARIS = ZoneInfo("Europe/Paris")


def _releve(**extra) -> pl.DataFrame:
    base = {
        "date_releve": [datetime(2024, 1, 1, tzinfo=PARIS)],
        "pdl": ["PDL00001"],
        "source": ["flux_R151"],
        "unite": ["kWh"],
        "precision": ["kWh"],
        "ordre_index": [False],
        # Requis par le dataframe-check verifier_presence_mesures du modèle.
        "id_calendrier_distributeur": [None],
        "index_base_kwh": [None],
        "index_hp_kwh": [None],
        "index_hc_kwh": [None],
        "index_hph_kwh": [None],
        "index_hpb_kwh": [None],
        "index_hch_kwh": [None],
        "index_hcb_kwh": [None],
    }
    base.update(extra)
    return pl.DataFrame(
        base,
        schema_overrides={
            "date_releve": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "id_calendrier_distributeur": pl.Utf8,
            **{
                c: pl.Float64
                for c in (
                    "index_base_kwh",
                    "index_hp_kwh",
                    "index_hc_kwh",
                    "index_hph_kwh",
                    "index_hpb_kwh",
                    "index_hch_kwh",
                    "index_hcb_kwh",
                )
            },
        },
    )


def test_releve_index_accepte_identite_complete():
    """Un relevé avec releve_id + id_releve + nature_index canonique valide."""
    df = _releve(
        releve_id=["flux_R151|PDL00001|2024-01-01|false"],
        id_releve=[None],
        nature_index=["réel"],
    ).with_columns(pl.col("id_releve").cast(pl.Utf8))
    RelevéIndex.validate(df)  # ne doit pas lever


def test_releve_index_nature_canonique_isin():
    """La nature canonique est contrainte à réel/estimé/corrigé."""
    df = _releve(nature_index=["estimé"])
    RelevéIndex.validate(df)


def test_releve_index_identite_optionnelle():
    """Une fixture légère sans les champs d'identité reste valide (nullable optionnel)."""
    RelevéIndex.validate(_releve())


def test_releve_index_rejette_nature_non_canonique():
    """Une nature hors vocabulaire canonique (ex. brut « REEL ») est rejetée."""
    import pandera.errors as pa_errors

    df = _releve(nature_index=["REEL"])  # valeur source non projetée
    try:
        RelevéIndex.validate(df)
    except (pa_errors.SchemaError, pa_errors.SchemaErrors):
        return
    raise AssertionError("nature_index hors isin aurait dû être rejetée")
