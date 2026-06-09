"""Helpers pour générer des liens vers des enregistrements Odoo."""

import polars as pl


def url_pour_enregistrement(base_url: str, model: str, record_id: int) -> str:
    return f"{base_url.rstrip('/')}/web#id={record_id}&model={model}&view_type=form"


def enrichir_liens(df: pl.DataFrame, base_url: str, model: str) -> pl.DataFrame:
    id_col = f"{model.replace('.', '_')}_id"
    if df.is_empty():
        return df.with_columns(pl.lit("").alias("url"))
    return df.with_columns(
        pl.col(id_col)
        .map_elements(lambda rid: url_pour_enregistrement(base_url, model, rid), return_dtype=pl.Utf8)
        .alias("url")
    )
