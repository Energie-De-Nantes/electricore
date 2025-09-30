import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import marimo as mo
    import polars as pl
    import sys
    from pathlib import Path
    from datetime import datetime, timezone, date
    import time
    import calendar
    from typing import Dict, List, Optional, Tuple

    # Ajouter le chemin du projet
    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    # Imports des loaders DuckDB
    from electricore.core.loaders.duckdb_loader import (
        f15, c15, 
        r151, r64, 
        execute_custom_query
    )


@app.cell
def _():
    r64_df = r64().lazy().collect()
    r64_df
    return (r64_df,)


@app.cell
def _(r64_df):
    r64_df.is_duplicated()
    return


@app.cell
def _():
    from electricore.core.loaders.duckdb_loader import releves_harmonises
    df = releves_harmonises().validate(True).lazy().collect()
    df
    return


@app.cell
def _():
    r151().collect()
    return


if __name__ == "__main__":
    app.run()
