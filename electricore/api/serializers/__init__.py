"""Sérialiseurs des artefacts API (XLSX multi-onglets, Arrow IPC, ZIP CSV) — issue #41.

Factorisent les patterns de sérialisation dupliqués entre les services pour
que les `api/services/*.py` se réduisent à `df = orchestration(...)` →
`return serialize(df)`.
"""

import io
import zipfile

import polars as pl
import xlsxwriter


def xlsx_multi_sheet(onglets: dict[str, pl.DataFrame]) -> bytes:
    """Construit un fichier XLSX multi-onglets à partir d'un dict {nom: DataFrame}.

    L'option `remove_timezone=True` est appliquée — nécessaire pour les DataFrames
    qui portent des colonnes `Europe/Paris` (XLSX ne stocke pas les timezones).

    Args:
        onglets: dict ordonné `{nom_onglet: DataFrame}`. Les onglets apparaissent
            dans l'ordre d'insertion.

    Returns:
        Bytes du XLSX (container ZIP).
    """
    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {"remove_timezone": True})
    for nom, df in onglets.items():
        df.write_excel(workbook=wb, worksheet=nom)
    wb.close()
    return buf.getvalue()


def arrow_stream(df: pl.DataFrame) -> bytes:
    """Sérialise un DataFrame Polars en flux Arrow IPC.

    Args:
        df: DataFrame à sérialiser.

    Returns:
        Bytes du flux Arrow IPC, re-lisible via `pl.read_ipc_stream(BytesIO(...))`.
    """
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    return buf.getvalue()


def zip_csv(documents: dict[str, pl.DataFrame]) -> bytes:
    """Construit un ZIP (deflated) où chaque entrée est un CSV Polars.

    Args:
        documents: dict `{nom_fichier_dans_le_zip: DataFrame}`. Chaque DataFrame
            est sérialisé via `df.write_csv()`.

    Returns:
        Bytes du ZIP.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for nom, df in documents.items():
            zf.writestr(nom, df.write_csv())
    return buf.getvalue()


__all__ = ["xlsx_multi_sheet", "arrow_stream", "zip_csv"]
