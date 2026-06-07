"""Sérialiseurs des artefacts API (XLSX multi-onglets, Arrow IPC) — issue #41.

Consommés directement par les endpoints stackant `@xlsx_endpoint`/`@arrow_endpoint`
au-dessus des orchestrations de `integrations.odoo`. `zip_csv` a été supprimé en
#78 (plus aucun caller — `/facturation/documents` migré en XLSX multi-onglets).
"""

import io

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


__all__ = ["xlsx_multi_sheet", "arrow_stream"]
