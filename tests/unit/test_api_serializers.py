"""Tests des sérialiseurs `api/serializers/` (issue #41).

Vérifient que chaque sérialiseur produit des bytes conformes au format
attendu — sans présumer de l'implémentation (xlsxwriter, polars, zipfile…).
"""

import io
import zipfile

import polars as pl


def test_xlsx_multi_sheet_produit_xlsx_valide_avec_onglets_nommes() -> None:
    """`xlsx_multi_sheet({nom: df, ...})` produit un XLSX (signature ZIP) avec un onglet par entrée."""
    from electricore.api.serializers import xlsx_multi_sheet

    onglets = {
        "Résumé": pl.DataFrame({"total": [42]}),
        "Détail": pl.DataFrame({"ligne": ["a", "b"], "valeur": [1, 2]}),
    }
    output = xlsx_multi_sheet(onglets)

    assert isinstance(output, bytes)
    assert output[:4] == b"PK\x03\x04", "XLSX = container ZIP, doit commencer par PK\\x03\\x04"

    with zipfile.ZipFile(io.BytesIO(output)) as zf:
        names = zf.namelist()
        assert "xl/workbook.xml" in names, "Structure XLSX attendue : xl/workbook.xml"
        # Un xl/worksheets/sheetN.xml par onglet
        sheet_files = [n for n in names if n.startswith("xl/worksheets/sheet")]
        assert len(sheet_files) == len(onglets), (
            f"Attendu {len(onglets)} feuilles, trouvé {len(sheet_files)} : {sheet_files}"
        )


def test_arrow_stream_produit_flux_relisable_avec_donnees_identiques() -> None:
    """`arrow_stream(df)` produit un flux Arrow IPC re-lisible avec les mêmes données."""
    from polars.testing import assert_frame_equal

    from electricore.api.serializers import arrow_stream

    df = pl.DataFrame({"pdl": ["123", "456"], "energie_kwh": [100.5, 200.0]})
    output = arrow_stream(df)

    assert isinstance(output, bytes)
    assert len(output) > 0

    df_round_trip = pl.read_ipc_stream(io.BytesIO(output))
    assert_frame_equal(df_round_trip, df)


def test_zip_csv_produit_zip_avec_chaque_csv_nomme_et_non_vide() -> None:
    """`zip_csv({nom: df, ...})` produit un ZIP avec un CSV par entrée, contenu = CSV Polars."""
    from electricore.api.serializers import zip_csv

    documents = {
        "tableau_a.csv": pl.DataFrame({"x": [1, 2]}),
        "tableau_b.csv": pl.DataFrame({"y": ["a"]}),
    }
    output = zip_csv(documents)

    assert isinstance(output, bytes)
    with zipfile.ZipFile(io.BytesIO(output)) as zf:
        assert set(zf.namelist()) == set(documents.keys())
        # Chaque entrée contient le CSV Polars correspondant
        for nom, df in documents.items():
            assert zf.read(nom).decode("utf-8") == df.write_csv()
