"""Tests de `presence_perimetre` / `pdls_actifs_a` (`core/pipelines/perimetre.py`, ADR-0052).

Span de présence `[debut, fin)` par RSC ; « actif à une date » demi-ouvert, dédup PDL.
"""

from datetime import date, datetime

import polars as pl

from electricore.core.pipelines.perimetre import pdls_actifs_a, presence_perimetre


def _c15(rows: list[tuple[str, str, datetime, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "pdl": pl.Utf8,
            "ref_situation_contractuelle": pl.Utf8,
            "date_evenement": pl.Datetime,
            "evenement_declencheur": pl.Utf8,
        },
        orient="row",
    )


_JEU = _c15(
    [
        ("B", "rB", datetime(2020, 1, 1), "MES"),  # span ouvert
        ("A", "rA", datetime(2020, 1, 1), "MES"),
        ("A", "rA", datetime(2021, 1, 1), "RES"),  # [2020, 2021)
        ("E", "rE", datetime(2020, 1, 1), "MES"),
        ("E", "rE", datetime(2020, 6, 1), "MCT"),  # modif au milieu → n'ouvre/ferme rien
        ("E", "rE", datetime(2021, 1, 1), "RES"),  # [2020, 2021)
        ("C", "rC1", datetime(2019, 1, 1), "MES"),
        ("C", "rC1", datetime(2020, 1, 1), "RES"),  # [2019, 2020)
        ("C", "rC2", datetime(2022, 1, 1), "CFNE"),  # ré-entrée = nouvelle RSC, ouverte
    ]
)


def test_presence_un_span_par_rsc():
    spans = presence_perimetre(_JEU)
    par_rsc = {r["ref_situation_contractuelle"]: (r["debut"], r["fin"]) for r in spans.iter_rows(named=True)}

    assert par_rsc["rB"] == (date(2020, 1, 1), None)  # ouvert
    assert par_rsc["rA"] == (date(2020, 1, 1), date(2021, 1, 1))
    assert par_rsc["rE"] == (date(2020, 1, 1), date(2021, 1, 1))  # la modif n'y change rien
    assert par_rsc["rC1"] == (date(2019, 1, 1), date(2020, 1, 1))
    assert par_rsc["rC2"] == (date(2022, 1, 1), None)


def test_actif_a_une_date_dedup_pdl():
    # 2020-06 : A (dans [2020,2021)), B (ouvert), E (dans) ; pas C (entre ses deux RSC).
    assert pdls_actifs_a(_JEU, date(2020, 6, 1))["pdl"].to_list() == ["A", "B", "E"]


def test_actif_a_borne_haute_exclue():
    # Demi-ouvert : résilié le 2021-01-01 ⟹ absent ce jour-là.
    assert pdls_actifs_a(_JEU, date(2021, 1, 1))["pdl"].to_list() == ["B"]


def test_actif_reentree_via_nouvelle_rsc():
    # 2023 : B (ouvert) + C (via rC2 ouverte) ; A et E sont sortis.
    assert pdls_actifs_a(_JEU, date(2023, 1, 1))["pdl"].to_list() == ["B", "C"]
    # 2019-06 : seul C, via rC1 (avant même l'entrée des autres).
    assert pdls_actifs_a(_JEU, date(2019, 6, 1))["pdl"].to_list() == ["C"]


def test_accepte_un_lazyframe():
    assert pdls_actifs_a(_JEU.lazy(), date(2020, 6, 1))["pdl"].to_list() == ["A", "B", "E"]
