"""Tests du service de déclaration CTA parc entier (#651, ADR-0058).

Seam : `contexte_du_mois` (monkeypatché par un `ContexteMensuel` synthétique) ; le
registre CTA est le **vrai** CSV versionné. L'horizon est injecté (1ᵉʳ juillet 2025) :
2025-T1 et 2025-T2 sont terminés, 2025-T3 est en cours.
"""

import datetime as dt
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from fastapi import HTTPException

from electricore.api.services import declaration_cta_service as service
from electricore.core.builds.contexte_mensuel import ContexteMensuel

PARIS = ZoneInfo("Europe/Paris")
HORIZON = dt.datetime(2025, 7, 1, tzinfo=PARIS)
MOIS = [(2025, 2), (2025, 3), (2025, 4), (2025, 7)]  # T1, T1, T2, T3 (en cours)


def _facturation(turpe_fixe_rsc1: float = 10.0) -> pl.DataFrame:
    lignes = [
        {
            "ref_situation_contractuelle": rsc,
            "pdl": pdl,
            "mois_annee": f"{annee}-{mois:02d}",
            "debut": dt.datetime(annee, mois, 1, tzinfo=PARIS),
            "turpe_fixe_eur": turpe_fixe_rsc1 if rsc == "RSC-1" else 20.0,
            "turpe_variable_eur": 5.0,
        }
        for annee, mois in MOIS
        for rsc, pdl in (("RSC-1", "12345678901234"), ("RSC-2", "12345678905678"))
    ]
    return pl.DataFrame(lignes)


def _brancher(monkeypatch, facturation: pl.DataFrame) -> None:
    vide = pl.LazyFrame()
    contexte = ContexteMensuel(
        mois="2025-07-01",
        historique_enrichi=vide,
        abonnements=vide,
        energie=vide,
        releves_utilises=vide,
        facturation_mensuelle=facturation,
    )
    monkeypatch.setattr(service, "contexte_du_mois", lambda mois=None, horizon=None: contexte)


def test_sans_trimestre_sert_les_trimestres_termines_du_parc_entier(monkeypatch):
    _brancher(monkeypatch, _facturation())
    df = service.declaration_cta(horizon=HORIZON)

    assert df["trimestre"].unique().sort().to_list() == ["2025-T1", "2025-T2"]
    assert df.height == 6  # 3 mois terminés × 2 RSC — le mois de T3 (en cours) est exclu
    assert df["pdl"].n_unique() == 2
    assert "turpe_variable_eur" not in df.columns
    assert (df["cta_eur"] > 0).all()


def test_empreinte_trimestre_par_trimestre_et_stable(monkeypatch):
    _brancher(monkeypatch, _facturation())
    df = service.declaration_cta(horizon=HORIZON)

    par_trimestre = df.group_by("trimestre").agg(pl.col("empreinte_trimestre").n_unique())
    assert (par_trimestre["empreinte_trimestre"] == 1).all()
    assert df["empreinte_trimestre"].n_unique() == 2
    assert df["source_hash"].n_unique() == df.height
    assert df.equals(service.declaration_cta(horizon=HORIZON))


def test_empreinte_change_si_un_montant_du_trimestre_change(monkeypatch):
    _brancher(monkeypatch, _facturation())
    avant = service.declaration_cta(horizon=HORIZON)
    _brancher(monkeypatch, _facturation(turpe_fixe_rsc1=11.0))
    apres = service.declaration_cta(horizon=HORIZON)

    def empreinte(df: pl.DataFrame, trimestre: str) -> str:
        return df.filter(pl.col("trimestre") == trimestre)["empreinte_trimestre"][0]

    assert empreinte(avant, "2025-T1") != empreinte(apres, "2025-T1")
    assert empreinte(avant, "2025-T2") != empreinte(apres, "2025-T2")


def test_empreinte_ne_depend_pas_des_autres_trimestres(monkeypatch):
    """L'empreinte d'un trimestre est la même qu'on le demande seul ou dans l'historique."""
    _brancher(monkeypatch, _facturation())
    tout = service.declaration_cta(horizon=HORIZON)
    seul = service.declaration_cta("2025-T1", horizon=HORIZON)

    assert seul["trimestre"].unique().to_list() == ["2025-T1"]
    assert seul["empreinte_trimestre"][0] == tout.filter(pl.col("trimestre") == "2025-T1")["empreinte_trimestre"][0]


@pytest.mark.parametrize("trimestre", ["2025-T3", "2026-T1"])
def test_trimestre_non_termine_refuse_en_precondition(monkeypatch, trimestre):
    _brancher(monkeypatch, _facturation())
    with pytest.raises(HTTPException) as exc:
        service.declaration_cta(trimestre, horizon=HORIZON)
    assert exc.value.status_code == 422
    assert exc.value.headers == {"X-Error-Kind": "precondition"}


def test_feuilles_resume_porte_empreinte_et_version(monkeypatch):
    _brancher(monkeypatch, _facturation())
    df = service.declaration_cta(horizon=HORIZON)
    feuilles = service.feuilles_declaration_cta(df)

    assert list(feuilles) == ["Résumé", "Par taux", "Détail"]
    resume = feuilles["Résumé"]
    assert resume["trimestre"].to_list() == ["2025-T1", "2025-T2"]
    assert resume["cta_total_eur"].to_list() == [
        round(df.filter(pl.col("trimestre") == t)["cta_eur"].sum(), 2) for t in ("2025-T1", "2025-T2")
    ]
    assert resume.join(df.select("trimestre", "empreinte_trimestre").unique(), on="trimestre").height == 2
    assert (resume["version_empreinte"] == service.VERSION_EMPREINTE).all()
    assert feuilles["Détail"].equals(df)
