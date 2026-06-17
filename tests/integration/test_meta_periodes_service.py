"""Tests du service méta-périodes : enrichissement réglementaire (ADR-0027, #228).

Le seam est `contexte_du_mois` (monkeypatché par un `ContexteMensuel` synthétique) ;
les registres de taux régulés (CTA, Accise) sont les **vrais** CSV versionnés — le
mois de test est passé (2025-03), donc les taux en vigueur y sont stables.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from electricore.api.services import meta_periodes_service
from electricore.core.builds.contexte_mensuel import ContexteMensuel

PARIS = ZoneInfo("Europe/Paris")


def _contexte_synthetique() -> ContexteMensuel:
    """`ContexteMensuel` minimal : seuls `mois` + `facturation_mensuelle` servent au service."""
    facturation = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-1", "RSC-2"],
            "pdl": ["12345678901234", "12345678905678"],
            "mois_annee": ["2025-03", "2025-03"],
            "debut": [datetime(2025, 3, 1, tzinfo=PARIS), datetime(2025, 3, 1, tzinfo=PARIS)],
            "fin": [datetime(2025, 4, 1, tzinfo=PARIS), datetime(2025, 4, 1, tzinfo=PARIS)],
            "nb_jours": [31, 31],
            "puissance_moyenne_kva": [6.0, 9.0],
            "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFCUST"],
            "energie_base_kwh": [None, 420.0],
            "energie_hp_kwh": [312.4, None],
            "energie_hc_kwh": [145.2, None],
            "turpe_fixe_eur": [10.0, 0.0],
            "turpe_variable_eur": [18.4, 22.0],
            "has_changement": [False, False],
            "qualite": ["réelle", "incalculable"],
            "statut_communication": ["communicante", "non_communicante"],
        }
    )
    vide = pl.LazyFrame()
    return ContexteMensuel(
        mois="2025-03-01",
        historique_enrichi=vide,
        abonnements=vide,
        energie=vide,
        releves_utilises=vide,
        facturation_mensuelle=facturation,
    )


def test_meta_periodes_expose_axes_statut(monkeypatch):
    """#326 : l'endpoint expose les verdicts méta jumeaux — qualité (ADR-0033) +
    statut de communication (ADR-0036) — à côté du réglementaire, pour consommation
    ERP / rapport de facturation."""
    monkeypatch.setattr(meta_periodes_service, "contexte_du_mois", lambda mois=None: _contexte_synthetique())

    _, df = meta_periodes_service.meta_periodes("2025-03-01")

    assert "qualite" in df.columns
    assert "statut_communication" in df.columns
    lignes = df.sort("ref_situation_contractuelle")
    assert lignes["qualite"].to_list() == ["réelle", "incalculable"]
    assert lignes["statut_communication"].to_list() == ["communicante", "non_communicante"]


def test_meta_periodes_ajoute_cta_eur_et_taux_accise(monkeypatch):
    """Le service enrichit le payload avec `cta_eur` (€) et `taux_accise_eur_mwh` (taux)."""
    monkeypatch.setattr(meta_periodes_service, "contexte_du_mois", lambda mois=None: _contexte_synthetique())

    mois, df = meta_periodes_service.meta_periodes("2025-03-01")

    assert mois == "2025-03-01"
    assert "cta_eur" in df.columns
    assert "taux_accise_eur_mwh" in df.columns

    # Pas d'accise_eur : electricore livre le taux, pas le montant (ADR-0027).
    assert "accise_eur" not in df.columns

    lignes = df.sort("ref_situation_contractuelle")
    # CTA = turpe_fixe × taux/100 : nul quand turpe_fixe = 0, positif sinon.
    assert lignes["cta_eur"][0] > 0.0  # RSC-1, turpe_fixe = 10
    assert lignes["cta_eur"][1] == 0.0  # RSC-2, turpe_fixe = 0
    # Taux accise standard en vigueur : positif et uniforme sur le mois.
    assert lignes["taux_accise_eur_mwh"][0] > 0.0
    assert lignes["taux_accise_eur_mwh"][0] == lignes["taux_accise_eur_mwh"][1]


def test_meta_periodes_source_hash_deterministe_et_distinct(monkeypatch):
    """`source_hash` : présent, déterministe (même état → même hash), distinct par contenu."""
    monkeypatch.setattr(meta_periodes_service, "contexte_du_mois", lambda mois=None: _contexte_synthetique())

    _, df1 = meta_periodes_service.meta_periodes("2025-03-01")
    _, df2 = meta_periodes_service.meta_periodes("2025-03-01")

    assert "source_hash" in df1.columns
    h1 = df1.sort("ref_situation_contractuelle")["source_hash"].to_list()
    h2 = df2.sort("ref_situation_contractuelle")["source_hash"].to_list()
    assert h1 == h2  # déterministe
    assert len(set(h1)) == 2  # deux lignes de contenu différent → deux hash


def test_meta_periodes_source_hash_change_si_quantite_change(monkeypatch):
    """Toute modification d'une quantité du payload change le `source_hash` de la ligne."""
    monkeypatch.setattr(meta_periodes_service, "contexte_du_mois", lambda mois=None: _contexte_synthetique())
    _, avant = meta_periodes_service.meta_periodes("2025-03-01")
    h_avant = avant.sort("ref_situation_contractuelle")["source_hash"][0]

    base = _contexte_synthetique()
    fm_modifie = base.facturation_mensuelle.with_columns(
        pl.when(pl.col("ref_situation_contractuelle") == "RSC-1")
        .then(pl.lit(999.0))
        .otherwise(pl.col("energie_hp_kwh"))
        .alias("energie_hp_kwh")
    )
    ctx_modifie = ContexteMensuel(
        mois="2025-03-01",
        historique_enrichi=base.historique_enrichi,
        abonnements=base.abonnements,
        energie=base.energie,
        releves_utilises=base.releves_utilises,
        facturation_mensuelle=fm_modifie,
    )
    monkeypatch.setattr(meta_periodes_service, "contexte_du_mois", lambda mois=None: ctx_modifie)
    _, apres = meta_periodes_service.meta_periodes("2025-03-01")
    h_apres = apres.sort("ref_situation_contractuelle")["source_hash"][0]

    assert h_avant != h_apres
