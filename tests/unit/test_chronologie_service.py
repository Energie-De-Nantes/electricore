"""Tests du service de la vue facturiste (chronologie + verdicts, #367).

`_tisser_frise` est le **cœur pur** du service : il tisse, à partir d'un `ContexteMensuel`
(reconstruit filtré par #366), la frise complète d'un point/contrat — **faits** (événements
C15 *y compris hors-comptage* + relevés, avec origine/nature) **+ verdicts dérivés** des
périodes (qualité/communication/énergie). On le teste sur un contexte synthétique pour
isoler la valeur ajoutée (tissage + projection), sans I/O DuckDB.

Garde-fous d'ADR-0027/0012 portés ici :
- **Pas de montants tarifaires** (turpe/cta/accise) dans la frise — différenciateur vs
  `/meta-periodes` ;
- un point à plusieurs RSC (changement de main) doit tracer ses charnières ;
- un point récemment activé (MDPRM) doit voir son verdict communication correct (post-#365 :
  le niveau n'est plus forward-fillé depuis les seuls relevés indexés).
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from electricore.api.services.chronologie_service import _tisser_frise
from electricore.core.builds.contexte_mensuel import ContexteMensuel

PARIS = ZoneInfo("Europe/Paris")


def _ctx(*, historique: pl.DataFrame, energie: pl.DataFrame, releves: pl.DataFrame) -> ContexteMensuel:
    """Emballe les trois frames dérivés dans un `ContexteMensuel` (les autres vides)."""
    return ContexteMensuel(
        mois="2024-03-01",
        historique_enrichi=historique.lazy(),
        abonnements=pl.LazyFrame({}),
        energie=energie.lazy(),
        releves_utilises=releves.lazy(),
        facturation_mensuelle=pl.DataFrame({}),
    )


def _historique(lignes: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        lignes,
        schema_overrides={"date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris")},
    )


def _energie(lignes: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        lignes,
        schema_overrides={
            "debut": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "fin": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
        },
    )


def _releves(lignes: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        lignes,
        schema_overrides={"date_releve": pl.Datetime(time_unit="us", time_zone="Europe/Paris")},
    )


# Faits minimaux d'un PDL : entrée MES, MDPRM hors-comptage, FACTURATION.
EVT_MES = {
    "date_evenement": datetime(2024, 1, 5, 0, 1, tzinfo=PARIS),
    "pdl": "PDL_X",
    "ref_situation_contractuelle": "REF_1",
    "source": "flux_C15",
    "type_fait": "evenement",
    "evenement_declencheur": "MES",
    "puissance_souscrite_kva": 6.0,
    "formule_tarifaire_acheminement": "BTINFCU4",
    "niveau_ouverture_services": "2",
    "impacte_abonnement": True,
    "resume_modification": "",
}
EVT_MDPRM = {
    "date_evenement": datetime(2024, 1, 16, 0, 1, tzinfo=PARIS),
    "pdl": "PDL_X",
    "ref_situation_contractuelle": "REF_1",
    "source": "flux_C15",
    "type_fait": "evenement",
    "evenement_declencheur": "MDPRM",  # hors-comptage (bascule niveau)
    "puissance_souscrite_kva": 6.0,
    "formule_tarifaire_acheminement": "BTINFCU4",
    "niveau_ouverture_services": "2",
    "impacte_abonnement": False,
    "resume_modification": "",
}
EVT_FACT = {
    "date_evenement": datetime(2024, 2, 1, 0, 0, tzinfo=PARIS),
    "pdl": "PDL_X",
    "ref_situation_contractuelle": "REF_1",
    "source": "synthese_mensuelle",
    "type_fait": "facturation",
    "evenement_declencheur": "FACTURATION",
    "puissance_souscrite_kva": 6.0,
    "formule_tarifaire_acheminement": "BTINFCU4",
    "niveau_ouverture_services": "2",
    "impacte_abonnement": True,
    "resume_modification": "",
}

PERIODE_ENERGIE = {
    "pdl": "PDL_X",
    "ref_situation_contractuelle": "REF_1",
    "debut": datetime(2024, 1, 5, tzinfo=PARIS),
    "fin": datetime(2024, 2, 1, tzinfo=PARIS),
    "nb_jours": 27,
    "qualite": "réelle",
    "statut_communication": "communicante",
    "energie_base_kwh": 50.0,
    "energie_hp_kwh": None,
    "energie_hc_kwh": None,
    "turpe_variable_eur": 9.99,  # montant tarifaire : DOIT être écarté de la frise
}

RELEVE = {
    "ref_situation_contractuelle": "REF_1",
    "date_releve": datetime(2024, 1, 5, tzinfo=PARIS),
    "ordre_index": False,
    "releve_id": "abcd1234",
    "nature_index": "réel",
    "source": "flux_R151",
    "evenement_declencheur": None,
    "index_base_kwh": 100,
}


class TestTissageFaitsEtVerdicts:
    def test_frise_contient_evenements_releves_et_periodes(self):
        frise = _tisser_frise(
            _ctx(
                historique=_historique([EVT_MES, EVT_FACT]),
                energie=_energie([PERIODE_ENERGIE]),
                releves=_releves([RELEVE]),
            )
        )
        types = set(frise["type_ligne"].to_list())
        # Trois familles de lignes tissées : événement, relevé, période d'énergie.
        assert types == {"evenement", "releve", "periode_energie"}

    def test_periode_porte_les_verdicts_sans_montant_tarifaire(self):
        frise = _tisser_frise(
            _ctx(
                historique=_historique([EVT_MES, EVT_FACT]),
                energie=_energie([PERIODE_ENERGIE]),
                releves=_releves([RELEVE]),
            )
        )
        periode = frise.filter(pl.col("type_ligne") == "periode_energie").row(0, named=True)
        # Verdicts présents.
        assert periode["qualite"] == "réelle"
        assert periode["statut_communication"] == "communicante"
        # Énergie physique présente (kWh, pas un montant).
        assert periode["energie_base_kwh"] == 50.0
        # AUCUN montant tarifaire dans la frise (différenciateur vs /meta-periodes).
        assert "turpe_variable_eur" not in frise.columns
        assert "turpe_fixe_eur" not in frise.columns
        assert "cta_eur" not in frise.columns

    def test_evenement_hors_comptage_mdprm_est_present(self):
        """Un MDPRM (hors-comptage, sans index) figure dans la frise comme fait."""
        frise = _tisser_frise(
            _ctx(
                historique=_historique([EVT_MES, EVT_MDPRM, EVT_FACT]),
                energie=_energie([PERIODE_ENERGIE]),
                releves=_releves([RELEVE]),
            )
        )
        evenements = frise.filter(pl.col("type_ligne") == "evenement")["evenement_declencheur"].to_list()
        assert "MDPRM" in evenements

    def test_releve_porte_son_origine_et_sa_nature(self):
        frise = _tisser_frise(
            _ctx(
                historique=_historique([EVT_MES, EVT_FACT]),
                energie=_energie([PERIODE_ENERGIE]),
                releves=_releves([RELEVE]),
            )
        )
        releve = frise.filter(pl.col("type_ligne") == "releve").row(0, named=True)
        assert releve["nature_index"] == "réel"
        assert releve["origine_releve"] == "périodique"  # source R151 → télérelevé

    def test_frise_triee_chronologiquement(self):
        frise = _tisser_frise(
            _ctx(
                historique=_historique([EVT_MES, EVT_MDPRM, EVT_FACT]),
                energie=_energie([PERIODE_ENERGIE]),
                releves=_releves([RELEVE]),
            )
        )
        dates = frise["date"].to_list()
        assert dates == sorted(dates)


class TestPlusieursRSC:
    """Grain point : un PDL qui change de main (REF_1 → REF_2) trace ses charnières."""

    def test_les_deux_rsc_apparaissent(self):
        evt_res = {
            **EVT_MES,
            "evenement_declencheur": "RES",
            "ref_situation_contractuelle": "REF_1",
            "date_evenement": datetime(2024, 6, 1, tzinfo=PARIS),
        }
        evt_cfne = {
            **EVT_MES,
            "evenement_declencheur": "CFNE",
            "ref_situation_contractuelle": "REF_2",
            "date_evenement": datetime(2024, 6, 2, tzinfo=PARIS),
        }
        frise = _tisser_frise(
            _ctx(
                historique=_historique([EVT_MES, evt_res, evt_cfne]),
                energie=_energie([PERIODE_ENERGIE]),
                releves=_releves([RELEVE]),
            )
        )
        rsc = set(frise.filter(pl.col("type_ligne") == "evenement")["ref_situation_contractuelle"].to_list())
        assert rsc == {"REF_1", "REF_2"}
