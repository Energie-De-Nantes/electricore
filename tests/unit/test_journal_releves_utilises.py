"""Tests du *journal des relevés utilisés* (`releves_utilises`, issue #233).

Le journal promeut la *Chronologie des relevés* enrichie — jusqu'ici calculée puis
détruite par `pipeline_energie` — en livrable conservé du `ContexteMensuel`. Il
conserve, par relevé bornant une période d'énergie du mois : identité (`releve_id`),
*nature d'index*, `source`, `date_releve`, et les **index en registres réels par
cadran** (jamais de cadran synthétisé). Voir `core/CONTEXT.md`, *Traçabilité des
index*.

Deux niveaux :
- `journal_releves_utilises` (`energie.py`) : le producteur — même assemblage que
  celui consommé en interne par `pipeline_energie`, donc énergies inchangées.
- `_scoper_journal_au_mois` (`contexte_mensuel.py`) : restreint le journal aux relevés
  bornant les périodes d'énergie du mois cible — c'est là que le cas MCT se vérifie
  sans aucun cas particulier.
"""

import datetime as dt
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from electricore.core.builds.contexte_mensuel import _scoper_journal_au_mois, charger
from electricore.core.models.cadrans import CADRANS, col_index
from electricore.core.pipelines.energie import journal_releves_utilises

PARIS = ZoneInfo("Europe/Paris")

_AUTRES_CADRANS = ("hp", "hc", "hch", "hph", "hcb", "hpb")


def _evenement(
    pdl: str,
    ref: str,
    date: datetime,
    declencheur: str,
    *,
    avant_base: float | None = None,
    apres_base: float | None = None,
    impacte_energie: bool = True,
) -> dict:
    """Ligne d'événement contractuel enrichi (index base seul) pour le journal."""
    autres = {f"{pos}_index_{cad}_kwh": None for pos in ("avant", "apres") for cad in _AUTRES_CADRANS}
    return {
        "pdl": pdl,
        "ref_situation_contractuelle": ref,
        "formule_tarifaire_acheminement": "BTINFCUST",
        "evenement_declencheur": declencheur,
        "date_evenement": date,
        "impacte_energie": impacte_energie,
        "avant_index_base_kwh": avant_base,
        "apres_index_base_kwh": apres_base,
        "avant_id_calendrier_distributeur": 1 if avant_base is not None else None,
        "apres_id_calendrier_distributeur": 1 if apres_base is not None else None,
        **autres,
    }


def _historique(lignes: list[dict]) -> pl.LazyFrame:
    df = pl.DataFrame(lignes)
    return df.with_columns(
        pl.col("date_evenement").dt.replace_time_zone("Europe/Paris"),
        pl.col("avant_id_calendrier_distributeur").cast(pl.Int64),
        pl.col("apres_id_calendrier_distributeur").cast(pl.Int64),
    ).lazy()


def _releve(pdl: str, date: datetime, source: str, base: float, *, ref: str | None = None) -> dict:
    d = {
        "pdl": pdl,
        "date_releve": date,
        "source": source,
        "index_base_kwh": base,
        "ordre_index": False,
        "id_calendrier_distributeur": "DI000001",
        "releve_id": f"{source}|{pdl}|{date.isoformat()}|false",
        "id_releve": None,
        "nature_index": "réel",
    }
    if ref is not None:
        d["ref_situation_contractuelle"] = ref
    return d


def _releves(lignes: list[dict]) -> pl.LazyFrame:
    if not lignes:
        # Frame périodique vide mais typé (aucun relevé R151/R64 disponible).
        return pl.LazyFrame(
            schema={
                "pdl": pl.Utf8,
                "date_releve": pl.Datetime("us", "Europe/Paris"),
                "source": pl.Utf8,
                "index_base_kwh": pl.Float64,
                "ordre_index": pl.Boolean,
                "id_calendrier_distributeur": pl.Utf8,
                "releve_id": pl.Utf8,
                "id_releve": pl.Utf8,
                "nature_index": pl.Utf8,
            }
        )
    df = pl.DataFrame(lignes)
    return df.with_columns(pl.col("date_releve").dt.replace_time_zone("Europe/Paris")).lazy()


# ---------------------------------------------------------------------------
# Producteur : journal_releves_utilises conserve les index réels + l'identité
# ---------------------------------------------------------------------------


def test_journal_conserve_index_reels_et_identite():
    """Le journal porte les colonnes-clés : identité, nature, source, date, index réels."""
    historique = _historique(
        [
            _evenement("PDL001", "REF001", datetime(2024, 1, 15), "MES", avant_base=1000.0, apres_base=1500.0),
            _evenement("PDL001", "REF001", datetime(2024, 2, 1), "FACTURATION"),
        ]
    )
    releves = _releves([_releve("PDL001", datetime(2024, 2, 1), "flux_R151", 2000.0, ref="REF001")])

    journal = journal_releves_utilises(historique, releves).collect()

    # Colonnes requises du contrat #233.
    requises = {"releve_id", "nature_index", "ref_situation_contractuelle", "date_releve", "ordre_index", "source"}
    assert requises <= set(journal.columns)
    # Index réels par cadran conservés (pas détruits par la sélection finale d'énergie).
    assert "index_base_kwh" in journal.columns
    assert set(journal["index_base_kwh"].drop_nulls().to_list()) == {1000.0, 1500.0, 2000.0}
    # releve_id non-null partout.
    assert journal["releve_id"].null_count() == 0


def test_journal_grain_un_par_triplet():
    """1 ligne par (RSC, date_releve, ordre_index) — grain métier de la chronologie."""
    historique = _historique(
        [
            _evenement("PDL001", "REF001", datetime(2024, 1, 15), "MES", avant_base=1000.0, apres_base=1500.0),
            _evenement("PDL001", "REF001", datetime(2024, 2, 1), "FACTURATION"),
            _evenement("PDL001", "REF001", datetime(2024, 3, 1), "FACTURATION"),
        ]
    )
    releves = _releves(
        [
            _releve("PDL001", datetime(2024, 2, 1), "flux_R151", 2000.0, ref="REF001"),
            _releve("PDL001", datetime(2024, 3, 1), "flux_R151", 3000.0, ref="REF001"),
        ]
    )

    journal = journal_releves_utilises(historique, releves).collect()
    cle = ["ref_situation_contractuelle", "date_releve", "ordre_index"]
    assert journal.select(cle).n_unique() == len(journal)


# ---------------------------------------------------------------------------
# Scopage au mois : les DEUX relevés bornant chaque période du mois figurent
# ---------------------------------------------------------------------------


def _energie_periode(ref: str, debut: datetime, fin: datetime, mois_annee: str) -> dict:
    return {"ref_situation_contractuelle": ref, "debut": debut, "fin": fin, "mois_annee": mois_annee}


def _energie(lignes: list[dict]) -> pl.LazyFrame:
    return pl.DataFrame(
        lignes,
        schema_overrides={
            "debut": pl.Datetime("us", "Europe/Paris"),
            "fin": pl.Datetime("us", "Europe/Paris"),
        },
    ).lazy()


def test_scopage_garde_les_deux_bornes_de_la_periode_du_mois():
    """Le journal scopé au mois contient l'index d'ouverture ET de clôture de la période."""
    historique = _historique(
        [
            _evenement("PDL001", "REF001", datetime(2024, 2, 1), "FACTURATION"),
            _evenement("PDL001", "REF001", datetime(2024, 3, 1), "FACTURATION"),
            _evenement("PDL001", "REF001", datetime(2024, 4, 1), "FACTURATION"),
        ]
    )
    releves = _releves(
        [
            _releve("PDL001", datetime(2024, 2, 1), "flux_R151", 2000.0, ref="REF001"),
            _releve("PDL001", datetime(2024, 3, 1), "flux_R151", 3000.0, ref="REF001"),
            _releve("PDL001", datetime(2024, 4, 1), "flux_R151", 4000.0, ref="REF001"),
        ]
    )
    journal = journal_releves_utilises(historique, releves)

    # Une période de mars : 1er mars → 1er avril.
    energie = _energie(
        [
            _energie_periode(
                "REF001",
                datetime(2024, 3, 1, tzinfo=PARIS),
                datetime(2024, 4, 1, tzinfo=PARIS),
                "2024-03",
            )
        ]
    )

    scope = _scoper_journal_au_mois(journal, energie, "2024-03-01")

    dates = sorted(scope["date_releve"].dt.strftime("%Y-%m-%d").to_list())
    # Les deux bornes (ouverture 1er mars, clôture 1er avril) sont conservées.
    assert dates == ["2024-03-01", "2024-04-01"]
    # Le relevé de février (borne d'une période d'un autre mois) est exclu.
    assert "2024-02-01" not in dates
    # Index réels conservés pour les deux bornes.
    assert sorted(scope["index_base_kwh"].to_list()) == [3000.0, 4000.0]


# ---------------------------------------------------------------------------
# Cas MCT : un changement de configuration en cours de mois ⇒ relevés
# intermédiaires en lignes supplémentaires du journal, SANS cas particulier
# ---------------------------------------------------------------------------


def test_mct_releves_intermediaires_dans_le_journal():
    """Un changement de config (MCT) au milieu du mois ⇒ les relevés avant/après
    intermédiaires figurent comme lignes supplémentaires du journal scopé.

    Février est découpé par le MCT du 15 en deux sous-périodes du même mois
    (1er→15 et 15→1er mars). Le journal du mois de février doit donc porter :
    - l'index d'ouverture (1er février, périodique) ;
    - les **deux** index intermédiaires du MCT (avant/après le 15) ;
    - l'index de clôture (1er mars, périodique).
    Aucun aiguillage MCT : ce sont simplement les bornes des sous-périodes.
    """
    historique = _historique(
        [
            _evenement("PDL001", "REF001", datetime(2024, 2, 1), "FACTURATION"),
            # MCT en cours de mois : porte les index avant/après réels.
            _evenement("PDL001", "REF001", datetime(2024, 2, 15), "MCT", avant_base=2500.0, apres_base=2500.0),
            _evenement("PDL001", "REF001", datetime(2024, 3, 1), "FACTURATION"),
        ]
    )
    releves = _releves(
        [
            _releve("PDL001", datetime(2024, 2, 1), "flux_R151", 2000.0, ref="REF001"),
            _releve("PDL001", datetime(2024, 3, 1), "flux_R151", 3000.0, ref="REF001"),
        ]
    )
    journal = journal_releves_utilises(historique, releves)

    # Énergie : deux sous-périodes de février (découpe par le MCT du 15).
    energie = _energie(
        [
            _energie_periode(
                "REF001", datetime(2024, 2, 1, tzinfo=PARIS), datetime(2024, 2, 15, tzinfo=PARIS), "2024-02"
            ),
            _energie_periode(
                "REF001", datetime(2024, 2, 15, tzinfo=PARIS), datetime(2024, 3, 1, tzinfo=PARIS), "2024-02"
            ),
        ]
    )

    scope = _scoper_journal_au_mois(journal, energie, "2024-02-01")

    dates = sorted(scope["date_releve"].dt.strftime("%Y-%m-%d").to_list())
    # Les trois dates bornantes du mois figurent (le 15 apparaît via le MCT).
    assert dates == ["2024-02-01", "2024-02-15", "2024-02-15", "2024-03-01"]

    # Les relevés intermédiaires du MCT (15 février, source C15) sont bien là,
    # un avant + un après, sans cas particulier.
    intermediaires = scope.filter(pl.col("date_releve") == datetime(2024, 2, 15, tzinfo=PARIS))
    assert len(intermediaires) == 2
    assert intermediaires["source"].unique().to_list() == ["flux_C15"]
    assert sorted(intermediaires["ordre_index"].to_list()) == [False, True]
    # Index réels portés (avant + après du MCT).
    assert intermediaires["index_base_kwh"].to_list() == [2500.0, 2500.0]


# ---------------------------------------------------------------------------
# Pas de cadran synthétisé : seuls des registres réels figurent
# ---------------------------------------------------------------------------


def test_pas_de_cadran_synthetise():
    """Le journal porte les registres réels par cadran, jamais un cadran fabriqué.

    Compteur HP/HC : seuls `index_hp_kwh` / `index_hc_kwh` sont renseignés. Le journal
    ne doit pas matérialiser un `index_base_kwh` synthétique (somme HP+HC) — `base`
    reste null faute de registre réel `base` sur ce compteur.
    """
    autres = {f"{pos}_index_{cad}_kwh": None for pos in ("avant", "apres") for cad in ("hch", "hph", "hcb", "hpb")}
    historique = _historique(
        [
            {
                "pdl": "PDL001",
                "ref_situation_contractuelle": "REF001",
                "formule_tarifaire_acheminement": "BTINFCUST",
                "evenement_declencheur": "MES",
                "date_evenement": datetime(2024, 1, 15),
                "impacte_energie": True,
                "avant_index_base_kwh": None,
                "apres_index_base_kwh": None,
                "avant_index_hp_kwh": 800.0,
                "apres_index_hp_kwh": 900.0,
                "avant_index_hc_kwh": 400.0,
                "apres_index_hc_kwh": 450.0,
                "avant_id_calendrier_distributeur": 2,
                "apres_id_calendrier_distributeur": 2,
                **autres,
            }
        ]
    )
    releves = _releves([])  # pas de périodique nécessaire ici

    journal = journal_releves_utilises(historique, releves).collect()

    # Registres réels présents (HP/HC), base jamais synthétisé.
    assert journal["index_hp_kwh"].drop_nulls().to_list() == [800.0, 900.0]
    assert journal["index_hc_kwh"].drop_nulls().to_list() == [400.0, 450.0]
    assert journal["index_base_kwh"].null_count() == len(journal)


# ---------------------------------------------------------------------------
# Bout en bout via charger() : releves_utilises peuplé ET énergies inchangées
# ---------------------------------------------------------------------------


def _historique_brut_c15(evenements: list[dict]) -> pl.LazyFrame:
    """Historique C15 brut (avant enrichissement) accepté par `pipeline_historique`."""
    data: dict[str, list] = {
        "pdl": [e["pdl"] for e in evenements],
        "ref_situation_contractuelle": [e["ref"] for e in evenements],
        "date_evenement": [e["date"] for e in evenements],
        "evenement_declencheur": [e["evenement"] for e in evenements],
        "type_evenement": [e.get("type_evenement", "reel") for e in evenements],
        "etat_contractuel": [e.get("etat", "EN SERVICE") for e in evenements],
        "segment_clientele": [e.get("segment", "C5") for e in evenements],
        "puissance_souscrite_kva": [e["puissance"] for e in evenements],
        "formule_tarifaire_acheminement": [e["fta"] for e in evenements],
        "type_compteur": [e.get("type_compteur", "LINKY") for e in evenements],
        "num_compteur": [e.get("num_compteur", "C0000001") for e in evenements],
        "avant_id_calendrier_distributeur": [e.get("avant_cal") for e in evenements],
        "apres_id_calendrier_distributeur": [e.get("apres_cal") for e in evenements],
    }
    schema_overrides: dict[str, pl.DataType] = {
        "avant_id_calendrier_distributeur": pl.Utf8,
        "apres_id_calendrier_distributeur": pl.Utf8,
    }
    for cadran in CADRANS:
        data[f"avant_{col_index(cadran)}"] = [e.get(f"avant_{col_index(cadran)}") for e in evenements]
        data[f"apres_{col_index(cadran)}"] = [e.get(f"apres_{col_index(cadran)}") for e in evenements]
        schema_overrides[f"avant_{col_index(cadran)}"] = pl.Float64
        schema_overrides[f"apres_{col_index(cadran)}"] = pl.Float64

    return pl.LazyFrame(data, schema_overrides=schema_overrides).with_columns(
        pl.col("date_evenement").dt.replace_time_zone("Europe/Paris")
    )


def _releve_index_brut(lignes: list[dict]) -> pl.LazyFrame:
    """Relevés périodiques bruts conformes `RelevéIndex` (unite/precision/source)."""
    return pl.DataFrame(
        lignes,
        schema_overrides={"date_releve": pl.Datetime("us", "Europe/Paris"), "id_releve": pl.Utf8},
    ).lazy()


def test_charger_peuple_releves_utilises_et_garde_les_energies():
    """`charger()` peuple `releves_utilises` ; les énergies de facturation sont inchangées.

    Régression : le journal est un artefact additionnel. On vérifie sur un mois donné
    que les énergies agrégées correspondent aux différences d'index des relevés
    consommés, eux-mêmes listés dans `releves_utilises`.
    """
    horizon = dt.datetime(2024, 4, 1, tzinfo=PARIS)
    historique = _historique_brut_c15(
        [
            {
                "ref": "REF001",
                "pdl": "PDL00001",
                "date": dt.datetime(2024, 1, 15, 9, 0, 0),
                "evenement": "MES",
                "puissance": 6.0,
                "fta": "BTINFCUST",
                "apres_index_base_kwh": 1000.0,
                "apres_cal": "DI000001",
            }
        ]
    )
    releves = _releve_index_brut(
        [
            {
                "pdl": "PDL00001",
                "ref_situation_contractuelle": "REF001",
                "formule_tarifaire_acheminement": "BTINFCUST",
                "date_releve": d,
                "ordre_index": False,
                "source": "flux_R151",
                "unite": "kWh",
                "precision": "kWh",
                "index_base_kwh": base,
                "id_calendrier_distributeur": "DI000001",
                "releve_id": f"flux_R151|PDL00001|{d.isoformat()}|false",
                "id_releve": None,
                "nature_index": "réel",
            }
            for d, base in [
                (dt.datetime(2024, 2, 1), 1200.0),
                (dt.datetime(2024, 3, 1), 1500.0),
                (dt.datetime(2024, 4, 1), 1700.0),
            ]
        ]
    )

    ctx = charger(historique=historique, releves=releves, mois="2024-02-01", horizon=horizon)

    # releves_utilises peuplé, avec les colonnes du contrat #233.
    journal = ctx.releves_utilises
    requises = {"releve_id", "nature_index", "ref_situation_contractuelle", "date_releve", "ordre_index", "source"}
    assert requises <= set(journal.columns)
    assert len(journal) > 0

    # Mois de février : la période 1er fév → 1er mars. Bornes = relevés 1200 et 1500.
    dates = sorted(journal["date_releve"].dt.strftime("%Y-%m-%d").to_list())
    assert dates == ["2024-02-01", "2024-03-01"]
    assert sorted(journal["index_base_kwh"].to_list()) == [1200.0, 1500.0]

    # Énergie de facturation du mois = 1500 - 1200 = 300 kWh (inchangée).
    fact_fev = ctx.facturation_mensuelle.filter(pl.col("mois_annee") == "2024-02")
    assert fact_fev["energie_base_kwh"].to_list() == [300.0]
