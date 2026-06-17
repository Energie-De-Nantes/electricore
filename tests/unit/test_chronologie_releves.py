"""Tests de la *Chronologie des relevés* (issue #180, ADR-0023, ADR-0028).

`chronologie_releves(historique, releves)` assemble la ligne de temps énergie d'un
contrat : relevés aux événements C15 (avant/après) + relevés périodiques interrogés aux
dates de facturation, dédoublonnés par source prioritaire et ordonnés.

Les invariants encodés jusqu'ici incidemment sont ici testés explicitement :
grain unique, RSC non-null, source dans l'énumération, priorité C15 > R64 > R151,
forward-fill RSC, dédoublonnage, tolérance ±4 h (constante nommée), et `ordre_index`
booléen.

Le contrat de bout en bout (`chronologie_releves`, décoré `@pa.check_types`) est exercé
par `test_chronologie_contrat_bout_en_bout` sur un `Historique` conforme. Les tests
d'invariants ciblent l'implémentation pure `_assembler_chronologie` (le chemin réellement
emprunté par `pipeline_energie`) pour rester sur des fixtures légères.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from electricore.core.models.chronologie_releves import SOURCES_CHRONOLOGIE, ChronologieReleves
from electricore.core.models.releve_index import RelevéIndex
from electricore.core.pipelines.energie import (
    PRIORITE_SOURCES,
    TOLERANCE_APPARIEMENT_RELEVES,
    _assembler_chronologie,
    chronologie_releves,
)
from electricore.core.pipelines.historique import detecter_points_de_rupture

PARIS = ZoneInfo("Europe/Paris")

# Cadrans index (hors base) à compléter à None dans les événements.
_AUTRES_CADRANS = ("hp", "hc", "hch", "hph", "hcb", "hpb")


def _evenement(
    pdl: str,
    ref: str,
    date: datetime,
    declencheur: str,
    avant_base: float | None = None,
    apres_base: float | None = None,
) -> dict:
    """Dict de ligne d'événement contractuel (index base seul) pour `_assembler_chronologie`."""
    autres = {f"{pos}_index_{cad}_kwh": None for pos in ("avant", "apres") for cad in _AUTRES_CADRANS}
    return {
        "pdl": pdl,
        "ref_situation_contractuelle": ref,
        "formule_tarifaire_acheminement": "BTINFCUST",
        "evenement_declencheur": declencheur,
        "date_evenement": date,
        "avant_index_base_kwh": avant_base,
        "apres_index_base_kwh": apres_base,
        "avant_id_calendrier_distributeur": 1 if avant_base is not None else None,
        "apres_id_calendrier_distributeur": 1 if apres_base is not None else None,
        **autres,
    }


def _historique_brut(lignes: list[dict]) -> pl.LazyFrame:
    """Frame d'événements minimal (non validé Pandera) pour `_assembler_chronologie`."""
    df = pl.DataFrame(lignes)
    return df.with_columns(
        pl.col("date_evenement").dt.replace_time_zone("Europe/Paris"),
        pl.col("avant_id_calendrier_distributeur").cast(pl.Int64),
        pl.col("apres_id_calendrier_distributeur").cast(pl.Int64),
    ).lazy()


def _releves(lignes: list[dict]) -> pl.LazyFrame:
    df = pl.DataFrame(lignes)
    # Index en kWh entiers (ADR-0034) : RelevéIndex/ChronologieReleves attendent Int64.
    index_cols = [c for c in df.columns if c.startswith("index_") and c.endswith("_kwh")]
    return df.with_columns(
        pl.col("date_releve").dt.replace_time_zone("Europe/Paris"),
        *[pl.col(c).cast(pl.Int64) for c in index_cols],
    ).lazy()


def _releve(
    pdl: str,
    date: datetime,
    source: str,
    base: float,
    *,
    ref: str | None = None,
    ordre_index: bool = False,
) -> dict:
    d = {
        "pdl": pdl,
        "date_releve": date,
        "source": source,
        "index_base_kwh": base,
        "ordre_index": ordre_index,
        "id_calendrier_distributeur": "DI000001",
    }
    if ref is not None:
        d["ref_situation_contractuelle"] = ref
    return d


# ---------------------------------------------------------------------------
# Tracer bullet : grain, RSC non-null, source dans l'énumération
# ---------------------------------------------------------------------------


def test_chronologie_grain_rsc_source():
    """1 ligne par (RSC, date_releve, ordre_index) ; RSC non-null ; source dans l'enum."""
    historique = _historique_brut(
        [
            _evenement("PDL001", "REF001", datetime(2024, 1, 15), "MES", 1000.0, 1500.0),
            _evenement("PDL001", "REF001", datetime(2024, 2, 1), "FACTURATION"),
        ]
    )
    releves = _releves([_releve("PDL001", datetime(2024, 2, 1), "flux_R151", 2000.0)])

    result = _assembler_chronologie(historique, releves).collect()

    cle = ["ref_situation_contractuelle", "date_releve", "ordre_index"]
    assert result.select(cle).n_unique() == len(result)
    assert result["ref_situation_contractuelle"].null_count() == 0
    assert set(result["source"].unique().to_list()) <= set(SOURCES_CHRONOLOGIE)
    assert result.schema["ordre_index"] == pl.Boolean


# ---------------------------------------------------------------------------
# Invariant 1 : priorité des sources explicite C15 > R64 > R151
# ---------------------------------------------------------------------------


def test_priorite_r64_bat_r151():
    """Même relevé logique dans R64 et R151 → R64 gagne (valeur + source)."""
    historique = _historique_brut([_evenement("PDL001", "REF001", datetime(2024, 2, 1), "FACTURATION")])
    # Deux relevés périodiques à la même date : R64 (corrigé) et R151 (workhorse).
    releves = _releves(
        [
            _releve("PDL001", datetime(2024, 2, 1), "flux_R151", 1111.0, ref="REF001"),
            _releve("PDL001", datetime(2024, 2, 1), "flux_R64", 2222.0, ref="REF001"),
        ]
    )

    result = _assembler_chronologie(historique, releves).collect()
    ligne = result.filter(pl.col("date_releve") == datetime(2024, 2, 1, tzinfo=PARIS))

    assert ligne["source"].to_list() == ["flux_R64"]
    assert ligne["index_base_kwh"].to_list() == [2222.0]


def test_priorite_c15_bat_periodique():
    """Même relevé logique en C15 et R151 → C15 gagne."""
    historique = _historique_brut(
        [
            _evenement("PDL001", "REF001", datetime(2024, 1, 15), "MES", 1000.0, 1500.0),
            _evenement("PDL001", "REF001", datetime(2024, 1, 15), "FACTURATION"),
        ]
    )
    # C15 (avant/après) et R151 au même jour : C15 vient désormais du modèle canonique
    # `releves` (ADR-0029), aux côtés du périodique. L'événement reste dans l'historique
    # pour le semi-join d'impact.
    releves = _releves(
        [
            _releve("PDL001", datetime(2024, 1, 15), "flux_C15", 1000.0, ref="REF001", ordre_index=False),
            _releve("PDL001", datetime(2024, 1, 15), "flux_C15", 1500.0, ref="REF001", ordre_index=True),
            _releve("PDL001", datetime(2024, 1, 15), "flux_R151", 9999.0, ref="REF001"),
        ]
    )

    result = _assembler_chronologie(historique, releves).collect()
    a_la_date = result.filter(pl.col("date_releve") == datetime(2024, 1, 15, tzinfo=PARIS))

    # Seuls les relevés C15 (avant/après) restent ; pas la valeur R151.
    assert a_la_date["source"].unique().to_list() == ["flux_C15"]
    assert 9999.0 not in a_la_date["index_base_kwh"].to_list()
    assert set(a_la_date["index_base_kwh"].to_list()) == {1000.0, 1500.0}


def test_table_priorite_ordre():
    """La table de priorité est explicite et ordonne C15 < R64 < R151 (rang croissant)."""
    assert PRIORITE_SOURCES["flux_C15"] < PRIORITE_SOURCES["flux_R64"]
    assert PRIORITE_SOURCES["flux_R64"] < PRIORITE_SOURCES["flux_R151"]


# ---------------------------------------------------------------------------
# Invariant 2 : attribution RSC par forward-fill sur le PDL
# ---------------------------------------------------------------------------


def test_forward_fill_rsc():
    """Un relevé périodique sans RSC hérite de la RSC de l'événement précédent (par PDL)."""
    historique = _historique_brut(
        [
            _evenement("PDL001", "REF001", datetime(2024, 1, 15), "MES", 1000.0, 1500.0),
            _evenement("PDL001", "REF001", datetime(2024, 2, 1), "FACTURATION"),
        ]
    )
    # Relevé R151 sans ref_situation_contractuelle.
    releves = _releves([_releve("PDL001", datetime(2024, 2, 1), "flux_R151", 2000.0)])

    result = _assembler_chronologie(historique, releves).collect()
    r151 = result.filter(pl.col("source") == "flux_R151")

    assert len(r151) == 1
    assert r151["ref_situation_contractuelle"].to_list() == ["REF001"]


# ---------------------------------------------------------------------------
# Invariant 3 : dédoublonnage sur le triplet (RSC, date_releve, ordre_index)
# ---------------------------------------------------------------------------


def test_dedup_sur_triplet():
    """Deux relevés même (RSC, date, ordre_index) → une seule ligne après dédoublonnage."""
    historique = _historique_brut([_evenement("PDL001", "REF001", datetime(2024, 2, 1), "FACTURATION")])
    releves = _releves(
        [
            _releve("PDL001", datetime(2024, 2, 1), "flux_R151", 1000.0, ref="REF001"),
            _releve("PDL001", datetime(2024, 2, 1), "flux_R151", 1000.0, ref="REF001"),
        ]
    )

    result = _assembler_chronologie(historique, releves).collect()
    cle = ["ref_situation_contractuelle", "date_releve", "ordre_index"]
    assert result.select(cle).n_unique() == len(result)
    assert len(result.filter(pl.col("date_releve") == datetime(2024, 2, 1, tzinfo=PARIS))) == 1


# ---------------------------------------------------------------------------
# Invariant 4 : tolérance ±4 h (constante nommée) résout le décalage C15 vs R151
# ---------------------------------------------------------------------------


def test_tolerance_constante_nommee():
    """La tolérance d'appariement est une constante nommée (pas un littéral noyé)."""
    assert TOLERANCE_APPARIEMENT_RELEVES == "4h"


def test_tolerance_resout_decalage_c15_r151():
    """Un événement à 00:01 trouve le relevé R151 à 02:00 (< 4 h) ; un à 06:01 ne le trouve pas (> 4 h)."""
    historique = _historique_brut(
        [
            _evenement("PDL001", "REF001", datetime(2024, 2, 1, 0, 1), "FACTURATION"),
            _evenement("PDL002", "REF002", datetime(2024, 2, 1, 6, 1), "FACTURATION"),
        ]
    )
    releves = _releves(
        [
            _releve("PDL001", datetime(2024, 2, 1, 2, 0), "flux_R151", 2000.0, ref="REF001"),
            _releve("PDL002", datetime(2024, 2, 1, 2, 0), "flux_R151", 3000.0, ref="REF002"),
        ]
    )

    result = _assembler_chronologie(historique, releves).collect()

    # PDL001 : décalage 1h59 < 4h → relevé trouvé.
    pdl001 = result.filter(pl.col("pdl") == "PDL001")
    assert pdl001["releve_manquant"].to_list() == [False]
    assert pdl001["index_base_kwh"].to_list() == [2000.0]

    # PDL002 : décalage 4h01 > 4h tolérance → relevé manquant.
    pdl002 = result.filter(pl.col("pdl") == "PDL002")
    assert pdl002["releve_manquant"].to_list() == [True]
    assert pdl002["index_base_kwh"].to_list() == [None]


# ---------------------------------------------------------------------------
# Invariant 5 : ordre_index booléen de bout en bout
# ---------------------------------------------------------------------------


def test_ordre_index_booleen_avant_apres():
    """ordre_index est booléen : False = avant / périodique, True = après C15."""
    historique = _historique_brut(
        [
            _evenement("PDL001", "REF001", datetime(2024, 1, 15), "MES", 1000.0, 1500.0),
            _evenement("PDL001", "REF001", datetime(2024, 2, 1), "FACTURATION"),
        ]
    )
    releves = _releves(
        [
            _releve("PDL001", datetime(2024, 1, 15), "flux_C15", 1000.0, ref="REF001", ordre_index=False),
            _releve("PDL001", datetime(2024, 1, 15), "flux_C15", 1500.0, ref="REF001", ordre_index=True),
            _releve("PDL001", datetime(2024, 2, 1), "flux_R151", 2000.0, ref="REF001"),
        ]
    )

    result = _assembler_chronologie(historique, releves).collect()
    assert result.schema["ordre_index"] == pl.Boolean

    c15 = result.filter(pl.col("source") == "flux_C15").sort("index_base_kwh")
    # avant (1000) → False, après (1500) → True
    assert c15["ordre_index"].to_list() == [False, True]

    r151 = result.filter(pl.col("source") == "flux_R151")
    assert r151["ordre_index"].to_list() == [False]


# ---------------------------------------------------------------------------
# Contrat de bout en bout : chronologie_releves sur un Historique conforme
# ---------------------------------------------------------------------------


def _historique_conforme() -> pl.LazyFrame:
    """Construit un `Historique` Pandera-conforme (MES + FACTURATION sur 1 contrat)."""
    base = {
        "segment_clientele": "C5",
        "etat_contractuel": "EN SERVICE",
        "type_evenement": "CONTRAT",
        "puissance_souscrite_kva": 6.0,
        "formule_tarifaire_acheminement": "BTINFCUST",
        "type_compteur": "LINKY",
        "num_compteur": "C001",
        "impacte_abonnement": True,
        "resume_modification": "",
    }
    lignes = [
        {
            **base,
            "pdl": "PDL001",
            "ref_situation_contractuelle": "REF001",
            "date_evenement": datetime(2024, 1, 15),
            "evenement_declencheur": "MES",
            "impacte_energie": True,
            "avant_index_base_kwh": 1000.0,
            "apres_index_base_kwh": 1500.0,
            "avant_id_calendrier_distributeur": "DI000001",
            "apres_id_calendrier_distributeur": "DI000001",
        },
        {
            **base,
            "pdl": "PDL001",
            "ref_situation_contractuelle": "REF001",
            "date_evenement": datetime(2024, 2, 1),
            "evenement_declencheur": "FACTURATION",
            "impacte_energie": True,
            "avant_index_base_kwh": None,
            "apres_index_base_kwh": None,
            "avant_id_calendrier_distributeur": None,
            "apres_id_calendrier_distributeur": None,
        },
    ]
    # Index avant/après en kWh entiers (Int64, ADR-0034/0035), comme le contrat Historique.
    autres = {
        f"{pos}_index_{cad}_kwh": pl.Series([None, None], dtype=pl.Int64)
        for pos in ("avant", "apres")
        for cad in _AUTRES_CADRANS
    }
    df = pl.DataFrame(lignes).with_columns(
        **autres,
        avant_index_base_kwh=pl.col("avant_index_base_kwh").cast(pl.Int64),
        apres_index_base_kwh=pl.col("apres_index_base_kwh").cast(pl.Int64),
    )
    return df.with_columns(pl.col("date_evenement").dt.replace_time_zone("Europe/Paris")).lazy()


def _releve_index_conforme() -> pl.LazyFrame:
    """Relevé R151 conforme `RelevéIndex` (date tz-aware, unite/precision)."""
    df = pl.DataFrame(
        {
            "pdl": ["PDL001"],
            "ref_situation_contractuelle": ["REF001"],
            "formule_tarifaire_acheminement": ["BTINFCUST"],
            "date_releve": [datetime(2024, 2, 1)],
            "ordre_index": [False],
            "source": ["flux_R151"],
            "unite": ["kWh"],
            "precision": ["kWh"],
            "index_base_kwh": [2000],
            "id_calendrier_distributeur": ["DI000001"],
        }
    )
    return df.with_columns(
        pl.col("date_releve").dt.replace_time_zone("Europe/Paris"),
        pl.col("index_base_kwh").cast(pl.Int64),  # kWh entiers (ADR-0034)
    ).lazy()


def test_chronologie_contrat_bout_en_bout():
    """`chronologie_releves` valide entrées (Historique/RelevéIndex) et sortie (ChronologieReleves)."""
    result = chronologie_releves(_historique_conforme(), _releve_index_conforme()).collect()

    # Le contrat Pandera a tenu : grain unique, RSC non-null, source dans l'enum.
    cle = ["ref_situation_contractuelle", "date_releve", "ordre_index"]
    assert result.select(cle).n_unique() == len(result)
    assert result["ref_situation_contractuelle"].null_count() == 0
    assert set(result["source"].unique().to_list()) <= set(SOURCES_CHRONOLOGIE)
    assert result.schema["ordre_index"] == pl.Boolean


def test_releve_index_porte_niveau_ouverture_services():
    """#324 (ADR-0036) : RelevéIndex (contrat du mart `releves`) déclare
    `niveau_ouverture_services` (Utf8, nullable) — la *jumelle* de `nature_index` pour
    l'axe « voie communicante ». Nullable : un relevé périodique avant tout C15 n'en
    porte pas (forward-fill amont absent)."""
    cols = RelevéIndex.to_schema().columns
    assert "niveau_ouverture_services" in cols, f"colonne absente du contrat, vu : {sorted(cols)}"
    assert cols["niveau_ouverture_services"].nullable


def test_chronologie_porte_niveau_ouverture_services():
    """#324 : la chronologie des relevés (entrée du calcul d'énergie) porte le niveau au
    contrat, typé et nullable — le verdict d'ouverture de période (#325) le rollupera."""
    cols = ChronologieReleves.to_schema().columns
    assert "niveau_ouverture_services" in cols, f"colonne absente du contrat, vu : {sorted(cols)}"
    assert cols["niveau_ouverture_services"].nullable


# ---------------------------------------------------------------------------
# Régression énergie : calculer_periodes_energie consomme la chronologie
# (ordre_index booléen) et rend EXACTEMENT les mêmes énergies qu'avant.
# ---------------------------------------------------------------------------


def test_calculer_periodes_energie_golden_avec_ordre_index_booleen():
    """Golden : énergies inchangées (cadrans BASE/HP/HC/4-slots) avec ordre_index booléen.

    Index croissants mensuels sur 1 contrat ; les énergies par cadran sont les
    différences d'index entre relevés consécutifs. Pin de non-régression du
    consommateur (`calculer_periodes_energie`) après l'unification du discriminant.
    """
    from electricore.core.pipelines.energie import calculer_periodes_energie

    releves = pl.LazyFrame(
        {
            "pdl": ["PDL001"] * 3,
            "ref_situation_contractuelle": ["REF001"] * 3,
            "date_releve": [
                datetime(2024, 1, 1, tzinfo=PARIS),
                datetime(2024, 2, 1, tzinfo=PARIS),
                datetime(2024, 3, 1, tzinfo=PARIS),
            ],
            "source": ["flux_C15", "flux_R151", "flux_R151"],
            "index_base_kwh": [1000.0, 2000.0, 3000.0],
            "index_hp_kwh": [500.0, 1000.0, 1500.0],
            "index_hc_kwh": [200.0, 400.0, 600.0],
            "index_hph_kwh": [100.0, 200.0, 300.0],
            "index_hpb_kwh": [150.0, 300.0, 450.0],
            "index_hch_kwh": [80.0, 160.0, 240.0],
            "index_hcb_kwh": [120.0, 240.0, 360.0],
            "releve_manquant": [None, False, False],
            # Discriminant booléen unifié (False = avant / périodique).
            "ordre_index": [False, False, False],
        },
        schema_overrides={"date_releve": pl.Datetime(time_unit="us", time_zone="Europe/Paris")},
    )

    result = calculer_periodes_energie(releves).collect()
    periodes = result.filter(pl.col("debut").is_not_null()).sort("debut")

    assert len(periodes) == 2
    # Période 1 (jan→fév) et 2 (fév→mar) : diffs d'index constantes.
    # energie_base = base brut + hp + hc (synthèse hiérarchique). Ici l'index "base"
    # est porté tel quel ; on vérifie les cadrans bruts non agrégés (hph/hpb/hch/hcb)
    # qui reflètent directement la différence d'index.
    assert periodes["energie_hph_kwh"].to_list() == [100.0, 100.0]
    assert periodes["energie_hpb_kwh"].to_list() == [150.0, 150.0]
    assert periodes["energie_hch_kwh"].to_list() == [80.0, 80.0]
    assert periodes["energie_hcb_kwh"].to_list() == [120.0, 120.0]
    # hp = hph + hpb (180 + 270 → mais aussi le hp brut 500). Synthèse : hp_brut + hph + hpb.
    assert periodes["energie_hp_kwh"].to_list() == [500.0 + 100.0 + 150.0] * 2
    assert periodes["energie_hc_kwh"].to_list() == [200.0 + 80.0 + 120.0] * 2


# ---------------------------------------------------------------------------
# Bascule du statut de communication (épique #313, AC #314) : le relevé porté par
# l'activation du calendrier Distributeur entre déjà dans la chronologie via
# `impacte_energie` — preuve par test, sans code dédié.
# ---------------------------------------------------------------------------


def test_bascule_cmat_releve_entre_dans_chronologie():
    """À la bascule communicante, le relevé de l'activation du calendrier Distributeur
    (`CMAT`) entre dans la chronologie parce que `impacte_energie` se déclenche sur le
    *changement de calendrier distributeur*, jamais sur le type d'événement. Le `MDPRM`
    qui acte le niveau d'ouverture n'impacte pas l'énergie (le statut route au grain
    méta, pas au grain relevé) : son relevé éventuel est filtré. Aucun code ajouté —
    on prouve le comportement existant (cf. CONTEXT « Voie communicante »)."""
    cmat = datetime(2024, 3, 1)
    mdprm = datetime(2024, 3, 15)
    cadrans_index = ("base", *_AUTRES_CADRANS)

    historique = (
        pl.DataFrame(
            {
                "pdl": ["PDL777", "PDL777"],
                "ref_situation_contractuelle": ["REF777", "REF777"],
                "date_evenement": [cmat, mdprm],
                "evenement_declencheur": ["CMAT", "MDPRM"],
                "puissance_souscrite_kva": [6.0, 6.0],
                "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4"],
                # CMAT : le calendrier distributeur change (DI000001 → DI000003), index base
                # inchangé → le *seul* déclencheur est le changement de calendrier.
                # MDPRM : ni calendrier ni index ne changent → n'impacte pas l'énergie.
                "avant_id_calendrier_distributeur": ["DI000001", "DI000003"],
                "apres_id_calendrier_distributeur": ["DI000003", "DI000003"],
                "avant_index_base_kwh": [5000, 5000],
                "apres_index_base_kwh": [5000, 5000],
                **{f"{pos}_index_{c}_kwh": [None, None] for pos in ("avant", "apres") for c in _AUTRES_CADRANS},
            }
        )
        .with_columns(
            pl.col("date_evenement").dt.replace_time_zone("Europe/Paris"),
            *[pl.col(f"{pos}_index_{c}_kwh").cast(pl.Int64) for pos in ("avant", "apres") for c in cadrans_index],
        )
        .lazy()
    )

    enrichi = detecter_points_de_rupture(historique)
    impacts = dict(enrichi.select("evenement_declencheur", "impacte_energie").collect().iter_rows())
    # (A) CMAT impacte l'énergie (changement de calendrier) ; MDPRM (niveau seul) non.
    assert impacts["CMAT"] is True
    assert impacts["MDPRM"] is False

    # (B) Le relevé C15 à la date du CMAT entre dans la chronologie (semi-join sur les
    #     événements `impacte_energie`, comme dans `pipeline_energie`) ; celui du MDPRM,
    #     filtré faute d'impact énergie.
    releves = _releves(
        [
            _releve("PDL777", cmat, "flux_C15", 5000.0, ref="REF777"),
            _releve("PDL777", mdprm, "flux_C15", 5000.0, ref="REF777"),
        ]
    )
    chronologie = enrichi.filter(pl.col("impacte_energie")).pipe(_assembler_chronologie, releves).collect()

    dates = chronologie["date_releve"].to_list()
    assert datetime(2024, 3, 1, tzinfo=PARIS) in dates
    assert datetime(2024, 3, 15, tzinfo=PARIS) not in dates
