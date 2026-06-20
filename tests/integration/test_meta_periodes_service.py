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


def _contexte_synthetique(releves_utilises: pl.LazyFrame | None = None) -> ContexteMensuel:
    """`ContexteMensuel` minimal : `mois` + `facturation_mensuelle` (+ `releves_utilises`).

    `releves_utilises` par défaut vide (les tests réglementaires n'en ont pas besoin) ;
    les tests d'index (#360) passent un frame peuplé via `_releves_utilises_synthetiques`.
    """
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
        releves_utilises=vide if releves_utilises is None else releves_utilises,
        facturation_mensuelle=facturation,
    )


def _releves_utilises_synthetiques(decalage_index: int = 0) -> pl.LazyFrame:
    """Frame `releves_utilises` (forme *Chronologie des relevés*, ADR-0029) pour #360.

    - **RSC-1** (mois `réelle`, HP/HC) : 3 relevés bornants — début, milieu (MCT), fin —
      pour exercer le cas MCT (> 2 entrées). `decalage_index` décale les index ABSOLUS
      des deux bornes extrêmes du même `+k` (delta kWh du mois inchangé) → test source_hash.
    - **RSC-2** (mois `incalculable`) : porte tout de même un relevé dans le frame, pour
      prouver que le gate `qualite` force `[]` même quand des relevés existent.
    - Un relevé `releve_manquant` (releve_id null) ne doit jamais ressortir.
    """
    k = decalage_index
    return pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-1", "RSC-1", "RSC-1", "RSC-2"],
            "date_releve": [
                datetime(2025, 3, 1, tzinfo=PARIS),
                datetime(2025, 3, 15, tzinfo=PARIS),
                datetime(2025, 4, 1, tzinfo=PARIS),
                datetime(2025, 3, 1, tzinfo=PARIS),
            ],
            "ordre_index": [False, False, False, False],
            "releve_id": ["a1b2c3d4e5f60718", "1122334455667788", "99aabbccddeeff00", "deadbeefdeadbeef"],
            "nature_index": ["réel", "réel", "réel", "réel"],
            # Compteur HP/HC : seuls hp/hc portés (le mart ne synthétise jamais → les
            # 4 cadrans saisonniers restent nuls, donc non exposés).
            "index_base_kwh": [None, None, None, 420],
            "index_hp_kwh": [1000 + k, 1150, 1312 + k, None],
            "index_hc_kwh": [500 + k, 580, 645 + k, None],
            "index_hph_kwh": [None, None, None, None],
            "index_hch_kwh": [None, None, None, None],
            "index_hpb_kwh": [None, None, None, None],
            "index_hcb_kwh": [None, None, None, None],
        }
    ).lazy()


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


# --- Relevés utilisés imbriqués (trace d'index légale, ADR-0038, #360) -------------


def test_releves_utilises_present_si_calculable_vide_si_incalculable(monkeypatch):
    """Invariant « vide ssi incalculable » (ADR-0038) : chaque méta-période porte un
    tableau `releves_utilises` ; non vide ⟺ `qualite ∈ {réelle, estimée}`,
    `incalculable ⟹ []` — plein-ou-rien, même si des relevés existent dans le frame."""
    ctx = _contexte_synthetique(_releves_utilises_synthetiques())
    monkeypatch.setattr(meta_periodes_service, "contexte_du_mois", lambda mois=None: ctx)

    _, df = meta_periodes_service.meta_periodes("2025-03-01")

    assert "releves_utilises" in df.columns
    lignes = df.sort("ref_situation_contractuelle")
    ru = lignes["releves_utilises"].to_list()

    # RSC-1 (réelle) → tableau non vide avec ses relevés bornants.
    assert len(ru[0]) >= 1
    # RSC-2 (incalculable) → [] même si un relevé existe dans le frame.
    assert ru[1] == []

    # Objet relevé = { releve_id, date_releve, nature_index, registres RÉELS uniquement }.
    premier = ru[0][0]
    assert set(premier) == {"releve_id", "date_releve", "nature_index", "index_hp_kwh", "index_hc_kwh"}
    assert "index_base_kwh" not in premier  # registre nul → jamais exposé
    assert premier["releve_id"] == "a1b2c3d4e5f60718"
    assert premier["nature_index"] == "réel"
    assert "2025-03-01" in str(premier["date_releve"])


def test_releves_utilises_expose_tous_les_cadrans_reels(monkeypatch):
    """Tous les **registres réels** du compteur sont exposés, pas seulement base/hp/hc.

    Un compteur 4-quadrants (Tempo/C4) porte ses index `hph/hch/hpb/hcb` ; le mart ne
    synthétise jamais (hp/hc restent nuls), donc le tableau doit rendre exactement les
    4 cadrans réels non nuls — jamais un cadran agrégé absent du compteur."""
    facturation = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-T"],
            "pdl": ["12345678909999"],
            "mois_annee": ["2025-03"],
            "debut": [datetime(2025, 3, 1, tzinfo=PARIS)],
            "fin": [datetime(2025, 4, 1, tzinfo=PARIS)],
            "nb_jours": [31],
            "puissance_moyenne_kva": [36.0],
            "formule_tarifaire_acheminement": ["BTSUPCU4"],
            "energie_base_kwh": [None],
            "energie_hp_kwh": [None],
            "energie_hc_kwh": [None],
            "turpe_fixe_eur": [50.0],
            "turpe_variable_eur": [120.0],
            "has_changement": [False],
            "qualite": ["réelle"],
            "statut_communication": ["communicante"],
        }
    )
    releves = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-T", "RSC-T"],
            "date_releve": [datetime(2025, 3, 1, tzinfo=PARIS), datetime(2025, 4, 1, tzinfo=PARIS)],
            "ordre_index": [False, False],
            "releve_id": ["1111111111111111", "2222222222222222"],
            "nature_index": ["réel", "réel"],
            "index_base_kwh": [None, None],
            "index_hp_kwh": [None, None],
            "index_hc_kwh": [None, None],
            "index_hph_kwh": [100, 250],
            "index_hch_kwh": [200, 360],
            "index_hpb_kwh": [300, 480],
            "index_hcb_kwh": [400, 590],
        }
    ).lazy()
    vide = pl.LazyFrame()
    ctx = ContexteMensuel(
        mois="2025-03-01",
        historique_enrichi=vide,
        abonnements=vide,
        energie=vide,
        releves_utilises=releves,
        facturation_mensuelle=facturation,
    )
    monkeypatch.setattr(meta_periodes_service, "contexte_du_mois", lambda mois=None: ctx)

    _, df = meta_periodes_service.meta_periodes("2025-03-01")
    objet = df["releves_utilises"].to_list()[0][0]

    assert set(objet) == {
        "releve_id",
        "date_releve",
        "nature_index",
        "index_hph_kwh",
        "index_hch_kwh",
        "index_hpb_kwh",
        "index_hcb_kwh",
    }
    assert objet["index_hph_kwh"] == 100 and objet["index_hcb_kwh"] == 400
    # Jamais de cadran agrégé absent du compteur (hp/hc nuls non synthétisés).
    assert "index_hp_kwh" not in objet and "index_hc_kwh" not in objet


def test_releves_utilises_inclut_releves_intermediaires_mct(monkeypatch):
    """Mois à MCT : les relevés intermédiaires utilisés figurent dans le tableau — il
    n'est PAS limité à 2 entrées (ADR-0038). RSC-1 porte début + milieu + fin."""
    ctx = _contexte_synthetique(_releves_utilises_synthetiques())
    monkeypatch.setattr(meta_periodes_service, "contexte_du_mois", lambda mois=None: ctx)

    _, df = meta_periodes_service.meta_periodes("2025-03-01")
    ru_rsc1 = df.sort("ref_situation_contractuelle")["releves_utilises"].to_list()[0]

    assert len(ru_rsc1) == 3, f"MCT : 3 relevés bornants attendus, vu {len(ru_rsc1)}"
    dates = [str(o["date_releve"]) for o in ru_rsc1]
    assert any("2025-03-15" in d for d in dates), "le relevé intermédiaire (MCT) doit figurer"


def test_source_hash_couvre_releves_utilises_a_delta_kwh_constant(monkeypatch):
    """`source_hash` couvre le tableau imbriqué (ADR-0038) : une correction ±k des index
    absolus AUX DEUX BORNES fait flipper le hash **alors même que le delta kWh du mois est
    inchangé** (la facturation mensuelle est identique). Sans le repli du tableau dans le
    hash, les deux états seraient indistinguables."""
    base = _contexte_synthetique(_releves_utilises_synthetiques(decalage_index=0))
    monkeypatch.setattr(meta_periodes_service, "contexte_du_mois", lambda mois=None: base)
    _, avant = meta_periodes_service.meta_periodes("2025-03-01")
    h_avant = avant.sort("ref_situation_contractuelle")["source_hash"][0]

    # Même facturation mensuelle (delta kWh constant), seuls les index absolus des bornes bougent.
    decale = _contexte_synthetique(_releves_utilises_synthetiques(decalage_index=100))
    monkeypatch.setattr(meta_periodes_service, "contexte_du_mois", lambda mois=None: decale)
    _, apres = meta_periodes_service.meta_periodes("2025-03-01")
    h_apres = apres.sort("ref_situation_contractuelle")["source_hash"][0]

    assert h_avant != h_apres, "une dérive d'index imprimé à delta constant doit flipper source_hash"
