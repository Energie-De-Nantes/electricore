"""Tests de contrat Pandera sur les seams des pipelines `abonnements` et `energie`.

Ces tests garantissent que les seams (`pipeline_abonnements`, `generer_periodes_abonnement`,
`pipeline_energie`, `calculer_periodes_energie`) valident leurs entrées et sorties au
boundary plutôt que de crasher au fond d'une stack-trace ou de pratiquer du self-repair
silencieux. Voir l'entrée *Contrat de pipeline* de `electricore/core/CONTEXT.md`.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pandera.errors
import polars as pl
import pytest

from electricore.core.pipelines.abonnements import (
    generer_periodes_abonnement,
    pipeline_abonnements,
)
from electricore.core.pipelines.energie import (
    pipeline_energie,
)

PARIS = ZoneInfo("Europe/Paris")

PANDERA_ERRORS: tuple[type[Exception], ...] = (
    pandera.errors.SchemaError,
    pandera.errors.SchemaErrors,
)


# =============================================================================
# FIXTURES — Historique enrichi minimal valide (sortie attendue de pipeline_historique)
# =============================================================================


def _historique_enrichi_lazyframe() -> pl.LazyFrame:
    """Historique enrichi minimal mais réaliste — un PDL avec MES + RES (donc une
    période d'abonnement bornée), et un autre PDL avec MES + MCT puissance.

    Toutes les colonnes non-nullables du schéma `Historique` sont présentes, avec les
    bons types Polars (DateTime[us, Europe/Paris], Boolean, Float64).
    """
    return pl.LazyFrame(
        {
            "pdl": ["PDL00001", "PDL00001", "PDL00002", "PDL00002"],
            "ref_situation_contractuelle": ["REF001", "REF001", "REF002", "REF002"],
            "date_evenement": [
                datetime(2024, 1, 1, 0, 0, tzinfo=PARIS),
                datetime(2024, 6, 1, 0, 0, tzinfo=PARIS),
                datetime(2024, 2, 1, 0, 0, tzinfo=PARIS),
                datetime(2024, 5, 1, 0, 0, tzinfo=PARIS),
            ],
            "segment_clientele": ["C5", "C5", "C5", "C5"],
            "etat_contractuel": ["EN SERVICE", "RESILIE", "EN SERVICE", "EN SERVICE"],
            "evenement_declencheur": ["MES", "RES", "MES", "MCT"],
            "type_evenement": ["contractuel", "contractuel", "contractuel", "contractuel"],
            "puissance_souscrite_kva": [6.0, 6.0, 9.0, 12.0],
            "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4", "BTINFCU4", "BTINFCU4"],
            "type_compteur": ["LINKY", "LINKY", "LINKY", "LINKY"],
            "num_compteur": ["123456", "123456", "789012", "789012"],
            "impacte_abonnement": [True, True, True, True],
            "impacte_energie": [True, True, True, True],
            "resume_modification": ["MES initiale", "Résiliation", "MES initiale", "Changement puissance"],
        },
        schema_overrides={
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
        },
    )


@pytest.fixture
def historique_enrichi_valide() -> pl.LazyFrame:
    return _historique_enrichi_lazyframe()


# =============================================================================
# Cycle 1 — pipeline_abonnements rejette un historique non enrichi
# =============================================================================


def test_pipeline_abonnements_rejette_historique_sans_impacte_abonnement():
    """RED puis GREEN après ajout de `@pa.check_types` : la colonne requise manquante
    doit produire une SchemaError nommant `impacte_abonnement`, pas un KeyError au fond
    de `generer_periodes_abonnement`."""
    historique_brut = pl.LazyFrame(
        {
            "pdl": ["PDL00001"],
            "ref_situation_contractuelle": ["REF001"],
            "date_evenement": [datetime(2024, 1, 1, 0, 0, tzinfo=PARIS)],
            "segment_clientele": ["C5"],
            "etat_contractuel": ["EN SERVICE"],
            "evenement_declencheur": ["MES"],
            "type_evenement": ["contractuel"],
            "puissance_souscrite_kva": [6.0],
            "formule_tarifaire_acheminement": ["BTINFCU4"],
            "type_compteur": ["LINKY"],
            "num_compteur": ["123456"],
            # Volontairement omis : impacte_abonnement, impacte_energie, resume_modification
        },
        schema_overrides={
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
        },
    )

    with pytest.raises(PANDERA_ERRORS):
        pipeline_abonnements(historique_brut).collect()


# =============================================================================
# Cycle 2 — pipeline_abonnements produit une sortie conforme à `PeriodeAbonnement`
# =============================================================================


def test_pipeline_abonnements_sortie_conforme_periode_abonnement(historique_enrichi_valide):
    """Happy path : sur un Historique enrichi valide, la sortie collectée du pipeline
    doit valider intégralement contre le schéma `PeriodeAbonnement` (présence des
    colonnes, types, nullability, range checks).

    Combine deux assertions :
    - Le `@pa.check_types` du décorateur valide l'entrée + le schéma de sortie au seam.
    - L'appel explicite `PeriodeAbonnement.validate(result)` exerce la validation deep
      au niveau DataFrame (les checks nullability/range qui ne s'exécutent pas sur
      LazyFrame). Détecte toute dérive schéma/pipeline.
    """
    from electricore.core.models.periode_abonnement import PeriodeAbonnement

    result = pipeline_abonnements(historique_enrichi_valide).collect()

    # Au moins une période bornée (PDL00001 : MES → RES)
    assert result.height >= 1

    # Validation deep : déclenche tous les checks (nullability, ge, etc.)
    PeriodeAbonnement.validate(result, lazy=True)


# =============================================================================
# Cycle 3 — generer_periodes_abonnement rejette l'historique non enrichi
# =============================================================================


def test_generer_periodes_abonnement_rejette_historique_non_enrichi():
    """Symétrique du Cycle 1 sur la fonction interne `generer_periodes_abonnement` :
    elle est listée par l'issue comme un seam et doit donc valider son entrée.
    """
    historique_brut = pl.LazyFrame(
        {
            "pdl": ["PDL00001"],
            "ref_situation_contractuelle": ["REF001"],
            "date_evenement": [datetime(2024, 1, 1, 0, 0, tzinfo=PARIS)],
            "segment_clientele": ["C5"],
            "etat_contractuel": ["EN SERVICE"],
            "evenement_declencheur": ["MES"],
            "type_evenement": ["contractuel"],
            "puissance_souscrite_kva": [6.0],
            "formule_tarifaire_acheminement": ["BTINFCU4"],
            "type_compteur": ["LINKY"],
            "num_compteur": ["123456"],
            # Volontairement omis : impacte_abonnement, impacte_energie, resume_modification
        },
        schema_overrides={
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
        },
    )

    with pytest.raises(PANDERA_ERRORS):
        generer_periodes_abonnement(historique_brut).collect()


# =============================================================================
# Cycle 4 — pipeline_energie rejette un historique non enrichi
# =============================================================================
#
# Important : ce test prouve à la fois la décoration `@pa.check_types` ET la
# suppression du self-repair `if "impacte_energie" not in schema_columns:
# historique = detecter_points_de_rupture(historique)` qui masquait jusqu'ici
# l'absence d'enrichissement.


def _historique_avec_index_lazyframe(impacte_energie: bool = False) -> pl.LazyFrame:
    """Historique enrichi avec toutes les colonnes `avant_*`/`apres_*` (nullable mais
    requises par les expressions polars du pipeline énergie qui les référencent dans
    les `with_columns` même quand le filtre vide la frame).
    """
    return pl.LazyFrame(
        {
            "pdl": ["PDL00001"],
            "ref_situation_contractuelle": ["REF001"],
            "date_evenement": [datetime(2024, 1, 1, tzinfo=PARIS)],
            "segment_clientele": ["C5"],
            "etat_contractuel": ["EN SERVICE"],
            "evenement_declencheur": ["MES"],
            "type_evenement": ["contractuel"],
            "puissance_souscrite_kva": [6.0],
            "formule_tarifaire_acheminement": ["BTINFCU4"],
            "type_compteur": ["LINKY"],
            "num_compteur": ["123456"],
            "impacte_abonnement": [True],
            "impacte_energie": [impacte_energie],
            "resume_modification": ["MES initiale"],
            # avant_*/apres_* — nullable mais doivent exister pour que le plan se résolve
            "avant_index_base_kwh": [None],
            "avant_index_hp_kwh": [None],
            "avant_index_hc_kwh": [None],
            "avant_index_hph_kwh": [None],
            "avant_index_hpb_kwh": [None],
            "avant_index_hch_kwh": [None],
            "avant_index_hcb_kwh": [None],
            "apres_index_base_kwh": [None],
            "apres_index_hp_kwh": [None],
            "apres_index_hc_kwh": [None],
            "apres_index_hph_kwh": [None],
            "apres_index_hpb_kwh": [None],
            "apres_index_hch_kwh": [None],
            "apres_index_hcb_kwh": [None],
            "avant_id_calendrier_distributeur": [None],
            "apres_id_calendrier_distributeur": [None],
            "avant_id_calendrier_fournisseur": [None],
            "apres_id_calendrier_fournisseur": [None],
        },
        schema_overrides={
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "avant_index_base_kwh": pl.Float64,
            "avant_index_hp_kwh": pl.Float64,
            "avant_index_hc_kwh": pl.Float64,
            "avant_index_hph_kwh": pl.Float64,
            "avant_index_hpb_kwh": pl.Float64,
            "avant_index_hch_kwh": pl.Float64,
            "avant_index_hcb_kwh": pl.Float64,
            "apres_index_base_kwh": pl.Float64,
            "apres_index_hp_kwh": pl.Float64,
            "apres_index_hc_kwh": pl.Float64,
            "apres_index_hph_kwh": pl.Float64,
            "apres_index_hpb_kwh": pl.Float64,
            "apres_index_hch_kwh": pl.Float64,
            "apres_index_hcb_kwh": pl.Float64,
            "avant_id_calendrier_distributeur": pl.Utf8,
            "apres_id_calendrier_distributeur": pl.Utf8,
            "avant_id_calendrier_fournisseur": pl.Utf8,
            "apres_id_calendrier_fournisseur": pl.Utf8,
        },
    )


def _releves_minimaux_lazyframe() -> pl.LazyFrame:
    """Relevés conformes à `RelevéIndex` avec les colonnes optionnelles requises par
    le pipeline énergie en aval (index, id_calendrier_distributeur, etc.).
    """
    return pl.LazyFrame(
        {
            "date_releve": [datetime(2024, 1, 1, tzinfo=PARIS), datetime(2024, 6, 1, tzinfo=PARIS)],
            "pdl": ["PDL00001", "PDL00001"],
            "source": ["flux_R151", "flux_R151"],
            "unite": ["kWh", "kWh"],
            "precision": ["kWh", "kWh"],
            "ordre_index": [False, False],
            "ref_situation_contractuelle": [None, None],
            "formule_tarifaire_acheminement": [None, None],
            "id_calendrier_distributeur": [None, None],
            "id_calendrier_fournisseur": [None, None],
            "index_base_kwh": [None, None],
            "index_hp_kwh": [None, None],
            "index_hc_kwh": [None, None],
            "index_hph_kwh": [None, None],
            "index_hpb_kwh": [None, None],
            "index_hch_kwh": [None, None],
            "index_hcb_kwh": [None, None],
        },
        schema_overrides={
            "date_releve": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "ref_situation_contractuelle": pl.Utf8,
            "formule_tarifaire_acheminement": pl.Utf8,
            "id_calendrier_distributeur": pl.Utf8,
            "id_calendrier_fournisseur": pl.Utf8,
            "index_base_kwh": pl.Float64,
            "index_hp_kwh": pl.Float64,
            "index_hc_kwh": pl.Float64,
            "index_hph_kwh": pl.Float64,
            "index_hpb_kwh": pl.Float64,
            "index_hch_kwh": pl.Float64,
            "index_hcb_kwh": pl.Float64,
        },
    )


def test_pipeline_energie_rejette_historique_sans_impacte_energie():
    """Sans `impacte_energie`, le pipeline doit lever une SchemaError au seam.
    Aujourd'hui, le self-repair des lignes 605-608 d'energie.py compense silencieusement
    l'absence — ce comportement doit disparaître."""
    historique_brut = pl.LazyFrame(
        {
            "pdl": ["PDL00001"],
            "ref_situation_contractuelle": ["REF001"],
            "date_evenement": [datetime(2024, 1, 1, tzinfo=PARIS)],
            "segment_clientele": ["C5"],
            "etat_contractuel": ["EN SERVICE"],
            "evenement_declencheur": ["MES"],
            "type_evenement": ["contractuel"],
            "puissance_souscrite_kva": [6.0],
            "formule_tarifaire_acheminement": ["BTINFCU4"],
            "type_compteur": ["LINKY"],
            "num_compteur": ["123456"],
            # Omis volontairement : impacte_abonnement / impacte_energie / resume_modification
        },
        schema_overrides={
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
        },
    )

    with pytest.raises(PANDERA_ERRORS):
        pipeline_energie(historique_brut, _releves_minimaux_lazyframe()).collect()


# =============================================================================
# Cycle 5 — pipeline_energie rejette des relevés non conformes à `RelevéIndex`
# =============================================================================


def test_pipeline_energie_rejette_releves_sans_source(historique_enrichi_valide):
    """Un relevé sans la colonne `source` (non-nullable) doit faire échouer la
    validation Pandera côté `releves` au seam (présence de colonne) plutôt qu'au fond
    du pipeline.

    Note : Pandera-Polars sur `LazyFrame[X]` réalise une validation de schéma (colonnes
    + types), pas une validation deep (isin, ge, nullability au runtime). On teste donc
    l'absence d'une colonne requise, pas une valeur hors-isin (qui ne déclencherait pas
    le seam mais un crash plus loin).
    """
    releves_invalides = pl.LazyFrame(
        {
            "date_releve": [datetime(2024, 1, 1, tzinfo=PARIS)],
            "pdl": ["PDL00001"],
            "ordre_index": [False],
            "unite": ["kWh"],
            "precision": ["kWh"],
            # Omis volontairement : source (non-nullable, requis)
        },
        schema_overrides={
            "date_releve": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
        },
    )

    with pytest.raises(PANDERA_ERRORS):
        pipeline_energie(historique_enrichi_valide, releves_invalides).collect()


def test_releve_index_accepte_la_forme_du_mart_sans_unite_precision():
    """Régression /facturation/meta-periodes : le modèle de relevés canonique `releves`
    ne porte NI `unite` NI `precision` — tout est en kWh entiers, le grain facturable
    atomique (ADR-0034). Le contrat `RelevéIndex` ne doit donc plus les exiger, sinon
    `pipeline_energie` (qui valide son entrée contre `RelevéIndex`) lève `SchemaError:
    column 'unite' not in dataframe` → 500 sur /meta-periodes. Garde le chemin réel
    (forme du mart) que les anciens tests, alimentés en frames déjà conformes, ne
    couvraient pas."""
    from electricore.core.models.releve_index import RelevéIndex

    frame = pl.DataFrame(
        {
            # Toutes les colonnes déclarées par RelevéIndex SAUF unite/precision (retirées).
            "source": ["flux_R64"],
            "pdl": ["PDL00001"],
            "date_releve": [datetime(2024, 1, 1, tzinfo=PARIS)],
            "ordre_index": [False],
            "ref_situation_contractuelle": ["RSC1"],
            "formule_tarifaire_acheminement": ["BTINFCUST"],
            "id_calendrier_fournisseur": [None],
            "id_calendrier_distributeur": ["DI000001"],
            "id_affaire": [None],
            "index_base_kwh": [100.0],
            "index_hp_kwh": [None],
            "index_hc_kwh": [None],
            "index_hph_kwh": [None],
            "index_hpb_kwh": [None],
            "index_hch_kwh": [None],
            "index_hcb_kwh": [None],
            "type_releve": [None],
            "contexte_releve": [None],
            "etape_metier": [None],
            "grandeur_physique": [None],
            "grandeur_metier": [None],
        },
        schema_overrides={
            "date_releve": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "id_calendrier_fournisseur": pl.Utf8,
            "id_affaire": pl.Utf8,
            "type_releve": pl.Utf8,
            "contexte_releve": pl.Utf8,
            "etape_metier": pl.Utf8,
            "grandeur_physique": pl.Utf8,
            "grandeur_metier": pl.Utf8,
            "index_hp_kwh": pl.Float64,
            "index_hc_kwh": pl.Float64,
            "index_hph_kwh": pl.Float64,
            "index_hpb_kwh": pl.Float64,
            "index_hch_kwh": pl.Float64,
            "index_hcb_kwh": pl.Float64,
        },
    )
    # Ne doit PAS lever : le contrat n'exige plus unite/precision (DI000001 ⟹ base présent,
    # verifier_presence_mesures satisfait).
    RelevéIndex.validate(frame, lazy=True)


# =============================================================================
# Cycle 6 — pipeline_energie produit une sortie conforme à `PeriodeEnergie`
# =============================================================================


def test_pipeline_energie_sortie_conforme_periode_energie():
    """Happy path : `pipeline_energie` doit produire une sortie validable par
    `PeriodeEnergie`. On choisit ici un historique où aucun événement n'impacte
    l'énergie (le filtre amène à un output vide) — un cas dégénéré mais qui doit
    quand même produire une `LazyFrame` au schéma conforme."""
    from electricore.core.models.periode_energie import PeriodeEnergie

    historique = _historique_avec_index_lazyframe(impacte_energie=False)

    result = pipeline_energie(historique, _releves_minimaux_lazyframe()).collect()

    # Schéma de sortie respecté même sur output vide (les colonnes doivent exister)
    assert "pdl" in result.columns
    assert "debut" in result.columns
    assert "fin" in result.columns
    assert "data_complete" in result.columns

    # Validation deep (DataFrame) : checks nullability/range s'exécutent
    PeriodeEnergie.validate(result, lazy=True)
