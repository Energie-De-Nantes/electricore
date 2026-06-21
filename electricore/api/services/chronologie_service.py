"""Vue facturiste : *Chronologie du point / du contrat* + verdicts (#367, ADR-0039/0041).

Endpoint de **lecture** (pattern « Odoo tire » [ADR-0027], read-only [ADR-0012]) qui rend
la **frise complète** d'un point (`pdl`) ou d'un contrat (`rsc`) : les **faits** (événements
C15 *y compris hors-comptage* — MDPRM de niveau, MCT puissance — et relevés R151/R64/C15,
avec leur origine/nature) **tissés** avec les **verdicts dérivés** de chaque période d'énergie
(qualité / communication / énergie). But : rendre lisible le **pourquoi** d'un mois (« borne
manquante → incalculable », « niveau 0 → non-communicante »), là où `/meta-periodes` est
l'extrait mensuel valorisé.

**Pas de montants tarifaires** (turpe/cta/accise) : c'est le différenciateur explicite vs
`/meta-periodes`. La frise porte l'énergie **physique** (kWh), pas son prix.

S'appuie sur la reconstruction **filtrée** du `ContexteMensuel` (#366) : le périmètre est
poussé dans DuckDB (`spine().filter(...)` / `chronologie().filter(...)`), pas de scan parc.

Grain :
- **point** (`pdl`) — toute l'histoire du PDL : RSC successives + charnières
  (déménagement/reprise) ;
- **contrat** (`rsc`) — une seule tenure bornée entrée→sortie.

ERP-agnostique : aucun import `integrations/odoo` (ADR-0016).
"""

import datetime as dt

import polars as pl

from electricore.core.builds.contexte_mensuel import ContexteMensuel, contexte_du_mois_filtre
from electricore.core.models.cadrans import CADRANS, col_index

# Version du contrat exposée dans l'enveloppe. Bump sur rupture (cf. /meta-periodes).
CONTRAT_VERSION = 1

# Registres réels d'index exposés sur une ligne de relevé (cf. /meta-periodes, ADR-0038) :
# seuls les non-nuls ressortent (jamais de cadran synthétisé).
REGISTRES_REELS: tuple[str, ...] = tuple(col_index(c) for c in CADRANS)

# Énergies physiques (kWh) portées par une ligne de période — PAS de montant tarifaire.
COLONNES_ENERGIE: tuple[str, ...] = tuple(c.replace("index_", "energie_") for c in REGISTRES_REELS)


def _lignes_evenements(historique: pl.LazyFrame) -> list[dict]:
    """Faits événementiels : événements C15 (y compris hors-comptage) + bornes FACTURATION.

    Chaque ligne porte la situation **au moment du fait** (puissance, FTA, niveau d'ouverture)
    et les annotations de rupture d'abonnement (`impacte_abonnement`, `resume_modification`).
    """
    colonnes = [
        "date_evenement",
        "pdl",
        "ref_situation_contractuelle",
        "source",
        "type_fait",
        "evenement_declencheur",
        "puissance_souscrite_kva",
        "formule_tarifaire_acheminement",
        "niveau_ouverture_services",
        "impacte_abonnement",
        "resume_modification",
    ]
    dispo = set(historique.collect_schema().names())
    df = historique.select([c for c in colonnes if c in dispo]).collect()
    lignes = []
    for r in df.iter_rows(named=True):
        lignes.append(
            {
                "type_ligne": "evenement",
                "date": r["date_evenement"],
                "pdl": r.get("pdl"),
                "ref_situation_contractuelle": r.get("ref_situation_contractuelle"),
                "source": r.get("source"),
                "type_fait": r.get("type_fait"),
                "evenement_declencheur": r.get("evenement_declencheur"),
                "puissance_souscrite_kva": r.get("puissance_souscrite_kva"),
                "formule_tarifaire_acheminement": r.get("formule_tarifaire_acheminement"),
                "niveau_ouverture_services": r.get("niveau_ouverture_services"),
                "impacte_abonnement": r.get("impacte_abonnement"),
                "resume_modification": r.get("resume_modification"),
            }
        )
    return lignes


def _lignes_releves(releves: pl.LazyFrame) -> list[dict]:
    """Faits de relevé : un relevé d'index utilisé, avec origine (périodique/événementiel),
    nature et registres réels non nuls. Un relevé sans identité (interrogé absent) est écarté.
    """
    dispo = set(releves.collect_schema().names())
    if "date_releve" not in dispo:
        return []
    colonnes = [
        c
        for c in (
            "ref_situation_contractuelle",
            "pdl",
            "date_releve",
            "ordre_index",
            "releve_id",
            "nature_index",
            "source",
            "evenement_declencheur",
            *REGISTRES_REELS,
        )
        if c in dispo
    ]
    df = releves.select(colonnes).collect()
    if "releve_id" in df.columns:
        df = df.filter(pl.col("releve_id").is_not_null())
    lignes = []
    for r in df.iter_rows(named=True):
        evenementiel = r.get("source") == "flux_C15"
        ligne = {
            "type_ligne": "releve",
            "date": r["date_releve"],
            "pdl": r.get("pdl"),
            "ref_situation_contractuelle": r.get("ref_situation_contractuelle"),
            "source": r.get("source"),
            "releve_id": r.get("releve_id"),
            "nature_index": r.get("nature_index"),
            "origine_releve": "événementiel" if evenementiel else "périodique",
            "ordre_index": r.get("ordre_index"),
        }
        if evenementiel and r.get("evenement_declencheur") is not None:
            ligne["evenement_declencheur"] = r["evenement_declencheur"]
        for registre in REGISTRES_REELS:
            if r.get(registre) is not None:
                ligne[registre] = r[registre]
        lignes.append(ligne)
    return lignes


def _lignes_periodes_energie(energie: pl.LazyFrame) -> list[dict]:
    """Périodes d'énergie dérivées : bornes + **verdicts** (qualité/communication) + énergie
    physique (kWh). **Aucun** montant tarifaire (`turpe_variable_eur` écarté ici, ADR-0027).
    """
    dispo = set(energie.collect_schema().names())
    if "debut" not in dispo:
        return []
    base = [
        "ref_situation_contractuelle",
        "pdl",
        "debut",
        "fin",
        "nb_jours",
        "qualite",
        "statut_communication",
    ]
    colonnes = [c for c in (*base, *COLONNES_ENERGIE) if c in dispo]
    df = energie.select(colonnes).collect()
    lignes = []
    for r in df.iter_rows(named=True):
        ligne = {
            "type_ligne": "periode_energie",
            # La période est ancrée sur sa borne de début (point d'apparition dans la frise).
            "date": r["debut"],
            "pdl": r.get("pdl"),
            "ref_situation_contractuelle": r.get("ref_situation_contractuelle"),
            "debut": r.get("debut"),
            "fin": r.get("fin"),
            "nb_jours": r.get("nb_jours"),
            "qualite": r.get("qualite"),
            "statut_communication": r.get("statut_communication"),
        }
        for col in COLONNES_ENERGIE:
            if col in dispo and r.get(col) is not None:
                ligne[col] = r[col]
        lignes.append(ligne)
    return lignes


def _tisser_frise(ctx: ContexteMensuel) -> pl.DataFrame:
    """Tisse la frise complète depuis un `ContexteMensuel` (cœur pur du service).

    Concatène les trois familles de lignes — **événements** (faits C15 + FACTURATION),
    **relevés** (faits de lecture), **périodes d'énergie** (dérivées, avec verdicts) — et
    les ordonne chronologiquement (`date`). Schéma hétérogène : chaque famille a ses clés ;
    une frame `pl.DataFrame` à colonnes unionnées (valeurs absentes → null) est plus lisible
    pour un facturier qu'une liste typée. Aucun montant tarifaire (différenciateur ADR-0027).
    """
    lignes = [
        *_lignes_evenements(ctx.historique_enrichi),
        *_lignes_releves(ctx.releves_utilises),
        *_lignes_periodes_energie(ctx.energie),
    ]
    if not lignes:
        return pl.DataFrame({"type_ligne": [], "date": []})
    # Union des clés (schéma hétérogène) → DataFrame, puis tri chronologique stable.
    frise = pl.DataFrame(lignes, infer_schema_length=None)
    return frise.sort("date", maintain_order=True)


def chronologie_point_ou_contrat(
    pdl: str | None = None,
    rsc: str | None = None,
    horizon: dt.datetime | None = None,
) -> pl.DataFrame:
    """Frise complète d'un point (`pdl`) ou d'un contrat (`rsc`) — vue facturiste (#367).

    Reconstruit le `ContexteMensuel` **filtré** (#366, prédicat poussé dans DuckDB) puis tisse
    la frise. La résolution du `mois` du contexte ne borne que `facturation_mensuelle` (non
    utilisée ici) : la frise couvre **toute l'histoire** jusqu'à l'horizon.

    Args:
        pdl: grain point — toute l'histoire du PDL (RSC successives + charnières).
        rsc: grain contrat — une tenure bornée entrée→sortie.
        horizon: borne de facturation (issue #179) ; `None` → 1er du mois courant.

    Returns:
        DataFrame de la frise (lignes hétérogènes, ordonnées par `date`).

    Raises:
        ValueError: si ni `pdl` ni `rsc` n'est fourni (un grain est requis).
        FileNotFoundError: si la base DuckDB est absente (levée par les loaders).
    """
    if pdl is None and rsc is None:
        raise ValueError("Un grain est requis : fournir `pdl` (point) ou `rsc` (contrat).")
    ctx = contexte_du_mois_filtre(pdl=pdl, rsc=rsc, horizon=horizon)
    return _tisser_frise(ctx)
