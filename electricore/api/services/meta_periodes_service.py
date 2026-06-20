"""Wire-up de l'endpoint de lecture des méta-périodes (ADR-0027, #227).

Résout le contexte mensuel (loaders DuckDB par défaut via `contexte_du_mois`),
filtre la facturation mensuelle sur le mois cible (+ RSC optionnelles) et projette
les colonnes du **contrat v1** (champs déjà portés par `PeriodeMeta`).

Le réglementaire suit la règle ADR-0027 : *montant € quand electricore possède
l'assiette, taux quand l'ERP la possède.* → `cta_eur` (assiette = `turpe_fixe_eur`,
possédée par electricore) en € ; `taux_accise_eur_mwh` en taux (assiette accise = le
facturé, possédé par l'ERP — pas d'`accise_eur` ici). ERP-agnostique : aucun import
`integrations/odoo` (ADR-0016). L'intégrité (`source_hash`) arrive en #229.
"""

import hashlib
import json

import polars as pl

from electricore.core.builds.contexte_mensuel import contexte_du_mois
from electricore.core.models.cadrans import CADRANS, col_index
from electricore.core.pipelines.accise import load_accise_rules
from electricore.core.pipelines.cta import ajouter_cta
from electricore.core.pipelines.taux import ajouter_taux_en_vigueur

# Version du contrat exposée dans l'enveloppe (#229). Bump sur rupture (cf. ADR-0027,
# évolution additive — un ajout de colonne optionnelle ne change pas la version).
# v2 (#317/#327, ADR-0033) : retrait cassant de `data_complete` / `coverage_abo` /
# `coverage_energie`, remplacés par les verdicts jumeaux `qualite` + `statut_communication`.
# v3 (#360, ADR-0038) : ajout (additif) du tableau imbriqué `releves_utilises` par
# méta-période + extension de `source_hash` à ce tableau. Bump car le contrat gagne un bloc.
CONTRAT_VERSION = 3

# Colonnes du contrat v2 — sous-ensemble de `PeriodeMeta` + bloc réglementaire (#228).
COLONNES_CONTRAT: tuple[str, ...] = (
    "ref_situation_contractuelle",
    "pdl",
    "mois_annee",
    "debut",
    "fin",
    "nb_jours",
    "puissance_moyenne_kva",
    "formule_tarifaire_acheminement",
    "energie_base_kwh",
    "energie_hp_kwh",
    "energie_hc_kwh",
    "turpe_fixe_eur",
    "turpe_variable_eur",
    "cta_eur",
    "taux_accise_eur_mwh",
    "has_changement",
    # Verdicts méta jumeaux — qualité (ADR-0033) + communication (ADR-0036). Remplacent
    # `data_complete` / `coverage_*` (retirés en v2, ADR-0033) : `qualite` distingue
    # réel/estimé/incalculable, `statut_communication` route l'énergie.
    "qualite",
    "statut_communication",
)

# Registres d'index RÉELS exposés dans la trace (ADR-0038), dérivés de la SOURCE UNIQUE
# des cadrans (`cadrans.py`, ADR-0035 §1) : les 7 registres canoniques (base, hp, hc + les
# 4 saisonniers hph/hch/hpb/hcb du C4/Tempo). Seuls les NON-NULS d'un relevé ressortent —
# et comme le mart ne synthétise jamais (`pivot_cadrans` ne pose un index que si le cadran
# est présent dans le flux), un registre exposé est toujours un cadran que le compteur
# affiche réellement (jamais de cadran agrégé synthétisé).
REGISTRES_REELS: tuple[str, ...] = tuple(col_index(cadran) for cadran in CADRANS)

# Colonnes lues dans `releves_utilises` (forme *Chronologie des relevés*, ADR-0029).
_COLONNES_RELEVE: tuple[str, ...] = (
    "ref_situation_contractuelle",
    "date_releve",
    "ordre_index",
    "releve_id",
    "nature_index",
    *REGISTRES_REELS,
)

# Qualités pour lesquelles le mois porte des relevés bornants — l'invariant plein-ou-rien
# d'ADR-0038 : `releves_utilises` non vide ⟺ `qualite ∈ {réelle, estimée}` ; tout le reste
# (incalculable, null, inconnu) ⟹ `[]`.
_QUALITES_AVEC_RELEVES: frozenset[str] = frozenset({"réelle", "estimée"})


def _objet_releve(ligne: dict) -> dict:
    """Projette une ligne de relevé sur l'objet de contrat (ADR-0038).

    `{ releve_id, date_releve (ISO), nature_index, <registres réels non nuls> }`.
    Un registre nul n'est pas exposé (registres réels uniquement — jamais de cadran
    synthétisé). `date_releve` est rendue en ISO8601 pour un payload + un `source_hash`
    déterministes.
    """
    date_releve = ligne["date_releve"]
    objet = {
        "releve_id": ligne["releve_id"],
        "date_releve": date_releve.isoformat() if hasattr(date_releve, "isoformat") else date_releve,
        "nature_index": ligne["nature_index"],
    }
    for registre in REGISTRES_REELS:
        if ligne.get(registre) is not None:
            objet[registre] = ligne[registre]
    return objet


def _slicer_releves_utilises(releves: pl.LazyFrame, periodes: pl.DataFrame) -> list[list[dict]]:
    """Découpe `releves_utilises` par méta-période et applique l'invariant ADR-0038.

    Pour chaque ligne de `periodes` (ordre préservé) : `[]` si `qualite` n'est pas dans
    `{réelle, estimée}` (plein-ou-rien), sinon les relevés de la même RSC dont la
    `date_releve` borne le mois (`debut <= date_releve <= fin`, MCT compris — non borné à
    2 entrées), projetés en objets de contrat. Un relevé sans identité (`releve_id` null,
    relevé interrogé absent) n'est jamais exposé.
    """
    nombre = periodes.height
    disponibles = set(releves.collect_schema().names())
    if not set(_COLONNES_RELEVE).issubset(disponibles):
        # Frame vide / sans les colonnes attendues (tests réglementaires) → aucune trace.
        return [[] for _ in range(nombre)]

    rel = (
        releves.filter(pl.col("releve_id").is_not_null())
        .select(_COLONNES_RELEVE)
        .sort(["ref_situation_contractuelle", "date_releve", "ordre_index"])
        .collect()
    )
    par_rsc: dict[str, list[dict]] = {}
    for ligne in rel.iter_rows(named=True):
        par_rsc.setdefault(ligne["ref_situation_contractuelle"], []).append(ligne)

    traces: list[list[dict]] = []
    for periode in periodes.iter_rows(named=True):
        if periode["qualite"] not in _QUALITES_AVEC_RELEVES:
            traces.append([])
            continue
        debut, fin = periode["debut"], periode["fin"]
        traces.append(
            [
                _objet_releve(ligne)
                for ligne in par_rsc.get(periode["ref_situation_contractuelle"], [])
                if debut <= ligne["date_releve"] <= fin
            ]
        )
    return traces


def meta_periodes(mois: str | None = None, rsc: list[str] | None = None) -> tuple[str, pl.DataFrame]:
    """Méta-périodes du mois cible, projetées sur le contrat v1.

    Args:
        mois: format `YYYY-MM-DD` (premier jour du mois). `None` → dernier mois
            disponible.
        rsc: filtre optionnel sur une liste de `ref_situation_contractuelle`.

    Returns:
        `(mois_résolu, df)` — le mois effectif (`YYYY-MM-DD`) porté par le contexte,
        et le DataFrame des méta-périodes du mois, colonnes du contrat v1.
    """
    contexte = contexte_du_mois(mois)

    mois_cible = pl.lit(contexte.mois).str.to_date()
    debut_mois = pl.col("debut").dt.truncate("1mo").dt.date()

    lf = contexte.facturation_mensuelle.lazy().filter(debut_mois == mois_cible)
    if rsc:
        lf = lf.filter(pl.col("ref_situation_contractuelle").is_in(rsc))

    # Bloc réglementaire (#228) : CTA en montant € (assiette electricore), accise en
    # taux seul (assiette ERP). Les deux s'appuient sur `debut` pour le taux en vigueur.
    lf = ajouter_cta(lf)
    lf = ajouter_taux_en_vigueur(lf, load_accise_rules(), date_col="debut", taux_col="taux_accise_eur_mwh")

    df = lf.select(COLONNES_CONTRAT).collect()

    # Bloc imbriqué `releves_utilises` (trace d'index légale, ADR-0038, #360). Colonne
    # `pl.Object` : tableau de longueur variable par ligne (irrégulier, registres réels
    # uniquement) — `to_dicts()` le rend tel quel à l'enveloppe JSON.
    traces = _slicer_releves_utilises(contexte.releves_utilises, df)
    df = df.with_columns(pl.Series("releves_utilises", traces, dtype=pl.Object))

    return contexte.mois, _ajouter_source_hash(df)


def _ajouter_source_hash(df: pl.DataFrame) -> pl.DataFrame:
    """Ajoute `source_hash` : sha256 (tronqué) de la ligne canonique du contrat (#229).

    Hash de contenu sur **toutes** les colonnes du contrat **et le tableau imbriqué
    `releves_utilises`** (ADR-0038, #360) — déterministe (même état DuckDB → même hash) et
    robuste aux versions de Polars (repr texte stable, pas le hash interne). Le repli du
    tableau (canonicalisation JSON à clés triées : `releve_id`, `date_releve`,
    `nature_index`, registres) rend le hash sensible à toute dérive d'un index **imprimé**,
    d'une nature ou d'une identité — **même à delta kWh du mois constant** (reset compteur,
    correction ±k aux deux bornes). Outille l'upsert non destructif côté Odoo : skip-si-
    inchangé + détection de dérive sous verrou (ADR-0027/0038).
    """
    canon = df.select(
        pl.concat_str(
            [pl.col(c).cast(pl.Utf8).fill_null("∅") for c in COLONNES_CONTRAT],
            separator="␟",
        ).alias("_canon")
    ).to_series()
    traces = df["releves_utilises"].to_list() if "releves_utilises" in df.columns else [None] * df.height
    hashes = [
        hashlib.sha256(
            f"{ligne}␟{json.dumps(trace, sort_keys=True, ensure_ascii=False, default=str)}".encode()
        ).hexdigest()[:16]
        for ligne, trace in zip(canon.to_list(), traces, strict=True)
    ]
    return df.with_columns(pl.Series("source_hash", hashes, dtype=pl.Utf8))
