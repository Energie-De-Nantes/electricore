"""Calculateur TURPE variable **sans état** : Odoo fournit l'assiette (ADR-0030, #247).

`POST /facturation/turpe-variable` est une **évaluation de fonction**, pas un feed :
l'appelant prête les énergies par cadran (+ FTA + `debut`), electricore applique la
formule réglementaire (`ajouter_turpe_variable`) et renvoie le **montant** €. Columnar-
natif (une passe Polars sur le lot), indépendant de l'ordre.

ERP-agnostique ([ADR-0016](docs/adr/0016-core-erp-agnostique.md)) : aucun import
`integrations.odoo` ; l'`id` opaque fourni par l'appelant n'est **jamais** interprété,
seulement ré-émis à côté du montant.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from electricore.core.models.cadrans import CADRANS, col_energie
from electricore.core.pipelines.turpe import ajouter_turpe_variable

PARIS = ZoneInfo("Europe/Paris")

# Version du contrat exposée dans l'enveloppe (cf. méta-périodes). Bump sur rupture ;
# un ajout de champ optionnel reste additif et ne change pas la version.
CONTRAT_VERSION = 1

# Colonnes d'énergie attendues dans la requête (les 7 cadrans, nullables → 0).
COLONNES_ENERGIE: tuple[str, ...] = tuple(col_energie(c) for c in CADRANS)


def calculer_turpe_variable(lignes: list[dict]) -> list[dict]:
    """Montant TURPE variable € pour chaque ligne du lot (cas nominal, #247).

    Args:
        lignes: lot de dicts `{id, formule_tarifaire_acheminement, debut,
            energie_*_kwh}`. Les 7 cadrans sont optionnels (absents/`None` → 0).
            `debut` est un `datetime` (tz-aware ou naïf, normalisé Europe/Paris).

    Returns:
        Liste de `{id, turpe_variable_eur}`, l'`id` ré-émis tel quel. Cas nominal :
        toutes les FTA sont connues et ont une règle à la date → aucun drop.
    """
    if not lignes:
        return []

    lf = _frame_assiette(lignes)
    resultat = ajouter_turpe_variable(lf).select("id", "turpe_variable_eur").collect()
    return resultat.to_dicts()


def _frame_assiette(lignes: list[dict]) -> pl.LazyFrame:
    """Construit le LazyFrame attendu par `ajouter_turpe_variable` à partir du lot.

    `debut` est porté en datetime tz `Europe/Paris` (sélection temporelle de la règle) ;
    les cadrans absents sont matérialisés en colonnes Float64 nulles.
    """
    data: dict[str, list] = {
        "id": [ligne["id"] for ligne in lignes],
        "formule_tarifaire_acheminement": [ligne["formule_tarifaire_acheminement"] for ligne in lignes],
    }
    for col in COLONNES_ENERGIE:
        data[col] = [ligne.get(col) for ligne in lignes]

    debut = pl.Series("debut", [_normaliser_paris(ligne["debut"]) for ligne in lignes])
    return pl.LazyFrame(data).with_columns(
        debut.dt.convert_time_zone("Europe/Paris"),
        *[pl.col(col).cast(pl.Float64) for col in COLONNES_ENERGIE],
    )


def _normaliser_paris(debut: datetime) -> datetime:
    """`debut` → instant tz-aware Europe/Paris (un naïf est *interprété* en heure de Paris)."""
    return debut.replace(tzinfo=PARIS) if debut.tzinfo is None else debut.astimezone(PARIS)
