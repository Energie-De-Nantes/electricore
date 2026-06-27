"""Schémas Pandera des deux bords de l'estimation de provision des lissés (ADR-0048).

Deux frontières typées (décision 6 d'ADR-0048) :

- `EnergieParPeriodeR67` — **frontière d'entrée** « énergie-par-période » : premier
  consommateur typé de `flux_r67` (lève le `validator=None` différé par ADR-0047 §9 *pour
  ce chemin*). Grain `(pdl, debut, fin)`, énergie déjà différenciée par cadran (PAS un index
  cumulé), `code_nature` R/E/C. `energie_base_kwh` porte toujours le total ; les cadrans fins
  sont nullables (peuplés seulement si la période les porte). Négatifs préservés (régul `C`,
  ADR-0047) — aucun `ge=0`.

- `EstimationProvision` — **frontière de sortie** : une ligne par PDL, sortie WIDE
  `energie_<cadran>_kwh` annuel + mensuel (`/12` plate), plus les métadonnées d'estimation
  (couverture, profondeur cadran déclarée, qualité, signal alertable). N'émet que des **kWh**
  — aucune valorisation € (le prix fournisseur appartient à l'ERP, ADR-0016/0027).

Voir `core/CONTEXT.md` (entrées *Estimation de provision*, *Provision d'énergie*,
*Cadran facturé*, *Formule tarifaire fournisseur*, *Qualité de période d'énergie*).
"""

import pandera.polars as pa
import polars as pl

from electricore.core.models.cadrans import col_energie

# =============================================================================
# FRONTIÈRE D'ENTRÉE — R67 énergie-par-période (ADR-0047 §9, ADR-0048 décision 6)
# =============================================================================


class EnergieParPeriodeR67(pa.DataFrameModel):
    """Frontière « énergie-par-période » du flux R67 (cold-start provision, ADR-0048).

    Grain : une ligne par `(pdl, debut, fin)` — énergie de consommation déjà différenciée
    par le distributeur sur l'intervalle demi-ouvert `[debut, fin)` (ADR-0047). Ce n'est
    **pas** un index cumulé : on ne route jamais R67 par `calculer_periodes_energie` (le
    piège « énergie de l'énergie »).

    `energie_base_kwh` est le **total** de la période (toujours présent) ; les cadrans fins
    (`hp`/`hc`, 4-cadrans) sont nullables — présents seulement si la période les porte.
    Négatifs préservés (régul `code_nature='C'`) : aucun `ge=0`.
    """

    pdl: pl.Utf8 = pa.Field(nullable=False)
    debut: pl.Date = pa.Field(nullable=False)
    fin: pl.Date = pa.Field(nullable=False)
    # R réel / E estimé / C corrigé-régul (ADR-0047) — alimente le flag qualité (#489).
    code_nature: pl.Utf8 = pa.Field(nullable=False, isin=["R", "E", "C"])
    # Total de la période : toujours présent.
    energie_base_kwh: pl.Int64 = pa.Field(nullable=False)
    # Cadrans fins : nullables (peuplés ssi la période les porte, profondeur cohérente).
    energie_hp_kwh: pl.Int64 = pa.Field(nullable=True)
    energie_hc_kwh: pl.Int64 = pa.Field(nullable=True)
    energie_hph_kwh: pl.Int64 = pa.Field(nullable=True)
    energie_hpb_kwh: pl.Int64 = pa.Field(nullable=True)
    energie_hch_kwh: pl.Int64 = pa.Field(nullable=True)
    energie_hcb_kwh: pl.Int64 = pa.Field(nullable=True)

    class Config:
        # `strict = False` : R67 porte d'autres colonnes (code_grille, periode_*, motif…)
        # gardées en annotation ; seules les colonnes ci-dessus sont contractuelles.
        strict = False
        coerce = True


# =============================================================================
# FRONTIÈRE DE SORTIE — RapportProvision (ADR-0048 décisions 1, 5, 7)
# =============================================================================

# Profondeurs cadran déclarables (décision 5 d'ADR-0048) : la lib émet la profondeur
# COHÉRENTE MAXIMALE — `base` toujours ; `hp_hc` si toute la fenêtre porte hp/hc ;
# `4_cadrans` si toute la fenêtre porte les 4. Le « cadran facturé » (collapse vers la
# formule tarifaire fournisseur) reste hors cœur — Odoo collapse.
PROFONDEURS: tuple[str, ...] = ("base", "hp_hc", "4_cadrans")

# Qualités dérivées du mix de natures R/E/C (décision 4, #489).
QUALITES: tuple[str, ...] = ("réelle", "estimée")


def _champ_kwh_annuel(cadran: str, nullable: bool) -> pa.Field:
    return pa.Field(nullable=nullable, alias=col_energie(cadran))


def _champ_kwh_mensuel(cadran: str, nullable: bool) -> pa.Field:
    return pa.Field(nullable=nullable, alias=_col_mensuel(cadran))


def _col_mensuel(cadran: str) -> str:
    """Nom de la colonne d'énergie mensuelle (`/12` plate) d'un cadran."""
    return col_energie(cadran).replace("_kwh", "_mensuel_kwh")


class EstimationProvision(pa.DataFrameModel):
    """Estimation de provision d'un PDL : sortie WIDE en kWh + métadonnées (ADR-0048).

    Une ligne par PDL. `energie_<cadran>_kwh` = estimation **annuelle** (somme nette sur la
    tranche de 12 mois glissants, toutes natures R+E+C, négatifs préservés) ;
    `energie_<cadran>_mensuel_kwh` = la **provision mensuelle** `/12` plate. `base` porte
    toujours le total ; les cadrans fins sont nuls hors de la profondeur déclarée.

    **Zéro €** : la valorisation appartient à l'ERP (ADR-0016/0027).
    """

    pdl: pl.Utf8 = pa.Field(nullable=False)

    # --- Estimation annuelle (kWh) — WIDE, base = total ---
    energie_base_kwh: pl.Float64 = _champ_kwh_annuel("base", nullable=False)
    energie_hp_kwh: pl.Float64 = _champ_kwh_annuel("hp", nullable=True)
    energie_hc_kwh: pl.Float64 = _champ_kwh_annuel("hc", nullable=True)
    energie_hph_kwh: pl.Float64 = _champ_kwh_annuel("hph", nullable=True)
    energie_hpb_kwh: pl.Float64 = _champ_kwh_annuel("hpb", nullable=True)
    energie_hch_kwh: pl.Float64 = _champ_kwh_annuel("hch", nullable=True)
    energie_hcb_kwh: pl.Float64 = _champ_kwh_annuel("hcb", nullable=True)

    # --- Provision mensuelle (kWh) — `/12` plate ---
    energie_base_mensuel_kwh: pl.Float64 = _champ_kwh_mensuel("base", nullable=False)
    energie_hp_mensuel_kwh: pl.Float64 = _champ_kwh_mensuel("hp", nullable=True)
    energie_hc_mensuel_kwh: pl.Float64 = _champ_kwh_mensuel("hc", nullable=True)
    energie_hph_mensuel_kwh: pl.Float64 = _champ_kwh_mensuel("hph", nullable=True)
    energie_hpb_mensuel_kwh: pl.Float64 = _champ_kwh_mensuel("hpb", nullable=True)
    energie_hch_mensuel_kwh: pl.Float64 = _champ_kwh_mensuel("hch", nullable=True)
    energie_hcb_mensuel_kwh: pl.Float64 = _champ_kwh_mensuel("hcb", nullable=True)

    # --- Métadonnées de couverture (décision 3, #487/#489) ---
    couverture_debut: pl.Date = pa.Field(nullable=True)  # borne de la fenêtre retenue
    couverture_fin: pl.Date = pa.Field(nullable=True)  # exclusive [debut, fin)
    couverture_mois: pl.Float64 = pa.Field(nullable=False, ge=0.0)  # nb de mois couverts
    couverture_suffisante: pl.Boolean = pa.Field(nullable=False)  # >= 12 mois

    # --- Profondeur cadran déclarée (décision 5, #488) ---
    profondeur_cadran: pl.Utf8 = pa.Field(nullable=False, isin=list(PROFONDEURS))

    # --- Qualité dérivée du mix R/E/C (décision 4, #489) ---
    qualite: pl.Utf8 = pa.Field(nullable=False, isin=list(QUALITES))
    presence_regularisation: pl.Boolean = pa.Field(nullable=False)

    # --- Signal alertable (décision 3, #489) — la lib expose, l'aval alerte (ADR-0037) ---
    signal_alertable: pl.Boolean = pa.Field(nullable=False)

    class Config:
        strict = True
        coerce = True
        unique = ["pdl"]
