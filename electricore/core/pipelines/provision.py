"""Estimation de provision des lissés : profil cadran + effondrement 12 mois (ADR-0048).

Capacité **cœur-pure** qui transforme un historique d'énergie *par cadran sur fenêtre
datée* (le *profil cadran*) en une **estimation de consommation annuelle par cadran**, puis
une *provision d'énergie* mensuelle **`/12` plate** (cf. `core/CONTEXT.md`, *Estimation de
provision*). N'émet que des **kWh** — la valorisation appartient à l'ERP (ADR-0016/0027).

Deux étages, conçus pour que le futur adaptateur *établi* (historique R151/R64 propre) se
branche sans refonte (décision 2 d'ADR-0048) :

1. **Profil cadran** `{pdl, cadran → kWh sur fenêtre datée}` — forme partagée. Seul
   l'adaptateur **cold-start R67** est livré ici (`profil_cadran_depuis_r67`). R67
   **contourne** `calculer_periodes_energie` : son énergie est déjà différenciée par
   période (ADR-0047), pas un index cumulé.
2. **Effondrement annuel** commun (`effondrer_profil`) : tranche de 12 mois glissants, somme
   nette par cadran (toutes natures R+E+C, négatifs préservés), `/12` plate, + métadonnées de
   couverture, profondeur cadran cohérente maximale, et qualité dérivée du mix R/E/C.

La sortie est validée par `EstimationProvision` (frontière de sortie typée, décision 6).
"""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import LazyFrame

from electricore.core.models.cadrans import CADRANS, SOUS_CADRANS, col_energie
from electricore.core.models.provision import EnergieParPeriodeR67, EstimationProvision

_PARIS = ZoneInfo("Europe/Paris")

# Fenêtre d'effondrement : 12 mois glissants (décision 3 d'ADR-0048).
FENETRE_MOIS = 12

# Colonnes d'énergie WIDE, dans l'ordre canonique des cadrans.
_COLS_ENERGIE: tuple[str, ...] = tuple(col_energie(c) for c in CADRANS)
# Cadrans fins (hors base) : peuplés seulement si toute la fenêtre les porte.
_CADRANS_FINS: tuple[str, ...] = tuple(c for c in CADRANS if c != "base")


def _col_mensuel(cadran: str) -> str:
    """Nom de la colonne d'énergie mensuelle (`/12` plate) d'un cadran."""
    return col_energie(cadran).replace("_kwh", "_mensuel_kwh")


def _moins_n_mois(jour: dt.date, n: int) -> dt.date:
    """Recule `jour` de `n` mois calendaires (pas 30×n jours).

    Tranche de 12 mois *glissants* ancrée sur le quantième : un `as_of` au 1ᵉʳ d'un mois
    recule au 1ᵉʳ du même mois un an plus tôt (cf. décision 3 d'ADR-0048). Clampé au dernier
    jour du mois cible quand le quantième n'existe pas (ex. 31 → 30/28).
    """
    mois_total = (jour.year * 12 + (jour.month - 1)) - n
    annee, mois = divmod(mois_total, 12)
    mois += 1
    # Dernier jour du mois cible (gère 31 → 30/28).
    if mois == 12:
        jour_max = 31
    else:
        jour_max = (dt.date(annee, mois + 1, 1) - dt.timedelta(days=1)).day
    return dt.date(annee, mois, min(jour.day, jour_max))


def horloge_par_defaut() -> dt.date:
    """Résout l'« as-of » par défaut (aujourd'hui, heure de Paris).

    Unique point de lecture d'horloge de l'estimation (décision 7 d'ADR-0048) : lu **une
    seule fois** au boundary du build, jamais dans le pipeline (pureté, rejeu déterministe —
    cf. `horizon_par_defaut` du chemin facturation).
    """
    return dt.datetime.now(_PARIS).date()


# =============================================================================
# ÉTAGE 1 — Profil cadran (adaptateur cold-start R67)
# =============================================================================


@pa.check_types(lazy=True)
def profil_cadran_depuis_r67(r67: pl.LazyFrame) -> LazyFrame[EnergieParPeriodeR67]:
    """Normalise `flux_r67` en *profil cadran* `{pdl, cadran → kWh sur fenêtre datée}`.

    Adaptateur **cold-start** (décision 2 d'ADR-0048) : R67 porte déjà l'énergie différenciée
    par cadran sur `[debut, fin)` — on projette les colonnes contractuelles de la frontière
    `EnergieParPeriodeR67` (grain `(pdl, debut, fin)`, `code_nature`, WIDE base+fins) **sans**
    passer par `calculer_periodes_energie` (le piège « énergie de l'énergie », ADR-0047).

    La sortie est la forme partagée que l'adaptateur *établi* (R151/R64) émettra à son tour ;
    `effondrer_profil` la consomme sans connaître sa provenance.
    """
    colonnes = ["pdl", "debut", "fin", "code_nature", *_COLS_ENERGIE]
    return r67.select(colonnes)  # type: ignore[return-value]


# =============================================================================
# ÉTAGE 2 — Effondrement annuel commun (12 mois glissants → /12 plate)
# =============================================================================


_CADRANS_4 = ("hph", "hpb", "hch", "hcb")
_CADRANS_HP_HC = ("hp", "hc")


def _total_periode_expr() -> pl.Expr:
    """Total d'énergie d'**une période**, par repli sur la représentation la plus profonde.

    `energie_base_kwh` doit **toujours** valoir le total (décision 5 d'ADR-0048), même quand
    le mart R67 laisse `base` à null parce que la grille a éclaté l'énergie en cadrans
    (grille D 4-cadrans). On reconstruit donc le total par coalesce : base nominale ≻ hp+hc
    ≻ Σ4. L'invariant `base = hp+hc = Σ4` est ainsi respecté période à période.
    """
    somme_hp_hc = pl.col(col_energie("hp")) + pl.col(col_energie("hc"))
    somme_4 = pl.sum_horizontal([pl.col(col_energie(c)) for c in _CADRANS_4])
    return pl.coalesce(
        pl.col(col_energie("base")),
        # hp+hc seulement si les DEUX sont présents (sinon coalesce passe au suivant).
        pl.when(pl.col(col_energie("hp")).is_not_null() & pl.col(col_energie("hc")).is_not_null())
        .then(somme_hp_hc)
        .otherwise(None),
        # Σ4 seulement si les QUATRE sont présents.
        pl.when(pl.all_horizontal([pl.col(col_energie(c)).is_not_null() for c in _CADRANS_4]))
        .then(somme_4)
        .otherwise(None),
    )


def _profondeur_coherente_expr() -> pl.Expr:
    """Profondeur cadran cohérente maximale (décision 5 d'ADR-0048).

    `4_cadrans` si **toute** la fenêtre porte les 4 (aucun null sur hph/hpb/hch/hcb) ;
    sinon `hp_hc` si toute la fenêtre porte hp+hc ; sinon `base`. La fenêtre est toujours
    non vide ici (groupes issus du filtre), donc `.all()` n'est jamais vacuously-true.
    """
    porte_4 = pl.all_horizontal([pl.col(col_energie(c)).is_not_null().all() for c in _CADRANS_4])
    porte_hp_hc = pl.all_horizontal([pl.col(col_energie(c)).is_not_null().all() for c in _CADRANS_HP_HC])
    return pl.when(porte_4).then(pl.lit("4_cadrans")).when(porte_hp_hc).then(pl.lit("hp_hc")).otherwise(pl.lit("base"))


def _qualite_expr() -> pl.Expr:
    """Qualité dérivée du mix de natures R/E/C (décision 4 d'ADR-0048, #489).

    « réelle » si la **majorité** des périodes de la fenêtre sont réelles (`R`), « estimée »
    sinon (E estimé, C régul, ou non-communicant majoritairement estimé). Couvre le cas
    non-Linky sans traitement spécial : son mix surtout-E le fait remonter « estimée ».
    """
    nb_total = pl.col("code_nature").len()
    nb_reel = (pl.col("code_nature") == "R").sum()
    return pl.when(nb_reel * 2 > nb_total).then(pl.lit("réelle")).otherwise(pl.lit("estimée"))


def effondrer_profil(profil: pl.LazyFrame, as_of: dt.date) -> pl.LazyFrame:
    """Effondre un *profil cadran* en estimation annuelle + provision mensuelle `/12` plate.

    Étage **commun** (décision 2 d'ADR-0048), agnostique de la provenance du profil :

    - **Tranche de 12 mois glissants** : ne garde que les périodes dont `debut` tombe dans
      `[as_of − 12 mois, as_of)` (décision 3).
    - **Somme nette par cadran**, toutes natures R+E+C confondues, négatifs préservés
      (décision 4) ; `base` = total, cadrans fins peuplés **ssi toute la fenêtre** les porte
      (profondeur cohérente maximale, décision 5).
    - **`/12` plate** : provision mensuelle = annuel / 12.
    - **Métadonnées** : fenêtre `[couverture_debut, couverture_fin)`, nb de mois couverts,
      flag de couverture suffisante (≥ 12 mois), profondeur déclarée, qualité, présence de
      régul, et **signal alertable** (couverture insuffisante OU qualité estimée — la lib
      expose, l'aval alerte, ADR-0037).

    Args:
        profil: profil cadran (`EnergieParPeriodeR67` ou forme *établi* équivalente).
        as_of: date de référence (« as-of ») de la fenêtre glissante, lue une seule fois au
            boundary du build.

    Returns:
        LazyFrame WIDE, une ligne par PDL, prêt pour la validation `EstimationProvision`.
    """
    debut_fenetre = _moins_n_mois(as_of, FENETRE_MOIS)
    jours_fenetre = (as_of - debut_fenetre).days

    dans_fenetre = profil.filter(
        (pl.col("debut") >= pl.lit(debut_fenetre)) & (pl.col("debut") < pl.lit(as_of))
    ).with_columns(
        _total_periode_expr().alias("_total_periode"),
        # Durée de recouvrement de la fenêtre : `fin` clampée à `as_of` — une période qui
        # déborde au-delà de l'as-of ne compte que jusqu'à l'as-of.
        (pl.min_horizontal("fin", pl.lit(as_of)) - pl.col("debut")).dt.total_days().alias("_duree_jours"),
    )

    # `base` = somme des TOTAUX par période (reconstruits par repli, décision 5) → toujours
    # le total, même quand le mart laisse base null sur une grille 4-cadrans. Cadrans fins :
    # sommés (négatifs préservés) seulement si TOUTE la fenêtre les porte ; sinon null (base
    # porte le total). L'invariant `base = hp+hc = Σ4` tient sur une fenêtre cohérente.
    sommes = [pl.col("_total_periode").sum().cast(pl.Float64).alias(col_energie("base"))]
    sommes += [
        pl.when(pl.col(col_energie(c)).is_not_null().all())
        .then(pl.col(col_energie(c)).sum().cast(pl.Float64))
        .otherwise(None)
        .alias(col_energie(c))
        for c in _CADRANS_FINS
    ]

    agrege = dans_fenetre.group_by("pdl").agg(
        [
            *sommes,
            pl.col("debut").min().alias("couverture_debut"),
            pl.col("fin").max().alias("couverture_fin"),
            pl.col("_duree_jours").sum().alias("_couverture_jours"),
            _profondeur_coherente_expr().alias("profondeur_cadran"),
            _qualite_expr().alias("qualite"),
            (pl.col("code_nature") == "C").any().alias("presence_regularisation"),
        ]
    )

    # Mois couverts = SOMME des durées de période (densité réelle de donnée, pas le span
    # global) : les périodes R67 ne se recouvrent pas (ADR-0047), donc Σ durées exclut les
    # trous — un historique creux (périodes disjointes) lit < span. ≈ 30.44 j/mois, pour
    # l'affichage uniquement. `couverture_debut/fin` restent l'étendue de la fenêtre.
    couverture_mois = (pl.col("_couverture_jours") / 30.4375).alias("couverture_mois")

    # Suffisance mesurée en JOURS contre la longueur réelle de la fenêtre (`jours_fenetre`),
    # pas en mois moyens : comparer `couverture_mois (jours/30.4375) >= 12` sous-flaggerait
    # une année calendaire pleine non bissextile (365 j = 11.99 < 12). Le seuil en jours
    # épouse l'ancrage calendaire de `_moins_n_mois` et reste exact (comparaison d'entiers).
    avec_couverture = (
        agrege.with_columns(couverture_mois)
        .with_columns((pl.col("_couverture_jours") >= jours_fenetre).alias("couverture_suffisante"))
        .drop("_couverture_jours")
    )

    # Aligne la sortie WIDE sur la profondeur déclarée (décision 5, #488) :
    # - `4_cadrans` : les 4 sont sommés ; on dérive hp/hc par la relation de synthèse
    #   (hp = hph+hpb, hc = hch+hcb, SOUS_CADRANS) — R67 grille D ne roule pas hp/hc.
    # - `hp_hc` : hp/hc sommés, mais les 4 sous-cadrans incohérents → null.
    # - `base` : tous les cadrans fins → null (base porte le total).
    # L'invariant `base = hp+hc = Σ4` tient alors sur une fenêtre cohérente.
    est_4 = pl.col("profondeur_cadran") == "4_cadrans"
    est_hp_hc = pl.col("profondeur_cadran") == "hp_hc"
    # hp/hc dérivés de leurs sous-cadrans via la relation canonique SOUS_CADRANS (source
    # unique de la synthèse) quand la fenêtre est 4_cadrans ; sinon repris tels quels.
    rollup = {
        cadran: pl.when(est_4)
        .then(pl.sum_horizontal([pl.col(col_energie(sc)) for sc in sous]))
        .otherwise(pl.col(col_energie(cadran)))
        for cadran, sous in SOUS_CADRANS.items()
    }
    aligne = avec_couverture.with_columns(
        # hp/hc : présents si 4_cadrans (dérivés) ou hp_hc (sommés) ; null sinon.
        pl.when(est_4 | est_hp_hc).then(rollup["hp"]).otherwise(None).alias(col_energie("hp")),
        pl.when(est_4 | est_hp_hc).then(rollup["hc"]).otherwise(None).alias(col_energie("hc")),
        # 4 sous-cadrans : présents seulement si 4_cadrans.
        *[pl.when(est_4).then(pl.col(col_energie(c))).otherwise(None).alias(col_energie(c)) for c in _CADRANS_4],
    )

    # Provision mensuelle `/12` plate (cadrans fins → null hors profondeur, propagé).
    mensuels = [(pl.col(col_energie(c)).cast(pl.Float64) / FENETRE_MOIS).alias(_col_mensuel(c)) for c in CADRANS]

    return aligne.with_columns(mensuels).with_columns(
        # Signal alertable (décision 3, #489) : couverture insuffisante OU qualité estimée.
        # La lib expose le signal — la notification reste à l'aval (surveillance, ADR-0037).
        (~pl.col("couverture_suffisante") | (pl.col("qualite") == "estimée")).alias("signal_alertable")
    )


# =============================================================================
# PIPELINE — assemblage cold-start (R67 → estimation validée)
# =============================================================================


@pa.check_types(lazy=True)
def pipeline_estimation_provision_r67(r67: pl.LazyFrame, as_of: dt.date) -> LazyFrame[EstimationProvision]:
    """Pipeline cold-start complet : `flux_r67` → estimation de provision validée.

    Compose les deux étages (profil cadran R67 → effondrement 12 mois) et projette la
    sortie WIDE dans l'ordre du contrat `EstimationProvision`. Lazy ; le `.collect()` est
    porté par le build (`core/builds/rapport_provision.py`, ADR-0019).
    """
    profil = profil_cadran_depuis_r67(r67)
    effondre = effondrer_profil(profil, as_of)

    colonnes = [
        "pdl",
        *_COLS_ENERGIE,
        *(_col_mensuel(c) for c in CADRANS),
        "couverture_debut",
        "couverture_fin",
        "couverture_mois",
        "couverture_suffisante",
        "profondeur_cadran",
        "qualite",
        "presence_regularisation",
        "signal_alertable",
    ]
    return effondre.select(colonnes).sort("pdl")  # type: ignore[return-value]


__all__ = [
    "FENETRE_MOIS",
    "CADRANS",
    "horloge_par_defaut",
    "profil_cadran_depuis_r67",
    "effondrer_profil",
    "pipeline_estimation_provision_r67",
]
