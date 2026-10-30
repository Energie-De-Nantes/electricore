"""
Check de péremption des taux régulés (issue #186, ADR-0024).

La surveillance réglementaire est déléguée — à un membre non technique de la
fédération ou à cette alerte automatisée. Les rythmes attendus sont des
**heuristiques** : un changement hors calendrier passe sous le radar (Limites
d'ADR-0024). Le check produit des avertissements, jamais d'auto-correction.

Rythmes encodés :
- TURPE — nouvelle grille effective au 1ᵉʳ août (indexation annuelle CRE) ;
- Accise — majoration ZNI indexée au 1ᵉʳ février et au 1ᵉʳ août (depuis 2025) ;
- CTA — pas de rythme connu, pas de check.
"""

import datetime as dt
from collections.abc import Mapping
from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True, slots=True)
class RythmeAttendu:
    """Heuristique de renouvellement d'un taux régulé : jalons annuels (mois, jour)."""

    taxe: str
    jalons: tuple[tuple[int, int], ...]
    consigne: str


RYTHMES: tuple[RythmeAttendu, ...] = (
    RythmeAttendu("TURPE", ((8, 1),), "vérifier la délibération CRE (indexation annuelle au 1ᵉʳ août)"),
    RythmeAttendu(
        "Accise",
        ((2, 1), (8, 1)),
        "vérifier l'arrêté tarifaire au JO (majoration ZNI indexée au 1ᵉʳ février et au 1ᵉʳ août)",
    ),
    # CTA : pas de rythme connu — changements apériodiques par arrêté, pas de check
)


@dataclass(frozen=True, slots=True)
class AvertissementPeremption:
    """Un taux présumé périmé : aucune ligne entrée en vigueur depuis le jalon attendu."""

    taxe: str
    attendu_depuis: dt.date
    dernier_start: dt.date
    consigne: str

    @property
    def message(self) -> str:
        return (
            f"aucune ligne {self.taxe} entrée en vigueur depuis le {self.attendu_depuis:%Y-%m-%d} "
            f"(dernière connue : {self.dernier_start:%Y-%m-%d}) — {self.consigne}"
        )


def dernier_jalon(rythme: RythmeAttendu, a_date: dt.date) -> dt.date:
    """Dernière échéance du rythme tombée au plus tard à `a_date`."""
    return max(
        jalon
        for annee in (a_date.year - 1, a_date.year)
        for mois, jour in rythme.jalons
        if (jalon := dt.date(annee, mois, jour)) <= a_date
    )


def _derniers_starts_des_csv() -> dict[str, dt.date]:
    """Dernière entrée en vigueur connue par fichier de taux versionné."""
    # Import local : éviter le coût des pipelines au chargement du module
    from electricore.core.pipelines.accise import load_accise_rules
    from electricore.core.pipelines.cta import load_cta_rules
    from electricore.core.pipelines.turpe import load_turpe_rules

    def _max_start(lf: pl.LazyFrame) -> dt.date:
        return lf.select(pl.col("start").max()).collect().item().date()

    return {
        "TURPE": _max_start(load_turpe_rules()),
        "Accise": _max_start(load_accise_rules()),
        "CTA": _max_start(load_cta_rules()),
    }


def avertissements_peremption(
    a_date: dt.date,
    derniers_starts: Mapping[str, dt.date] | None = None,
) -> tuple[AvertissementPeremption, ...]:
    """Avertissements de péremption à `a_date` : taxes sans ligne depuis leur jalon attendu.

    Args:
        a_date: date du check.
        derniers_starts: dernière entrée en vigueur par taxe — dérivée des CSV
            versionnés si None. Les taxes absentes du mapping sont ignorées.
    """
    if derniers_starts is None:
        derniers_starts = _derniers_starts_des_csv()

    avertissements = []
    for rythme in RYTHMES:
        dernier_start = derniers_starts.get(rythme.taxe)
        if dernier_start is None:
            continue
        jalon = dernier_jalon(rythme, a_date)
        if dernier_start < jalon:
            avertissements.append(
                AvertissementPeremption(
                    taxe=rythme.taxe,
                    attendu_depuis=jalon,
                    dernier_start=dernier_start,
                    consigne=rythme.consigne,
                )
            )
    return tuple(avertissements)
