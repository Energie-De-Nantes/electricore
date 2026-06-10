"""Cadrans temporels : liste canonique et convention de nommage des colonnes.

Source unique du concept *Cadran* (cf. CONTEXT.md) côté code : la liste des
7 cadrans, la relation de synthèse saison haute/basse → cadran principal, et
les constructeurs de noms de colonnes au format `grandeur_cadran_unité`
(issue #119).

Importable par pipelines, builds et loaders (ADR-0019). Les expressions
Polars qui consomment ces noms restent dans `core/pipelines/`.
"""

# Les 7 cadrans canoniques : Base (tarif unique), HP/HC (heures pleines /
# creuses), et le découpage 4 cadrans croisant saison (Haute = nov-mars,
# Basse = avr-oct) et plage horaire — obligatoire en C4, utilisé en Tempo/EJP.
CADRANS: tuple[str, ...] = ("base", "hp", "hc", "hph", "hch", "hpb", "hcb")

# Relation de synthèse : un cadran principal s'obtient en sommant ses
# sous-cadrans saisonniers (hp ← hph + hpb, hc ← hch + hcb).
SOUS_CADRANS: dict[str, tuple[str, str]] = {
    "hp": ("hph", "hpb"),
    "hc": ("hch", "hcb"),
}


def _verifier(cadran: str) -> str:
    if cadran not in CADRANS:
        raise ValueError(f"Cadran inconnu : {cadran!r} (cadrans canoniques : {', '.join(CADRANS)})")
    return cadran


def col_energie(cadran: str) -> str:
    """Nom de la colonne d'énergie d'un cadran (ex : `energie_hp_kwh`)."""
    return f"energie_{_verifier(cadran)}_kwh"


def col_index(cadran: str) -> str:
    """Nom de la colonne d'index d'un cadran (ex : `index_hp_kwh`)."""
    return f"index_{_verifier(cadran)}_kwh"
