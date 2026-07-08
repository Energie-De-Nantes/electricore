"""Cadrans temporels : liste canonique et convention de nommage des colonnes.

Source unique du concept *Cadran* (cf. CONTEXT.md) côté code : la liste des
7 cadrans, la relation de synthèse saison haute/basse → cadran principal, et
les constructeurs de noms de colonnes au format `grandeur_cadran_unité`
(issue #119).

Importable par pipelines, builds et loaders (ADR-0019). Les expressions
Polars qui consomment ces noms restent dans `core/pipelines/`.

SOURCE UNIQUE du fan-out cadran (ADR-0035 §1) : `CADRANS` est importé par les
schémas Pandera des relevés (`RelevéIndex`/`ChronologieReleves` en dérivent leurs
colonnes `index_*_kwh`) et **injecté** côté dbt via la var `cadrans_releve`
(dbt_project.yml → macro `pivot_cadrans`). La cohérence des deux côtés est prouvée
par `tests/ingestion/test_cadrans_source_unique.py` — la liste cesse d'être recopiée.
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


# Table DI→famille (ADR-0035 §1, #603) : SOURCE UNIQUE du mapping entre l'identifiant
# calendrier distributeur Enedis (`Id_Calendrier_Distributeur`, natif C15/R151/R64/R15) et
# la *Famille de cadrans* du glossaire (CONTEXT.md) — le calendrier de comptage débarrassé
# de l'identifiant Enedis. Les 3 valeurs connues ; tout DI hors de cette table est inconnu.
FAMILLES_CADRANS: dict[str, str] = {
    "DI000001": "base",
    "DI000002": "hp_hc",
    "DI000003": "4_cadrans",
}


def famille_cadrans(id_calendrier_distributeur: str | None) -> str | None:
    """Famille de cadrans d'un relevé, dérivée de son `id_calendrier_distributeur`.

    `None` si le calendrier est absent (null) ou hors des trois DI connus — au consommateur
    (Odoo) de garder son inférence en repli dans ce cas.
    """
    if id_calendrier_distributeur is None:
        return None
    return FAMILLES_CADRANS.get(id_calendrier_distributeur)
