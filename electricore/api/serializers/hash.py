"""Kernel unique de hash de contenu canonique (#625).

Remplace les deux canonicaliseurs divergents historiques — `_ajouter_source_hash`
(méta-périodes) et `_ajouter_reference` (prestations F15) — par une seule politique
dtype→string, robuste aux versions de Polars.

**Pourquoi une boucle Python pure plutôt qu'une expression Polars** : sha256 n'existe
pas en expression Polars native, `pl.Expr.hash()` est instable entre versions, et un
blob `pl.Object` (cas `releves_utilises`) est opaque aux expressions — une expr pure
est impossible sans plugin, de toute façon. La boucle Python est le *mécanisme* de la
garantie (repr texte stable du langage), pas un détail de perf : `str(float)` est un
contrat du langage, `pl.col(c).cast(pl.Utf8)` ne l'est pas — le formateur float de
Polars peut dériver entre versions (cf. `docs/contrat-prestations.md`), re-keyant
silencieusement des lignes déjà upsertées côté Odoo (ADR-0027/0038).
"""

import hashlib
import json
from datetime import date, datetime

import polars as pl


def _canon_valeur(valeur: object) -> str:
    """Rend une valeur de cellule en texte canonique, stable inter-versions Polars."""
    if valeur is None:
        return "∅"
    if isinstance(valeur, (date, datetime)):
        return valeur.isoformat()
    if isinstance(valeur, float):
        return str(valeur)
    if isinstance(valeur, (list, dict)):
        return json.dumps(valeur, sort_keys=True, ensure_ascii=False, default=str)
    return str(valeur)


def empreinte_contenu(df: pl.DataFrame, colonnes: list[str]) -> pl.Series:
    """Hash de contenu déterministe des `colonnes` de `df`, une ligne à la fois.

    Politique dtype→string (boucle Python pure, pas `pl.concat_str(cast(Utf8))`) :
    `null` → `∅`, `date`/`datetime` → `.isoformat()`, `float` → `str()` (repr Python,
    stable par le langage — jamais le formateur `Utf8` de Polars), `list`/`dict`/
    `Object` → JSON à clés triées, sinon `str()`. Champs joints par `␟`
    (séparateur d'unité, improbable dans les données Enedis), sha256 tronqué à 16.

    Args:
        df: DataFrame source.
        colonnes: colonnes à inclure dans le canon, dans l'ordre — l'ordre fait
            partie du contrat d'octets (une colonne `pl.Object` en dernière position
            se comporte comme les autres : sérialisée par la même politique).

    Returns:
        `pl.Series` (`Utf8`) de même longueur que `df`, un hash hex 16 caractères
        par ligne.
    """
    valeurs_par_colonne = [df[c].to_list() for c in colonnes]
    hashes = [
        hashlib.sha256("␟".join(_canon_valeur(v) for v in ligne).encode()).hexdigest()[:16]
        for ligne in zip(*valeurs_par_colonne, strict=True)
    ]
    return pl.Series(hashes, dtype=pl.Utf8)
