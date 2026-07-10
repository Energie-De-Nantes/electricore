"""Tests du kernel `empreinte_contenu` (#625) : hash de contenu canonique unique.

Remplace les deux canonicaliseurs divergents `_ajouter_source_hash` (méta-périodes)
et `_ajouter_reference` (prestations) — cf. `docs/contrat-prestations.md` pour le
concept de dérive du formateur float Polars que ce kernel évite.
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl

from electricore.api.serializers import empreinte_contenu

PARIS = ZoneInfo("Europe/Paris")


def test_empreinte_contenu_golden_ligne_mixte():
    """Ligne mélangeant float (sensible au formateur Polars), datetime tz-aware,
    null et dict à clés désordonnées — golden 16 caractères épinglé en dur. Casse si
    le canon glisse vers `pl.concat_str(cast(Utf8))` (dérive du formateur float)."""
    df = pl.DataFrame(
        {
            "montant_eur": [10.2],
            "horodatage": [datetime(2025, 3, 1, tzinfo=PARIS)],
            "commentaire": [None],
            "detail": pl.Series([{"b": 2, "a": 1}], dtype=pl.Object),
        }
    )
    empreinte = empreinte_contenu(df, ["montant_eur", "horodatage", "commentaire", "detail"])
    assert empreinte.to_list() == ["cafa70cdd2f3c3d8"]


def test_empreinte_contenu_invariante_a_l_ordre_des_cles_dict():
    """Deux dicts au contenu identique mais aux clés dans un ordre différent
    produisent la même empreinte (JSON à clés triées)."""
    df1 = pl.DataFrame({"detail": pl.Series([{"b": 2, "a": 1}], dtype=pl.Object)})
    df2 = pl.DataFrame({"detail": pl.Series([{"a": 1, "b": 2}], dtype=pl.Object)})

    assert empreinte_contenu(df1, ["detail"]).to_list() == empreinte_contenu(df2, ["detail"]).to_list()


def test_empreinte_contenu_deterministe_sur_double_appel():
    df = pl.DataFrame(
        {
            "a": [1, 2, None],
            "b": ["x", "y", "z"],
            "c": [date(2025, 1, 1), None, date(2025, 1, 3)],
        }
    )
    h1 = empreinte_contenu(df, ["a", "b", "c"]).to_list()
    h2 = empreinte_contenu(df, ["a", "b", "c"]).to_list()
    assert h1 == h2
    assert len(h1[0]) == 16


def test_empreinte_contenu_null_devient_symbole_vide():
    avec_null = empreinte_contenu(pl.DataFrame({"a": [None]}), ["a"]).item()
    avec_symbole = empreinte_contenu(pl.DataFrame({"a": ["∅"]}), ["a"]).item()
    assert avec_null == avec_symbole
