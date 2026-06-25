"""Parité du *Contexte mensuel* filtré par PDL/RSC (#366, ADR-0041/ADR-0039).

`contexte_du_mois_filtre()` reconstruit la chronologie d'un seul point/contrat **sans
tourner le pipeline sur tout le parc** : le filtre `pdl`/`rsc` est poussé au boundary de
chargement (clause `WHERE` paramétrée descendue dans DuckDB). Comme le pipeline de
chronologie est **partition-local** (`group_by`/`over` par RSC et PDL) et que l'horizon
est un **paramètre** (#179, jamais un `max()` parc), filtrer l'entrée à X donne un
résultat **identique** au run plein restreint à X.

Deux familles de tests :

1. **Parité de composition** (`charger`) : on prouve sur des frames synthétiques
   (substrat *spine* + *Chronologie des relevés*) que `charger(plein ∩ X)` ≡
   `charger(plein) ∩ X` sur énergie + abonnement + méta-période. La fixture porte un
   **deuxième PDL/RSC volumineux** dont les dates débordent le périmètre filtré : un
   horizon dérivé d'un `max()` parc déplacerait la borne du run plein et **casserait** la
   parité — c'est la garde explicite de l'invariant « horizon = paramètre ».

2. **Poussée du prédicat** (`contexte_du_mois_filtre`) : on espionne les loaders pour
   vérifier que `pdl`/`rsc` atteignent `spine_contrat().filter(...)` / `chronologie_releves().filter(...)`
   (symétrique sur les deux entrées, deux grains), donc que la base ne sert que le
   périmètre demandé (pas de scan parc).
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from electricore.core.builds.contexte_mensuel import (
    _filtrer_point_ou_contrat,
    charger,
)
from electricore.core.models.cadrans import CADRANS, col_index

PARIS = ZoneInfo("Europe/Paris")

# Horizon FIXE : un paramètre, pas une lecture d'horloge ni un max() parc.
HORIZON = datetime(2024, 4, 1, tzinfo=PARIS)


def _ligne_spine(
    *, date: datetime, pdl: str, rsc: str, source: str, type_fait: str, evt: str, puissance: float
) -> dict:
    """Une ligne du substrat *spine* (forme du mart `spine_contrat`)."""
    return {
        "date_evenement": date,
        "pdl": pdl,
        "ref_situation_contractuelle": rsc,
        "source": source,
        "type_fait": type_fait,
        "evenement_declencheur": evt,
        "type_evenement": "contractuel" if source == "flux_C15" else "artificiel",
        "segment_clientele": "C5",
        "etat_contractuel": "EN SERVICE",
        "puissance_souscrite_kva": puissance,
        "formule_tarifaire_acheminement": "BTINFCU4",
        "type_compteur": "LINKY",
        "num_compteur": "123456",
        "categorie": None,
        "ref_demandeur": None,
        "id_affaire": None,
        "niveau_ouverture_services": "2",
        "date_changement_niveau_ouverture_services": None,
    }


def _spine_parc() -> pl.LazyFrame:
    """Spine de DEUX points : PDL_A (1 RSC, petit) et PDL_B (1 RSC, dates débordantes).

    PDL_B porte des FACTURATION jusqu'en mars 2025 — bien au-delà de l'horizon avril 2024.
    Si l'horizon était dérivé d'un `max(date_evenement)` parc, le run plein bornerait PDL_A
    à mars 2025 et la parité avec le run filtré (borné par le même horizon) tomberait.
    """
    lignes = [
        # PDL_A / REF_A : entrée janvier + grille FACTURATION fév/mars 2024.
        _ligne_spine(
            date=datetime(2024, 1, 5, 0, 1, tzinfo=PARIS),
            pdl="PDL_A",
            rsc="REF_A",
            source="flux_C15",
            type_fait="evenement",
            evt="MES",
            puissance=6.0,
        ),
        _ligne_spine(
            date=datetime(2024, 2, 1, 0, 0, tzinfo=PARIS),
            pdl="PDL_A",
            rsc="REF_A",
            source="synthese_mensuelle",
            type_fait="facturation",
            evt="FACTURATION",
            puissance=6.0,
        ),
        _ligne_spine(
            date=datetime(2024, 3, 1, 0, 0, tzinfo=PARIS),
            pdl="PDL_A",
            rsc="REF_A",
            source="synthese_mensuelle",
            type_fait="facturation",
            evt="FACTURATION",
            puissance=6.0,
        ),
        # PDL_B / REF_B : un parc « volumineux » qui déborde l'horizon (jusqu'en 2025).
        _ligne_spine(
            date=datetime(2024, 1, 10, 0, 1, tzinfo=PARIS),
            pdl="PDL_B",
            rsc="REF_B",
            source="flux_C15",
            type_fait="evenement",
            evt="MES",
            puissance=9.0,
        ),
        _ligne_spine(
            date=datetime(2024, 2, 1, 0, 0, tzinfo=PARIS),
            pdl="PDL_B",
            rsc="REF_B",
            source="synthese_mensuelle",
            type_fait="facturation",
            evt="FACTURATION",
            puissance=9.0,
        ),
        _ligne_spine(
            date=datetime(2025, 3, 1, 0, 0, tzinfo=PARIS),
            pdl="PDL_B",
            rsc="REF_B",
            source="synthese_mensuelle",
            type_fait="facturation",
            evt="FACTURATION",
            puissance=9.0,
        ),
    ]
    return pl.LazyFrame(
        lignes,
        schema_overrides={
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "categorie": pl.Utf8,
            "ref_demandeur": pl.Utf8,
            "id_affaire": pl.Utf8,
            "date_changement_niveau_ouverture_services": pl.Date,
        },
    )


def _ligne_releve(*, date: datetime, pdl: str, rsc: str, ordre: bool, index_base: int) -> dict:
    """Une ligne de la *Chronologie des relevés* (forme du mart `chronologie_releves`)."""
    ligne = {
        "pdl": pdl,
        "ref_situation_contractuelle": rsc,
        "formule_tarifaire_acheminement": "BTINFCU4",
        "niveau_ouverture_services": "2",
        "date_releve": date,
        "ordre_index": ordre,
        "source": "flux_R151",
        "releve_id": f"{rsc}-{date:%Y%m%d}",
        "nature_index": "réel",
        "evenement_declencheur": None,
        "id_calendrier_distributeur": "DI000001",
        "releve_manquant": False,
    }
    # Les 7 registres de cadran doivent exister (l'énergie shift/diff sur tous) ; seul `base`
    # porte une valeur, les autres restent null (compteur Base).
    for cadran in CADRANS:
        ligne[col_index(cadran)] = index_base if cadran == "base" else None
    return ligne


def _chronologie_parc() -> pl.LazyFrame:
    """Relevés des deux points. PDL_B déborde l'horizon (relevé en 2025)."""
    lignes = [
        # PDL_A : relevés mensuels bornant fév/mars 2024.
        _ligne_releve(date=datetime(2024, 1, 5, tzinfo=PARIS), pdl="PDL_A", rsc="REF_A", ordre=False, index_base=100),
        _ligne_releve(date=datetime(2024, 2, 1, tzinfo=PARIS), pdl="PDL_A", rsc="REF_A", ordre=False, index_base=150),
        _ligne_releve(date=datetime(2024, 3, 1, tzinfo=PARIS), pdl="PDL_A", rsc="REF_A", ordre=False, index_base=210),
        # PDL_B : déborde jusqu'en 2025.
        _ligne_releve(date=datetime(2024, 1, 10, tzinfo=PARIS), pdl="PDL_B", rsc="REF_B", ordre=False, index_base=500),
        _ligne_releve(date=datetime(2024, 2, 1, tzinfo=PARIS), pdl="PDL_B", rsc="REF_B", ordre=False, index_base=560),
        _ligne_releve(date=datetime(2025, 3, 1, tzinfo=PARIS), pdl="PDL_B", rsc="REF_B", ordre=False, index_base=9000),
    ]
    return pl.LazyFrame(
        lignes,
        schema_overrides={
            "date_releve": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "ordre_index": pl.Boolean,
            "releve_manquant": pl.Boolean,
            # All-None mais typé String : le contrat `ChronologieReleves` exige le dtype.
            "evenement_declencheur": pl.Utf8,
            # Tous les registres de cadran en Int64 (kWh entiers, ADR-0034).
            **{col_index(c): pl.Int64 for c in CADRANS},
        },
    )


def _restreindre(lf: pl.LazyFrame, rsc: str) -> pl.DataFrame:
    """Run plein restreint à une RSC, trié canoniquement pour comparaison."""
    cols = lf.collect_schema().names()
    df = lf.filter(pl.col("ref_situation_contractuelle") == rsc).collect()
    tri = [c for c in ("ref_situation_contractuelle", "debut", "fin") if c in cols]
    return df.sort(tri) if tri else df


class TestPariteCompositionFiltree:
    """`charger(plein ∩ X)` ≡ `charger(plein) ∩ X` sur les trois frames dérivés."""

    def test_energie_abonnement_meta_identiques_pour_la_rsc_filtree(self):
        # Run PLEIN (parc à 2 points) — horizon fixe (paramètre, jamais max() parc).
        plein = charger(_spine_parc(), _chronologie_parc(), mois="2024-02-01", horizon=HORIZON)

        # Run FILTRÉ : seul REF_A entre dans les frames (filtre au boundary de chargement).
        filtre = charger(
            _spine_parc().filter(pl.col("ref_situation_contractuelle") == "REF_A"),
            _chronologie_parc().filter(pl.col("ref_situation_contractuelle") == "REF_A"),
            mois="2024-02-01",
            horizon=HORIZON,
        )

        # Énergie : le découpage de REF_A doit être identique des deux côtés.
        ener_plein = _restreindre(plein.energie, "REF_A")
        ener_filtre = filtre.energie.collect().sort(["ref_situation_contractuelle", "debut", "fin"])
        assert ener_plein.equals(ener_filtre)

        # Abonnement : idem.
        abo_plein = _restreindre(plein.abonnements, "REF_A")
        abo_filtre = filtre.abonnements.collect().sort(["ref_situation_contractuelle", "debut", "fin"])
        assert abo_plein.equals(abo_filtre)

        # Méta-période : la facturation mensuelle de REF_A est la même.
        meta_plein = plein.facturation_mensuelle.filter(pl.col("ref_situation_contractuelle") == "REF_A").sort(
            ["ref_situation_contractuelle", "debut"]
        )
        meta_filtre = filtre.facturation_mensuelle.sort(["ref_situation_contractuelle", "debut"])
        assert meta_plein.equals(meta_filtre)

    def test_parite_par_pdl(self):
        """Le grain PDL fonctionne aussi (filtre sur `pdl`, pas seulement `rsc`)."""
        plein = charger(_spine_parc(), _chronologie_parc(), mois="2024-02-01", horizon=HORIZON)
        filtre = charger(
            _spine_parc().filter(pl.col("pdl") == "PDL_A"),
            _chronologie_parc().filter(pl.col("pdl") == "PDL_A"),
            mois="2024-02-01",
            horizon=HORIZON,
        )
        ener_plein = (
            plein.energie.collect()
            .filter(pl.col("pdl") == "PDL_A")
            .sort(["ref_situation_contractuelle", "debut", "fin"])
        )
        ener_filtre = filtre.energie.collect().sort(["ref_situation_contractuelle", "debut", "fin"])
        assert ener_plein.equals(ener_filtre)


class TestPredicatPousseDansDuckDB:
    """Le filtre `pdl`/`rsc` descend bien au loader (espion `.filter`)."""

    def test_filtre_helper_symetrique_pdl_et_rsc(self):
        """`_filtrer_point_ou_contrat` empile une clause par grain, sur la colonne câblée."""

        class _SpyQuery:
            def __init__(self):
                self.filters: list[dict] = []

            def filter(self, f):
                self.filters.append(f)
                return self

        q = _filtrer_point_ou_contrat(_SpyQuery(), pdl="PDL_A", rsc="REF_A")
        assert q.filters == [{"pdl": "PDL_A"}, {"ref_situation_contractuelle": "REF_A"}]

    def test_contexte_filtre_pousse_le_predicat_sur_les_deux_entrees(self, monkeypatch):
        """`contexte_du_mois_filtre` pousse pdl+rsc dans spine_contrat() ET chronologie_releves() — pas de scan
        parc puis filtre aval (les frames servies ne portent que le périmètre demandé)."""
        import electricore.core.builds.contexte_mensuel as cm

        spy: dict[str, list] = {"spine": [], "chronologie": []}

        class _SpyQuery:
            def __init__(self, nom, lf):
                self._nom = nom
                self._lf = lf

            def filter(self, f):
                spy[self._nom].append(f)
                # Applique vraiment le filtre → la frame servie est restreinte (pas un scan parc).
                col, val = next(iter(f.items()))
                self._lf = self._lf.filter(pl.col(col) == val)
                return self

            def lazy(self):
                return self._lf

        monkeypatch.setattr(cm, "spine_contrat", lambda: _SpyQuery("spine", _spine_parc()))
        monkeypatch.setattr(cm, "chronologie_releves", lambda: _SpyQuery("chronologie", _chronologie_parc()))

        ctx = cm.contexte_du_mois_filtre(pdl="PDL_A", rsc="REF_A", mois="2024-02-01", horizon=HORIZON)

        # Le prédicat a atteint LES DEUX loaders (symétrie), pour LES DEUX grains.
        assert spy["spine"] == [{"pdl": "PDL_A"}, {"ref_situation_contractuelle": "REF_A"}]
        assert spy["chronologie"] == [{"pdl": "PDL_A"}, {"ref_situation_contractuelle": "REF_A"}]

        # Et le contexte produit ne contient que le périmètre filtré.
        rsc_servies = set(ctx.energie.collect()["ref_situation_contractuelle"].to_list())
        assert rsc_servies == {"REF_A"}
