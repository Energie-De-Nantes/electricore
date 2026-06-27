"""Contrat Pandera du seam `affaires_ouvertes` ← `flux_affaires` (#295, ADR-0035).

Le modèle `AffaireJalon` garde la **consommation** de `flux_affaires` par
`affaires_ouvertes` (grain = un jalon par affaire) : il déclare les colonnes que le
pipeline lit, leurs dtypes Polars et leur nullabilité-règle. Ces tests vérifient qu'une
frame au format `flux_affaires` passe, que `statut` reste nullable (#296), et qu'un
dtype faux sur une colonne du contrat est rejeté.

La parité dbt↔Pandera (le type réellement émis par le mart) est prouvée séparément par
le test de frontière du golden affaires (`test_dbt_affaires_respecte_le_contrat_pandera`).
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from electricore.core.models.affaire_jalon import AffaireJalon

PARIS = ZoneInfo("Europe/Paris")


def _frame_affaires(statut: str | None = "COURS") -> pl.DataFrame:
    """Une ligne au format `flux_affaires` (grain = un jalon), dtypes émis par dbt."""
    return pl.DataFrame(
        {
            "affaire_id": ["AFF1"],
            "origine": ["initiee"],
            "prestation": ["CFN"],
            "prestation_libelle": ["Changement de fournisseur"],
            "statut": [statut],
            "pdl": ["99000000000017"],
            "segment": ["C5"],
            "jalon_num": [1],
            "jalon_date_heure": [datetime(2024, 11, 20, 9, tzinfo=PARIS)],
            "affaire_etat": ["DMTR"],
            "affaire_etat_libelle": ["Demande transmise"],
        },
        schema_overrides={
            "statut": pl.Utf8,
            # jalon_num : INTEGER côté dbt → Int32 (cast `as integer`).
            "jalon_num": pl.Int32,
            "jalon_date_heure": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
        },
    )


def test_accepte_la_forme_de_flux_affaires():
    """Le contrat valide une frame au format `flux_affaires` consommée par le seam."""
    AffaireJalon.validate(_frame_affaires(), lazy=True)


def test_statut_nullable():
    """`statut` est nullable (#296) : une affaire fraîchement initiée arrive sans statut
    (null) — le contrat ne doit pas la rejeter (cf. affaires_ouvertes la traite comme
    ouverte « en attente Enedis »)."""
    AffaireJalon.validate(_frame_affaires(statut=None), lazy=True)


def test_rejette_jalon_num_non_entier():
    """Garde-fou de dtype : `jalon_num` est la clé de tri du rollup (max jalon) — un type
    faux (ici String) doit être rejeté au seam."""
    frame = _frame_affaires().with_columns(pl.col("jalon_num").cast(pl.Utf8))
    with pytest.raises(Exception):  # noqa: B017,PT011 — SchemaError(s) Pandera
        AffaireJalon.validate(frame, lazy=True)


def test_colonne_supplementaire_toleree():
    """Le loader `affaires()` est un `SELECT *` : il sert des colonnes hors seam
    (`affaire_jalon_id`, `affaire_date_effet`, `source`). Config.strict=False les tolère."""
    frame = _frame_affaires().with_columns(
        pl.lit("AFF1#1").alias("affaire_jalon_id"),
        pl.lit("flux_X12X13").alias("source"),
    )
    AffaireJalon.validate(frame, lazy=True)
