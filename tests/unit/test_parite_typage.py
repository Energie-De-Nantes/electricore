"""Harnais de parité de typage dbt↔cœur (ADR-0035, #291).

Teste le *fil de détente* lui-même : la table de correspondance SQL↔Polars et le
helper `ecarts_de_typage`. Le test de frontière sur le vrai seam des relevés
(`test_releves_dbt_respecte_le_contrat_pandera`) vit dans le golden dbt — ici on
prouve que le mécanisme **détecte** une divergence de chaque côté (critère #291 :
casser le cast dbt OU le dtype Pandera fait rougir), sans dépendre de dbt.
"""

import pandera.polars as pa
import polars as pl
import pytest

from electricore.core.models.parite_typage import (
    SQL_VERS_POLARS,
    TypeSQLInconnu,
    ecarts_de_typage,
)
from electricore.core.models.releve_index import RelevéIndex

# Schéma SQL conforme aux types arrêtés (ADR-0034) : ce que `contrat_releve()` émet pour
# les colonnes du contrat `RelevéIndex` — index en bigint (kWh entiers).
SCHEMA_RELEVES_CONFORME: dict[str, str] = {
    "date_releve": "TIMESTAMP WITH TIME ZONE",
    "ordre_index": "BOOLEAN",
    "pdl": "VARCHAR",
    "ref_situation_contractuelle": "VARCHAR",
    "formule_tarifaire_acheminement": "VARCHAR",
    "id_calendrier_distributeur": "VARCHAR",
    "source": "VARCHAR",
    "index_base_kwh": "BIGINT",
    "index_hp_kwh": "BIGINT",
    "index_hc_kwh": "BIGINT",
    "index_hph_kwh": "BIGINT",
    "index_hpb_kwh": "BIGINT",
    "index_hch_kwh": "BIGINT",
    "index_hcb_kwh": "BIGINT",
}


def test_parite_verte_sur_schema_conforme():
    """Aucun écart quand les types des deux côtés concordent (contrôle positif sans dbt)."""
    assert ecarts_de_typage(SCHEMA_RELEVES_CONFORME, RelevéIndex) == {}


def test_casser_le_type_dbt_rougit():
    """Re-typer un index côté dbt (bigint → double) est détecté comme divergence."""
    schema_casse = {**SCHEMA_RELEVES_CONFORME, "index_base_kwh": "DOUBLE"}

    ecarts = ecarts_de_typage(schema_casse, RelevéIndex)

    assert "index_base_kwh" in ecarts
    assert ecarts["index_base_kwh"] == (pl.Float64, pl.Int64)


def test_casser_le_dtype_pandera_rougit():
    """Changer un dtype côté contrat Pandera (pdl Utf8 → Int64) est détecté."""

    class ContratFaux(pa.DataFrameModel):
        pdl: pl.Int64  # le mart émet un VARCHAR (Utf8)
        source: pl.Utf8

    ecarts = ecarts_de_typage(SCHEMA_RELEVES_CONFORME, ContratFaux)

    assert ecarts == {"pdl": (pl.String, pl.Int64)}


def test_colonnes_hors_intersection_ignorees():
    """Présence/nullabilité hors périmètre (ADR-0035 §5) : une colonne d'un seul côté
    n'est pas un écart de *type*."""
    # `releve_id` n'est pas dans RelevéIndex ; `type_releve` n'est pas dans ce schéma dbt.
    schema = {**SCHEMA_RELEVES_CONFORME, "releve_id": "VARCHAR"}

    assert ecarts_de_typage(schema, RelevéIndex) == {}


def test_type_sql_inconnu_leve():
    """La table est l'unique endroit qui connaît les deux langages : un type SQL absent
    sur une colonne comparée doit lever, pas passer en silence."""
    schema = {**SCHEMA_RELEVES_CONFORME, "pdl": "DECIMAL(10,2)"}

    with pytest.raises(TypeSQLInconnu):
        ecarts_de_typage(schema, RelevéIndex)


def test_table_de_correspondance_couvre_les_types_du_contrat_releve():
    """Garde-fou : les types SQL émis par `contrat_releve()` (le contrat dbt des relevés)
    sont tous connus de la table — sinon le test de frontière lèverait au lieu de comparer."""
    types_contrat = {"BIGINT", "VARCHAR", "BOOLEAN", "TIMESTAMP WITH TIME ZONE"}

    assert types_contrat <= set(SQL_VERS_POLARS)
