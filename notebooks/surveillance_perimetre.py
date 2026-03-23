import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import marimo as mo
    import polars as pl
    import sys
    from pathlib import Path
    from datetime import date

    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    from electricore.core.loaders.duckdb import duckdb_connection, DuckDBConfig


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Surveillance du périmètre R64

    Vérifie que pour chaque PDL actif dans le périmètre (entre son entrée CFNE/MES/PMES
    et sa sortie CFNS/RES), un relevé R64 existe pour le premier jour de chaque mois.

    Le résultat est un CSV listant les **(PDL, date)** manquants à demander à Enedis.
    """)
    return


@app.cell
def _():
    _config = DuckDBConfig()
    with duckdb_connection(_config.database_path) as _conn:
        _entrees = _conn.execute("""
            SELECT pdl, ref_situation_contractuelle, MIN(date_evenement)::DATE AS date_entree
            FROM flux_enedis.flux_c15
            WHERE evenement_declencheur IN ('CFNE', 'MES', 'PMES')
            GROUP BY pdl, ref_situation_contractuelle
        """).pl()

        _sorties = _conn.execute("""
            SELECT pdl, ref_situation_contractuelle, MIN(date_evenement)::DATE AS date_sortie
            FROM flux_enedis.flux_c15
            WHERE evenement_declencheur IN ('RES', 'CFNS')
            GROUP BY pdl, ref_situation_contractuelle
        """).pl()

    periodes = _entrees.join(_sorties, on=["pdl", "ref_situation_contractuelle"], how="left")
    periodes
    return (periodes,)


@app.cell
def _(periodes):
    periodes_mois = (
        periodes
        .with_columns([
            # Mois suivant l'entrée : on n'attend pas de R64 avant rattachement
            pl.col("date_entree").dt.truncate("1mo").dt.offset_by("1mo").alias("premier_mois"),
            # PDL encore actif → aujourd'hui comme borne de fin
            pl.col("date_sortie").fill_null(date.today()).dt.truncate("1mo").alias("dernier_mois"),
        ])
        .with_columns([
            pl.date_ranges(
                start=pl.col("premier_mois"),
                end=pl.col("dernier_mois"),
                interval="1mo",
            ).alias("debut_mois")
        ])
        .explode("debut_mois")
        .drop_nulls("debut_mois")
        .select(["pdl", "ref_situation_contractuelle", "debut_mois"])
    )
    return (periodes_mois,)


@app.cell
def _():
    _config = DuckDBConfig()
    with duckdb_connection(_config.database_path) as _conn:
        r64_mois = _conn.execute("""
            SELECT DISTINCT pdl, DATE_TRUNC('month', date_releve::DATE)::DATE AS debut_mois
            FROM flux_enedis.flux_r64
        """).pl()
    return (r64_mois,)


@app.cell
def _(periodes_mois, r64_mois):
    manquants = (
        periodes_mois
        .join(r64_mois, on=["pdl", "debut_mois"], how="anti")
        .sort(["debut_mois", "pdl"])
    )
    return (manquants,)


@app.cell(hide_code=True)
def _(manquants, periodes, periodes_mois):
    n_pdl_manquants = manquants["pdl"].n_unique()
    n_total_attendus = len(periodes_mois)
    n_manquants = len(manquants)
    taux_couverture = (1 - n_manquants / n_total_attendus) * 100 if n_total_attendus > 0 else 100.0

    resume_par_pdl = (
        manquants
        .group_by("pdl")
        .agg(pl.len().alias("mois_manquants"), pl.col("debut_mois").min().alias("premier_manquant"))
        .sort("mois_manquants", descending=True)
    )

    mo.vstack([
        mo.md(f"""
        ## Résumé

        | Indicateur | Valeur |
        |---|---|
        | PDL dans le périmètre | **{periodes["pdl"].n_unique()}** |
        | PDL avec R64 manquant | **{n_pdl_manquants}** |
        | Combinaisons (PDL × mois) attendues | **{n_total_attendus}** |
        | Combinaisons manquantes | **{n_manquants}** |
        | Taux de couverture | **{taux_couverture:.1f}%** |
        """),
        mo.md("### Manquants par PDL"),
        mo.ui.table(resume_par_pdl),
    ])
    return


@app.cell(hide_code=True)
def _(manquants):
    mo.vstack([
        mo.md("## Détail des manquants"),
        mo.ui.table(manquants),
    ])
    return


@app.cell
def _(manquants):
    export_dir = Path("perimetre_manquants")
    export_dir.mkdir(exist_ok=True)

    mois_list = sorted(manquants["debut_mois"].unique().to_list())
    for _mois in mois_list:
        _pdls = manquants.filter(pl.col("debut_mois") == _mois).select("pdl")
        _pdls.write_csv(export_dir / f"{_mois.strftime('%Y-%m')}.csv")

    mo.md(f"**{len(mois_list)} fichiers** exportés dans `{export_dir}/` — ex: `{mois_list[0].strftime('%Y-%m')}.csv` … `{mois_list[-1].strftime('%Y-%m')}.csv`" if mois_list else "Aucun manquant — périmètre carré ✓")
    return


if __name__ == "__main__":
    app.run()
