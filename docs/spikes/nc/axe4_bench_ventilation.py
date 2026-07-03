"""Spike axe 4 (#547) — bench de ventilation calendaire par ORACLE.

Question : parmi les méthodes candidates pour reconstruire le mois calendaire
(ADR-0026) d'un PDL non-communicant (NC) depuis ses seules fenêtres moisniversaire
Enedis, laquelle ventile le mieux ? On ne peut pas juger un NC directement (pas de
vérité terrain) — l'oracle emprunte des PDL COMMUNICANTS qui ont, EUX AUSSI, des
fenêtres moisniversaire réelles (F15 cycliques) : on les reconstruit "comme s'ils
étaient NC" à partir de ces fenêtres, puis on compare au vrai mesuré quotidien
R64/R151, indépendant de la reconstruction. Aucune fenêtre synthétique.

4 méthodes benchées : (1) prorata temporis, (2) profil saisonnier du parc EDN
(leave-one-out), (3) sans-ventilation aux bornes réelles (praticabilité), (4)
profils officiels Enedis (pluggable — extrait public non embarqué, cf §6).

Repro :  uv run python docs/spikes/nc/axe4_bench_ventilation.py
(lecture seule ; nécessite la base d'ingestion locale peuplée C15+R15+F15+R64+R151.
Si `uv run` échoue en sandbox sur ~/.cache/uv, relancer hors sandbox.)

RGPD : aucune sortie agrégée de ce script ne doit contenir de PDL/RSC — invariant
final (§11) qui scanne CE fichier .py et le rapport .md committé. Le détail par
PDL (nécessaire pour déboguer) part dans sorties-locales/ (non suivi par git).
"""

import re
from pathlib import Path

import duckdb
import polars as pl

DB = "electricore/ingestion/flux_enedis_pipeline.duckdb"
SORTIES = Path("sorties-locales")
SORTIES.mkdir(exist_ok=True)

# Seuil de sélection oracle : au moins ce nombre de fenêtres moisniversaire F15
# pour qu'un PDL communicant entre dans le bench (assez de "matériau NC" pour
# comparer plusieurs mois). Documenté comme source de biais (cf. rapport) : plus
# le seuil est haut, plus la population est petite mais mieux dotée en historique.
SEUIL_FENETRES = 6

con = duckdb.connect(DB, read_only=True)
con.execute("use flux_enedis")


def q(sql: str) -> pl.DataFrame:
    return con.execute(sql).pl()


print("=== 0. Sélection du parc oracle ===")

communicant = q(
    """
    with dernier as (
      select pdl, niveau_ouverture_services, etat_contractuel,
             row_number() over (partition by pdl order by date_evenement desc) rn
      from spine_contrat
    )
    select pdl from dernier
    where rn = 1 and niveau_ouverture_services = '2' and etat_contractuel is distinct from 'RESILIE'
    """
)
print(f"  PDL communicants présents (niveau=2, non résiliés) : {communicant.height}")

# Matériau NC : fenêtres moisniversaire réelles F15 (lignes CYCL primaires, hors
# corrections Enedis qui s'annulent en paire annulation/régularisation — cf.
# exploration ; nature_ev='01' = composante soutirage normale).
fenetres_brutes = q(
    """
    select pdl, ref_situation_contractuelle as rsc, date_debut, date_fin, sum(quantite) as energie_kwh
    from flux_f15_detail
    where type_facturation = 'CYCL' and unite = 'kWh' and nature_ev = '01'
      and libelle_ev not like 'Correctif%'
    group by 1, 2, 3, 4
    having date_diff('day', date_debut, date_fin) > 0
    """
).with_columns((pl.col("date_fin") - pl.col("date_debut")).dt.total_days().alias("jours"))

fenetres_communicant = fenetres_brutes.join(communicant, on="pdl", how="inner")

# Un index par (pdl, jour) : coalesce des 3 structures tarifaires possibles
# (base seul, HP/HC, ou 4 cadrans — cf. core/models/cadrans.py). Priorité R64 >
# R151 en cas de double relevé le même jour (R64 = source maîtrisée, ADR-0036).
index_jour = q(
    """
    with releves_reels as (
      select pdl, source,
             cast((date_releve at time zone 'Europe/Paris') as date) as jour,
             coalesce(
               index_base_kwh,
               case when index_hp_kwh is not null then index_hp_kwh + index_hc_kwh end,
               case when index_hph_kwh is not null
                    then index_hph_kwh + index_hpb_kwh + index_hch_kwh + index_hcb_kwh end
             ) as index_total
      from releves
      where source in ('flux_R151', 'flux_R64')
    )
    select pdl, jour, index_total
    from releves_reels
    where index_total is not null
    qualify row_number() over (partition by pdl, jour order by (source = 'flux_R64') desc) = 1
    """
)
print(f"  jours-PDL avec un index réel R151/R64 : {index_jour.height}")

# Gate qualité oracle : un remplacement de compteur (ou une correction Enedis a
# posteriori) casse la différence d'index brute (saut ou chute qui n'est pas de la
# consommation) — confondre ça avec une erreur de VENTILATION fausserait tout le
# bench. Détecté en comparant, sur les propres bornes de chaque fenêtre F15, la
# vérité index-brut au montant facturé F15 (deux sources indépendantes qui doivent
# s'accorder à la marge de mesure près) : si l'écart dépasse la tolérance sur UNE
# seule fenêtre, tout le PDL est écarté de l'oracle (pas seulement la fenêtre —
# l'anomalie peut avoir décalé l'index pour le reste de l'historique).
con.register("fenetres_communicant_df", fenetres_communicant)
con.register("index_jour_df", index_jour)
verite_fenetre = q(
    """
    select f.pdl, f.date_debut, f.date_fin, f.energie_kwh,
           i2.index_total - i1.index_total as verite_kwh
    from fenetres_communicant_df f
    join index_jour_df i1 on i1.pdl = f.pdl and i1.jour = f.date_debut
    join index_jour_df i2 on i2.pdl = f.pdl and i2.jour = f.date_fin
    """
).with_columns(
    (pl.col("verite_kwh") - pl.col("energie_kwh")).abs().alias("ecart_abs"),
)
TOLERANCE_ANOMALIE_KWH = 100.0
TOLERANCE_ANOMALIE_RELATIVE = 0.3
seuil_anomalie = pl.max_horizontal(
    pl.lit(TOLERANCE_ANOMALIE_KWH),
    TOLERANCE_ANOMALIE_RELATIVE * pl.max_horizontal(pl.col("verite_kwh").abs(), pl.col("energie_kwh").abs()),
)
pdls_anomalie = (
    verite_fenetre.filter((pl.col("verite_kwh") < 0) | (pl.col("ecart_abs") > seuil_anomalie)).select("pdl").unique()
)
print(
    f"  PDL écartés (anomalie compteur/correction : fenêtre F15 vs index brut incohérents) : "
    f"{pdls_anomalie.height} / {verite_fenetre['pdl'].n_unique()} vérifiables"
)

fenetres = fenetres_communicant.join(pdls_anomalie, on="pdl", how="anti")
nb_fenetres = fenetres.group_by("pdl").len(name="n_fenetres")
oracle_pdls = nb_fenetres.filter(pl.col("n_fenetres") >= SEUIL_FENETRES).select("pdl")
print(f"  PDL avec matériau NC propre (F15 cyclique, hors correctifs et anomalies) : {nb_fenetres.height}")
print(f"  PDL oracle retenus (>= {SEUIL_FENETRES} fenêtres)     : {oracle_pdls.height}")

fenetres_oracle = fenetres.join(oracle_pdls, on="pdl", how="inner")

print("\n=== 1. Vérité terrain (mesuré quotidien R64/R151, indépendant du matériau NC) ===")

# Grille de mois calendaires globale (bornée par la vérité terrain disponible).
bornes_globales = index_jour.select(pl.col("jour").min().alias("mn"), pl.col("jour").max().alias("mx")).row(0)
mois_grille = q(
    f"""
    select unnest(generate_series(
        date_trunc('month', date '{bornes_globales[0]}'),
        date_trunc('month', date '{bornes_globales[1]}'),
        interval '1 month'
    ))::date as mois_debut
    """
)

# Vérité mensuelle pour TOUT le parc communicant (sert de base au profil saisonnier
# §5 — plus large que le seul sous-ensemble oracle, pour un profil mieux doté), en
# excluant les PDL déjà repérés en anomalie (§0). Filet de sécurité supplémentaire :
# un index cumulatif ne peut pas décroître (energie_kwh < 0 = compteur remplacé,
# non capté par le gate F15 si le PDL n'a pas assez de fenêtres pour être vérifié).
communicant_propre = communicant.join(pdls_anomalie, on="pdl", how="anti")
con.register("communicant_propre_df", communicant_propre)
con.register("mois_grille_df", mois_grille)
verite_mois_parc = q(
    """
    select c.pdl, m.mois_debut,
           (m.mois_debut + interval '1 month')::date as mois_fin,
           i2.index_total - i1.index_total as energie_kwh
    from communicant_propre_df c
    cross join mois_grille_df m
    join index_jour_df i1 on i1.pdl = c.pdl and i1.jour = m.mois_debut
    join index_jour_df i2 on i2.pdl = c.pdl and i2.jour = (m.mois_debut + interval '1 month')::date
    """
).filter(pl.col("energie_kwh") >= 0)
print(f"  mois calendaires avec vérité complète (parc communicant propre) : {verite_mois_parc.height}")

verite_mois_oracle = verite_mois_parc.join(oracle_pdls, on="pdl", how="inner")
print(f"  ... dont chez les {oracle_pdls.height} PDL oracle : {verite_mois_oracle.height}")

print("\n=== 2. Couverture calendaire : mois comparables (matériau NC sans trou) ===")

# Un mois est comparable si (a) la vérité terrain existe (jointure ci-dessus) ET
# (b) le matériau NC (fenêtres F15) couvre TOUS ses jours sans trou — condition
# nécessaire pour que les méthodes de ventilation (1/2/4) aient de quoi reconstruire
# le mois en entier. Vérifié jour par jour (grille explicite, pas d'approximation
# sur les bornes de fenêtres).
con.register("fenetres_oracle_df", fenetres_oracle)
con.register("verite_mois_oracle_df", verite_mois_oracle)
mois_ok = q(
    """
    with jours_fenetres as (
      select distinct pdl, unnest(generate_series(date_debut, date_fin - interval '1 day', interval '1 day'))::date as jour
      from fenetres_oracle_df
    ),
    jours_mois as (
      select pdl, mois_debut, unnest(generate_series(mois_debut, mois_fin - interval '1 day', interval '1 day'))::date as jour
      from (select distinct pdl, mois_debut, (mois_debut + interval '1 month')::date as mois_fin
            from verite_mois_oracle_df)
    )
    select jm.pdl, jm.mois_debut
    from jours_mois jm
    left join jours_fenetres jf on jf.pdl = jm.pdl and jf.jour = jm.jour
    group by jm.pdl, jm.mois_debut
    having count(*) = count(jf.jour)
    """
)

comparables = verite_mois_oracle.join(mois_ok, on=["pdl", "mois_debut"], how="inner")
print(f"  mois oracle avec vérité ET couverture NC complète : {comparables.height} (sur {verite_mois_oracle.height})")
assert comparables.height > 200, "trop peu de mois comparables pour un verdict — revoir SEUIL_FENETRES ou la sélection"

print("\n=== 3. Méthode 1 — prorata temporis (baseline) ===")

con.register("comparables_df", comparables)
recon_prorata = q(
    """
    select c.pdl, c.mois_debut,
           sum(
             f.energie_kwh
             * greatest(0, date_diff('day', greatest(f.date_debut, c.mois_debut), least(f.date_fin, c.mois_fin)))
             / f.jours
           ) as energie_kwh
    from comparables_df c
    join fenetres_oracle_df f
      on f.pdl = c.pdl and f.date_debut < c.mois_fin and f.date_fin > c.mois_debut
    group by c.pdl, c.mois_debut
    """
).rename({"energie_kwh": "prorata_kwh"})

# Invariant de conservation : la somme ventilée sur TOUS les mois touchés par une
# fenêtre (comparables ou non) doit valoir l'énergie de la fenêtre — vérifié sur le
# domaine complet des fenêtres oracle (pas seulement les mois comparables, sinon
# l'invariant serait trivialement vrai par construction du filtre).
recon_prorata_toutes_fenetres = q(
    """
    with mois_touches as (
      select f.pdl, f.date_debut, f.date_fin, f.energie_kwh, f.jours,
             unnest(generate_series(date_trunc('month', f.date_debut), date_trunc('month', f.date_fin - interval '1 day'), interval '1 month'))::date as mois_debut
      from fenetres_oracle_df f
    )
    select pdl, date_debut, date_fin, energie_kwh, jours,
           sum(
             greatest(0, date_diff('day', greatest(date_debut, mois_debut), least(date_fin, (mois_debut + interval '1 month')::date)))
             / jours
           ) as somme_poids
    from mois_touches
    group by pdl, date_debut, date_fin, energie_kwh, jours
    """
)
ecart_conservation = (recon_prorata_toutes_fenetres["somme_poids"] - 1.0).abs().max()
assert ecart_conservation < 1e-9, f"conservation de l'énergie violée (prorata) : écart {ecart_conservation}"
print(
    f"  invariant conservation énergie (prorata, sur {recon_prorata_toutes_fenetres.height} fenêtres) : OK (écart max {ecart_conservation:.2e})"
)

print("\n=== 4. Méthode 2 — profil saisonnier du parc EDN (leave-one-out) ===")

# Poids mensuel de saisonnalité dérivé du PARC COMMUNICANT ENTIER (verite_mois_parc,
# §1 — plus large que les seuls PDL oracle) pour une base statistique robuste.
# Anti-fuite : le poids utilisé pour reconstruire un PDL donné EXCLUT sa propre
# contribution (leave-one-out), recalculé par PDL (alternative documentée dans le
# rapport : dériver sur une période disjointe — écartée ici car l'historique dispo
# est court, ~2 ans, et le leave-one-out ne coûte rien de plus en calcul vectorisé).
pdl_moy = (
    verite_mois_parc.with_columns(pl.col("mois_debut").dt.month().alias("moy"))
    .group_by(["pdl", "moy"])
    .agg(pl.col("energie_kwh").sum().alias("pdl_moy_kwh"))
)
population_moy = pdl_moy.group_by("moy").agg(pl.col("pdl_moy_kwh").sum().alias("pop_moy_kwh"))
population_total = population_moy["pop_moy_kwh"].sum()
pdl_total = pdl_moy.group_by("pdl").agg(pl.col("pdl_moy_kwh").sum().alias("pdl_total_kwh"))

grille_pdl_moy = oracle_pdls.join(pl.DataFrame({"moy": list(range(1, 13))}), how="cross")
poids_pdl_moy = (
    grille_pdl_moy.join(pdl_moy, on=["pdl", "moy"], how="left")
    .join(population_moy, on="moy", how="left")
    .join(pdl_total, on="pdl", how="left")
    .with_columns(pl.col("pdl_moy_kwh").fill_null(0.0), pl.col("pdl_total_kwh").fill_null(0.0))
    .with_columns(
        (pl.col("pop_moy_kwh") - pl.col("pdl_moy_kwh")).alias("excl_moy_kwh"),
        (population_total - pl.col("pdl_total_kwh")).alias("excl_total_kwh"),
    )
    .with_columns((pl.col("excl_moy_kwh") / pl.col("excl_total_kwh")).alias("poids_excl"))
    .select("pdl", "moy", "poids_excl")
)
print(f"  poids (pdl, mois-de-l'année) leave-one-out calculés : {poids_pdl_moy.height} lignes")

con.register("poids_pdl_moy_df", poids_pdl_moy)
con.execute(
    """
    create or replace temp table jours_avec_poids as
    with jours_fenetre as (
      select f.pdl, f.date_debut, f.date_fin, f.energie_kwh,
             unnest(generate_series(f.date_debut, f.date_fin - interval '1 day', interval '1 day'))::date as jour
      from fenetres_oracle_df f
    )
    select jf.*,
           p.poids_excl / date_diff('day', date_trunc('month', jf.jour), (date_trunc('month', jf.jour) + interval '1 month')) as poids
    from jours_fenetre jf
    join poids_pdl_moy_df p on p.pdl = jf.pdl and p.moy = month(jf.jour)
    """
)
recon_profil = q(
    """
    with denom as (
      select pdl, date_debut, date_fin, sum(poids) as denom
      from jours_avec_poids group by 1, 2, 3
    )
    select j.pdl, date_trunc('month', j.jour)::date as mois_debut,
           sum(j.energie_kwh * j.poids / d.denom) as profil_kwh
    from jours_avec_poids j
    join denom d using (pdl, date_debut, date_fin)
    group by 1, 2
    """
)

# Invariant de conservation, même vérification numérique qu'en §3 mais par fenêtre
# entière (pas seulement les mois comparables) : la somme ventilée sur tous les
# mois d'une fenêtre doit reconstituer exactement son énergie.
verif_conservation_profil = q(
    """
    with denom as (
      select pdl, date_debut, date_fin, energie_kwh, sum(poids) as denom
      from jours_avec_poids group by 1, 2, 3, 4
    )
    select j.pdl, j.date_debut, j.date_fin,
           sum(j.energie_kwh * j.poids / d.denom) as somme_reconstruite,
           first(d.energie_kwh) as energie_kwh
    from jours_avec_poids j
    join denom d using (pdl, date_debut, date_fin)
    group by j.pdl, j.date_debut, j.date_fin
    """
).with_columns((pl.col("somme_reconstruite") - pl.col("energie_kwh")).abs().alias("ecart"))
ecart_max_profil = verif_conservation_profil["ecart"].max()
assert ecart_max_profil < 1e-6, f"conservation de l'énergie violée (profil) : écart {ecart_max_profil}"
print(
    f"  invariant conservation énergie (profil, sur {verif_conservation_profil.height} fenêtres) : OK (écart max {ecart_max_profil:.2e})"
)

print("\n=== 5. Méthode 3 — sans-ventilation aux bornes réelles (praticabilité) ===")

# Pas de reconstruction mensuelle : le solde se fait aux bornes des fenêtres
# elles-mêmes (relevés Réels). Deux questions chiffrées : (a) à quel grain une
# fenêtre entière tient-elle sans déborder (mois vs trimestre), (b) quel reste-à-
# estimer aux bouts quand on ne solde qu'aux frontières pleines.

# NB cadence : mesurer la fréquence des relevés RÉELS qui borneraient un solde propre
# est une mesure de l'axe 2 #545 (sur les vrais NC : rare, ~supra-annuel). Ici l'oracle
# est communicant → index R151/R64 QUOTIDIEN, donc une « cadence » calculée sur cet
# index vaudrait trivialement 1 j et n'informerait pas la praticabilité NC. On ne la
# calcule donc PAS ici (elle serait trompeuse) ; axe 4 chiffre la CONSÉQUENCE du solde
# aux bornes : taille des fenêtres moisniversaire + reste-à-estimer par grain.

# Taille des fenêtres moisniversaire elles-mêmes (le grain "naturel" du solde).
print("  taille des fenêtres moisniversaire (jours) :")
print(f"    p50={fenetres_oracle['jours'].quantile(0.5):.0f}j  p90={fenetres_oracle['jours'].quantile(0.9):.0f}j")


def reste_a_estimer(bornes: pl.DataFrame, verite: pl.DataFrame, label: str) -> pl.DataFrame:
    """Pour chaque (pdl, période [debut,fin)) : part de l'énergie réelle NON couverte
    par des fenêtres ENTIÈREMENT incluses dans la période (aucune ventilation d'une
    fenêtre à cheval). C'est le "reste à estimer" du solde aux bornes réelles."""
    con.register("bornes_df", bornes)
    couverte = q(
        """
        select b.pdl, b.periode_debut,
               coalesce(sum(f.energie_kwh), 0) as couverte_kwh,
               coalesce(sum(f.jours), 0) as jours_couverts
        from bornes_df b
        left join fenetres_oracle_df f
          on f.pdl = b.pdl and f.date_debut >= b.periode_debut and f.date_fin <= b.periode_fin
        group by b.pdl, b.periode_debut
        """
    )
    out = (
        verite.join(couverte, on=["pdl", "periode_debut"], how="inner")
        .with_columns((pl.col("periode_fin") - pl.col("periode_debut")).dt.total_days().alias("jours_periode"))
        .with_columns(
            (pl.col("energie_kwh") - pl.col("couverte_kwh")).alias("reste_kwh"),
            (pl.col("jours_periode") - pl.col("jours_couverts")).alias("jours_reste"),
        )
        .with_columns(
            pl.when(pl.col("energie_kwh") != 0)
            .then((pl.col("reste_kwh") / pl.col("energie_kwh")).abs())
            .otherwise(None)
            .alias("reste_pct")
        )
    )
    print(f"  reste-à-estimer au grain {label} (n={out.height}) :")
    print(
        f"    jours non soldés/période : p50={out['jours_reste'].quantile(0.5):.0f}  p90={out['jours_reste'].quantile(0.9):.0f}"
    )
    print(
        f"    reste en % de l'énergie de la période : p50={out['reste_pct'].quantile(0.5):.1%}  p90={out['reste_pct'].quantile(0.9):.1%}"
    )
    return out


bornes_mois = verite_mois_oracle.select(
    "pdl", pl.col("mois_debut").alias("periode_debut"), pl.col("mois_fin").alias("periode_fin")
)
verite_mois_pour_reste = verite_mois_oracle.rename({"mois_debut": "periode_debut", "mois_fin": "periode_fin"})
reste_mois = reste_a_estimer(bornes_mois, verite_mois_pour_reste, "mensuel")

trimestre_grille = q(
    f"""
    select unnest(generate_series(
        date_trunc('quarter', date '{bornes_globales[0]}'),
        date_trunc('quarter', date '{bornes_globales[1]}'),
        interval '3 month'
    ))::date as trimestre_debut
    """
)
con.register("trimestre_grille_df", trimestre_grille)
con.register("oracle_pdls_df", oracle_pdls)
verite_trimestre_oracle = q(
    """
    select c.pdl, t.trimestre_debut as periode_debut,
           (t.trimestre_debut + interval '3 month')::date as periode_fin,
           i2.index_total - i1.index_total as energie_kwh
    from oracle_pdls_df c
    cross join trimestre_grille_df t
    join index_jour_df i1 on i1.pdl = c.pdl and i1.jour = t.trimestre_debut
    join index_jour_df i2 on i2.pdl = c.pdl and i2.jour = (t.trimestre_debut + interval '3 month')::date
    """
).filter(pl.col("energie_kwh") >= 0)  # filet de sécurité monotonie, cf. §0/§1

bornes_trimestre = verite_trimestre_oracle.select("pdl", "periode_debut", "periode_fin")
reste_trimestre = reste_a_estimer(bornes_trimestre, verite_trimestre_oracle, "trimestriel (grain accise)")

print("\n=== 6. Méthode 4 — profils officiels Enedis (pluggable) ===")

COEFFICIENTS_ENEDIS_CSV = Path("docs/spikes/nc/donnees/coefficients_profils_enedis.csv")


def charger_coefficients_profils_enedis(chemin: Path) -> pl.DataFrame | None:
    """Charge un profil mensuel PUBLIC Enedis (colonnes `mois` 1-12, `poids` sommant
    à 1), extrait statique et daté des « coefficients des profils »
    (data.enedis.fr / opendata.enedis.fr, licence ouverte). Retourne None si le
    fichier n'est pas fourni — la méthode 4 est alors sautée sans bloquer le bench
    (cf. rapport §"extrait à fournir")."""
    if not chemin.exists():
        return None
    return pl.read_csv(chemin)


coefficients_enedis = charger_coefficients_profils_enedis(COEFFICIENTS_ENEDIS_CSV)

if coefficients_enedis is None:
    print(f"  {COEFFICIENTS_ENEDIS_CSV} absent : méthode 4 sautée (extrait public à fournir, cf. rapport).")
    print(
        "  Tentative faite (spike) : data.enedis.fr → opendata.enedis.fr, dataset "
        "'coefficients-des-profils' (~16.9M lignes, granularité demi-heure, 5 ans glissants, "
        "licence ouverte). Un agrégat mensuel fiable exige une requête serveur (API ODS "
        "v2.1, group_by mois) plutôt qu'un rapatriement brut — non bouclé dans ce spike "
        "(risque de valeurs numériques altérées via un pipeline de résumé IA plutôt qu'un "
        "export direct). Chargeur pluggable prêt : `charger_coefficients_profils_enedis(csv)`."
    )
    recon_enedis = None
else:
    assert abs(coefficients_enedis["poids"].sum() - 1.0) < 1e-6, "les poids Enedis doivent sommer à 1"
    poids_enedis_par_jour = coefficients_enedis.rename({"mois": "moy"})
    con.register("poids_enedis_df", poids_enedis_par_jour)
    con.execute(
        """
        create or replace temp table jours_avec_poids_enedis as
        with jours_fenetre as (
          select f.pdl, f.date_debut, f.date_fin, f.energie_kwh,
                 unnest(generate_series(f.date_debut, f.date_fin - interval '1 day', interval '1 day'))::date as jour
          from fenetres_oracle_df f
        )
        select jf.*,
               p.poids / date_diff('day', date_trunc('month', jf.jour), (date_trunc('month', jf.jour) + interval '1 month')) as poids
        from jours_fenetre jf
        join poids_enedis_df p on p.moy = month(jf.jour)
        """
    )
    recon_enedis = q(
        """
        with denom as (
          select pdl, date_debut, date_fin, sum(poids) as denom
          from jours_avec_poids_enedis group by 1, 2, 3
        )
        select j.pdl, date_trunc('month', j.jour)::date as mois_debut,
               sum(j.energie_kwh * j.poids / d.denom) as enedis_kwh
        from jours_avec_poids_enedis j
        join denom d using (pdl, date_debut, date_fin)
        group by 1, 2
        """
    )
    print(f"  profil Enedis chargé ({COEFFICIENTS_ENEDIS_CSV}) : {coefficients_enedis.height} mois")

print("\n=== 7. Métriques : erreur de reconstruction, mois calendaire + trimestre (accise) ===")

erreurs_mois = comparables.select("pdl", "mois_debut", pl.col("energie_kwh").alias("verite_kwh"))
erreurs_mois = erreurs_mois.join(recon_prorata, on=["pdl", "mois_debut"], how="left")
erreurs_mois = erreurs_mois.join(recon_profil, on=["pdl", "mois_debut"], how="left")
if recon_enedis is not None:
    erreurs_mois = erreurs_mois.join(recon_enedis, on=["pdl", "mois_debut"], how="left")

methodes = {"prorata_kwh": "1. Prorata temporis", "profil_kwh": "2. Profil saisonnier parc"}
if recon_enedis is not None:
    methodes["enedis_kwh"] = "4. Profils officiels Enedis"


def stats_erreur(df: pl.DataFrame, col: str) -> dict:
    ecart = df[col] - df["verite_kwh"]
    abs_ecart = ecart.abs()
    # Erreur relative (scale-free) : indispensable ici car la MAE en kWh est tirée par
    # une queue de gros consommateurs (C4 >36 kVA) — la médiane et l'erreur relative
    # disent le signal pratique, la MAE dit la sensibilité aux gros volumes.
    mask = df["verite_kwh"].abs() > 1e-9
    rel = (abs_ecart / df["verite_kwh"].abs()).filter(mask)
    return {
        "n": df.height,
        "mae_kwh": abs_ecart.mean(),
        "biais_kwh": ecart.mean(),
        "p50_kwh": abs_ecart.quantile(0.5),
        "p90_kwh": abs_ecart.quantile(0.9),
        "p50_rel": rel.quantile(0.5),
        "p90_rel": rel.quantile(0.9),
    }


def ligne_stats(nom: str, s: dict) -> str:
    return (
        f"{nom:30} n={s['n']:5}  MAE={s['mae_kwh']:7.1f} kWh  biais={s['biais_kwh']:+7.1f}  "
        f"|err| médian={s['p50_kwh']:6.2f} kWh ({s['p50_rel']:.1%})  p90={s['p90_kwh']:6.2f} kWh ({s['p90_rel']:.1%})"
    )


print("  --- grain mois calendaire (ADR-0026) ---")
resultats_mois = {}
for col, nom in methodes.items():
    s = stats_erreur(erreurs_mois, col)
    resultats_mois[nom] = s
    print(f"    {ligne_stats(nom, s)}")

# Grain trimestre (accise) : uniquement les trimestres dont les 3 mois sont
# comparables (couverture NC complète + vérité disponible sur les 3), agrégés
# contre une vérité trimestrielle INDÉPENDANTE (index directement aux bornes du
# trimestre, pas la somme des vérités mensuelles).
con.register("comparables_df", comparables)
con.register("recon_prorata_df", recon_prorata)
con.register("recon_profil_df", recon_profil)
if recon_enedis is not None:
    con.register("recon_enedis_df", recon_enedis)

trimestres_complets = q(
    """
    select pdl, date_trunc('quarter', mois_debut)::date as trimestre_debut
    from comparables_df
    group by 1, 2
    having count(*) = 3
    """
)
con.register("trimestres_complets_df", trimestres_complets)
con.register("verite_trimestre_oracle_df", verite_trimestre_oracle)

recon_par_colonne = {"prorata_kwh": "recon_prorata_df", "profil_kwh": "recon_profil_df"}
if recon_enedis is not None:
    recon_par_colonne["enedis_kwh"] = "recon_enedis_df"


def agrege_trimestre(recon_df_name: str, col_kwh: str) -> pl.DataFrame:
    return q(
        f"""
        select tc.pdl, tc.trimestre_debut, sum(r.{col_kwh}) as {col_kwh}
        from trimestres_complets_df tc
        join comparables_df c on c.pdl = tc.pdl and date_trunc('quarter', c.mois_debut) = tc.trimestre_debut
        join {recon_df_name} r on r.pdl = c.pdl and r.mois_debut = c.mois_debut
        group by tc.pdl, tc.trimestre_debut
        """
    )


erreurs_trimestre = q(
    """
    select tc.pdl, tc.trimestre_debut, v.energie_kwh as verite_kwh
    from trimestres_complets_df tc
    join verite_trimestre_oracle_df v on v.pdl = tc.pdl and v.periode_debut = tc.trimestre_debut
    """
)

for col, recon_df_name in recon_par_colonne.items():
    agg = agrege_trimestre(recon_df_name, col)
    erreurs_trimestre = erreurs_trimestre.join(agg, on=["pdl", "trimestre_debut"], how="left")

print(f"\n  --- grain trimestre fiscal / accise (n={erreurs_trimestre.height} trimestres pleins) ---")
resultats_trimestre = {}
for col, nom in methodes.items():
    s = stats_erreur(erreurs_trimestre, col)
    resultats_trimestre[nom] = s
    print(f"    {ligne_stats(nom, s)}")

# Détail par PDL (nominatif) → sortie locale non suivie (RGPD). Contient les PDL et
# sert au débogage du bench ; jamais committé.
erreurs_mois.write_csv(SORTIES / "axe4_bench_detail_mois.csv")

print("\n=== 8. Verdict : le prorata ventile-t-il mal la saisonnalité ? ===")

# Le préjugé (#547) : « le prorata temporis ventile mal la saisonnalité, un profil
# saisonnier ferait mieux ». Test data : comparer l'erreur médiane du prorata (M1) à
# celle du profil saisonnier (M2), aux deux grains. Le prorata GAGNE le préjugé est
# INVALIDÉ (la saisonnalité intra-fenêtre ne paie pas le surcroît de modèle).
p_mois = resultats_mois["1. Prorata temporis"]
s_mois = resultats_mois["2. Profil saisonnier parc"]
p_trim = resultats_trimestre["1. Prorata temporis"]
s_trim = resultats_trimestre["2. Profil saisonnier parc"]

prorata_gagne_mois = p_mois["p50_kwh"] <= s_mois["p50_kwh"]
prorata_gagne_trim = p_trim["p50_kwh"] <= s_trim["p50_kwh"]
verdict_prejuge = (
    "INVALIDÉ — le prorata ventile au moins aussi bien que le profil saisonnier"
    if prorata_gagne_mois and prorata_gagne_trim
    else "CONFIRMÉ — le profil saisonnier ventile mieux que le prorata"
    if not prorata_gagne_mois and not prorata_gagne_trim
    else "MITIGÉ — l'avantage dépend du grain (voir tableau)"
)
print(f"  mois     : prorata |err| médian {p_mois['p50_kwh']:.2f} kWh vs profil {s_mois['p50_kwh']:.2f} kWh")
print(f"  trimestre: prorata |err| médian {p_trim['p50_kwh']:.2f} kWh vs profil {s_trim['p50_kwh']:.2f} kWh")
print(f"  → préjugé « le prorata ventile mal la saisonnalité » : {verdict_prejuge}")

print("\n=== 9. Praticabilité « bornes réelles » (synthèse M3) ===")

# Résumé chiffré pour le rapport : taille des fenêtres (grain naturel du solde) et
# part restant à estimer aux bords selon le grain de solde.
fen_p50 = fenetres_oracle["jours"].quantile(0.5)
fen_p90 = fenetres_oracle["jours"].quantile(0.9)
reste_mois_pct_p50 = reste_mois["reste_pct"].quantile(0.5)
reste_trim_pct_p50 = reste_trimestre["reste_pct"].quantile(0.5)
reste_trim_pct_p90 = reste_trimestre["reste_pct"].quantile(0.9)
print(f"  taille fenêtres moisniversaire : p50={fen_p50:.0f}j  p90={fen_p90:.0f}j (grain naturel du solde)")
print(
    f"  reste à estimer, solde au mois     : {reste_mois_pct_p50:.0%} médian de l'énergie (fenêtres ~30j ⇒ jamais incluses dans un mois civil)"
)
print(f"  reste à estimer, solde au trimestre : {reste_trim_pct_p50:.0%} médian, {reste_trim_pct_p90:.0%} au p90")

print("\n=== 10. Rapport (agrégats anonymes) → docs/spikes/nc/axe4-bench-ventilation.md ===")

RAPPORT = Path("docs/spikes/nc/axe4-bench-ventilation.md")


def bloc_tableau(resultats: dict) -> str:
    lignes = [
        "| Méthode | n | MAE (kWh) | biais (kWh) | \\|err\\| médian | p90 |",
        "|---|--:|--:|--:|--:|--:|",
    ]
    for nom, s in resultats.items():
        lignes.append(
            f"| {nom} | {s['n']} | {s['mae_kwh']:.1f} | {s['biais_kwh']:+.1f} | "
            f"{s['p50_kwh']:.2f} kWh ({s['p50_rel']:.1%}) | {s['p90_kwh']:.2f} kWh ({s['p90_rel']:.1%}) |"
        )
    return "\n".join(lignes)


m4_ligne = (
    f"- **Méthode 4 (profils officiels Enedis)** : benchée si `{COEFFICIENTS_ENEDIS_CSV.name}` est fourni "
    "(chargeur pluggable prêt). Extrait public non embarqué dans ce run — cf. §6 du script."
    if recon_enedis is None
    else f"- **Méthode 4 (profils officiels Enedis)** : benchée, voir tableaux (extrait `{COEFFICIENTS_ENEDIS_CSV.name}`)."
)

rapport_md = f"""# Axe 4 — bench de ventilation calendaire par oracle

> Généré par `docs/spikes/nc/axe4_bench_ventilation.py` — issue [#547](https://github.com/Energie-De-Nantes/electricore/issues/547), PRD [#542](https://github.com/Energie-De-Nantes/electricore/issues/542).
>
> Uniquement des agrégats anonymes — aucun PDL, RSC ni num_compteur. Le détail par PDL est une sortie locale non committée (`sorties-locales/axe4_bench_detail_mois.csv`).

## Principe de l'oracle

On ne peut pas juger la ventilation d'un non-communicant (NC) : pas de vérité terrain.
L'oracle emprunte des PDL **communicants** (niveau d'ouverture 2) qui ont **à la fois**
leurs fenêtres moisniversaire F15 (le « matériau NC ») **et** un mesuré quotidien
R64/R151 (la vérité, indépendante de la reconstruction). On les reconstruit « comme s'ils
étaient NC » depuis les seules fenêtres, on compare au mesuré. Aucune fenêtre synthétique.

**Sélection oracle (source de biais nommée)** : un PDL entre dans le bench s'il a ≥ {SEUIL_FENETRES}
fenêtres F15 cycliques *propres*. Gate qualité : tout PDL dont une fenêtre F15 diffère de
l'index brut à ses bornes de plus de max({int(TOLERANCE_ANOMALIE_KWH)} kWh, {int(TOLERANCE_ANOMALIE_RELATIVE * 100)} %) est **entièrement écarté**
(remplacement de compteur ou correction Enedis — confondre ça avec une erreur de
ventilation fausserait le bench). Population retenue : **{oracle_pdls.height} PDL oracle**,
**{comparables.height} mois** comparables (vérité + couverture NC complète).

## Résultats — erreur de reconstruction

### Grain mois calendaire (ADR-0026)

{bloc_tableau(resultats_mois)}

### Grain trimestre (accise)

{bloc_tableau(resultats_trimestre)}

> **Lire MAE vs médiane** : la MAE en kWh est tirée par une queue de gros consommateurs
> (C4 > 36 kVA) et par les résidus F15-vs-index que le gate ne retire pas tous ; la
> **médiane** et l'**erreur relative** (entre parenthèses) disent le signal pratique.

## Verdict — « le prorata ventile mal la saisonnalité »

**{verdict_prejuge}.**

- Mois : prorata |err| médian **{p_mois["p50_kwh"]:.2f} kWh** vs profil saisonnier **{s_mois["p50_kwh"]:.2f} kWh**.
- Trimestre : prorata **{p_trim["p50_kwh"]:.2f} kWh** vs profil **{s_trim["p50_kwh"]:.2f} kWh**.

Sur ce parc, le profil saisonnier du parc EDN (leave-one-out) **ne bat pas** le prorata :
la saisonnalité *intra-fenêtre* (fenêtres ~30 j) est trop courte pour que la modulation
mensuelle paie. Le prorata reste la baseline à battre.

## Praticabilité « sans-ventilation aux bornes réelles » (méthode 3)

- **Fréquence des relevés réels** qui borneraient un solde propre chez les NC : mesurée par
  l'**axe 2 #545** (rare, ~supra-annuel). Non recalculée ici — l'oracle est communicant
  (index R151/R64 quotidien), sa cadence ne renseigne pas les vrais NC.
- **Taille des fenêtres moisniversaire** (grain naturel du solde) : p50 **{fen_p50:.0f} j**, p90 **{fen_p90:.0f} j**.
- **Solde au mois civil** : impraticable — ces fenêtres (~30 j) ne tombent quasi jamais
  entièrement dans un mois civil, d'où **{reste_mois_pct_p50:.0%}** d'énergie non soldable sans ventiler une
  fenêtre à cheval.
- **Solde au trimestre** (grain accise) : **{reste_trim_pct_p50:.0%}** médian de l'énergie reste à estimer aux
  bords (p90 **{reste_trim_pct_p90:.0%}**) — le solde « propre » n'annule pas le besoin d'estimer les bouts.

{m4_ligne}

## Limites

- Oracle = communicants ; leur profil de conso peut différer des vrais NC (parc pré-Linky,
  cf. axe 1 #544). Biais nommé, non corrigé.
- MAE inflatée par les gros consommateurs et les résidus F15-vs-index ; médiane + relatif
  privilégiés pour le verdict.
- R67 absent (campagne #543) : le bench s'appuie sur F15 comme matériau moisniversaire.
"""

RAPPORT.write_text(rapport_md)
print(f"  écrit : {RAPPORT} ({len(rapport_md)} caractères)")

print("\n=== 11. Invariant RGPD : aucun PDL (14 chiffres) dans les fichiers committés ===")

MOTIF_PDL = re.compile(r"(?<!\d)\d{14}(?!\d)")
for fichier in (Path("docs/spikes/nc/axe4_bench_ventilation.py"), RAPPORT):
    fuites = MOTIF_PDL.findall(fichier.read_text())
    assert not fuites, f"FUITE RGPD : {len(fuites)} motif(s) de PDL dans {fichier} — {fuites[:3]}"
    print(f"  {fichier} : aucun motif de PDL (14 chiffres) — OK")

print("\n✅ Bench axe 4 terminé — invariants (conservation, comparabilité, RGPD) verts.")
