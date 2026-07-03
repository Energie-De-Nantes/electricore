"""Spike #544 (axe 1 du PRD #542 — observabilité des non-communicants) — inventaire du
parc NC : qui est au niveau d'ouverture de services 0, depuis quand (ancienneté), sur
quel palier technologique (Linky à collecte fermée/refusée vs matériel pré-Linky),
combien de bascules 0⇄2 et d'entrées/sorties du parc NC sur tout l'historique C15.

Repro :  uv run python docs/spikes/nc/axe1_inventaire.py
(lecture seule ; nécessite la base d'ingestion locale peuplée en C15, pattern ADR-0052).

Base : flux_c15 seul (niveau_ouverture_services est **toujours transmis** nativement,
cf. CONTEXT.md — pas besoin du forward-fill dbt de `spine_contrat` pour cet axe) +
présence au périmètre par RSC (ADR-0052, réutilise `core.pipelines.perimetre`).

RGPD : le script n'imprime que des agrégats — jamais de PDL/RSC/num_compteur en console
ni dans le rapport committé (`axe1-inventaire.md`). Le détail nominatif par PDL part en
sortie locale gitignorée (`sorties-locales/axe1_parc_nc.csv`). Une assertion finale
scanne le rapport committé à la recherche de motifs à 14 chiffres (PDL).
"""

import re
from datetime import date
from pathlib import Path

import duckdb
import polars as pl

from electricore.core.pipelines.perimetre import presence_perimetre

DB = "electricore/ingestion/flux_enedis_pipeline.duckdb"
CSV_DETAIL = Path("sorties-locales/axe1_parc_nc.csv")
RAPPORT_MD = Path("docs/spikes/nc/axe1-inventaire.md")

# Miroir de SORTIES_C15 (core/loaders/duckdb/registry.py) — un pipeline/spike n'importe
# pas le registre loader (même raisonnement que core/pipelines/perimetre.py).
_SORTIES_C15 = ("RES", "CFNS")

AUJOURDHUI = date.today()

con = duckdb.connect(DB, read_only=True)
con.execute("use flux_enedis")
c15 = con.sql("select * from flux_c15").pl()
con.close()

lignes_rapport: list[str] = []


def rapport(*lignes: str) -> None:
    """Imprime en console ET accumule pour le rapport .md committé (agrégats only)."""
    for ligne in lignes:
        print(ligne)
    lignes_rapport.extend(lignes)


# =============================================================================
# 0. Volumétrie (recoupement)
# =============================================================================
rapport("## 0. Volumétrie\n")
n_evt = c15.height
n_rsc = c15["ref_situation_contractuelle"].n_unique()
n_pdl = c15["pdl"].n_unique()
rapport(
    f"- lignes `flux_c15` : {n_evt}",
    f"- RSC distinctes : {n_rsc}",
    f"- PDL distincts : {n_pdl}",
)
assert c15["niveau_ouverture_services"].null_count() == 0, (
    "niveau_ouverture_services doit être toujours transmis (cf. CONTEXT.md) — sinon "
    "cet axe a besoin du forward-fill dbt de spine_contrat, pas juste du C15 brut."
)
assert c15.filter(pl.col("ref_situation_contractuelle").is_null()).height == 0, (
    "flux_c15 réel : toute ligne porte une RSC (sinon exclure comme presence_perimetre)"
)

# =============================================================================
# 1. Présence au périmètre (ADR-0052, réutilisée telle quelle) + situation courante
# =============================================================================
spans = presence_perimetre(c15)  # [ref_situation_contractuelle, pdl, debut, fin]
assert spans.height == n_rsc, "un span de présence par RSC"

presents_rsc = spans.filter(pl.col("fin").is_null())
sortis_rsc = spans.filter(pl.col("fin").is_not_null())
n_presents = presents_rsc.height
assert n_presents + sortis_rsc.height == n_rsc
assert presents_rsc["pdl"].n_unique() == n_presents, (
    "un PDL présent ne porte qu'une seule RSC présente à la fois (vérifié sur ce jeu réel)"
)

# Situation courante = dernier événement par RSC (niveau natif, jamais forward-fillé
# ici puisque jamais nul dans le C15 brut).
c15_trie = c15.sort(["ref_situation_contractuelle", "date_evenement"])
situation = c15_trie.filter(
    pl.col("date_evenement") == pl.col("date_evenement").max().over("ref_situation_contractuelle")
)
assert situation.height == n_rsc, "une seule situation courante par RSC (pas de doublon de date_evenement)"

situation_presente = situation.join(
    presents_rsc.select("ref_situation_contractuelle"), on="ref_situation_contractuelle", how="inner"
)
assert situation_presente.height == n_presents

rapport(
    "",
    "## 1. Présence au périmètre",
    "",
    f"- RSC présentes (ADR-0052) : {n_presents} / {n_rsc} (sorties : {n_rsc - n_presents})",
)

niveaux = situation_presente.group_by("niveau_ouverture_services").len().sort("niveau_ouverture_services")
assert niveaux["len"].sum() == n_presents
rapport("- Répartition par niveau d'ouverture des services (RSC présentes) :")
for niveau, n in niveaux.iter_rows():
    pct = 100 * n / n_presents
    rapport(f"    - niveau {niveau} : {n} ({pct:.1f} %)")

nc = situation_presente.filter(pl.col("niveau_ouverture_services") == "0")
n_nc = nc.height
rapport("", f"→ **NC actuel (présent + niveau 0) : {n_nc} PDL**")
assert n_nc == nc["pdl"].n_unique(), "grain PDL == grain RSC sur le sous-ensemble NC (cf. invariant ci-dessus)"

# =============================================================================
# 2. Palier technologique — type_compteur / categorie, NC vs parc présent
# =============================================================================
rapport("", "## 2. Palier technologique", "")


def _distribution(df: pl.DataFrame, colonne: str) -> list[tuple]:
    return df.group_by(colonne).len().sort("len", descending=True).rows()


rapport("- `type_compteur`, parc présent (tous niveaux) :")
for val, n in _distribution(situation_presente, "type_compteur"):
    rapport(f"    - {val!r} : {n}")

rapport("", "- `type_compteur`, NC (niveau 0) :")
for val, n in _distribution(nc, "type_compteur"):
    rapport(f"    - {val!r} : {n}")

rapport("", "- `categorie`, NC (niveau 0) :")
for val, n in _distribution(nc, "categorie"):
    rapport(f"    - {val!r} : {n}")

# Règle de classement empirique (pas de table de codes Enedis dans le repo — inférée par
# corrélation sur ce jeu réel, à confirmer côté métier) : le type_compteur communicant
# (Linky) domine le parc présent ; on vérifie s'il apparaît aussi côté NC (= Linky à
# collecte fermée/refusée, action commerciale possible) ou si le NC est exclusivement du
# matériel pré-Linky (électromécanique/électronique, pas d'action commerciale possible).
type_dominant_communicant = (
    situation_presente.filter(pl.col("niveau_ouverture_services") != "0")
    .group_by("type_compteur")
    .len()
    .sort("len", descending=True)
    .row(0)[0]
)
n_linky_nc = nc.filter(pl.col("type_compteur") == type_dominant_communicant).height
n_pre_linky_nc = n_nc - n_linky_nc
rapport(
    "",
    f"- Type de compteur dominant côté communicant (niveau ≥ 1) : `{type_dominant_communicant}` "
    "(hypothèse de travail : Linky).",
    f"- NC sur ce même type (**Linky à collecte fermée/refusée, action client envisageable**) : {n_linky_nc}",
    f"- NC sur un autre type (**matériel pré-Linky, ouverture non pertinente**) : {n_pre_linky_nc}",
)

# =============================================================================
# 3. Ancienneté au niveau 0
# =============================================================================
rapport("", "## 3. Ancienneté au niveau 0", "")

# Segments contigus à niveau constant par RSC (détection de rupture via shift+cum_sum,
# pattern déjà utilisé par core/pipelines/historique.py pour les ruptures d'abonnement).
c15_segments = c15_trie.with_columns(
    (pl.col("niveau_ouverture_services") != pl.col("niveau_ouverture_services").shift(1))
    .over("ref_situation_contractuelle")
    .fill_null(True)
    .cast(pl.Int32)
    .cum_sum()
    .over("ref_situation_contractuelle")
    .alias("_segment")
)
segments = c15_segments.group_by(["ref_situation_contractuelle", "_segment"], maintain_order=True).agg(
    pl.col("pdl").first(),
    pl.col("niveau_ouverture_services").first().alias("niveau"),
    pl.col("date_evenement").min().alias("debut_segment"),
)
assert segments.height >= n_rsc, "au moins un segment par RSC"

dernier_segment = segments.filter(pl.col("_segment") == pl.col("_segment").max().over("ref_situation_contractuelle"))
assert dernier_segment.height == n_rsc, "un seul segment courant par RSC"

nc_segment = dernier_segment.join(
    presents_rsc.select("ref_situation_contractuelle"), on="ref_situation_contractuelle", how="inner"
).filter(pl.col("niveau") == "0")
assert nc_segment.height == n_nc, (
    "recoupement : le segment niveau-0 courant des RSC présentes == NC courant "
    "(deux calculs indépendants — group_by dernier événement vs segments de rupture)"
)

# Champ natif Enedis de la date de bascule — vérifié vide à 100 % sur ce jeu (2026-07-03),
# gardé en repli explicite pour rester correct si Enedis se met à le renseigner.
natif = situation_presente.filter(pl.col("niveau_ouverture_services") == "0").select(
    "ref_situation_contractuelle", "date_changement_niveau_ouverture_services"
)
n_natif_rempli = natif["date_changement_niveau_ouverture_services"].drop_nulls().len()
rapport(
    f"- Champ natif `date_changement_niveau_ouverture_services` renseigné pour "
    f"{n_natif_rempli}/{n_nc} RSC NC actuelles "
    f"({'utilisé en priorité' if n_natif_rempli else 'vide à 100 % → repli sur l’historique C15'})."
)

nc_detail = (
    nc_segment.join(natif, on="ref_situation_contractuelle", how="left")
    .join(
        nc.select("ref_situation_contractuelle", "type_compteur", "categorie"),
        on="ref_situation_contractuelle",
        how="left",
    )
    .with_columns(
        pl.when(pl.col("date_changement_niveau_ouverture_services").is_not_null())
        .then(pl.col("date_changement_niveau_ouverture_services"))
        .otherwise(pl.col("debut_segment").dt.date())
        .alias("depuis_le"),
        pl.when(pl.col("date_changement_niveau_ouverture_services").is_not_null())
        .then(pl.lit("champ_natif"))
        .otherwise(pl.lit("historique_c15"))
        .alias("methode"),
    )
    .with_columns((pl.lit(AUJOURDHUI) - pl.col("depuis_le")).dt.total_days().alias("anciennete_jours"))
    .select(
        "ref_situation_contractuelle", "pdl", "type_compteur", "categorie", "methode", "depuis_le", "anciennete_jours"
    )
    .sort("anciennete_jours", descending=True)
)
assert nc_detail.height == n_nc
assert (nc_detail["anciennete_jours"] >= 0).all(), "aucune ancienneté négative (bascule dans le futur)"

CSV_DETAIL.parent.mkdir(parents=True, exist_ok=True)
nc_detail.write_csv(CSV_DETAIL)

bornes_jours = [90, 180, 365, 730]
labels_buckets = ["≤3 mois", "3-6 mois", "6-12 mois", "12-24 mois", ">24 mois"]
distribution = (
    nc_detail.with_columns(pl.col("anciennete_jours").cut(breaks=bornes_jours, labels=labels_buckets).alias("bucket"))
    .group_by("bucket")
    .len()
    .sort("bucket")
)
assert distribution["len"].sum() == n_nc
rapport("", f"- Détail par PDL exporté (sortie locale, non committée) : `{CSV_DETAIL}`", "")
rapport("- Distribution d'ancienneté au niveau 0 (RSC NC présentes) :")
for bucket, n in distribution.iter_rows():
    rapport(f"    - {bucket} : {n}")

# =============================================================================
# 4. Bascules de niveau sur tout l'historique
# =============================================================================
rapport("", "## 4. Bascules de niveau (tout l'historique C15)", "")

segments_ordonnes = segments.sort(["ref_situation_contractuelle", "_segment"])
transitions = segments_ordonnes.with_columns(
    pl.col("niveau").shift(1).over("ref_situation_contractuelle").alias("niveau_precedent")
).filter(pl.col("niveau_precedent").is_not_null())

bascules = transitions.group_by(["niveau_precedent", "niveau"]).len()
niveaux_connus = sorted(set(c15["niveau_ouverture_services"].unique().to_list()))
rapport("- Matrice des bascules observées (niveau précédent → nouveau niveau) :")
for depuis in niveaux_connus:
    for vers in niveaux_connus:
        if depuis == vers:
            continue
        n = bascules.filter((pl.col("niveau_precedent") == depuis) & (pl.col("niveau") == vers))["len"].sum()
        rapport(f"    - {depuis} → {vers} : {n}")

bascules_depuis_0 = bascules.filter(pl.col("niveau_precedent") == "0")["len"].sum() or 0
bascules_vers_0 = bascules.filter(pl.col("niveau") == "0")["len"].sum() or 0
rapport(
    "",
    f"- Total bascules **quittant** le niveau 0 (0→1 ou 0→2) : {bascules_depuis_0}",
    f"- Total bascules **revenant** au niveau 0 (1→0 ou 2→0) : {bascules_vers_0}",
)

# =============================================================================
# 5. Entrées/sorties du parc NC par année
# =============================================================================
rapport("", "## 5. Entrées / sorties du parc NC par année", "")

statut = (
    pl.when(pl.col("evenement_declencheur").is_in(_SORTIES_C15))
    .then(pl.lit("sorti"))
    .when(pl.col("niveau_ouverture_services") == "0")
    .then(pl.lit("nc"))
    .otherwise(pl.lit("communicant"))
)
c15_statut = c15_trie.with_columns(statut.alias("statut")).with_columns(
    pl.col("statut").shift(1).over("ref_situation_contractuelle").alias("statut_precedent")
)
assert c15_statut.filter(pl.col("statut_precedent") == "sorti").height == 0, (
    "invariant ADR-0052 : une RSC sortie (RES/CFNS) n'a plus jamais d'événement après"
)

entrees = c15_statut.filter(
    (pl.col("statut") == "nc") & (pl.col("statut_precedent").is_null() | (pl.col("statut_precedent") != "nc"))
)
sorties = c15_statut.filter((pl.col("statut_precedent") == "nc") & (pl.col("statut") != "nc"))

entrees_par_an = (
    entrees.with_columns(pl.col("date_evenement").dt.year().alias("annee")).group_by("annee").len().sort("annee")
)
sorties_par_an = (
    sorties.with_columns(pl.col("date_evenement").dt.year().alias("annee"))
    .group_by(["annee", "statut"])
    .len()
    .sort(["annee", "statut"])
)

rapport("- Entrées dans le parc NC (nouvelle RSC ou bascule vers niveau 0), par année :")
for annee, n in entrees_par_an.iter_rows():
    rapport(f"    - {annee} : {n}")

rapport("", "- Sorties du parc NC, par année et par cause :")
for annee, cause, n in sorties_par_an.iter_rows():
    libelle = "bascule vers un niveau supérieur" if cause == "communicant" else "sortie du périmètre (RES/CFNS)"
    rapport(f"    - {annee} ({libelle}) : {n}")

total_entrees = entrees.height
total_sorties = sorties.height
rapport(
    "",
    f"- Total entrées historique : {total_entrees} — total sorties historique : {total_sorties} "
    f"— solde : {total_entrees - total_sorties}",
)
assert total_entrees - total_sorties == n_nc, (
    "recoupement : solde (entrées - sorties) de la timeline == NC courant du snapshot "
    f"({total_entrees} - {total_sorties} != {n_nc})"
)

sorties_bascule = sorties.filter(pl.col("statut") == "communicant").height
assert sorties_bascule == bascules_depuis_0, (
    "recoupement : sorties du parc NC par bascule (axe 5) == bascules quittant le niveau 0 (axe 4) "
    f"({sorties_bascule} != {bascules_depuis_0})"
)

# =============================================================================
# 6. Auto-vérification RGPD du rapport committé
# =============================================================================
print("\n=== 6. Auto-vérification RGPD ===")

RAPPORT_MD.parent.mkdir(parents=True, exist_ok=True)
entete = (
    "# Axe 1 — inventaire du parc non-communicant\n\n"
    f"> Généré par `docs/spikes/nc/axe1_inventaire.py` le {AUJOURDHUI.isoformat()} "
    "— issue [#544](https://github.com/Energie-De-Nantes/electricore/issues/544), "
    "PRD [#542](https://github.com/Energie-De-Nantes/electricore/issues/542).\n"
    ">\n"
    "> Uniquement des agrégats anonymes — aucun PDL, RSC ni num_compteur. Le détail par "
    "PDL est une sortie locale non committée (`sorties-locales/axe1_parc_nc.csv`).\n"
)
synthese = [
    "## Synthèse",
    "",
    f"- **{n_nc} PDL** sont non-communicants (NC) aujourd'hui : présents au périmètre et au "
    f"niveau d'ouverture 0, sur {n_presents} RSC présentes ({100 * n_nc / n_presents:.1f} %).",
    f"- **Palier technologique** : {n_linky_nc}/{n_nc} sont sur le type de compteur communicant "
    f"dominant (`{type_dominant_communicant}`, Linky à collecte fermée/refusée — action client "
    f"envisageable) ; {n_pre_linky_nc}/{n_nc} sont sur un autre type (matériel pré-Linky).",
    f"- **Ancienneté** : le champ natif Enedis de bascule est vide sur {n_nc - n_natif_rempli}/{n_nc} "
    "RSC NC actuelles ; ancienneté dérivée de l'historique C15 pour celles-ci (détail : axe 3).",
    f"- **Bascules** : {bascules_depuis_0} bascules ont quitté le niveau 0 sur tout l'historique, "
    f"**{bascules_vers_0}** y sont revenues (aucune bascule 2→0 ni 1→0 observée à ce jour).",
    f"- **Mouvements du parc NC** : {total_entrees} entrées / {total_sorties} sorties historiques "
    f"(solde {total_entrees - total_sorties}, cohérent avec le NC actuel).",
    "",
]
RAPPORT_MD.write_text(entete + "\n" + "\n".join(synthese) + "\n" + "\n".join(lignes_rapport) + "\n", encoding="utf-8")

contenu_rapport = RAPPORT_MD.read_text(encoding="utf-8")
motif_pdl = re.compile(r"(?<!\d)\d{14}(?!\d)")
trouvailles = motif_pdl.findall(contenu_rapport)
assert not trouvailles, f"RGPD : motif(s) à 14 chiffres (PDL ?) trouvé(s) dans {RAPPORT_MD} : {trouvailles}"
print(f"OK — aucun motif à 14 chiffres dans {RAPPORT_MD} ({len(contenu_rapport)} caractères scannés)")
print(f"Rapport écrit : {RAPPORT_MD}")
print("\nTous les invariants passent.")
