"""Wire-up de l'endpoint prestations F15 : pull-tout des refacturables (souscriptions_odoo#37).

Filtre le flux F15 sur `unite = 'UNITE'` (prestations et indemnités ponctuelles —
même filtre que l'onglet « F15 prestations » du livrable XLSX, cf.
`contexte_mensuel.documents`) **sans fenêtre temporelle** : pull-tout-et-dédup,
les lignes F15 arrivent en retard datées dans le passé, un curseur de date les
manquerait (ADR 0009 côté souscriptions_odoo).

`reference` : le F15 n'a **aucun identifiant de ligne** (`id_ev` est un code
d'événement, ex. `DCOUP_PEN` ; le `Num_Sequence` d'`Element_Valorise` est
inutilisable — nul sur les pénalités, non-unique, non-stable) — la
`reference` est une **référence de contenu electricore** : un sha256 tronqué
du contenu canonique de la ligne (pas une référence Enedis). Deux lignes
strictement identiques d'une même facture fusionnent en une seule prestation
— forcé par le pull-tout-et-dédup, pas un choix libre. Contrat complet,
5 exclusions motivées et preuve de collision : `docs/contrat-prestations.md`.
"""

import hashlib

import polars as pl

from electricore.core.loaders import f15_detail

# Version du contrat exposée dans les en-têtes. Bump sur rupture (une évolution
# additive est tolérée par les clients, `extra="ignore"`).
CONTRAT_VERSION = 1

# Colonnes projetées du flux F15 — la `reference` s'y ajoute, calculée ici.
COLONNES_CONTRAT: tuple[str, ...] = (
    "pdl",
    "ref_situation_contractuelle",
    "id_ev",
    "nature_ev",
    "libelle_ev",
    "taux_tva_applicable",
    "prix_unitaire",
    "quantite",
    "montant_ht",
    "date_debut",
    "date_fin",
    "num_facture",
    "date_facture",
)

# Assiette de la clé de dédup (référence de contenu electricore) : l'identité
# d'une ligne par son contenu. 5 exclusions motivées (détail :
# docs/contrat-prestations.md) : `libelle_ev`/`taux_tva_applicable` (une
# retouche Enedis n'est pas une nouvelle prestation) ; `nature_ev`
# (fonctionnellement déterminé par `id_ev`) ;
# `ref_situation_contractuelle`/`date_facture` (métadonnées, pas de contenu).
_COLONNES_REFERENCE: tuple[str, ...] = (
    "num_facture",
    "pdl",
    "id_ev",
    "date_debut",
    "date_fin",
    "prix_unitaire",
    "quantite",
    "montant_ht",
)


def prestations(rsc: list[str] | None = None) -> pl.DataFrame:
    """Toutes les lignes F15 `unite='UNITE'`, projetées sur le contrat v1.

    Args:
        rsc: filtre optionnel sur une liste de `ref_situation_contractuelle`.

    Returns:
        DataFrame des prestations (colonnes du contrat + `reference`), dates
        des JOURS CIVILS rendus en ISO (`YYYY-MM-DD`).
    """
    lf = f15_detail().lazy().filter(pl.col("unite") == "UNITE")
    if rsc:
        lf = lf.filter(pl.col("ref_situation_contractuelle").is_in(rsc))
    df = lf.select(COLONNES_CONTRAT).collect()
    # Dates F15 = jours civils : ISO déterministe pour le payload ET l'assiette du hash.
    df = df.with_columns(pl.col("date_debut", "date_fin", "date_facture").cast(pl.Utf8))
    return _ajouter_reference(df)


def _ajouter_reference(df: pl.DataFrame) -> pl.DataFrame:
    """Ajoute `reference` : sha256 (tronqué à 16) du contenu canonique de la ligne.

    Canon construit en **Python pur** (`str()` par champ, séparateur `␟`, nuls
    rendus `∅`) plutôt que via `pl.concat_str(cast(Utf8))` : sortie déterministe,
    indépendante du formateur float de Polars (peut varier entre versions —
    cf. `docs/contrat-prestations.md`). Assiette de 8 colonnes : voir
    `_COLONNES_REFERENCE`.
    """
    colonnes = [df[c].to_list() for c in _COLONNES_REFERENCE]
    hashes = [
        hashlib.sha256("␟".join("∅" if v is None else str(v) for v in ligne).encode()).hexdigest()[:16]
        for ligne in zip(*colonnes, strict=True)
    ]
    return df.with_columns(pl.Series("reference", hashes, dtype=pl.Utf8))
