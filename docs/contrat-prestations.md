# Contrat — prestations F15

> Contrat d'intégration **figé** pour le consommateur (addon `souscriptions_odoo`).
> Périmètre : `GET /facturation/prestations` (souscriptions_odoo#37 — upsert côté Odoo
> sur contrainte d'unicité de `reference`). Deux sessions de grill ont arbitré la clé de
> dédup ci-dessous ; ce document fige les décisions pour éviter un re-spike.

`GET /facturation/prestations` est un **pull-tout** des lignes F15 `unite = 'UNITE'`
(prestations et indemnités ponctuelles) : pas de fenêtre temporelle, pas de curseur — les
lignes F15 arrivent en retard datées dans le passé, un curseur de date les manquerait. Le
consommateur **dédup par `reference`** à chaque appel.

## `reference` : une référence de contenu electricore, pas Enedis

Le F15 **n'a aucun identifiant de ligne**. `id_ev` est un code d'événement (ex. `DCOUP_PEN`
pour une pénalité de coupure), pas une clé. `reference` est donc **fabriquée par
electricore** : un sha256 tronqué (16 caractères hex) du **contenu canonique** de la ligne.
Ce n'est **pas** une référence Enedis — le terme a été corrigé partout dans ce sens.

## Assiette (8 colonnes)

```
num_facture, pdl, id_ev, date_debut, date_fin, prix_unitaire, quantite, montant_ht
```

Les dates entrent dans le canon au format ISO jour civil (`YYYY-MM-DD`), déjà la
représentation du payload.

## 5 exclusions motivées

| Colonne exclue | Raison |
|---|---|
| `libelle_ev` | Une retouche de libellé Enedis (texte affiché) n'est pas une nouvelle prestation. |
| `taux_tva_applicable` | Même raison : une correction de taux affichée n'est pas un nouveau fait facturable. |
| `nature_ev` | Fonctionnellement **déterminé** par `id_ev` — vérifié empiriquement : 0 `id_ev` porte plusieurs `nature_ev` distincts dans les flux réels. L'inclure serait redondant, pas plus précis. |
| `ref_situation_contractuelle` | Métadonnée d'attachement, pas de contenu de la prestation. C'est justement la clé par laquelle l'addon Odoo rattache la ligne (résolution **par RSC seule**, arbitrage souscriptions_odoo#147), pas un attribut du fait facturable. |
| `date_facture` | Métadonnée de facturation, pas de contenu de la prestation — deux ré-émissions de la même ligne sur deux factures F15 différentes (retard de traitement Enedis) doivent fusionner. |

## Canonicalisation — Python pur, pas `pl.concat_str`

Le canon est construit **en Python pur** (`_ajouter_reference`,
`electricore/api/services/prestations_service.py`) : `str()` appliqué champ par champ, `None
→ '∅'`, séparateur `'␟'` (symbole de séparation d'unité, improbable dans les données
Enedis), puis `hashlib.sha256(...).hexdigest()[:16]`.

**Pourquoi pas `pl.concat_str([pl.col(c).cast(pl.Utf8) ...])`** : le formateur `Utf8` des
flottants de Polars **n'est pas garanti stable inter-versions** (arrondis, notation
scientifique...) — un canon qui en dépend peut changer de valeur au fil d'un simple bump de
dépendance, cassant silencieusement toutes les `reference` déjà upsertées côté Odoo. Le
canon Python (`str(float)`) est un contrat du langage, pas d'une lib tierce.

Un golden test (`tests/integration/test_prestations_service.py::
test_reference_golden_canon_python_pur`) fige une `reference` attendue en dur pour verrouiller
cette propriété.

## Fusion intra-facture : forcée par l'idempotence, pas un choix libre

Deux lignes **strictement identiques** sur les 8 colonnes de l'assiette fusionnent en une
seule prestation (même `reference`). Ce n'est **pas** un choix arbitraire de simplicité :
c'est la seule option compatible avec le **pull-tout-et-dédup**.

Le pull n'a pas de curseur temporel (raison ci-dessus) : chaque appel re-télécharge tout et
laisse le consommateur dédupliquer par `reference`. Si la clé incluait un **rang
d'occurrence** (ex. compteur "2ème ligne identique de cette facture"), l'ordre des lignes
dans la réponse deviendrait significatif — un re-pull qui les ré-ordonne (tri différent côté
DuckDB, ajout d'une colonne intermédiaire, etc.) réattribuerait les rangs, produirait des
`reference` **neuves** pour des lignes déjà upsertées, et **doublerait la facturation** côté
Odoo. Fusionner les doublons de contenu est donc le prix à payer pour que `reference` reste
stable face à un pull rejoué dans un ordre différent. Assumé et documenté ; aucun cas
observé dans les flux réels à ce jour.

## Preuve : `Num_Sequence` est inutilisable comme clé source

Un spike (deux sessions de grill) a exploré `Num_Sequence`, porté par `Element_Valorise`
dans le XML F15 source, comme clé de ligne native — **rejeté**, à ne pas re-spiker :

- **Nul sur toutes les pénalités** (`id_ev = DCOUP_PEN` et consorts) — la moitié du
  périmètre `unite='UNITE'` en serait privée.
- **Non-unique par facture** : c'est un **compteur local** (observé 1–4) qui se répète à
  l'intérieur d'une même facture (ex. factures `2143`, `2199` de l'échantillon d'audit).
- **Non-stable** : rien ne garantit qu'un re-traitement Enedis réémette le même
  `Num_Sequence` pour la même ligne de contenu.

`flux_f15_detail` (dbt) le jette déjà à l'ingestion — il n'est pas exposé par le contrat. Le
hash de contenu est donc **réellement forcé**, pas une préférence de conception.

## Limite connue : collision même-PDL/même-jour

Perte silencieuse d'une ligne (fusion à tort) **uniquement** si deux prestations
distinctes partagent : même `pdl`, même `date_debut`/`date_fin`, même `montant_ht`
(et par construction même `prix_unitaire`/`quantite`/`num_facture`/`id_ev`).

**Observé : 0 collision / 621 lignes `UNITE`** dans l'échantillon d'audit (dont 78
pénalités). Le `pdl` dans la clé borne le risque : deux pénalités le même jour sur deux PDLs
différents ne collisionnent **pas** — exemple réel, facture `1305456GF0327` : une coupure
16-17 juin facturée sur deux PDLs produit 4 pénalités de −12 € chacune, toutes distinctes
(clés différentes par `pdl`). Enedis facture au plus **une ligne par jour et par PDL** pour
un même type d'événement, ce qui explique le 0 observé.

## Versionnement

`CONTRAT_VERSION = 1` (`electricore/api/services/prestations_service.py`), transmis en
en-tête `X-Contract-Version`. Une évolution **additive** (nouvelle colonne) ne bump pas le
contrat — le modèle client `PrestationF15` est `extra="ignore"`. Un changement de l'assiette
de `reference`, un renommage ou un retrait de colonne, **bump** le contrat.
