# Présence au périmètre : spans de contrat par RSC, fermeture sur le code de sortie

## Statut

Accepté — grill `/grill-with-docs` du 2026-07-01, **adossé à un spike sur le C15 réel**
(2204 événements, 1372 PDL, 1413 RSC ; repro : [`0052-spike-perimetre.py`](0052-spike-perimetre.py)). Premier consommateur :
la commande bot `/perimetre pdls` → CSV des PDL à déposer sur le portail SGE pour une
**demande M023** (collecte des relevés quotidiens → R64).

- **Prolonge** [ADR-0019](0019-roles-loaders-pipelines-builds-integrations.md) (pipeline pur :
  la date de référence est un **paramètre**, l'aujourd'hui est injecté au boundary — comme
  `affaires_ouvertes(maintenant=…)`), [ADR-0042](0042-convention-date-instant-jour-civil-boundary-flux.md)
  (bornes de période = **jour civil `DATE`**, pas des instants).
- **S'appuie sur** les constantes canoniques `ENTREES_C15` / `SORTIES_C15` du registre loader
  (`core/loaders/duckdb/registry.py`) et les vues `c15().entrees()` / `.sorties()`.
- **Respecte** [ADR-0016](0016-core-erp-agnostique.md) : périmètre = donnée réseau pure, aucune
  dépendance ERP.

## Contexte

Besoin récurrent — et jusqu'ici **sans primitive** : « quels PDL sont *présents dans le
périmètre* à une date ? ». Le premier appel est la demande M023 (liste des PDL du fournisseur
pour activer la collecte quotidienne R64), mais l'objet resservira (rapprochement, reporting,
« depuis quand », chevauchements).

Ce qui existe ne répond pas :

- `abonnements` construit des périodes bornées via `shift(-1)` sur la RSC — ce qui **jette la
  dernière période ouverte** ; les contrats vivants ne sont bornés que parce que la spine
  injecte une **grille FACTURATION mensuelle**. Inadapté à « qui est là aujourd'hui ».
- `c15().entrees()` / `.sorties()` donnent les **événements bruts**, pas l'appartenance courante.

Deux signaux candidats pour décider « présent ou sorti » : (1) les **codes** `evenement_declencheur`
d'entrée `{PMES, MES, CFNE}` / de sortie `{RES, CFNS}` ; (2) le **flag** `etat_contractuel`
(`EN SERVICE` / `RESILIE`). La décision entre les deux était un **inconnu empirique** → spike.

### Ce que le spike a prouvé (C15 réel)

| Sonde | Résultat |
|---|---|
| `etat_contractuel` | `EN SERVICE` 2015 · `RESILIE` 189 · **0 null** |
| Codes de sortie | `RES`→RESILIE 141 · `CFNS`→RESILIE 47 (100 % RESILIE) |
| **Accord flag ⟺ code au grain RSC** | **188 = 188, zéro désaccord** |
| Dernier evt / RSC : actif_flag vs actif_code | **0 désaccord** |
| Périmètre actif du jour (grain PDL) | flag **1216** = code **1216** |
| Après une sortie, dans la même RSC | **0 événement** — jamais rouverte |
| Ré-entrées | toujours une **nouvelle RSC** (0 entrée post-sortie) |
| Multi-RSC / PDL | 1335×1 · 34×2 · 2×3 · 1×4 |

Nuance isolée : un `MDPRM | RESILIE` **le même jour** que la `RES` (contrat PMES→RES en 3 jours,
emménagement avorté) — le flag peut *baver* sur une modif coïncidente, donc **les codes portent
la date de `fin`** (événement unique), pas le flag.

## Décision

1. **Objet = span de présence, un par RSC** : `[debut, fin)`. Grain **RSC**
   (`ref_situation_contractuelle`) ; la projection en **PDL distincts** est faite par le
   consommateur (un PDL multi-RSC — ré-entrée, changement de fournisseur — reste une ligne).
2. **`debut`** = jour du **1er événement** de la RSC (robuste à une entrée antérieure à notre
   fenêtre de données). **`fin`** = jour du **code sortie** `{RES, CFNS}`, ou `null` (encore présente).
3. Le **flag `etat_contractuel` n'est pas la source de la date** : il sert d'**invariant de
   cohérence** (« un `RESILIE` ⟺ une sortie au grain RSC » — vrai à 100 % au spike, à surveiller
   par un test data si Enedis diverge). ⚠️ `niveau_ouverture_services` est **autre chose** (niveau
   d'ouverture des services, routage `energie`) — **jamais** l'appartenance au périmètre.
4. **Bornes en jour civil `DATE`** (Europe/Paris, ADR-0042), intervalle **demi-ouvert** :
   `actif_à(D)` ⟺ `debut ≤ D` **et** (`fin` nulle **ou** `D < fin`). Résilié le J ⟹ **absent dès J**.
5. **Pureté** : la date de référence est un **paramètre explicite** ; l'aujourd'hui (impur) est
   injecté au boundary (endpoint API).

Implémentation : `core/pipelines/perimetre.py` — `presence_perimetre(c15) -> [rsc, pdl, debut, fin]`
et `pdls_actifs_a(c15, a_date) -> [pdl]`.

## Conséquences

- **Primitive réutilisable** : « actif à D » tombe en un `.filter(...).select("pdl").unique()` ;
  les spans resservent tels quels ailleurs.
- **Invariant à surveiller** : l'accord flag ⟺ code est un contrat de données (candidat à un test
  dbt sur `flux_c15` au grain RSC), pas juste une hypothèse.
- **Premier livrable** : `GET /perimetre/pdls.csv?jour=…` (défaut : aujourd'hui) → CSV une colonne
  `pdl`, exposé au bot par `/perimetre pdls`.

### Alternatives écartées

- **« Dernier événement du PDL ∉ {RES, CFNS} »** (impl initiale) : grain PDL, **sans date**, énumère
  les sorties, perd la notion de span. Correct sur l'instant présent, mais referme la porte.
- **Fermer `fin` sur la bascule du flag** : bave possible (`MDPRM|RESILIE` coïncident) ; les codes
  donnent un événement unique et net. Le flag reste, en garde-fou.
