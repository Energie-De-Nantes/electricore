# Contrat — résolution RSC

> Contrat d'intégration **figé** pour le consommateur (addon `souscriptions_odoo`).
> Périmètre : [issue #282](https://github.com/Energie-De-Nantes/electricore/issues/282)
> (endpoint #3), réconciliation par `id_Affaire` (souscriptions_odoo ADR-0010, issue #5).
> Vocabulaire : [`electricore/core/CONTEXT.md`](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/core/CONTEXT.md) (entrées
> *Affaire*, *Id_Affaire*, *Ref_Situation_Contractuelle*).

`POST /facturation/rsc` est une **résolution sans état** : l'appelant prête un lot
d'`id_Affaire` et electricore renvoie le `ref_situation_contractuelle` (RSC) correspondant.
C'est une **évaluation de fonction**, pas une synchro : electricore ne *possède* pas la
correspondance, il la **recoupe** sur des flux Enedis le temps de la requête. electricore
reste **read-only** vis-à-vis d'Odoo ([ADR-0012](adr/0012-api-read-only-odoo.md)).

## Mécanisme — X12 ⨝ C15

L'`id_Affaire` (l'identifiant d'affaire SGE que connaît Odoo) est **le même identifiant**
que l'`Id_Affaire` porté par l'événement déclencheur d'un flux **C15** (cf.
`core/CONTEXT.md`). La résolution est donc un **match exact** :

- **C15** (`flux_c15`) porte sur le même événement l'`id_affaire` **et** le
  `ref_situation_contractuelle` → on lit la RSC directement sur l'événement dont
  `id_affaire` matche (valeur **native**, pas la valeur forward-fillée du mart
  `spine_contrat`).
- **X12** (`flux_affaires`) sert au **recoupement d'existence** : il distingue une affaire
  *connue mais sans situation contractuelle* (précurseur en cours, ou affaire non
  contractuelle type AME) d'une affaire *inconnue*.

Aucune heuristique temporelle (le notebook `injection_rsc` n'asof-joint par PDL + date que
parce qu'un `sale.order` Odoo n'a, lui, pas d'`Id_Affaire`).

## Requête

```
POST /facturation/rsc
Content-Type: application/json
X-API-Key: <clé>
```

Corps = un **lot** d'`id_Affaire` (le cas mono est `n = 1`) :

```json
{ "ids": ["38233180", "38233181"] }
```

| Champ | Type | Null ? | Rôle |
|---|---|---|---|
| `ids` | `array[str]` | non | Lot d'`id_Affaire` (Id_Affaire Enedis), **ré-émis tels quels**. electricore ne les interprète jamais. Lot vide ⇒ `results: []`. |

**Auth :** header `X-API-Key` (obligatoire ; `401` sinon).

## Réponse (enveloppe JSON)

```json
{
  "contract_version": 1,
  "results": [
    { "id_affaire": "38233180", "ref_situation_contractuelle": "248912973" }
  ]
}
```

| Champ | Type | Rôle |
|---|---|---|
| `contract_version` | `int` | Version du contrat. `1` aujourd'hui (aussi dans l'en-tête `X-Contract-Version`). À asserter côté consommateur. |
| `results` | `array` | Un résultat **par `id_affaire` envoyé** (même cardinalité que `ids`). |

### Résultat (xor par `id_affaire`)

Chaque résultat porte l'`id_affaire` ré-émis et **soit** une RSC **soit** une erreur, jamais
les deux :

| Champ | Type | Présent quand | Rôle |
|---|---|---|---|
| `id_affaire` | `str` | toujours | L'`id_Affaire` de l'entrée, tel quel. |
| `ref_situation_contractuelle` | `str` | succès | La RSC résolue. |
| `error` | `str` | erreur | Motif lisible (voir *Succès partiel*). |

L'ordre des résultats suit l'ordre d'entrée, mais l'appariement **doit se faire par
`id_affaire`** (pas par position) — contrat indépendant de l'ordre.

## Succès partiel (une résolution n'omet jamais une réponse demandée)

Un `id_Affaire` non résolu **ne fait pas échouer le lot** : les autres sont résolus
normalement, et chaque `id_affaire` revient avec son motif — jamais de silent-drop.

Motifs d'erreur :

| Cas | `error` (forme) |
|---|---|
| **Affaire inconnue** (absente de X12 *et* de C15) | `Affaire inconnue : <id>` |
| **Connue sans RSC** (présente en X12, aucun événement contractuel C15) | `Affaire connue (X12) sans situation contractuelle C15 (précurseur en cours ou affaire non contractuelle).` |
| **Résolution ambiguë** (plusieurs RSC distinctes sur cet `id_Affaire`) | `Résolution ambiguë : N situations contractuelles pour l'affaire <id> (<rsc…>).` |

Une affaire *connue sans RSC* qui **acquiert** une RSC plus tard (le C15 arrive) relève de
la **régularisation** côté Odoo ([#191](https://github.com/Energie-De-Nantes/electricore/issues/191)) :
re-résoudre suffit, electricore reste sans état.

## Exemple complet (nominal + erreurs)

Requête (lot mixte) :

```json
{ "ids": ["38233180", "AME001", "ZZZ999"] }
```

Réponse :

```json
{
  "contract_version": 1,
  "results": [
    { "id_affaire": "38233180", "ref_situation_contractuelle": "248912973" },
    { "id_affaire": "AME001",  "error": "Affaire connue (X12) sans situation contractuelle C15 (précurseur en cours ou affaire non contractuelle)." },
    { "id_affaire": "ZZZ999",  "error": "Affaire inconnue : ZZZ999" }
  ]
}
```

## Invariants de contrat

- **Une réponse par entrée.** `len(results) == len(ids)`, appariées par `id_affaire`.
- **Évolution additive.** De nouveaux champs optionnels (ex. `pdl`, `statut`) peuvent
  apparaître sans bump de `contract_version` — un lecteur tolérant survit.
  Renommage/suppression/changement de sémantique ⇒ nouvelle version.
- **Déterminisme.** À données Enedis constantes (C15 + X12), une même requête renvoie un
  même résultat.

## Hors périmètre v1

- **Résolution par PDL + date** (asof) : non exposée — réservée aux cas sans `Id_Affaire`
  (cf. notebook `injection_rsc` pour les `sale.order` legacy).
- **Salvage des affaires sans RSC** via R15 : différé
  ([#322](https://github.com/Energie-De-Nantes/electricore/issues/322)).
