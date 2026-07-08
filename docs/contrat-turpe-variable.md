---
fraicheur: 2026-07-08
---

# Contrat — calculateur TURPE variable

> Contrat d'intégration **figé** pour le consommateur (addon `souscriptions_odoo`).
> Décision et justifications : [ADR-0030](adr/0030-calculateur-turpe-variable-odoo-fournit-assiette.md).
> Vocabulaire : [`electricore/core/CONTEXT.md`](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/core/CONTEXT.md) (entrées *Cadran*, *FTA*, *TURPE*).

`POST /facturation/turpe-variable` est un **calculateur sans état** : l'appelant **fournit
l'assiette** (énergies par cadran + FTA + `debut`) et electricore renvoie le **montant**
TURPE variable €. C'est une **évaluation de fonction**, pas une synchro : electricore ne
*possède* pas l'assiette (Odoo en reste le système de référence), il l'**évalue** sur des
entrées prêtées le temps de la requête. electricore reste **read-only** vis-à-vis d'Odoo
([ADR-0012](adr/0012-api-read-only-odoo.md)).

Complément du feed `GET /facturation/meta-periodes` ([contrat](contrat-meta-periodes.md)) :
le GET renvoie le `turpe_variable_eur` calculé depuis l'énergie **electricore** (DuckDB) ;
le POST le recalcule depuis une énergie que **seul Odoo possède** (saisie manuelle,
cf. ADR-0005 souscriptions_odoo). Les deux renvoient un **montant**, consommés
identiquement côté Odoo — seule diffère la provenance de l'assiette.

## Requête

```
POST /facturation/turpe-variable
Content-Type: application/json
X-API-Key: <clé>
```

Corps = un **lot** de lignes (le cas mono-période est `n = 1`) :

```json
{
  "lignes": [
    {
      "id": "periode-42",
      "formule_tarifaire_acheminement": "BTINFCUST",
      "debut": "2025-09-01",
      "energie_base_kwh": 1000.0,
      "energie_hp_kwh": null,
      "energie_hc_kwh": null,
      "energie_hph_kwh": null,
      "energie_hpb_kwh": null,
      "energie_hch_kwh": null,
      "energie_hcb_kwh": null
    }
  ]
}
```

### Ligne (schéma figé v1)

| Champ | Type | Unité | Null ? | Rôle |
|---|---|---|---|---|
| `id` | `str` | — | non | **Identifiant opaque** fourni par l'appelant (ex. `periode_id` Odoo), **ré-émis tel quel**. electricore ne l'interprète jamais. |
| `formule_tarifaire_acheminement` | `str` | — | non | FTA, ex. `BTINFCUST`. Sélectionne la grille de coefficients. |
| `debut` | `str` ISO8601 | — | non | Début de période. Parsé en datetime tz `Europe/Paris` ; **sélection temporelle** de la règle (barème en vigueur à cette date). Une date nue (`YYYY-MM-DD`) vaut minuit Paris. |
| `energie_base_kwh` | `float` | kWh | **oui** | Cadran Base. `null` → `0`. |
| `energie_hp_kwh` | `float` | kWh | **oui** | Cadran HP. `null` → `0`. |
| `energie_hc_kwh` | `float` | kWh | **oui** | Cadran HC. `null` → `0`. |
| `energie_hph_kwh` | `float` | kWh | **oui** | Cadran HPH. `null` → `0`. |
| `energie_hpb_kwh` | `float` | kWh | **oui** | Cadran HPB. `null` → `0`. |
| `energie_hch_kwh` | `float` | kWh | **oui** | Cadran HCH. `null` → `0`. |
| `energie_hcb_kwh` | `float` | kWh | **oui** | Cadran HCB. `null` → `0`. |

**Auth :** header `X-API-Key` (obligatoire ; `401` sinon).

## Réponse (enveloppe JSON)

```json
{
  "contract_version": 1,
  "results": [
    { "id": "periode-42", "turpe_variable_eur": 12.50 }
  ]
}
```

| Champ | Type | Rôle |
|---|---|---|
| `contract_version` | `int` | Version du contrat. `1` aujourd'hui. À asserter côté consommateur. |
| `results` | `array` | Un résultat **par ligne envoyée** (même cardinalité que `lignes`). |

### Résultat (xor par `id`)

Chaque résultat porte l'`id` ré-émis et **soit** un montant **soit** une erreur, jamais
les deux :

| Champ | Type | Présent quand | Rôle |
|---|---|---|---|
| `id` | `str` | toujours | L'`id` de la ligne, tel quel. |
| `turpe_variable_eur` | `float` | succès | Montant €, arrondi à 2 décimales. |
| `error` | `str` | erreur | Motif lisible (voir *Succès partiel*). |

L'ordre des résultats suit l'ordre d'entrée, mais l'appariement **doit se faire par `id`**
(pas par position) — c'est le contrat columnar/indépendant de l'ordre.

## Pourquoi un montant (et pas un taux)

Règle d'[ADR-0027](adr/0027-endpoint-lecture-meta-periodes-odoo-tire.md) : *electricore
livre le **montant** € quand il possède l'assiette, le **taux** quand l'ERP la possède.*
Ici l'assiette **arrive dans la requête** — la crainte que protégeait la règle (devoir une
assiette qu'electricore ne voit pas) se dissout. On garde le montant pour (1) **une seule
implémentation de la formule** (electricore : conversion c€→€, sélection temporelle,
sommation des cadrans) et (2) la **symétrie avec le GET**, qui renvoie aussi un montant.
Le calcul : `turpe_variable_eur = Σ energie_cadran × c_cadran(FTA, debut) / 100`.

## Invariant FTA — « envoyer les 7 cadrans, les zéros arbitrent »

**Toujours envoyer les 7 cadrans** (les non-applicables à `null`). electricore **n'opère
aucun roll-up** : c'est la règle FTA qui arbitre la granularité, via ses **coefficients à
zéro**. Une FTA C5 communicante (`BTINF*` à 4 cadrans) tarife `hph/hpb/hch/hcb` ; ses
`c_base/c_hp/c_hc` sont nuls, donc une énergie envoyée sur `base`/`hp`/`hc` est multipliée
par 0 → **pas de double comptage**.

Cette correction **repose sur un invariant load-bearing** : *chaque FTA a des coefficients
`c_*` non-nuls à **exactement une** granularité* (`base`, `hp_hc`, ou `4_cadrans`). Si une
règle en portait deux, envoyer les 7 énergies **double-compterait en silence**. L'invariant
est **figé sous test** : [`tests/unit/test_turpe_rules_granularite.py`](https://github.com/Energie-De-Nantes/electricore/blob/main/tests/unit/test_turpe_rules_granularite.py)
(#252) casse, en nommant la FTA fautive, si une future ligne de `turpe_rules.csv` le viole.

## Succès partiel (un calculateur n'omet jamais une réponse demandée)

Une ligne en erreur **ne fait pas échouer le lot** : les lignes valides sont calculées
normalement, et chaque `id` revient avec son motif. Remplace **délibérément** le
*silent-drop* du pipeline feed (`valider_regles_presentes`), qui reste en place côté GET.

Motifs d'erreur :

| Cas | `error` (forme) |
|---|---|
| **FTA inconnue** (absente de `turpe_rules.csv`) | `FTA inconnue : <FTA>` |
| **Aucune règle pour la date** (`debut` hors de toute plage `start`/`end`) | `Aucune règle TURPE pour la FTA <FTA> à la date <YYYY-MM-DD>` |

## Exemple complet (nominal + erreur)

Requête (lot mixte) :

```json
{
  "lignes": [
    { "id": "ok-base",  "formule_tarifaire_acheminement": "BTINFCUST", "debut": "2025-09-01", "energie_base_kwh": 1000.0 },
    { "id": "ok-q4",    "formule_tarifaire_acheminement": "BTINFCU4",  "debut": "2025-09-01",
      "energie_hph_kwh": 100.0, "energie_hpb_kwh": 200.0, "energie_hch_kwh": 50.0, "energie_hcb_kwh": 80.0 },
    { "id": "ko-fta",   "formule_tarifaire_acheminement": "FTA_BIDON", "debut": "2025-09-01", "energie_base_kwh": 1000.0 },
    { "id": "ko-date",  "formule_tarifaire_acheminement": "BTINFCUST", "debut": "1990-01-01", "energie_base_kwh": 1000.0 }
  ]
}
```

Réponse (barème en vigueur au 2025-08-01) :

```json
{
  "contract_version": 1,
  "results": [
    { "id": "ok-base", "turpe_variable_eur": 48.40 },
    { "id": "ok-q4",   "turpe_variable_eur": 13.72 },
    { "id": "ko-fta",  "error": "FTA inconnue : FTA_BIDON" },
    { "id": "ko-date", "error": "Aucune règle TURPE pour la FTA BTINFCUST à la date 1990-01-01" }
  ]
}
```

Détail des montants : `ok-base` = 1000 × 4.84 / 100 ; `ok-q4` = (100×7.49 + 200×1.66 +
50×3.97 + 80×1.16) / 100.

## Invariants de contrat

- **Une réponse par entrée.** `len(results) == len(lignes)`, appariées par `id`.
- **Évolution additive.** De nouveaux champs optionnels (ex. TURPE fixe via
  `puissance_souscrite*`, dépassement C4 via `duree_depassement_h`) peuvent apparaître
  sans bump de `contract_version` — un lecteur tolérant survit. Renommage/suppression/
  changement de sémantique ⇒ nouvelle version.
- **Déterminisme.** À version electricore constante (donc `turpe_rules.csv` constant), une
  même requête renvoie un même résultat.

## Hors périmètre v1

- **TURPE fixe** : non calculé ici (ne dépend pas des cadrans, déjà correct via le GET).
  Trivialement ajoutable (`ajouter_turpe_fixe` + `puissance_souscrite`).
- **Composante de dépassement C4** (`duree_depassement_h`) : hors v1 (parc C5).
