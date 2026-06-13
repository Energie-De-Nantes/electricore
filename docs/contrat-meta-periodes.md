# Contrat — endpoint de lecture des méta-périodes

> Contrat d'intégration pour le consommateur (addon `souscriptions_odoo`).
> Décision et justifications : [ADR-0027](adr/0027-endpoint-lecture-meta-periodes-odoo-tire.md).
> Vocabulaire : [`electricore/core/CONTEXT.md`](../electricore/core/CONTEXT.md) (entrées *Méta-période mensuelle*, *Accise physique vs Accise de déclaration*).

`GET /facturation/meta-periodes` expose les **méta-périodes mensuelles** d'electricore :
des quantités physiques et des montants réseau **non valorisés aux prix fournisseur**.
Odoo **tire** ce flux et construit/upsert ses `souscription.periode`. electricore reste
**read-only** vis-à-vis d'Odoo ([ADR-0012](adr/0012-api-read-only-odoo.md)) et **n'écrit
jamais**.

## Requête

```
GET /facturation/meta-periodes?mois=YYYY-MM-DD&rsc=RSC-A&rsc=RSC-B&limit=500&offset=0
```

| Param | Défaut | Description |
|---|---|---|
| `mois` | dernier mois disponible | Mois cible, format `YYYY-MM-DD` (1er du mois). |
| `rsc` | — | Filtre, **répétable** : une ou plusieurs `ref_situation_contractuelle`. |
| `limit` | `500` (max `2000`) | Pagination — nombre de lignes. |
| `offset` | `0` | Pagination — lignes ignorées. |

**Auth :** header `X-API-Key` (obligatoire ; `401` sinon).

## Réponse (enveloppe JSON)

```json
{
  "mois": "2025-03-01",
  "contract_version": 1,
  "filters": { "rsc": ["RSC-A"] },
  "pagination": { "limit": 500, "offset": 0, "returned": 2, "total": 2 },
  "data": [
    {
      "ref_situation_contractuelle": "RSC-A",
      "pdl": "12345678901234",
      "mois_annee": "2025-03",
      "debut": "2025-03-01T00:00:00+01:00",
      "fin": "2025-04-01T00:00:00+02:00",
      "nb_jours": 31,
      "puissance_moyenne_kva": 6.0,
      "formule_tarifaire_acheminement": "BTINFCUST",
      "energie_base_kwh": null,
      "energie_hp_kwh": 312.4,
      "energie_hc_kwh": 145.2,
      "turpe_fixe_eur": 9.13,
      "turpe_variable_eur": 18.40,
      "cta_eur": 19.18,
      "taux_accise_eur_mwh": 33.7,
      "data_complete": true,
      "coverage_abo": 1.0,
      "coverage_energie": 1.0,
      "has_changement": false,
      "source_hash": "9f2b1c7d4e5a6b08"
    }
  ]
}
```

### Enveloppe

| Champ | Type | Rôle |
|---|---|---|
| `mois` | `str` `YYYY-MM-DD` | Mois effectivement résolu (utile quand la requête omet `mois`). |
| `contract_version` | `int` | Version du contrat. `1` aujourd'hui. À asserter côté consommateur. |
| `filters` | `obj \| null` | Écho des filtres appliqués (`rsc`). |
| `pagination` | `obj` | `limit`, `offset`, `returned` (lignes de cette page), `total` (lignes du mois). |
| `data` | `array` | Les méta-périodes. |

### Ligne `data` (schéma figé v1)

Grain : **une ligne par `(ref_situation_contractuelle, debut, fin)`** — c'est la clé
d'upsert recommandée côté Odoo. Pas par PDL : un PDL qui change de RSC en cours de mois
porte deux lignes.

| Champ | Type | Unité | Null ? | Rôle |
|---|---|---|---|---|
| `ref_situation_contractuelle` | `str` | — | non | **Clé d'articulation.** |
| `pdl` | `str` | — | non | Attribut d'affichage. |
| `mois_annee` | `str` `YYYY-MM` | — | non | Clé mensuelle calculable/triable. |
| `debut` | `str` ISO8601 (Europe/Paris) | — | non | Borne de période (clé d'upsert). |
| `fin` | `str` ISO8601 (Europe/Paris) | — | non | Borne de période (clé d'upsert). |
| `nb_jours` | `int` | jours | non | Quantité (part fixe). |
| `puissance_moyenne_kva` | `float` | kVA | non | Quantité (pondérée par les jours). |
| `formule_tarifaire_acheminement` | `str` | — | non | FTA — informatif (contrôle de cohérence). |
| `energie_base_kwh` | `float` | kWh | **oui** | Quantité énergie cadran Base (`null` si non applicable). |
| `energie_hp_kwh` | `float` | kWh | **oui** | Quantité énergie cadran HP. |
| `energie_hc_kwh` | `float` | kWh | **oui** | Quantité énergie cadran HC. |
| `turpe_fixe_eur` | `float` | € | non | **Montant final** (réseau). |
| `turpe_variable_eur` | `float` | € | non | **Montant final** (réseau). |
| `cta_eur` | `float` | € | non | **Montant final** (assiette = `turpe_fixe_eur`, possédée par electricore). |
| `taux_accise_eur_mwh` | `float` | €/MWh | non | **Taux** standard en vigueur. *Pas de montant* : Odoo calcule l'accise facturée. |
| `data_complete` | `bool` | — | non | `false` ⇒ période partielle/estimée. |
| `coverage_abo` | `float` | [0,1] | non | Couverture temporelle abonnement. |
| `coverage_energie` | `float` | [0,1] | non | Couverture temporelle énergie. |
| `has_changement` | `bool` | — | non | Changement (puissance/énergie) en cours de mois. |
| `source_hash` | `str` (hex) | — | non | Empreinte de contenu de la ligne (cf. *Upsert non destructif*). |

#### Pourquoi un taux pour l'accise mais des montants pour le reste

Règle (ADR-0027) : *electricore livre le **montant** € quand il possède l'assiette, le
**taux** quand l'ERP la possède.*
- TURPE, CTA → assiette réseau / `turpe_fixe` (electricore) → **montant €**.
- Accise → assiette = le **facturé** (= Σ provisions pour un lissé, quantité Odoo) → **taux seul**.
  Odoo calcule l'**accise facturée** en champ calculé : `taux_accise_eur_mwh × quantité facturée`.

Les **prix fournisseur** (énergie, abonnement) ne sont jamais dans le flux : Odoo applique
ses grilles datées.

## Invariants

- **Feed = source, pas synchro destructive.** L'upsert Odoo doit être *insert-or-update
  sur les brouillons uniquement, jamais `delete`*, et **ne jamais toucher** une
  `souscription.periode` validée/éditée à la main. electricore ne peut pas écraser de
  donnée Odoo (read-only) ; la garantie est portée par l'upsert.
- **Évolution additive.** De nouvelles colonnes optionnelles peuvent apparaître sans
  bump de `contract_version` — un lecteur tolérant (qui ignore les champs inconnus)
  survit. Renommage/suppression/changement de sémantique ⇒ nouvelle version (`/v2/…`).
- **Déterminisme.** À état DuckDB constant et version electricore constante, le payload
  et chaque `source_hash` sont identiques d'un appel à l'autre.
- **Bord — déplacement des bornes.** `debut`/`fin` font partie de la clé d'upsert, mais
  une **correction de date contractuelle** (entrée/sortie) les déplace : l'ancienne ligne
  devient orpheline. L'upsert Odoo doit réconcilier — p.ex. *delete-orphan* sur
  `(RSC, mois)`, ou clé `(RSC, mois_annee)` pour les périodes tronquées.

## Signalement partiel / estimé

Une `(RSC, mois)` incomplète arrive avec `data_complete = false` et `coverage_* < 1.0`.
Odoo construit alors la `souscription.periode` comme **brouillon à compléter** par un·e
facturiste (cf. ADR-0002 souscriptions_odoo : la Période est un brouillon éditable).

## Upsert non destructif (mécanisme `source_hash`)

`source_hash` est une empreinte de contenu de la ligne (sha256 tronqué sur **toutes** les
colonnes de quantités/montants/complétude). Il outille un upsert qui préserve le travail
humain :

1. Odoo stocke le `source_hash` reçu sur chaque `souscription.periode`.
2. Au re-pull : si le `source_hash` entrant **est identique** → la source n'a pas bougé →
   **ne rien faire** (les éditions manuelles sont préservées sans condition).
3. Si **différent** et la période est **brouillon** → rafraîchir.
4. Si **différent** et la période est **verrouillée/éditée** → **ne pas écraser**, mais
   signaler la dérive au facturiste (la donnée Enedis a bougé depuis l'édition).

## Accise — découvertes ouvertes (côté valorisation ERP)

La valorisation de l'accise *facturée* vit côté Odoo, mais dépend de deux points non
tranchés (suivis côté electricore) :

- **Assiette accise — comptage réglementaire exact** : [issue #225](https://github.com/Energie-De-Nantes/electricore/issues/225).
- **Catégories de taux accise** (le taux n'est pas uniforme — dérivable du flux Enedis ou
  saisi côté ERP ?) : [issue #226](https://github.com/Energie-De-Nantes/electricore/issues/226).
  Tant que non tranché, l'endpoint livre le **taux standard**.

## Hors périmètre v1

- **C4 / 4 cadrans réseau** : la méta-période v1 est C5 (`base`/`hp`/`hc`, une puissance).
  Le détail 4 cadrans + 4 puissances existe en amont et sera exposé en **colonnes
  additionnelles** (sans rupture) le jour où un C4 entre au périmètre.
- **`accise_eur`** : non exposé (cf. règle taux vs montant ci-dessus).
