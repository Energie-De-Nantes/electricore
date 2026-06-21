# Modèles mart exposés hors du namespace /flux/*

Le namespace `/flux/*` est réservé aux **flux Enedis bruts** (c15, r151, r15, r64,
f15). Les modèles *mart* dérivés — premier occupant : le modèle de relevés canonique
`releves` ([ADR-0029](0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md)),
union arbitrée C15/R64/R151 — sont exposés sous une route nommée par le domaine
(`/releves`), **pas** `/flux/releves` : un mart n'est pas un flux, et le glossaire
garde les deux distincts. L'endpoint est adossé au loader `releves()`, pas au
registre `FLUX_CONFIGS`, et offre la parité de formats des flux (JSON / `.arrow` /
`.xlsx` / `/info`) pour son consommateur direct, les notebooks distants
(`ElectricoreClient.releves()`).

## Options considérées

- **`/flux/releves`** — rejeté : range un modèle dérivé sous le namespace des flux
  bruts ; obligerait à élargir la définition glossaire de `/flux/*` et à étendre
  `make_query`/`FLUX_CONFIGS` (le loader `releves()` utilise un `base_sql` + cast
  custom, pas un schéma typé).
- **`/marts/*`** — rejeté : « mart » est du jargon entrepôt de données, pas du
  vocabulaire métier ; briserait le précédent `/flux` (nommé par concept métier, non
  par dossier dbt).
- **`/enriched/*`** — rejeté : collision avec *historique enrichi* (sortie du
  pipeline historique, `impacte_abonnement`…).

## Conséquences

Une catégorie partagée `/canonique/*` est différée (YAGNI : un seul mart exposé
aujourd'hui). Si un second mart est exposé, regrouper impliquera de déplacer
`/releves` — changement d'URL cassant (alias/redirect à prévoir pour les clients
existants).

## Amendement (#389, juin 2026)

Le registre `FLUX_CONFIGS` mentionné ci-dessus a été renommé `FLUX_DESCRIPTORS`, et
`FluxSchema`/`QueryConfig` fusionnés en un descripteur unique `FluxDescriptor` (#389). La
décision de cet ADR est inchangée : `releves()` reste adossé à un `base_sql` (passthrough
`SELECT *`, `transform=None`) plutôt qu'au registre des flux bruts, et l'endpoint `/releves`
reste hors du namespace `/flux/*`.
