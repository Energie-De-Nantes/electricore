# API electricore read-only sur Odoo

L'API electricore ne contient **aucun endpoint d'écriture vers Odoo**. Les calculs métier (rapprochement facturation, accise, CTA) sont exposés en lecture seule via des endpoints structurés (Arrow IPC) ; toute modification dans Odoo — création/mise à jour de `sale.order`, lignes de facture, attribution de RSC — est appliquée depuis des notebooks par un opérateur humain.

## Pourquoi

La facturation et l'attribution des RSC ont un fort impact métier (factures clients, comptabilité) : une erreur silencieuse coûte cher. La séparation lecture/écriture impose une validation humaine *à vue* avant propagation, et garantit qu'aucun service automatique (cron, bot Telegram, webhook) ne pousse de modification sans contrôle.

## Conséquences — règle pour les notebooks

- **OdooReader / OdooWriter en notebook** :
  - ✅ Autorisé pour **enrichissement ad-hoc** (croisements de données, exploration, consultations supplémentaires).
  - ✅ Autorisé pour les **écritures validées par l'opérateur**.
  - ❌ Interdit **en amont d'une pipeline de facturation** : les inputs sont chargés côté serveur, le notebook consomme le résultat structuré via `ElectricoreClient`.

- **Accès DuckDB direct en notebook** : jamais (cf. [ADR-0011](0011-deploiement-vps-docker.md), la base vit sur le VPS). Tout passe par l'API.

## Limites à connaître avant un changement d'échelle

Si le besoin d'automatisation pousse un jour à exposer une écriture (cas typique : workflow validé end-to-end, pas de jugement humain requis pour cette opération), la décision devra être prise pipeline par pipeline et documentée — pas de bascule globale du principe read-only.
