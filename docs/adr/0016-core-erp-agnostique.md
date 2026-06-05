# `core/` strictement ERP-agnostique

## Contexte

Le sous-package [electricore/core/](../../electricore/core/) a été pensé dès le départ comme couche métier portant les calculs énergie purs (périmètre, abonnements, énergie, TURPE, accise, facturation Enedis). En pratique, cette intention a glissé : [electricore/core/loaders/odoo/](../../electricore/core/loaders/odoo/) et [electricore/core/writers/odoo.py](../../electricore/core/writers/odoo.py) ont été introduits parce qu'au démarrage seule l'instance EDN existait, et Odoo était le seul ERP en jeu. Aucun ADR n'avait inscrit la règle, donc le code l'a discrètement érodée.

Le coût de cette dérive devient lisible maintenant que l'instance Enargia (cf. [ADR-0015](0015-deploiement-multi-instance.md)) consomme `core/` depuis des notebooks hébergés hors de ce dépôt pour ses propres calculs métier — sans Odoo direct. Côté EDN, les orchestrations qui rapprochent Enedis et Odoo (cf. [ADR-0014](0014-lignes-factures-du-mois-avec-flags.md), [ADR-0013](0013-renommage-perimetre-historique.md)) sont par nature instance-spécifiques mais elles vivent aujourd'hui à côté des pipelines purs, ce qui brouille le contrat.

## Décision

`core/` ne dépend que de **Polars, DuckDB, Pandera, et de la stdlib Python**. Aucune dépendance d'intégration ERP (Odoo, Haulogy, autre) ne traverse son interface ni ses imports.

Concrètement :

- **Nouveau sous-package [electricore/integrations/](../../electricore/integrations/)** qui accueille les adaptateurs vers les ERP et systèmes externes. Premier occupant : `integrations/odoo/`.
- `core/loaders/odoo/` → `integrations/odoo/loaders/` (les query builders `OdooReader`, `OdooQuery` et leurs helpers).
- `core/writers/odoo.py` → `integrations/odoo/writers.py`.
- Les orchestrations qui composent du métier core et de l'ERP (par ex. `facturation_du_mois`, `accise_du_trimestre`, le rapprochement `rapprocher_facturation_mensuelle`) vivent dans `integrations/odoo/` — elles sont EDN-shaped aujourd'hui et serviront de prototype pour un futur module Odoo libre.
- Les modèles Pandera qui décrivent des objets Odoo (`core/models/odoo/`) suivent dans `integrations/odoo/models/`.
- **Test d'imports en CI** : `tests/architecture/test_core_purity.py` parse tous les fichiers `electricore/core/**/*.py` et échoue si l'un d'eux importe `electricore.integrations`, `electricore.api`, `electricore.bot`, ou une lib ERP (`odoorpc`, etc.). C'est la garantie exécutable que le contrat ne se ré-érode pas silencieusement.

`api/services/` se réduit au binding HTTP + à la sérialisation (XLSX/Arrow/ZIP, factorisée). Les services importent désormais à la fois `core/` et `integrations/odoo/`, et c'est attendu — l'API est le hub (cf. [ADR-0009](0009-architecture-api-centrique.md)).

## Raison

Trois facteurs cumulés :

1. **Le seam est *earning its keep* aujourd'hui.** Enargia est un consommateur réel de `core/` qui n'a pas Odoo dans la boucle. Garder Odoo dans `core/` rend le module impossible à utiliser pour ce cas — ce n'est plus une pureté esthétique, c'est un blocage.
2. **L'horizon ERP libre.** Le projet vise à émerger comme support des fournisseurs alternatifs/anticapitalistes, avec un futur module Odoo couplé à ElectriCore. La règle « `core/` = énergie pure, `integrations/<erp>/` = colle métier » trace dès maintenant l'arborescence dans laquelle ce module pourra atterrir sans refactor.
3. **Le test d'imports rend la règle exécutable.** Une convention écrite seule dans CONTEXT.md se contourne sous pression. Un test CI qui échoue est lisible par tout futur contributeur — humain ou agent.

Alternatives écartées :

- **Acter qu'Odoo est in-core, écrire l'ADR « `core/` est Odoo-aware ».** Rapide, mais ferme la porte à Enargia et à l'évolution multi-instance. Le coût de la migration aujourd'hui (~30 fichiers, imports à mettre à jour) est inférieur au coût futur de refactor sous contrainte de production.
- **Introduire un seam ERP générique (`ERPAdapter` interface, Odoo et Haulogy comme adaptateurs).** Hypothétique aujourd'hui : Haulogy est lui-même basé sur Odoo et le second consommateur ERP n'est pas en prod ici. *Un adapter = seam hypothétique* — on attend qu'il devienne réel pour le nommer.

## Conséquences

- **ADRs à mettre à jour** :
  - [ADR-0007 (query builders)](0007-query-builders.md) — `OdooQuery` n'est plus en `core/` mais en `integrations/odoo/`. Reformuler la note d'uniformité d'API (un caller à cheval `core`/`integrations` reste légitime via l'API ou via les services).
  - [ADR-0009 (API-centrique)](0009-architecture-api-centrique.md) — la phrase « DuckDB, les pipelines Polars et `OdooReader/Writer` sont des composants internes consommés via l'API » devient « DuckDB et les pipelines Polars (core) + les adaptateurs (integrations) sont consommés via l'API ».
  - [ADR-0012 (API read-only Odoo)](0012-api-read-only-odoo.md) — chemins d'import à mettre à jour, principe inchangé.
  - [ADR-0013 (renommage Périmètre → Historique)](0013-renommage-perimetre-historique.md) — pas d'impact structurel, mention résiduelle d'Odoo à reformuler si présente.

- **CONTEXT-MAP.md** : ajouter `electricore/integrations/` comme contexte au même titre que `core`, `etl`, `api`, `bot`. Glossaire dédié `integrations/odoo/CONTEXT.md` accueillant les termes Odoo-spécifiques (sale.order, account.move, x_invoicing_state, RSC côté écriture). Le glossaire `core/CONTEXT.md` garde les concepts métier neutres (Historique, Périmètre, Abonnement, RSC en tant que concept Enedis) ; les sections « Intégration Odoo » et « Rapprochement PDL ↔ RSC » migrent vers `integrations/odoo/CONTEXT.md`.

- **Documentation** : README, CLAUDE.md, `docs/odoo-query-builder.md` — mise à jour des imports et de l'arborescence.

- **Notebooks** : les imports `from electricore.core.loaders import OdooReader, OdooQuery, ...` deviennent `from electricore.integrations.odoo import OdooReader, OdooQuery, ...`. Aucun changement de signature.

## Limites à connaître avant un changement d'échelle

- **Pas de seam ERP-générique aujourd'hui.** Si une seconde intégration ERP (Haulogy ou autre) entre en prod, on aura *deux adaptateurs réels* — c'est alors le moment de nommer le seam (interface `ERPAdapter` ou équivalent). Tant qu'on en a un seul, la duplication éventuelle entre `integrations/odoo/` et un futur `integrations/<x>/` reste préférable à une abstraction prématurée.
- **Le bot Telegram reste Odoo-couplé.** Plusieurs commandes du bot (`/check odoo`, exports facturation/taxes) supposent un ERP Odoo configuré. Avant de poser un seam ERP, étape intermédiaire à prévoir : rendre le bot fonctionnel sur une instance sans ERP connecté, au minimum pour les commandes ETL et exports flux Enedis. Issue à ouvrir après les PR de migration.
- **La règle ne couvre pas `etl/`.** Le sous-package ETL ingère les flux Enedis (XML/CSV via SFTP, déchiffrement AES) et reste *non-Enedis*-agnostique — c'est sa raison d'être. Le contrat « pas d'ERP » s'applique uniquement à `core/`.
