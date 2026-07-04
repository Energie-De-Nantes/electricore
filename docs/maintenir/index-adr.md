---
fraicheur: 2026-07-04
---

# Index thématique des ADRs

Le [registre des décisions d'architecture](../adr/0001-monorepo.md) (`docs/adr/`) est
**immuable** : une décision ne se corrige ni ne se supprime, elle se remplace par un
nouvel ADR qui la **supersède** explicitement (voir par exemple
[ADR-0037](../adr/0037-trousseau-cles-aes-n-cles-selection-par-essai.md), qui supersède
[ADR-0008](../adr/0008-rotation-cles-aes.md)). Cette page ne réécrit aucun ADR — elle les
regroupe par thème pour retrouver une décision sans grep. Un ADR peut apparaître dans
deux thèmes quand la décision touche vraiment aux deux.

## Architecture & rôles des modules

- [ADR-0001 — Monorepo](../adr/0001-monorepo.md) — un seul dépôt pour ingestion/core/api/bot/integrations, pas de paquets séparés.
- [ADR-0002 — Polars exclusivement](../adr/0002-polars-uniquement.md) — pas de pandas ; Polars pur sur toute la couche calcul.
- [ADR-0004 — Langue française](../adr/0004-langue-francaise.md) — code, documentation et domaine métier en français.
- [ADR-0005 — DuckDB comme stockage](../adr/0005-duckdb-stockage.md) — DuckDB comme couche de stockage analytique entre ingestion et pipelines.
- [ADR-0007 — Query builders](../adr/0007-query-builders.md) — `DuckDBQuery`/`OdooQuery` : builders fluents, immuables, filtres poussés au `WHERE`.
- [ADR-0009 — Architecture API-centrique](../adr/0009-architecture-api-centrique.md) — l'API FastAPI est le hub ; bot, notebooks et intégrations passent tous par elle.
- [ADR-0016 — `core/` ERP-agnostique](../adr/0016-core-erp-agnostique.md) — `core/` ne dépend que de Polars/DuckDB/Pandera/stdlib ; tout adaptateur ERP vit dans `integrations/`.
- [ADR-0018 — Classes justifiées par l'état](../adr/0018-classes-justifiees-par-l-etat.md) — une classe se justifie par un état interne réel ; sinon fonctions + dataclasses frozen.
- [ADR-0019 — Rôles des dossiers](../adr/0019-roles-loaders-pipelines-builds-integrations.md) — un rôle par dossier (loaders/pipelines/builds/writers/integrations/api), imports par rôle vérifiés en CI.
- [ADR-0032 — Marts hors namespace `/flux`](../adr/0032-modeles-marts-hors-flux-namespace.md) — les marts transverses (relevés, spine…) sortent du namespace `/flux/*`.
- [ADR-0043 — `electricore-client` séparé](../adr/0043-electricore-client-paquet-separe.md) — client léger httpx+pydantic distribué à part (PyPI), JSONL streamé, modèles single-source.
- [ADR-0050 — Deux entrées du contexte mensuel](../adr/0050-entrees-contexte-mensuel-parc-et-point-distinctes.md) — parc complet et point unique restent des entrées distinctes de `contexte_du_mois`.

## Ingestion & flux Enedis

- [ADR-0006 — DLT pour l'ETL](../adr/0006-dlt-etl.md) — dlt orchestre l'ELT SFTP → DuckDB (landing brut).
- [ADR-0008 — Rotation des clés AES (stopgap)](../adr/0008-rotation-cles-aes.md) — format `secrets.toml` à deux clés, **superseded par ADR-0037**.
- [ADR-0020 — Linéarisation en dbt (prototype)](../adr/0020-linearisation-en-dbt.md) — prototype de linéarisation SQL des flux Enedis en dbt.
- [ADR-0021 — Bascule production dbt](../adr/0021-bascule-production-dbt.md) — dbt devient le chemin de production de la linéarisation.
- [ADR-0035 — Typage de la chaîne ingestion↔cœur](../adr/0035-typage-chaine-ingestion-coeur-proprietaire-par-fait.md) — un propriétaire par fait, parité vérifiée aux frontières.
- [ADR-0037 — Trousseau de clés AES N-clés](../adr/0037-trousseau-cles-aes-n-cles-selection-par-essai.md) — sélection par essai (oracle PKCS7/ZIP), escalade d'échec per-flux.
- [ADR-0040 — Schéma de déchiffrement AES à IV préfixé](../adr/0040-schema-dechiffrement-aes-iv-prefixe.md) — réalise le seam ouvert par l'ADR-0037 (AES-256).
- [ADR-0047 — Flux R67](../adr/0047-flux-r67-energie-par-periode-distributeur-hors-releves.md) — énergie par période livrée par le distributeur, asset parallèle hors union `releves`.
- [ADR-0051 — Flux C12](../adr/0051-flux-c12-spine-c4.md) — ingestion du flux C12 (PRM > 36 kVA) + spine contractuelle C4.
- [ADR-0053 — Parité des sites de déclaration d'un flux connu](../adr/0053-parite-sites-declaration-flux-connu.md) — vérifie que les quatre sites où se déclare un flux restent en phase.

## Données & conventions

- [ADR-0003 — Harmonisation des dates R151](../adr/0003-r151-date-harmonisation.md) — ajustement +1 jour du R151 pour s'harmoniser avec R64/R15/C15.
- [ADR-0013 — Renommage Périmètre → Historique](../adr/0013-renommage-perimetre-historique.md) — le vocabulaire du cœur adopte « Historique ».
- [ADR-0023 — Périodisations séparées](../adr/0023-periodisations-separees-abonnement-energie.md) — abonnement et énergie restent deux frises séparées, jamais imbriquées.
- [ADR-0026 — Facturation calendaire](../adr/0026-facturation-calendaire-pas-moisniversaire.md) — facturation du 1ᵉʳ au 1ᵉʳ, pas au « moisniversaire » Enedis.
- [ADR-0028 — Identité de relevé](../adr/0028-identite-releve-cle-metier-priorite-sources.md) — clé métier stable + priorité explicite des sources (C15 > R64 > R151).
- [ADR-0029 — Modèle de relevés canonique](../adr/0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md) — le mart `releves` assemble en dbt, le cœur se contente d'arbitrer/filtrer.
- [ADR-0033 — Qualité de période](../adr/0033-qualite-periode-remplace-data-complete-coverage.md) — remplace `data_complete`/`coverage` par un axe réelle/estimée/incalculable.
- [ADR-0034 — Index en kWh entiers](../adr/0034-index-kwh-entiers-floor-au-boundary-dbt.md) — normalisation floor au boundary dbt.
- [ADR-0036 — Statut de communication](../adr/0036-statut-communication-routage-energie-grain-meta.md) — axe jumeau de la qualité, porté par `Niveau_Ouverture_Services`.
- [ADR-0038 — Relevés utilisés imbriqués](../adr/0038-releves-utilises-imbriques-meta-periodes.md) — trace d'index légale imbriquée dans chaque méta-période.
- [ADR-0039 — Chronologie : attributs de situation hors mart](../adr/0039-chronologie-substrat-attributs-situation-hors-mart.md) — attributs (FTA, puissance…) forward-fillés en SQL, hors du mart relevés.
- [ADR-0041 — Chronologie du contrat en spine dbt](../adr/0041-chronologie-contrat-spine-relationnelle-dbt.md) — la chronologie devient une spine relationnelle assemblée entièrement en dbt.
- [ADR-0042 — Convention de date instant/jour civil](../adr/0042-convention-date-instant-jour-civil-boundary-flux.md) — TIMESTAMPTZ vs DATE harmonisés au boundary `flux_*`, fuseau Europe/Paris à la lecture.
- [ADR-0045 — Relations de la spine par clé naturelle](../adr/0045-relations-spine-reference-cle-normalisation-chronologie-releves.md) — `releve_id` référencé tel quel, pas de surrogate/FK.
- [ADR-0052 — Présence au périmètre](../adr/0052-presence-perimetre-spans-rsc-fermeture-code-sortie.md) — spans `[début, fin)` par RSC, fermeture sur le code de sortie.

## Facturation & taxes

- [ADR-0014 — Lignes de factures du mois avec flags](../adr/0014-lignes-factures-du-mois-avec-flags.md) — expose les lignes avec des flags d'état plutôt que de filtrer à zéro hors période.
- [ADR-0024 — Trois registres de savoir](../adr/0024-trois-registres-de-savoir.md) — distingue runtime / structurel / réglementaire ; gouvernance détaillée dans [gouvernance-reglementaire.md](gouvernance-reglementaire.md).
- [ADR-0027 — Odoo tire les méta-périodes](../adr/0027-endpoint-lecture-meta-periodes-odoo-tire.md) — `GET /facturation/meta-periodes`, cœur read-only, pas de `POST recompute`.
- [ADR-0030 — Calculateur TURPE variable](../adr/0030-calculateur-turpe-variable-odoo-fournit-assiette.md) — Odoo fournit l'assiette de consommation, electricore livre le montant.
- [ADR-0048 — Estimation de provision des lissés](../adr/0048-estimation-provision-lisses-coeur-kwh-cold-start-r67.md) — dérivation cœur-pure en kWh, amorcée par R67 (cold-start).

## Déploiement & secrets

- [ADR-0011 — Déploiement VPS Docker](../adr/0011-deploiement-vps-docker.md) — VPS + Docker Compose comme socle de déploiement.
- [ADR-0015 — Déploiement multi-instance](../adr/0015-deploiement-multi-instance.md) — un VPS par fournisseur, pas de mutualisation multi-tenant.
- [ADR-0017 — Layout `/srv/<slug>/`](../adr/0017-layout-deploiement-srv-slug.md) — une instance vit sous `/srv/<slug>/`, avec un utilisateur système dédié.
- [ADR-0025 — Registre runtime pydantic-settings](../adr/0025-registre-runtime-pydantic-settings.md) — `runtime.py`, domaines indépendants, validation fail-fast par point d'entrée.
- [ADR-0031 — Durcissement SSH du VPS](../adr/0031-durcissement-ssh-vps-utilisateur-ops.md) — utilisateur admin `ops`, root sans accès SSH.
- [ADR-0037 — Trousseau de clés AES N-clés](../adr/0037-trousseau-cles-aes-n-cles-selection-par-essai.md) — voir aussi thème Ingestion ci-dessus.
- [ADR-0040 — Schéma de déchiffrement AES à IV préfixé](../adr/0040-schema-dechiffrement-aes-iv-prefixe.md) — voir aussi thème Ingestion ci-dessus.
- [ADR-0044 — Secrets-as-code SOPS + age](../adr/0044-secrets-as-code-sops-age.md) — secrets chiffrés versionnés, déchiffrement en image, identité par instance.
- [ADR-0046 — Convention de nommage env](../adr/0046-convention-noms-env-par-domaine-identite-secrets.md) — `<DOMAINE>__<CHAMP>` + modèle d'identité/accès des secrets.
- [ADR-0049 — Schéma des secrets en SSOT pydantic](../adr/0049-schema-secrets-ssot-pydantic-deploy-valide-le-split.md) — le schéma pydantic fait autorité, le bash de déploiement ne valide que la politique de split.

## Bot & API

- [ADR-0009 — Architecture API-centrique](../adr/0009-architecture-api-centrique.md) — voir aussi thème Architecture ci-dessus.
- [ADR-0010 — Bot Telegram, UI opérationnelle](../adr/0010-bot-telegram-ui-operationnelle.md) — le bot est l'UI opérationnelle quotidienne du·de la facturiste, client de l'API.
- [ADR-0012 — API read-only sur Odoo](../adr/0012-api-read-only-odoo.md) — l'API n'écrit jamais dans Odoo ; toute écriture reste un acte humain via notebook.
- [ADR-0022 — Surface du bot par domaines hybrides](../adr/0022-surface-bot-domaines-hybrides.md) — commandes organisées par domaine métier (ingestion, flux, périmètre, taxes, facturation).
- [ADR-0027 — Odoo tire les méta-périodes](../adr/0027-endpoint-lecture-meta-periodes-odoo-tire.md) — voir aussi thème Facturation & taxes ci-dessus.

## Documentation & savoir

- [ADR-0024 — Trois registres de savoir](../adr/0024-trois-registres-de-savoir.md) — voir aussi thème Facturation & taxes ci-dessus.
- [ADR-0054 — Chemin de documentation par rôles](../adr/0054-chemin-documentation-roles-escalier.md) — 5 sections par rôle de lecture, principe d'escalier et passerelles montantes ; décision fondatrice de cette section Maintenir.

## Pour aller plus loin

[Registre des ADR](../adr/0001-monorepo.md) (ordre chronologique, `docs/adr/`) ·
[Trois registres de savoir](../adr/0024-trois-registres-de-savoir.md) ·
[Gouvernance du registre réglementaire](gouvernance-reglementaire.md).

[Retour à Maintenir](index.md).
