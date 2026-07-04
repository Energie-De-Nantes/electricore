# Contexte — api (service REST)

Vocabulaire spécifique au service REST qui expose `core` et l'orchestration de l'`ingestion`. Hub central de l'architecture — voir [ADR-0009](../../docs/adr/0009-architecture-api-centrique.md).

## API

**API** :
Service REST FastAPI ([electricore/api/](.)) exposant les flux Enedis bruts (`/flux/*`), les relevés canoniques (`/releves`, cf. *Endpoint relevés canoniques*), les déclenchements d'ingestion (`/ingestion/*`), les calculs de taxes (`/taxes/*`), les exports de facturation et vérifications pré-facturation (`/facturation/*`, dont `/facturation/check/odoo.xlsx`), et la lecture des méta-périodes mensuelles (`/facturation/meta-periodes`, cf. *Endpoint méta-périodes*).

**Endpoint méta-périodes** :
`GET /facturation/meta-periodes` — endpoint de lecture par lequel un ERP **tire** les *méta-périodes mensuelles* d'electricore (Odoo construit ses `souscription.periode` à partir de ce flux). JSON enveloppé (`mois` / `contract_version` / `filters` / `pagination` / `data`), ERP-agnostique (zéro `integrations/odoo`, [ADR-0027](../../docs/adr/0027-endpoint-lecture-meta-periodes-odoo-tire.md)). Contrat figé : [docs/contrat-meta-periodes.md](../../docs/contrat-meta-periodes.md). Distinct des autres `/facturation/*` qui, eux, lisent Odoo. Charge utile **non valorisée aux prix fournisseur** : quantités physiques + montants réseau (TURPE, CTA) + *taux* accise.

**Endpoint relevés canoniques** :
`GET /releves` (+ `.arrow`, `.xlsx`, `/info`) — lecture HTTP du *modèle de relevés canonique* (mart dbt `releves`, [ADR-0029](../../docs/adr/0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md)), consommé par les notebooks distants via `ElectricoreClient.releves()`. Délibérément **hors** `/flux/*`, qui reste réservé aux **flux Enedis bruts** : `releves` est un modèle dérivé (union arbitrée C15/R64/R151), pas un flux. Adossé au loader `releves()` (pas au registre `FLUX_DESCRIPTORS`). Filtrable par PDL, *source* (`flux_R151`/`R64`/`C15`) et fenêtre de dates. Première occupante de la *couche mart* exposée ; une catégorie `/canonique/*` n'est introduite que si un second modèle dérivé est exposé (cf. [ADR-0032](../../docs/adr/0032-modeles-marts-hors-flux-namespace.md)).
_Éviter_ : `/flux/releves` (range un mart sous le namespace flux brut), `/marts/*` (jargon entrepôt, pas vocabulaire métier), `/enriched/*` (collision avec *historique enrichi*).

**Calculateur turpe-variable** :
`POST /facturation/turpe-variable` — calculateur **sans état** où Odoo POST l'assiette (énergies par cadran + FTA + `debut`) et electricore renvoie le **montant** € (et non le taux : l'assiette arrive dans la requête, [ADR-0030](../../docs/adr/0030-calculateur-turpe-variable-odoo-fournit-assiette.md)). Lot + `id` opaque ré-émis, **succès partiel** par ligne (montant xor motif d'erreur), 7 cadrans passés et arbitrage par les zéros de la règle FTA (invariant sous test, #252). JSON enveloppé (`contract_version` / `results`), ERP-agnostique (zéro `integrations/odoo`). Contrat figé : [docs/contrat-turpe-variable.md](../../docs/contrat-turpe-variable.md). Complément du feed `GET /facturation/meta-periodes` (recalcule le TURPE variable depuis une énergie que seul Odoo possède — saisie manuelle).

**Résolution RSC** :
`POST /facturation/rsc` — résolution **sans état** où Odoo POST un lot d'`id_Affaire` et electricore renvoie le `ref_situation_contractuelle` correspondant, par **recoupement X12 ⨝ C15** (#282, souscriptions_odoo #5). Match **exact** sur l'`id_affaire` que portent les événements C15 (= l'`Id_Affaire` de l'affaire X12, cf. [core/CONTEXT.md](../core/CONTEXT.md)), lu sur le `flux_c15` natif (pas le mart `spine_contrat` forward-fillé) ; X12 (`flux_affaires`) sert au **recoupement d'existence** (distingue *affaire connue sans RSC* d'*affaire inconnue*). Lot + `id_affaire` opaque ré-émis, **succès partiel** par entrée (RSC xor motif d'erreur). JSON enveloppé (`contract_version` / `results`), ERP-agnostique (zéro `integrations/odoo`). Contrat figé : [docs/contrat-rsc.md](../../docs/contrat-rsc.md). Aucune heuristique temporelle (le notebook `injection_rsc` n'asof-joint par PDL que faute d'`Id_Affaire` sur les `sale.order` legacy).

**Contrat figé (`X-Contract-Version`)** :
Garde de version portée par les seuls endpoints facturiste **typés** (JSON/JSONL enveloppé : méta-périodes, chronologie, turpe-variable, RSC, provision), où un client pydantic `extra="ignore"` masquerait silencieusement un champ disparu. Les endpoints **Arrow** (`*.arrow`) n'en portent **pas, par choix** : ce sont des passe-plats dont le schéma voyage avec la donnée (Arrow IPC auto-décrit) — un `except ContractVersionError` sur un appel Arrow du client est donc du code défensif dormant, pas un bug.
_Éviter_ : ajouter la garde aux `*.arrow` « pour cohérence » (slice serveur + client + release PyPI sans champ à protéger).

**Endpoint sécurisé** :
Endpoint nécessitant la clé `X-API-Key` (header) ou `?api_key=` (query). Tous les endpoints sont sécurisés sauf `/`, `/health`, `/docs`, `/redoc`.

**`/health`** :
Endpoint public qui retourne l'état de l'API, de la base DuckDB (accessible, dernière écriture, comptes par table) et du bot. Utilisé pour les checks de déploiement et le monitoring externe.

**Service** :
Module de [services/](services/) qui contient la logique d'un endpoint (ex : `duckdb_service.py` pour les requêtes flux, `facturation_service.py` pour la réconciliation Odoo↔Enedis). Permet de séparer la couche HTTP de la logique d'accès aux données.
