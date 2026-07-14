# Changelog

Toutes les modifications notables de ce projet seront documentées dans ce fichier.

Le format est basé sur [Keep a Changelog](https://keepachangelog.com/fr/1.0.0/),
et ce projet adhère au [Semantic Versioning](https://semver.org/lang/fr/).

---

## [Unreleased]

## [3.7.0rc3] - 2026-07-14

Release candidate : **fin de souscription Odoo** — l'endpoint sorties C15 arrive
côté moteur, consommable par `electricore-client` 0.5.0 (en ligne sur PyPI).

### ✨ Ajouté

- **`POST /perimetre/sorties`** (#632) : sorties du périmètre par lot de RSC
  (événement C15 `RES`/`CFNS`), filtre à la requête (l'autorité du « à
  nous » est la souscription Odoo). Consommé par le client
  `electricore-client` 0.5.0 (`sorties(rsc=...)`).

### ♻️ Modifié

- **`GET /provision/estimation`** (#630) : le 503 « `flux_r67` non matérialisé »
  est tagué `X-Error-Kind: precondition` (code HTTP inchangé) → le client typé
  le mappe en `PreconditionNonRemplie` au lieu d'un `HTTPStatusError`
  inattrapable. Nouvelle méthode client `provision_estimation(pdl)` (0.5.0).

## [3.7.0rc2] - 2026-07-11

Release candidate : **empreinte de contenu** — kernel unique de hash canonique,
embarqué avant toute utilisation prod du canon `reference` des prestations.

### ♻️ Modifié

- **`empreinte_contenu`** (#625) : kernel Python pur de hash de contenu canonique,
  remplace `_ajouter_source_hash` (meta_periodes_service) et `_ajouter_reference`
  (prestations_service). Corrige le bug de canonisation des flottants
  (`cast(Utf8)` Polars) : les valeurs de `source_hash` et `reference` changent
  par rapport à la rc1 — aucune n'a encore été consommée en prod.

### 📝 Documentation

- ADR-0056 : secrets dev via Proton Pass par espace.
- Glossaire core : « Empreinte de contenu » (#625).

## [3.7.0rc1] - 2026-07-09

Release candidate : **endpoint prestations** pour souscriptions_odoo, `famille_cadrans`
par relevé, date de bascule dans `memo_puissance`, et réparation de l'export bot.

### ✨ Ajouté

- **`GET /facturation/prestations`** (souscriptions_odoo#37) : pull-tout des refacturables
  F15, `resoudre_rsc()` côté client. Consommé par le client `electricore-client` 0.4.0.
- **`famille_cadrans` par relevé** dans `releves_utilises` (#603) : la famille de cadrans
  est exposée relevé par relevé côté API.
- **Date de bascule intercalée dans `memo_puissance`** (#598) : la bascule de puissance
  est tracée dans le mémo de facturation.

### 🐛 Corrigé

- **Export bot réparé** (#596) : le registre des flux est keyé par extraction (f15→f15_detail,
  +r15_acc) et non par flux, ce qui répare l'export « flux → N extractions ».
- **Canon `reference` prestations en Python pur** (#600).

### ✅ Tests

- Garde anti-drift : `construire_dbt` matérialise tous les marts dbt (#553).

## [3.6.1] - 2026-07-05

Patch **documentaire** : la frontière **actuel (legacy) / futur (ERP tire)** est
matérialisée des deux côtés — Swagger et site de doc (#584). Aucun changement de
comportement ni d'URL.

### ✨ Ajouté

- **Tag OpenAPI `legacy`** (#584) : les 12 endpoints couplés au vieil Odoo (lecture
  XML-RPC) sont regroupés sous un tag dédié dans Swagger, avec une description franche
  (chemin de production du cycle actuel notebook→Odoo, disparaîtront à la bascule
  ERP-tire, ADR-0027). Le tag `facturation` ne garde que la famille ERP-tire
  (méta-périodes, chronologie, turpe-variable, RSC) ; `taxes` est recentré sur le
  registre des taux régulés. Test d'invariant `legacy ⟺ requires_odoo`
  (+ `check/odoo` à garde inline), marqueur introspectable posé par `binary_endpoint`.
- Glossaire API : entrée **« Endpoint legacy »** dans `electricore/api/CONTEXT.md`,
  par contraste avec le socle durable qui survit à la bascule.

### 📚 Documentation

- **Porte d'entrée de la section Facturer** (`docs/facturiste/index.md`, #584) :
  le cycle actuel au premier plan (vue d'ensemble, pas à pas, préparation du poste,
  filet de sécurité — basculés depuis le changelog facturiste), inventaire des outils
  actuels, futur ERP-tire clairement balisé. Nav « Guide facturiste » → « Ce qui a
  changé » ; l'entrée v3.4 ne présente plus la chronologie comme outillée (exposée
  par l'API, outillage à venir).

## [3.6.0] - 2026-07-05

Minor centrée sur l'**outillage facturiste** : fiabilisation des notebooks de campagne
(enquête « 0 draft » de juin 2026 — #579/#580/#581), série d'ergonomie notebooks (#571,
#554), et le **site de documentation** étoffé (section Maintenir, garde-fous CI).
Purement additive — aucun changement incompatible.

### 🐛 Corrigé — campagne de facturation (enquête « 0 draft », juin 2026)

- **Règle draft null-safe** (#579) : la cellule statut du notebook de facturation traitait
  `qualite` null (sans correspondance Enedis) et `x_lisse` null (jamais `false` côté Odoo)
  comme « à jour » — propagation de null Polars, `pl.when(null)` vaut faux — donc un mois
  incalculable non lissé passait en *checked* au lieu de *draft*. Règle cible :
  draft ⟺ `qualite` null OU (incalculable ET non lissé), harmonisée avec
  `filtre_a_injecter` ; table de vérité `qualite` × `x_lisse` en test.
- **Attribution de la RSC en service** (#580) : `injection_rsc` résolvait la RSC par asof
  `nearest` sur (PDL, `date_order`) — il figeait la RSC contemporaine de la souscription,
  jamais réparée après re-création du contrat Enedis (RES→MES le même jour, CFN entrant).
  Nouvelle règle : la RSC du PDL dont le **dernier état C15** n'est pas `RESILIE` ;
  0 ou plusieurs candidates → commandes listées à part, **jamais d'écriture silencieuse** ;
  l'asof reste affiché à titre de diagnostic.

### ✨ Ajouté

- **Signalement des brouillons post-mois facturé** (#581) : section lecture seule du
  notebook de facturation qui nomme la cause des brouillons sans correspondance Enedis
  dont le contrat démarre **après** la fin du mois facturé (date de mise en service
  affichée) ; un contrat entré en cours de mois (prorata légitime) n'est pas signalé.
- **Notebooks opérateur** (#571, #554) : vérification de l'API à l'ouverture (URL tentée
  affichée si injoignable, `api_version` lue du `/health`), messages d'erreur sans
  traceback et prose facturiste des sections Préparation ; saisie du mois en `YYYY-MM`
  avec défaut au dernier mois révolu ; flag `--edit` sur `electricore-notebooks`
  (délègue à `marimo edit`).

### 🔧 Modifié

- **`OdooWriter.update()` rapporte par-record** (#571) : un enregistrement en échec
  n'interrompt plus le lot — le résultat détaille réussites et échecs.

### 📚 Documentation

- **Section Maintenir** du site (#559) : processus de release (bump en PR, tag publiant,
  frontière à l'image), gouvernance du registre réglementaire.
- **Garde-fous CI docs** (#560) : build strict sur PR touchant la doc, tests de parité
  nav + fraîcheur des pages (front-matter `fraicheur`), validation des liens resserrée,
  liens morts réparés.
- **Palette Warm Cosy** du site MkDocs : papier `#faf4ed`, titres Excalifont, scopée sur
  le thème clair.
- Glossaire core : vocabulaire des communs (Commun, Usager·ère).

## [3.5.1] - 2026-07-04

Patch de la **campagne de facturation** : sélection des factures Odoo du mois par
**date-ancre** (ADR-0055, #561) et garde-fou pré-campagne (#564). Débloque l'injection
de la campagne de juin 2026.

### 🐛 Corrigé

- **Sélection par date-ancre** (#561, ADR-0055) : `lignes_factures_du_mois` sélectionne les
  factures par égalité stricte `invoice_date == 05/(M+1)` (rollover décembre → `05/01/N+1`)
  au lieu de la fenêtre mensuelle `[M, M+1)` — la fenêtre ramassait la campagne précédente
  validée, et les brouillons de campagne (générés sans date) étaient invisibles. Convention
  inter-systèmes : Odoo pose la date à la génération, electricore la lit.
- **Faux positifs des checks pré-facturation** (#564) : discriminant énergie
  `x_pdl != False` généralisé aux 5 checks existants — les commandes hors énergie
  (ex. S00583) ne sont plus ratissées.

### ✨ Ajouté

- **Check pré-campagne « brouillons hors ancre »** (#564) : tout brouillon de facture d'une
  commande énergie daté hors de l'ancre courante (le 05 du mois courant) ou sans date
  remonte dans `/check odoo` avant le lancement de la campagne — détection bruyante d'une
  violation de la convention date-ancre.
- Glossaire `integrations/odoo/CONTEXT.md` : entrées **date-ancre** et
  **campagne de facturation**.

## [3.5.0] - 2026-07-01

Premier stable de la ligne **3.5** : aboutissement du cycle rc1→rc3. La minor **ouvre le
segment C2-C4 côté ingestion** (flux C12 — ADR-0051), **discrimine les erreurs** client/API
au-delà du code HTTP (`X-Error-Kind` — #424), et publie le **site de documentation** MkDocs
(#525). Purement additive — aucun changement incompatible. Détail rc-par-rc plus bas.

### ✨ Temps forts

- **Ouverture du segment C2-C4 — ingestion du flux C12** (ADR-0051, #344). Ingestion du flux
  **C12** (description contractuelle des PRM **>36 kVA**, SGE GUI 0129), même plomberie ELT que
  C15 : source dlt + modèles dbt `stg_c12`/`flux_c12` + loader `c12()`, **spine contractuelle C4**
  (puissances souscrites pivotées par classe temporelle TURPE, option tarifaire, domaine de
  tension, personne morale `raison_sociale`/`code_ape` — proxy de la catégorie d'accise #226),
  segment **inféré**, golden XSD + garde-fou anti-dérive de schéma. Côté ingestion : le contrat
  de consommation aval est délibérément différé (ADR-0051, « le premier consommateur l'établira »).
- **Discrimination des erreurs client/API** (#424). En-tête **`X-Error-Kind`** qualifiant la
  nature d'une erreur au-delà du seul code HTTP : `ingestion-lock` sur le 503 du verrou DuckDB
  (verrou transitoire vs vraie indisponibilité), routage des exceptions client sur cet en-tête,
  et **précondition RSC 422 actionnable** en amont de `rapprocher()`.
- **Site de documentation MkDocs** (#525). Site **MkDocs Material** publié sur GitHub Pages
  (build + deploy CI sur push `main`) : guides bot facturiste & facturiste, runbook d'onboarding
  du notebook opérateur, badge et lien depuis le README.

### 🔧 Modifié

- **Taux régulés** : lecteur partagé `charger_regles_taux` des registres `*_rules.csv`
  (factorisation du chargement CSV, #520).

### 🐛 Corrigé

- **Bot** : cockpit `/perimetre affaires` muet au-delà de ~46 affaires — `_formater_affaires`
  tronque désormais aux frontières de lignes sous la limite Telegram (4096 car.), en gardant les
  affaires les plus anciennes.
- **DuckDB** : `_is_lock_error` restreint aux vrais verrous — ne masque plus d'autres erreurs (#424).

### 🧹 Divers

- [ADR-0050](docs/adr/0050-entrees-contexte-mensuel-parc-et-point-distinctes.md) : les deux
  entrées « contexte mensuel » (parc / point) restent délibérément distinctes.
- Tests : retrait des tests d'intégration `OdooWriter` (config morte).

## [3.5.0rc3] - 2026-06-30

Troisième release candidate de la **minor 3.5**.

### 🐛 Corrigé — cockpit `/perimetre affaires` muet (bot)

La vue « Affaires en cours » du bot construisait un message dépassant la limite dure
de **4096 caractères** de l'API Telegram dès qu'assez d'affaires étaient en cours
(≈ 46 suffisaient), faisant échouer `edit_message_text` hors `try/except` : le spinner
« ⏳ Affaires en cours… » restait bloqué, le bot paraissait muet. `_formater_affaires`
tronque désormais la liste aux frontières de lignes sous la limite, en gardant les
affaires les plus anciennes (les plus actionnables) et en signalant le reste — même
garde-fou que le récap de facturation.

## [3.5.0rc2] - 2026-06-30

Deuxième release candidate de la **minor 3.5** — discrimination des erreurs côté
client/API et publication du **site de documentation**.

### ✨ Ajouté — discrimination des erreurs (#424)

En-tête **`X-Error-Kind`** renvoyé par l'API pour qualifier la nature d'une erreur
au-delà du seul code HTTP, et consommé par le client :

- **503 verrou d'ingestion** : `X-Error-Kind: ingestion-lock` sur le 503 du verrou DuckDB,
  pour distinguer un verrou transitoire d'une vraie indisponibilité ;
- **client** : route les exceptions sur `X-Error-Kind` plutôt que sur le code HTTP seul ;
- **précondition RSC** : `rapprocher()` renvoie un **422 actionnable** en amont plutôt
  qu'une erreur opaque.

### 📚 Documentation — site MkDocs (#525)

Site **MkDocs Material** publié sur GitHub Pages (build + deploy en CI sur push `main`) :
guide du bot facturiste, guide facturiste, runbook d'onboarding du notebook opérateur,
badge et lien depuis le README.

### 🐛 Corrigé

- **DuckDB** : `_is_lock_error` restreint aux vrais verrous DuckDB — ne masque plus d'autres
  erreurs (#424).
- **Site** : page d'accueil (404 à la racine) et liens du CHANGELOG en URL absolue GitHub.

## [3.5.0rc1] - 2026-06-30

Première release candidate de la **minor 3.5** — ouverture du segment **C2-C4** côté ingestion.

### ✨ Ajouté — flux C12 (#344)

Ingestion du **flux C12** (description contractuelle des PRM du segment **C2-C4, >36 kVA**,
Enedis SGE GUI 0129), suivant la même plomberie ELT que C15 :

- nouvelle source dlt + modèles dbt `stg_c12` / `flux_c12` + loader `c12()` ;
- **spine contractuelle C4** : puissances souscrites pivotées par classe temporelle TURPE
  (`puissance_souscrite_{hph,hch,hpb,hcb}_kva` + scalaire mono), option tarifaire TURPE,
  domaine de tension, et personne morale (`raison_sociale`, `code_ape`) — proxy de la
  catégorie d'accise (#226) ;
- segment **inféré** (pas de `Segment_Clientele` natif dans le XSD C12), classes HTA non
  mappées (gardées) ;
- golden XSD (mono + 4 puissances) + garde-fou anti-dérive de schéma. Le golden sur données
  réelles est **différé** (nécessite le trousseau AES).

Décision : [ADR-0051](docs/adr/0051-flux-c12-spine-c4.md).

### 🔧 Modifié

- **Taux régulés** : lecteur partagé `charger_regles_taux` des registres `*_rules.csv`
  (factorisation du chargement CSV, #520).

### 🧹 Divers

- [ADR-0050](docs/adr/0050-entrees-contexte-mensuel-parc-et-point-distinctes.md) : les deux
  entrées « contexte mensuel » (parc / point) restent délibérément distinctes.
- Tests : retrait des tests d'intégration `OdooWriter` (config morte).

## [3.4.1] - 2026-06-29

Patch stable **3.4.1** (promotion de la rc1) — durcissement et élagage post-stable, **sans
changement de comportement fonctionnel** (suite verte, symboles disparus).

### 🧹 Élagage de la dette de sur-ingénierie (épic #505)

Audit ponytail repo-wide → retrait de **~766 lignes** de code mort / flexibilité spéculative,
en 8 tranches indépendantes vérifiées (suite verte + symbole disparu) :

- **Loaders DuckDB** : machinerie de projection morte retirée (résidu post-#311) — la
  construction de requête se réduit à `SELECT *` + WHERE, validation toujours active (#506).
- **Intégration Odoo** : surface morte (`OdooConfig`/résolution double-clé, overrides de
  `OdooReader`, `OdooQuery.rename`, helpers sans appelant) + 3 schémas Pandera inutilisés
  retirés (#507, #508).
- **Pipelines** : `expr_has_changement` retirée (duplication inline conservée) ; le validateur
  C4 standalone documenté est conservé (#509).
- **API** : décorateurs binaires effondrés en un `binary_endpoint` unique ; feature
  d'introspection `/admin/api-keys` sans consommateur retirée (#510, #511).
- **Ingestion** : runner + transformers nettoyés (fabrique crypto, `match_xml_pattern`→`fnmatch`,
  alias `reset` déprécié, gardes mortes, déduplications) — déchiffrement par trousseau
  (ADR-0037) inchangé (#512).
- **Dépendances** : `numpy` (dépendance directe inutilisée) retirée — reste tirée
  transitivement par polars/pyarrow (#513).

### 🔐 Schéma des secrets en SSOT pydantic (ADR-0049)

- Validation de **format** des secrets centralisée dans des `field_validators` pydantic
  (source unique de vérité) ; `deploy/` ne valide plus que la **politique de split**, le
  contenu étant validé par le vrai conteneur — supprime le preflight bash redondant et le
  chemin legacy `.env` mort (#500, #501).
- Couverture **anti-régression de l'onboarding** : entrypoint docker fail-fast sur secret
  manquant, round-trip crypto sur binaires réels (#453).

### 🐛 Corrections

- Déploiement : `sops encrypt --age` explicite (parité `add-provider.sh`, revue #504).

## [3.4.0] - 2026-06-29

Premier stable de la ligne **3.4** : aboutissement du cycle rc1→rc12, validé en production.
La ligne achève la **descente d'assemblage en dbt** (le cœur consomme, dbt assemble —
ADR-0041/0042), ouvre electricore comme **source que l'ERP _tire_ sans tirer le moteur**
(client léger + flux JSONL — ADR-0043), bascule le déploiement en **secrets-as-code**
(SOPS + age — ADR-0044/0046), et amorce l'**estimation de provision des contrats lissés**
(cold-start R67 — ADR-0048). Détail rc-par-rc plus bas.

### ✨ Temps forts

- **Descente d'assemblage en dbt — le cœur consomme, dbt assemble** (ADR-0041/0042). La
  *Chronologie du contrat* est désormais une **spine assemblée en dbt** (forward-fill SQL +
  grille FACTURATION mensuelle) que `pipeline_historique` se contente de **filtrer par horizon**
  et de découper en ruptures ; la branche abonnement bascule dessus (parité byte-identique),
  **~2700 lignes** quittent le cœur. Les loaders `/flux` deviennent de **vrais `SELECT *`**
  typeless — la forme (types, renames, ancrage de date) descend dans les modèles `flux_*` : un
  **instant** est un `TIMESTAMPTZ` harmonisé au boundary (le « +1 jour » R151 devenu natif), un
  **jour civil** un `DATE`, fuseau de session épinglé `Europe/Paris`. La chronologie devient
  **filtrable par PDL/RSC** au chargement (prédicat poussé en DuckDB, invariant « horizon =
  paramètre » garanti par test de parité).
- **electricore, source que l'ERP _tire_ — sans tirer le moteur** (ADR-0043). Extraction du
  paquet léger **`electricore-client`** (httpx + pydantic seuls, `[arrow]` en extra, publié sur
  PyPI par tag `client-v*`) : `souscriptions_odoo` consomme l'API sans polars/duckdb/fastapi, sur
  des **modèles de contrat single-source** gardés par en-tête de version. Les endpoints facturiste
  passent en **flux JSONL typé** (`/facturation/chronologie`, `/facturation/meta-periodes`,
  métadonnées en en-têtes), et le trio de pull facturiste se complète : **`POST /facturation/rsc`**
  (résolution `id_Affaire` → `ref_situation_contractuelle`, X12 ⨝ C15, #282) et la **vue
  facturiste `GET /facturation/chronologie`** (la frise faits + verdicts d'un PDL/RSC, sans montant).
- **Déploiement en secrets-as-code** (ADR-0044/0046, #418–#420). Les secrets quittent le `.env`
  en clair pour un chiffrement **SOPS + age** versionné, déchiffré dans l'image à l'exécution
  (`sops exec-env`, **fail-fast** sans clé) ; `config.env` (clair) + `secrets.env` (chiffré),
  chaque box génère ses identités, onboarding GitOps auto-pull, clé admin d'**escrow** hors-ligne.
  Convention de noms unifiée **`<DOMAINE>__<CHAMP>`** sur tout le runtime, **trousseau API
  étiqueté** (`API__TROUSSEAU__<consommateur>__KEY`, révocation ciblée), **bloc Odoo unique**
  `ODOO__*` (fin du sélecteur test/prod).
- **Estimation de provision des contrats lissés** (ADR-0048, #487/#488/#489). Capacité
  **cœur-pure** qui amorce (cold-start) la provision d'énergie d'un contrat lissé depuis
  `flux_r67` (M023) : profil cadran sur **12 mois glissants** → plate, sortie **WIDE** par cadran
  + métadonnées de couverture/qualité + signal alertable, livrée **bout-en-bout** (build
  `RapportProvision` → `GET /provision/estimation` → bot `/provision`). **kWh uniquement, zéro €** ;
  en amont de la régularisation (#191).
- **Robustesse d'ingestion R67** (#492/#494/#495). R67 re-liste tout (`incremental: false`) —
  l'incrémental dlt vivant sur le *listing* sautait à jamais un fichier listé mais échoué en aval
  (avait vidé `raw_r67` en prod) ; `--resync` ciblé par flux purge un curseur bloqué sans
  re-télécharger ; R67 surfacé dans l'API et le bot d'ingestion.

### ⚠️ Changements incompatibles

- **Déploiement** : chemin d'installation `.env` legacy **retiré** (ADR-0044 §8) — `--deploy-repo`
  obligatoire, le split `config.env`/`secrets.env` est le seul chemin. Variables d'environnement
  renommées en `<DOMAINE>__<CHAMP>` (`DUCKDB__PATH`, `API__*`, `BOT__*`, `ODOO__*`,
  `API__TROUSSEAU__<consommateur>__KEY` à la place de `API_KEY`/`API_KEYS`).
- **Endpoints `/flux`** : `/flux/r64`, `/flux/r151` servent des **instants** `TIMESTAMPTZ`
  (R151 = J+1 harmonisé, endpoint déprécié au profit du mart `releves`) ; `/flux/f15` sert des
  **`DATE`** (jour civil). Empreinte canonique `/releves` et `releve_id` **stables**.
- **Surface loaders / notebooks** : 14 notebooks de démo/validation morts supprimés (seul subsiste
  le trio lanceur opérateur) ; `execute_custom_query`, `DuckDBQuery.exec()`, le modèle Pandera
  `RequêteRelevé` et les loaders Parquet `core.loaders.parquet` retirés (tous sans appelant).
  `RelevéIndex` (contrat vivant) **conservé**.

## [3.4.0rc12] - 2026-06-28

### 🐛 Corrections

- **Config Odoo : dé-quote `ODOO__DB` / `ODOO__USERNAME` / `ODOO__PASSWORD`** (suite #454) :
  en prod `sops exec-env` exporte les valeurs dotenv **verbatim** (guillemets compris) là
  où le `.env` dev passe par python-dotenv qui les retire. Un secret écrit
  `ODOO__DB="<base>"` atteignait `authenticate()` quotes incluses → Postgres répondait
  `database ""<base>"" does not exist` (échec d'auth XML-RPC cryptique). Le nettoyage
  `_dequote` (jusque-là réservé à `ODOO__URL`, #454) couvre désormais tout le bloc
  `ODOO__*`, refermant l'écart dev↔prod.

## [3.4.0rc11] - 2026-06-28

### 🛠️ Robustesse ingestion (curseur incrémental R67)

- **R67 re-liste tout à chaque run (`incremental: false`)** (#495) : l'incrémental dlt vit
  sur le **listing** (`modification_date`), pas sur le **landing** — un flux qui liste un
  fichier mais échoue en aval (decrypt/parse → 0 document) avançait quand même son curseur
  et **sautait le fichier à jamais**. C'est ce qui a vidé `raw_r67` en prod (M023 du jour
  skippés → `flux_r67` absent → `/provision/estimation` en erreur). R67 étant un flux
  ponctuel et peu volumineux, il éteint l'incrémental : il liste tout, le `merge` sur
  `file_name` dédoublonne, un fichier non atterri se rejoue. Les flux quotidiens
  (R64/R151/C15…) gardent leur incrémental.
- **`--resync` ciblé sur le CLI** (#494) : `python -m electricore.ingestion <flux> --resync`
  purge l'état incrémental des **seuls** flux sélectionnés (`drop_resources` scopé au run),
  pour rejouer un curseur bloqué sans re-télécharger tous les flux (le mode `resync` global
  reste `drop_sources`).

### 🐛 Corrections

- **R67 surfacé dans l'API et le bot d'ingestion** (#492) : `mode: "r67"` accepté par
  `POST /ingestion/run` et bouton dédié dans le bot.
- **Déploiement : override `--version` rebranché** (#491) : l'option `--version` de
  `install.sh`, rendue inerte par #299, réécrit de nouveau `ELECTRICORE_VERSION` dans
  `config.env` ; garde anti-régression ajoutée.

## [3.4.0rc10] - 2026-06-27

### ✨ Nouveautés (estimation de provision des lissés — cold-start R67)

- **Estimation de provision des lissés en kWh, amorcée par R67** (#487/#488/#489, ADR-0048) :
  capacité **cœur-pure** qui amorce (cold-start) la *provision d'énergie* d'un *contrat lissé*
  depuis `flux_r67` (mesures facturantes M023). Profil cadran → effondrement sur **12 mois
  glissants** (somme nette R+E+C, négatifs préservés) → `/12` **plate**. Sortie **WIDE** par
  cadran (profondeur cohérente maximale, invariant `base = hp+hc = Σ4`, repli base sur
  historique grille-mixé) + métadonnées de **couverture** (densité réelle, seuil de suffisance
  en jours) et de **qualité** (mix réel/estimé/corrigé) + **signal alertable** (la lib expose,
  l'aval alerte, ADR-0037). Frontières Pandera typées des deux bords (R67 en entrée — premier
  consommateur typé). Livré **bout-en-bout** : build autonome `RapportProvision` → API
  `GET /provision/estimation?pdl=…` → bot `/provision <pdl>`. **kWh uniquement, zéro €** (prix
  fournisseur = ERP, ADR-0016/0027) ; `core/` reste pur. En amont de la régularisation (#191 ;
  thermostat, pas solde).

### ✨ Nouveautés (résolution RSC — api/client)

- **`POST /facturation/rsc` — résolution `id_Affaire` → `ref_situation_contractuelle`** (#282) :
  dernier des 3 livrables d'exposition facturation pour le pull Odoo (après
  `GET /facturation/meta-periodes` et `POST /facturation/turpe-variable`). Recoupe **X12**
  (`flux_affaires`) ⨝ **C15** (`flux_c15`) — match **exact** sur l'`id_affaire` natif que
  portent les événements C15 (= l'`Id_Affaire` de l'affaire X12), X12 recoupant l'existence
  (distingue *affaire connue sans RSC* d'*affaire inconnue*). Lot + `id_affaire` opaque
  ré-émis, **succès partiel** par entrée (RSC xor motif d'erreur), enveloppe JSON
  `{contract_version, results}`, ERP-agnostique (zéro `integrations/odoo`). Modèles
  single-sourcés dans `electricore_client` (`ResolutionRscRequest` / `ResultatResolutionRsc`,
  ADR-0043) + méthode client `resoudre_rsc`. Contrat figé : `docs/contrat-rsc.md`.

## [3.4.0rc9] - 2026-06-26

### 🐛 Correctifs

- **ETL test install : poll du statut job au lieu du 202 immédiat** (#299) : `run_ingestion_test`
  retournait dès réception du HTTP 202, affichant « réussi » avant que le job s'exécute.
  Désormais poll sur `GET /ingestion/jobs/{id}` jusqu'à `completed`/`failed`/timeout (30 × 4 s).
  12 tests unitaires couvrent les parseurs et les 4 branches de poll.

## [3.4.0rc8] - 2026-06-26

Nettoyage avant sortie stable : suppression des notebooks morts et de la surface loaders
qu'ils maintenaient, plus hygiène dépôt et correctif déploiement backups.

### 🗑️ Suppressions (Breaking Changes)

- **14 notebooks de démo/validation obsolètes supprimés** (+ `README_validation_turpe.md`) (#466) :
  cassés à l'import depuis les refactors ADR-0013 (`pipeline_*` → `pipelines.*`) et ADR-0019
  (orchestration → `builds.contexte_mensuel`), tous orphelins — aucun n'était testé, empaqueté,
  ni référencé. Seul subsiste le **trio lanceur** opérateur (`accueil.py`, `facturation.py`,
  `injection_rsc.py`, seuls force-inclus et testés). La boucle de vérif des calculs **TURPE ↔ F15**
  que portaient les `validations/*` sera relogée en harnais CI (issue #468 ; cf. #215 pour
  l'oracle énergie R65).
- **API publique loaders : `execute_custom_query` retiré** (#466) : déf + ré-exports + `__all__` dans
  `core/loaders` et `core/loaders/duckdb`. Son seul usage était les notebooks supprimés ; le
  chemin canonique reste les query builders (`c15()`, `releves()`…) ou `.collect()`. Garde
  anti-réintroduction ajoutée au test `#181`.
- **`DuckDBQuery.exec()` retiré** (#466) : méthode dépréciée qui ne faisait que déléguer à `.collect()`,
  zéro appelant.
- **Modèle Pandera `RequêteRelevé` retiré** (#466) : jamais référencé. `RelevéIndex` (contrat vivant)
  est inchangé.

### 🐛 Corrections

- **Backups inaccessibles sous Docker** (#459) : `chown_instance_home` écrasait silencieusement le
  propriétaire du répertoire `backups/` (uid 1000 au lieu du slug) → `Permission denied` à l'écriture.
  `ensure_backups_dir` (setgid 2750) + `ensure_slug_in_container_group` créent la structure avant le
  `chown` général ; `install.sh` ré-asserte l'ownership après.

### 🔧 Hygiène dépôt

- Fichier `--context` (0 octet, nom parsé comme option par ripgrep) retiré du suivi (#467).
- `.gitignore` étendu : `dist-client/`, `.ruff_cache/`, `node_modules/` (#467).
- **Doc** : `docs/qualite-donnees-r151.md` ne pointe plus vers les notebooks de validation supprimés (#466).

## [3.4.0rc7] - 2026-06-26

Correction issue du test de rc6 sur l'instance EDN : le flux JSONL facturiste reste affiché
« [object Blob] » dans Swagger UI malgré la doc rc6.

### 🐛 Corrections

- **Flux JSONL facturiste servi en `application/x-ndjson`** (#455) : le rc6 documentait la 200
  (itemSchema OpenAPI 3.2.0) mais gardait le `Content-Type: application/jsonl` runtime — d'où,
  dans le panneau *Execute* de Swagger UI, le « can't parse JSON. Raw result: [object Blob] »
  inchangé. swagger-js ne lit en **texte** que les types matchant `/(json|xml|yaml|text)\b/` :
  `application/jsonl` échoue (pas de frontière de mot après « json ») → corps lu en `Blob` →
  `JSON.parse(blob)` → `[object Blob]`. `/facturation/{chronologie,meta-periodes}` répondent
  désormais en `application/x-ndjson` : Swagger affiche les **lignes brutes** (avec une note
  « can't parse JSON » bénigne — du NDJSON n'est pas un JSON unique). Le client `electricore-client`
  (lecture `iter_lines`) est insensible au `Content-Type`.

## [3.4.0rc6] - 2026-06-26

Corrections issues du test de rc5 sur l'instance EDN : connexion Odoo, découvrabilité du flux
facturiste, et onboarding secrets-as-code de la box.

### 🐛 Corrections

- **`ODOO__URL` normalisée au chargement** (#454) : `runtime.Odoo` retire l'espace et les
  guillemets appariés puis exige un schéma http(s). En prod, `sops exec-env` exporte les valeurs
  dotenv **verbatim** (guillemets compris) — un `ODOO__URL="https://…"` cité atteignait
  `ServerProxy` tel quel → `503 « unsupported XML-RPC protocol »` sur `/facturation/check/odoo`.
  `ConfigurationManquante` remonte désormais aussi les valeurs présentes-mais-malformées.
- **Flux JSONL facturiste découvrable** (#455) : `/facturation/chronologie` et
  `/facturation/meta-periodes` documentent leur réponse `application/jsonl` avec un `itemSchema`
  OpenAPI **3.2.0** typé par ligne (union discriminée `LigneChronologie` / `PeriodeMeta`), au lieu
  d'un `application/json` implicite que Swagger UI affichait en « [object Blob] ». Le
  *validate-then-stream* atomique (#426/#427) est conservé — pas de bascule vers la génération
  native FastAPI ; le document passe en `openapi_version` 3.2.0.
- **Onboarding box : crypto sur l'hôte** (sops+age) : la box installe et utilise ses propres
  outils (`age-keygen`, `ssh-keygen`, `sops`) au lieu de les lancer via l'image runtime (qui
  fail-fast sans secrets montés et n'embarque ni `age` ni `openssh`) ; `SECRETS_IMAGE`/`DOCKER_BIN`
  retirés de `secrets.sh` — corrige 4 bugs d'onboarding révélés par la bascule EDN.

## [3.4.0rc5] - 2026-06-25

**Secrets-as-code** : les secrets de déploiement quittent le `.env` en clair pour un
chiffrement **SOPS + age** versionné dans un dépôt privé, déchiffré dans l'image à
l'exécution. Convention de noms d'environnement unifiée `<DOMAINE>__<CHAMP>`, identité/accès
des secrets modélisés, et retrait du chemin d'installation `.env` legacy — un seul cutover
(ADR-0044 / ADR-0046).

### ✨ Temps forts

- **Secrets-as-code SOPS + age** (ADR-0044, #418–#420) : `config.env` (clair, versionné) +
  `secrets.env` (chiffré SOPS+age) ; déchiffrement à l'entrypoint de l'image (`sops exec-env`,
  **fail-fast** sans clé, bypass dev `ELECTRICORE_DECRYPT=off`) ; chaque box génère ses deux
  identités (clé age + deploy key SSH) ; onboarding **en deux temps** + auto-pull GitOps ;
  scaffolding multi-cible `providers/<slug>/` + `add-provider.sh` + procédure de rotation.
- **Convention de noms `<DOMAINE>__<CHAMP>`** (ADR-0046, #436) sur les domaines runtime
  (`SFTP`/`AES` déjà conformes ; `DUCKDB__PATH`, `API__TITLE/VERSION/DESCRIPTION`, `BOT__*`),
  avec un **test de parité de frontière** (les noms lus par le runtime == ceux des exemples deploy).
- **Trousseau API étiqueté** (#438) : `API__TROUSSEAU__<consommateur>__KEY` — une clé par
  consommateur (révocation ciblée + attribution dans les logs via le label) ; clé dédiée pour
  le scheduler d'ingestion in-conteneur.
- **Bloc Odoo unique** `ODOO__{URL,DB,USERNAME,PASSWORD}` (#439) — suppression du sélecteur
  test/prod (`ODOO_ENV`/`_SelecteurOdoo`) ; clôture de #190 (Odoo read-only). no-ERP préservé.
- **Clé admin d'escrow** (#437) : deux destinataires admin distincts (opérationnel + escrow
  hors-ligne), destinataires permanents de chaque règle SOPS — plus de point unique de
  défaillance sur l'autorité de déchiffrement au re-keying.
- **Frontière JSONL durcie** (#426–#428) : helper `jsonl_response` (valide-puis-streame), adopté
  par `/facturation/meta-periodes` et `/facturation/chronologie` (corrige une coupure en plein
  flux) ; validation amont de `/flux/{table}/info` + SQL `get_table_info` paramétré.
- **Chronologie dbt normalisée** (#431/#432) : `chronologie_releves` = mart mince + vue star-join ;
  loaders `spine()`/`chronologie()` dés-abrégés (vocabulaire « Chronologie du périmètre »).
- **Lanceur opérateur `electricore-notebooks`** (#414/#423) : validation env + app ASGI marimo,
  page d'accueil, notebooks force-inclus dans le wheel (pont transitoire avant `souscriptions_odoo`).

### 🗑️ Suppressions (Breaking Changes)

- **Chemin d'installation `.env` legacy retiré** (ADR-0044 §8) : `--deploy-repo` devient
  **obligatoire** ; `install.sh` ne télécharge ni n'édite plus de `.env`
  (`deploy/docker/.env.example`, `validate_env_file`, la boucle d'édition et `--env-from`
  supprimés). Le split `config.env`/`secrets.env` est le **seul** chemin d'installation.
- **Variables d'environnement renommées** : `DUCKDB_PATH` → `DUCKDB__PATH`,
  `API_TITLE/VERSION/DESCRIPTION` → `API__*`, `TELEGRAM_*` → `BOT__*`, bloc Odoo → `ODOO__*`,
  et bare `API_KEY`/`API_KEYS` → `API__TROUSSEAU__<consommateur>__KEY`. Infra/compose
  (`INSTANCE_SLUG`, `ELECTRICORE_VERSION`, `BACKUPS_PATH`) restent plats.
- `electricore.core.loaders.parquet` (`charger_releves` / `charger_historique`) → derniers
  chargeurs Parquet pré-DuckDB, sans appelant (moteur, tests, notebooks). Le chargement passe
  désormais par les query builders DuckDB et les loaders Polars qui lisent les marts dbt. Le
  contrat Pandera `RelevéIndex` est conservé (toujours utilisé par les validateurs de flux
  DuckDB et `pipeline_energie`). #429

### 🛠️ Release / CI

- **CI : build + smoke-test de l'image Docker sur les PR** (#435) via workflow réutilisable —
  plus seulement au tag de release ; rattrape les casses build (notebooks force-inclus) et
  entrypoint (smoke SOPS) avant qu'elles n'atteignent `main`.
- Smoke de release : bypass de l'entrypoint SOPS (`ELECTRICORE_DECRYPT=off`) ; image construite
  avec les notebooks force-inclus du wheel (#414).

## [3.4.0rc4] - 2026-06-23

Extraction du **client léger `electricore-client`** (paquet distribué séparément) et bascule
des endpoints facturiste en **flux JSONL typé**, pour que `souscriptions_odoo` consomme l'API
sans tirer polars/duckdb/fastapi.

### ✨ Temps forts

- **Paquet `electricore-client`** (httpx + pydantic seuls, `[arrow]` en extra) : modèles de
  contrat single-source, méthodes client, garde de version par en-tête (ADR-0043, #406–#411).
- **Endpoints facturiste en JSONL streaming** (`/facturation/chronologie`, `/facturation/meta-periodes`) :
  métadonnées en en-têtes HTTP, plus d'enveloppe ni de pagination ; `turpe_variable` en RPC POST typé.
- **Contrat single-source** : les routers importent les modèles depuis `electricore-client`
  (le moteur en dépend via le workspace uv).
- Client Arrow historique migré derrière l'extra `[arrow]` (polars en import paresseux,
  garde de pureté en CI).

### 🛠️ Release

- Image Docker : `electricore-client` installé en **wheel** dans le venv (le runtime n'embarque
  pas le source `packages/`).
- Smoke-test de release élargi à `import electricore.api.main` (exerce réellement `electricore_client`).
- Publication PyPI du client par tag `client-v*` (Trusted Publishing / OIDC), versionné indépendamment.

## [3.4.0rc3] - 2026-06-21

La *Chronologie du contrat* devient interrogeable **par point ou par contrat**, et expose
une **vue facturiste** : la frise complète d'un PDL/RSC tissant faits et verdicts, sans montant.

### ✨ Temps forts

- **Chronologie filtrable par PDL/RSC au boundary de chargement** (ADR-0041, #366).
  `contexte_du_mois_filtre(pdl, rsc, …)` pousse le prédicat dans DuckDB (clause `WHERE`
  paramétrée sur les marts `spine()` / `chronologie()`, pas de scan parc). Le pipeline de
  chronologie reste **inchangé** (partition-local + horizon paramétrique #179), garanti par
  un test de parité « run filtré sur X ≡ run plein ∩ X » qui garde explicitement l'invariant
  « horizon = paramètre ».
- **Endpoint `GET /facturation/chronologie?pdl|rsc`** — vue facturiste (ADR-0041/0039, #367).
  Read-only, authentifié (`X-API-Key`), JSON enveloppé cohérent avec `/meta-periodes` et
  `/releves`. Renvoie la frise complète d'un point (toute l'histoire du PDL, RSC successives +
  charnières) ou d'un contrat (une tenure bornée) : les **faits** (événements C15 *y compris
  hors-comptage* — MDPRM, MCT — et relevés R151/R64/C15) tissés avec les **verdicts dérivés**
  qualité / communication / énergie de chaque période. **Sans montant tarifaire** (turpe/cta/
  accise) — différenciateur explicite vs `/meta-periodes`.

## [3.4.0rc2] - 2026-06-21

Les loaders `/flux` deviennent de **vrais `SELECT *`** (convention de date unifiée ADR-0042),
et la branche abonnement affine sa consommation de la spine *Chronologie du contrat*.

### ✨ Temps forts

- **Loaders `/flux` = `SELECT *` typeless** (ADR-0042, #393→#398). La forme résiduelle (types,
  littéraux `source`/`unite`, renames, placeholders null) et l'ancrage de date descendent dans
  les modèles dbt `flux_*`. Convention tranchée : un **instant** est un `TIMESTAMPTZ` harmonisé
  **au boundary `flux_*`** (R64 → instant heure-mur Paris ; R151 → instant minuit Paris **J+1**,
  le « +1 jour » devenu natif), un **jour civil** reste un `DATE` (F15, bascules de niveau, dates
  d'effet d'affaire). Le **fuseau de session** DuckDB est épinglé à `Europe/Paris` à la lecture
  (`duckdb_readonly_conn`) → instants déterministes quel que soit le fuseau de l'hôte (poste/CI/VPS)
  et filtres de date interprétés en heure de Paris, sans ancrage par colonne. Retrait de la
  machinerie `FormeTemporelle`/`col_*`/`convert_time_zone`/enveloppe de filtre (#391).
- **Affinage de la consommation de la spine** (ADR-0041). `detecter_points_de_rupture` force les
  bornes FACTURATION sur le discriminant **typé non-null `type_fait`** de la spine (`SpineContrat`)
  plutôt que sur `evenement_declencheur` (nullable) ; le filtre d'horizon de la *Chronologie des
  relevés* passe par le même helper `_horizon_expr` que `pipeline_historique` (cohérence).

### ⚠️ Changements de sortie (endpoints `/flux`, assumés)

- `/flux/r64`, `/flux/r151` servent désormais des **instants** (`TIMESTAMPTZ`) ; `/flux/r151` sert
  l'instant **J+1** harmonisé (endpoint déprécié — l'aval consomme le mart `releves`).
- `/flux/f15` sert `date_facture`/`date_debut`/`date_fin` en **`DATE`** (jour civil), plus en instant.
- Empreinte canonique `/releves` **stable** ; `releve_id` inchangés ; instants exacts préservés.

## [3.4.0rc1] - 2026-06-21

Fin de la **descente d'assemblage d'ADR-0041** : le cœur consomme, dbt assemble. La branche
abonnement bascule sur la spine de la *Chronologie du contrat* et `pipeline_historique` est
rétréci à son strict minimum.

### ✨ Temps forts

- **Abonnement sur la spine + `pipeline_historique` rétréci** (ADR-0041, #378). `pipeline_historique`
  ne ré-assemble plus rien : il lit la **spine** (mart `spine_contrat`, loader `spine()`), déjà
  forward-fillée et augmentée de la grille FACTURATION mensuelle **en dbt** (#375), et ne fait
  plus que (1) **filtrer l'horizon** (`date_evenement <= horizon`, l'horizon reste un *filtre* —
  pureté #179) et (2) **détecter les ruptures d'abonnement**. Les bornes FACTURATION sont forcées
  à `impacte_abonnement=True` dans `detecter_points_de_rupture` (bornes de période) ;
  `expr_impacte_abonnement` reste une détection pure.
- **Cœur allégé** : retrait de la génération FACTURATION (`inserer_evenements_facturation` & co.),
  du forward-fill de situation, et de toute la détection énergie (`expr_impacte_energie`,
  `expr_changement_index`, `expr_changement_avant_apres`) — désormais portés par dbt (spine +
  *Chronologie des relevés*). Le modèle `Historique` est **rétréci** : plus d'`impacte_energie`
  ni de colonnes d'index/calendrier/relevé (`avant_*`/`apres_*`), il garde l'épine + la situation
  + l'enrichissement abonnement. Le build (`contexte_du_mois`/`charger`/`_composer`) branche le
  loader `spine()` ; le loader `c15` n'est plus tiré pour la branche abonnement.
- **Parité** : abonnements + facturation mensuelle **inchangés** (snapshots d'abonnement
  byte-identiques). Seul écart attendu, le fix `month_start` de bord de mois hérité de la spine
  (#380) : une borne FACTURATION manquante apparaît dans le cas-limite → la période devient
  facturable.

### 🧹 Coulisses

- ~2700 lignes retirées (cœur + tests des fonctions d'assemblage supprimées). Parité circulaire
  spine ↔ `pipeline_historique` retirée de `test_dbt_spine_contrat` (miroir de #377) ; le bug
  `month_start` reste documenté en test pur-spine. 883 tests verts.

## [3.3.0] - 2026-06-20

Deux chantiers de fond : la **trace d'index légale** exposée à Odoo (ADR-0038) et la **bascule
de chiffrement AES-256** d'Enedis menée à bout jusqu'en prod (ADR-0040) ; plus la correction
d'un attribut de situation périmé sur le mart `releves` (ADR-0039).

### ✨ Temps forts

- **Schéma de déchiffrement AES à IV préfixé** (ADR-0040, #370). Le premier vrai fichier
  AES-256 d'Enedis a révélé que la bascule AES-128 → AES-256 n'est **pas** « le même schéma,
  clé plus longue » (prémisse d'ADR-0037, corrigée) : Enedis ne livre que la **clé** (64 hex),
  **sans IV** — l'IV est les **16 premiers octets de chaque fichier**, en clair, frais par
  fichier (pattern AES-CBC canonique). Le trousseau distingue les schémas par la **présence
  d'IV** : une entrée **avec** `__IV` ⇒ schéma **IV-fixe** (AES-128 legacy, IV en config) ;
  **sans** `__IV` ⇒ schéma **IV-préfixé** (AES-256, IV lu en tête de fichier).
  `decrypt_with_key_chain` route par essai sur la présence d'IV ; `decrypt_file_aes` (primitif +
  oracle PKCS7/ZIP) est inchangé ; les deux schémas coexistent sans faux positif croisé.
  `PaireCles.iv` devient optionnel, et toute la chaîne de déploiement (validateur `.env`,
  gabarit, guide de rotation, README, docstrings) cesse d'exiger ou de montrer un `__IV` pour
  AES-256. **Migration prod #354 menée à bout** : resync OK, R64 (AES-256) déchiffré en prod.
- **Trace d'index légale exposée à Odoo** (ADR-0038, #359/#360). `GET /facturation/meta-periodes`
  porte un tableau `releves_utilises` par méta-période — les relevés bornant le mois, objet
  `{ releve_id, date_releve, nature_index, registres réels }` — pour qu'Odoo tire et stocke la
  *Traçabilité des index* (exigence légale + espace usager). Les **7 registres canoniques**
  ressortent (`base`/`hp`/`hc` C5 **et** les 4 quadrants `hph`/`hch`/`hpb`/`hcb` C4/Tempo,
  source unique `cadrans.py`), limités aux registres réellement présents sur le compteur (le mart
  ne synthétise jamais). Chaque relevé porte `origine_releve` (`périodique` R151/R64 vs
  `événementiel` C15) et, pour un événementiel, le code `evenement` (ex. `MCT`). Invariant
  plein-ou-rien : non vide ⟺ `qualite ∈ {réelle, estimée}`, `incalculable ⟹ []` ; les relevés
  intermédiaires d'un mois à MCT figurent. `source_hash` étendu au tableau : une dérive d'index
  imprimé / nature / identité flippe le hash **même à delta kWh constant**. `releve_id` passe en
  **hash court** (`substr(md5(...), 1, 16)`) — encodage seul, identité (ADR-0028) inchangée.

### 🐛 Corrections

- **Attributs de situation hors du mart `releves`** (ADR-0039, #365). Le mart ne **recopie plus**
  (forward-fill par PDL) `ref_situation_contractuelle` / `formule_tarifaire_acheminement` /
  `niveau_ouverture_services` sur les relevés périodiques : la recopie périmait dès qu'un attribut
  changeait sur un événement C15 **sans index** (`MDPRM` de niveau, jamais un relevé car
  `int_releves__c15` gate `index is not null`). Bug prod (RSC `834877952`) : la borne du 01/04
  héritait niveau 0 du dernier C15 indexé alors qu'un `MDPRM` du 16/03 l'avait relevé à 2 → mois
  `réelle` faussement `non_communicante`. Désormais un relevé C15 garde sa valeur **native**, un
  télérelevé périodique reste `null`, et la chronologie source le niveau (comme déjà RSC/FTA)
  depuis la requête FACTURATION du cœur. `/releves` sert des **lectures pures**.

### ⚠️ Contrat

- **`CONTRAT_VERSION` 2 → 3** sur `/facturation/meta-periodes` : évolution **additive stricte**
  (enveloppe + pagination intactes, colonnes existantes inchangées) — un consommateur tolérant
  survit. `docs/contrat-meta-periodes.md` décrit le bloc `releves_utilises` + l'invariant.

## [3.1.0] - 2026-06-19

Trousseau de clés AES (ADR-0037) : déverrouille l'ingestion bloquée depuis la bascule
Enedis **AES-128 → AES-256 (8-9 juin 2026)** et met fin à l'échec de déchiffrement silencieux.

### ✨ Temps forts

- **Trousseau de clés AES N-clés** (ADR-0037, #352) : le domaine `aes` du registre runtime
  porte un `trousseau: dict[str, PaireCles]` de taille arbitraire, alimenté par
  `AES__TROUSSEAU__<label>__{KEY,IV}`. La bonne clé est **sélectionnée par essai** (oracle
  PKCS7 + magic bytes ZIP), sans date ni protocole — AES-128 et AES-256 sont le même schéma,
  la longueur de clé est auto-sélectionnée. Le `<label>` parlant remonte dans les logs.
  Supersède la cascade à deux clés d'[ADR-0008](docs/adr/0008-rotation-cles-aes.md).
- **Escalade d'échec de déchiffrement per-flux** (ADR-0037, #353) : fin du *fail silencieux*.
  `crypto.py` n'avale plus l'échec ; le runner agrège succès/échec **par flux**, et un flux qui
  a des fichiers mais **0 déchiffrement réussi** fait passer le job à `failed` → la surveillance
  bot alerte (chaîne existante). Un échec isolé (fichier corrompu) reste toléré, compté, warn-loggé.

### ⚠️ Ruptures

- **Format `.env` des clés AES** : `AES__CURRENT__*`, `AES__PREVIOUS__*` et le plat-v1
  `AES__KEY` / `AES__IV` sont **retirés** au profit de `AES__TROUSSEAU__<label>__{KEY,IV}`
  (rupture assumée, instance unique — ADR-0015). La compat de *données* est préservée : les
  anciennes clés AES-128 deviennent des entrées labellisées du trousseau, l'archive historique
  reste déchiffrable. Migration opérateur (réécriture du `.env` + resync) : **#354**.
  Le validateur de déploiement (`deploy/lib/env_validate.sh`) attend désormais le format trousseau.

### 🐛 Corrections

- **Message de l'ETL test de déploiement honnête** : l'étape `ETL test` (`mode test`)
  affichait « clés AES OK » alors qu'elle n'échantillonne que 2 fichiers dans l'ordre de
  listing (non trié par date) — sur un état dlt vierge, des fichiers anciens (AES-128) que
  n'importe quelle clé legacy déchiffre. Un trou de clé courante (ex. AES-256 manquante)
  passait donc le test au vert. Le message reflète désormais ce qui est réellement prouvé
  (chaîne SFTP→déchiffrement→DuckDB OK sur un échantillon) et invite à un **resync** pour
  valider la couverture du trousseau. Correctif message uniquement (`install.sh`,
  `lib/ingestion.sh`), pas de changement de comportement.

## [3.0.0] - 2026-06-18

Premier stable de la ligne **3.0** : aboutissement du cycle rc1→rc17. Stabilise sur données
réelles la réécriture ELT (dbt, ligne 2.0) et **pose un modèle de relevés canonique**, ouvre
electricore comme **source de vérité dont l'ERP _tire_** (méta-périodes, TURPE variable), et
durcit le déploiement. Détail rc-par-rc plus bas.

### ✨ Temps forts

- **Modèle de relevés canonique en dbt** (`releves` — ADR-0028/0029/0032) : ligne de temps
  unique des relevés, union arbitrée **C15 > R64 > R151** assemblée à la source, clé métier
  déterministe `releve_id`, dépivot des index contractuels C15, adapters conformés + macro de
  contrat. Fonde la traçabilité des index jusqu'à la facture (`releves_utilises`). Index
  normalisés **Wh→kWh** (floor entier) au boundary dbt (ADR-0034).
- **electricore source de vérité, l'ERP tire** : `GET /facturation/meta-periodes` (ADR-0027 —
  Odoo construit ses périodes depuis ce flux, `source_hash` pour upsert non destructif) et
  `POST /facturation/turpe-variable` (ADR-0030 — calculateur sans état). Routers
  ERP-agnostiques, JSON enveloppé versionné, `X-API-Key`.
- **Mart `releves` exposé hors `/flux`** : `GET /releves` (JSON paginé + `.xlsx` + `.arrow` +
  `/info`), filtres prm/source/fenêtre, `client.releves()` (ADR-0032).
- **Affaires SGE (X12/X13)** : linéarisation `flux_affaires` (grain = jalon, dédup des snapshots
  cumulatifs) + cockpit read-only `GET /perimetre/affaires` et vue bot (#275/#276).
- **Verdicts jumeaux qualité + communication** (ADR-0033/0036) : `qualite`
  (réelle/estimée/incalculable, rollup pire-gagne) et `statut_communication` (communicante/non)
  remplacent les anciens flags de complétude.
- **Configuration runtime centralisée** (#141, ADR-0024/0025) : lecteur unique
  pydantic-settings par domaine, précédence env > `.env`, validation fail-fast par point d'entrée.
- **Durcissement VPS** (ADR-0031) : utilisateur ops, sshd root-off, fail2ban,
  unattended-upgrades, `harden.sh` / `unharden.sh`.
- **Socle property-based testing** (#194–#197) : stratégies Hypothesis dérivées des schémas
  Pandera + invariants de conservation (TURPE, taxes, facturation, énergie).

### ⚠️ Ruptures (cumul du cycle — détail par rc)

- **`etl` → `ingestion`** : package, CLI (`python -m electricore.ingestion`), extra
  (`--extra ingestion`), routes API (`/ingestion/*`, anciens `/etl/*` en 404), bot, compose (rc1).
- **Clés AES en variables d'environnement uniquement** (retrait `.dlt/secrets.toml`),
  `API_BASE_URL` retiré, `env.py` supprimé (rc1).
- **Contrat `/facturation/meta-periodes` v2** : retrait de `data_complete` / `coverage_*` au
  profit de `qualite` + `statut_communication` ; `CONTRAT_VERSION` 1 → 2 (rc14).
- **API loaders legacy retirée** (`load_historique` / `load_releves`) au profit des query
  builders + `charger_*` (rc1).

### 🐛 Stabilisation prod (rc4→rc17)

Éprouvé sur le corpus réel : OOM dbt sur `flux_r64` / `flux_r151` (threads + spill disque),
index R151/R64 corrigés (Wh→kWh ÷1000, rc9), parsing des attributs XML (affaires `statut`),
l'épic **#332** de régressions de validation/schéma sur endpoints prod (`/flux/r64`, exports
Accise, facturation, puis le frère accise `ge=0` #341) — même classe « schéma plus étroit que la
donnée prod », chacune avec un test de garde sur donnée prod-réaliste — et la **stabilité du
namespace d'état incrémental** (#346, les runs mono-flux ne re-téléchargent plus tout) avec le
nettoyage des outils d'ingestion périmés post-bascule legacy→dbt (#345, rc17).

> **⚠️ Migration** : le chemin énergie requiert le mart dbt `releves`. Sur une base existante,
> **relancer `dbt build`** (pipeline d'ingestion) pour matérialiser `flux_enedis.releves` avant
> de calculer la facturation.

## [3.0.0rc17] - 2026-06-18

Stabilise l'**état incrémental de l'ingestion** (raw JSON) et aligne les outils de
diagnostic hérités de la bascule legacy→dbt (ADR-0020).

### 🐛 Corrigé

- **Incrémental : re-téléchargement complet sur un run mono-flux** — le curseur dlt vit
  sur la resource `filesystem` interne, non liée à la `@dlt.source` ; sa clé d'état se
  résolvait sous un namespace variable selon le nombre de flux du run (`ingestion <flux>`
  seul → nom de la source, `ingestion all` → nom dérivé du pipeline). Deux namespaces : un
  run mono-flux repartait d'un curseur vide et **re-téléchargeait tout le flux** (dédupliqué
  par le merge sur `file_name`, donc sans duplication de lignes, mais re-fetch +
  re-déchiffrement + re-parse intégral). Le `source_name` de la resource filesystem est
  désormais épinglé → mono et multi partagent un namespace unique ; test paramétré de garde
  ([#346](https://github.com/Energie-De-Nantes/electricore/issues/346)).

  > _Déploiement : aucune action requise. Le premier `ingestion all` ré-aligne les curseurs
  > en re-listant le SFTP pour les flux concernés et **merge** dans les `raw_*` existants —
  > sans perte ni duplication, un run un peu plus lourd (≈ fenêtre de rétention SFTP), puis
  > incrémental normal. Ne pas utiliser `resync` (il droppe les `raw_*` → perte de
  > l'historique aged-off du SFTP)._

### 🧹 Nettoyage (outils d'ingestion)

- **Outils alignés post-bascule (ADR-0020)** — `check_incremental_state` résout le vrai
  pipeline (`flux_brut_<stem>`) et expose tous les namespaces d'état (au lieu de pointer le
  pipeline legacy `flux_enedis` supprimé) ; suppression des outils morts `debug_single_flux`
  (import cassé) et `comparaison_bases` (échafaudage de bascule) ; `diagnostic_flux` réduit à
  une découverte SFTP read-only et `reset_incremental_state` repointé sur le dataset `flux_raw`
  ([#345](https://github.com/Energie-De-Nantes/electricore/issues/345)).

## [3.0.0rc16] - 2026-06-18

Suite de l'épic [#332](https://github.com/Energie-De-Nantes/electricore/issues/332) :
déverrouille les exports Accise restés en **503** après `rc15` — frère du
[#334](https://github.com/Energie-De-Nantes/electricore/issues/334), même classe
« schéma plus étroit que la donnée prod ».

### 🐛 Corrigé

- **Exports Accise (503 `energie_kwh ≥ 0`)** — `pipeline_accise` somme les lignes de
  factures Odoo par (PDL, mois) ; un avoir ou une régularisation peut rendre un mois
  net-négatif, ce qui violait `AcciseMensuel`. Le `ge=0` est retiré sur
  `energie_kwh`/`energie_mwh`/`accise_eur` (le grain mensuel peut être < 0 ; la déclaration
  accise est trimestrielle et nette positif) ; `taux_accise_eur_mwh` le conserve. Band-aid
  assumé imparfait — la correctness exacte de l'assiette relève du modèle de facturé maîtrisé
  (discovery [#225](https://github.com/Energie-De-Nantes/electricore/issues/225), refonte
  [#282](https://github.com/Energie-De-Nantes/electricore/issues/282))
  ([#341](https://github.com/Energie-De-Nantes/electricore/issues/341)).

## [3.0.0rc15] - 2026-06-18

Correctifs des **régressions de validation/schéma** détectées sur les endpoints prod en
`rc14` (épic [#332](https://github.com/Energie-De-Nantes/electricore/issues/332)) — même
classe « schéma/loader plus étroit que la donnée prod ». Chaque correctif embarque un test
de garde anti-dérive sur donnée prod-réaliste.

### 🐛 Corrigé

- **`/flux/r64` (500/503)** — `SCHEMA_R64` déclarait des colonnes (`modification_date`,
  `_source_zip`, `_flux_type`, `_json_name`) qu'aucun mart `flux_r64` ne projette ; alignement
  du schéma loader sur les colonnes réelles du mart
  ([#333](https://github.com/Energie-De-Nantes/electricore/issues/333)).
- **Exports Accise (503 `pdl null`)** — `pipeline_accise` agrégeait les lignes Odoo sans
  `x_pdl`, créant un bucket `pdl=null` qui violait `AcciseMensuel` ; exclusion des lignes sans
  PDL de l'assiette ([#334](https://github.com/Energie-De-Nantes/electricore/issues/334)).
- **Facturation rapport/detail/documents (503)** — les catégories produit hors scope
  facturation legacy (`Prestation-Enedis`, racine Odoo `All`) faisaient échouer la validation ;
  écartées avant rapprochement
  ([#335](https://github.com/Energie-De-Nantes/electricore/issues/335)).

## [3.0.0rc14] - 2026-06-18

Finalisation de la **refonte des flags de qualité** : retrait cassant des anciens signaux
de complétude au profit des verdicts jumeaux *qualité* + *communication*.

### ⚠️ Cassant — contrat `/facturation/meta-periodes` v2

- **Retrait de `data_complete` / `coverage_abo` / `coverage_energie`**
  ([#317](https://github.com/Energie-De-Nantes/electricore/issues/317),
  [#327](https://github.com/Energie-De-Nantes/electricore/issues/327),
  [#278](https://github.com/Energie-De-Nantes/electricore/issues/278),
  [ADR-0033](docs/adr/0033-qualite-periode-remplace-data-complete-coverage.md)) — remplacés
  par `qualite` (`réelle` / `estimée` / `incalculable`, rollup *pire-gagne*) et
  `statut_communication` (`communicante` / `non_communicante`), déjà livrés en additif
  (rc précédentes). `CONTRAT_VERSION` **1 → 2**. Raffinement strict : l'ancien
  `data_complete=True` se scinde en `{réelle, estimée}`, le `False` devient `incalculable`.
  Retrait de bout en bout — modèles (`PeriodeEnergie`, `PeriodeMeta`, `EnergieMensuel`,
  `LignesFactureRapprochees`…), pipeline facturation, contexte mensuel, contrat API et
  notebooks rebranchés sur `qualite`.

### 🧹 Nettoyage (core)

- **Retrait du forward-fill RSC/FTA no-op de la chronologie des relevés**
  ([#330](https://github.com/Energie-De-Nantes/electricore/pull/330),
  [ADR-0029](docs/adr/0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md)) :
  `_assembler_chronologie` ré-attribuait RSC/FTA par PDL, mais l'attribution est déjà
  garantie en amont (mart `releves` forward-fillé + requête FACTURATION qui porte la RSC).
  No-op prouvé par deletion-test (suite verte) ; l'attribution contractuelle vit désormais
  en un seul endroit.

## [3.0.0rc13] - 2026-06-16

Hotfix rc12 : `/facturation/meta-periodes` renvoyait une 500 — `RelevéIndex` exigeait
`unite`/`precision` que le modèle de relevés canonique ne porte pas.

### 🐛 Correctif (core)

- **Retrait de `unite`/`precision` du contrat `RelevéIndex`** : vestiges de l'ère Wh,
  rendus inutiles par [ADR-0034](docs/adr/0034-index-kwh-entiers-floor-au-boundary-dbt.md)
  (tout est en kWh entiers — le grain facturable atomique). Le modèle de relevés canonique
  `releves` (#248) ne les a jamais produits → `pipeline_energie`, qui valide son entrée
  contre `RelevéIndex`, levait `SchemaError: column 'unite' not in dataframe` = 500 sur
  `/facturation/meta-periodes`. Bug **pré-existant** (depuis la bascule mart canonique),
  révélé en testant rc12. Régression couverte par un test sur la **forme réelle du mart**
  (les anciens tests, alimentés en frames déjà conformes, ne traversaient pas ce chemin).
  Les loaders `/flux` conservent leur colonne `unite` (tolérée hors contrat, `strict=False`).

## [3.0.0rc12] - 2026-06-16

Hotfix rc11 : l'ingestion de prod échouait au `dbt build` — l'adapter intermédiaire
`int_releves__c15` introduit en rc11 n'était pas construit par le runner.

### 🐛 Correctif (ingestion)

- **Le runner construit `int_releves__c15`** ([#304](https://github.com/Energie-De-Nantes/electricore/issues/304)) :
  `construire_dbt` sélectionnait `releves` nu (sans ancêtres), s'appuyant sur le fait que
  les ancêtres de `releves` étaient déjà couverts par les `+flux_*`. rc11 a introduit
  l'adapter intermédiaire `int_releves__c15` — un ancêtre de `releves` qui **n'est pas** un
  `flux_*` → non construit → `Catalog Error: Table int_releves__c15 does not exist` au
  `dbt build` de prod. Le runner sélectionne désormais `+releves` (graph operator tirant
  tous les ancêtres). Régression couverte par un test qui exerce la **vraie** sélection du
  runner — le golden utilisait `+releves` codé en dur et ne traversait pas ce chemin.

## [3.0.0rc11] - 2026-06-16

Approfondissement du modèle de relevés canonique : l'assemblage multi-sources passe d'une
union bouchée à des adapters conformés. Comportement préservé, deux corrections de fidélité
incluses — RC à éprouver sur données réelles.

### ♻️ Refactor (ingestion dbt)

- **`releves` assemblé par adapters conformés + macro conformer** ([#304](https://github.com/Energie-De-Nantes/electricore/issues/304)) :
  l'union à quatre branches bouchées (`cast(null)` répété par source) devient des adapters
  étroits (CTE R151/R64 + modèle intermédiaire `int_releves__c15` pour le dépivot C15) plus un
  macro `conformer_au_contrat_releve()` qui porte le contrat de colonnes en un seul endroit et
  remplit explicitement les manques par source. Comportement préservé (diff `EXCEPT` origin/main
  vs branche = 0 ligne sur les 16 colonnes partagées), prouvé par le golden `releves`. Nouvelle
  couche dbt `intermediate` (vues) ; tests de contrat ajoutés (`accepted_values` source, `not_null` pdl).

### 🐛 Corrections de fidélité (incluses dans #304)

- **R64 porte son calendrier distributeur** : `flux_r64` filtrait sur `id_calendrier` (DI00000X)
  puis le jetait ; `releves` codait `id_calendrier_distributeur = NULL` pour R64. Désormais conservé
  — le cœur en dérive précision et cadrans ([`RelevéIndex`](electricore/core/models/releve_index.py)).
- **`id_releve` natif retiré du contrat canonique** : toujours NULL pour les trois sources vivantes,
  aucun consommateur ne le lit. La traçabilité repose sur `releve_id` (clé métier) + `occurrence_id`
  (provenance). `flux_r15`/`flux_r15_acc` le conservent. Suivi :
  [#305](https://github.com/Energie-De-Nantes/electricore/issues/305) (réconciliation R15).

### 🔧 Dépendances

- Bump aiohttp 3.14.0 → 3.14.1 (Dependabot).

## [3.0.0rc10] - 2026-06-16

Robustesse des affaires SGE et lisibilité des échecs d'ingestion — deux correctifs
révélés en déployant rc9 sur données réelles.

### 🐛 Correctifs (ingestion + core)

- **`flux_affaires.statut` nullable + cockpit « en attente Enedis »** ([#296](https://github.com/Energie-De-Nantes/electricore/issues/296)) :
  Enedis envoie un `<statut>` vide (→ null) pour une affaire fraîchement initiée qu'il n'a
  pas encore rangée (jalon 0, demande transmise, pas d'objet) ; le `not_null` faisait échouer
  `dbt build` en prod (45 lignes) → job d'ingestion en échec. `statut` devient nullable (fidèle
  à la source, [ADR-0029](docs/adr/0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md)),
  et `affaires_ouvertes` traite null comme **ouvert** au même titre que `COURS` — les demandes
  tout juste déposées apparaissent au cockpit avec leur dernier état. rc8 n'avait couvert que le
  cas `<statut code="X"/>` (attribut sur feuille), pas le `<statut>` vide.
- **Job d'ingestion : la vraie erreur au lieu d'un « exit code 1 » nu** ([#298](https://github.com/Energie-De-Nantes/electricore/issues/298)) :
  dlt/dbt loguent leurs diagnostics sur stdout. Un job en échec capture désormais cette sortie
  (`output` dans tous les cas, `error` = stderr sinon le tail de stdout) au lieu de la jeter —
  un échec d'ingestion devient lisible au niveau job/API. Suivi : [#299](https://github.com/Energie-De-Nantes/electricore/issues/299)
  (le step « ETL test » de l'installeur doit poller l'issue du job au lieu de verdir sur le 202).

### 🔧 Dépendances

- Bump starlette 1.2.1 → 1.3.1, cryptography 48.0.0 → 48.0.1 (Dependabot).

## [3.0.0rc9] - 2026-06-16

Correctif de correctness : les index R151/R64 étaient ~1000× trop grands dans le mart
relevés canonique (Wh stockés dans des colonnes `index_*_kwh`, mélangés aux index C15
nativement en kWh) — corrompant énergie, TURPE variable, accise et facturation.

### 🐛 Correctifs (ingestion + core)

- **Normalisation des index Wh→kWh au boundary dbt** ([#285](https://github.com/Energie-De-Nantes/electricore/issues/285), [ADR-0034](docs/adr/0034-index-kwh-entiers-floor-au-boundary-dbt.md)) :
  R151 et R64 sont livrés en **Wh** par Enedis ; la conversion vivait dans les transforms
  Polars du loader et a été perdue à la bascule relevés canoniques (rc7, [#248](https://github.com/Energie-De-Nantes/electricore/issues/248)).
  Elle descend dans la **linéarisation dbt** (`flux_r151`/`flux_r64` : `floor(valeur/1000)`
  par index → kWh entier, `unite='kWh'`), honorée à toute couche (mart, API `/flux`). Retrait
  du convertisseur loader (sinon double-division des endpoints `r151()`/`r64()`) et du no-op
  d'arrondi en cœur. Garde-fous dbt (tests singuliers) : ces modèles n'émettent **jamais**
  `'Wh'`. Floor par index sûr — l'erreur télescope, bornée **< 1 kWh** sur la vie d'un registre
  (~< 0,20 €) ; parité `/releves` rc7 préservée. Golden `flux_r151`/`flux_r64` régénérés (÷1000).
- Suivi : [#286](https://github.com/Energie-De-Nantes/electricore/issues/286) — vérifier l'unité
  native de R15 (`col_literal('kWh')` masque un bug Wh latent éventuel ; R15 hors mart `releves`).

## [3.0.0rc8] - 2026-06-16

Correctif d'ingestion : `ingestion all` échouait en prod sur le data test `not_null`
de `flux_affaires.statut` (rc7).

### 🐛 Correctifs (ingestion)

- **`xml_vers_dict` garde les attributs des feuilles** : en données réelles, certains
  `<statut code="COURS"/>` n'ont pas d'enfant `<libelle>` (feuille porteuse d'attribut) ;
  le parseur ne capturait les attributs que sur les *conteneurs* → `@code` perdu →
  `statut` null → `not_null` KO → `dbt build` KO → job d'ingestion en échec. Une feuille
  à attribut devient désormais un nœud `{"@code": …}` listé (même forme que le conteneur,
  accès `statut[0]."@code"` uniforme) ; une feuille sans attribut reste scalaire (zéro
  régression element-only). Vérifié sur le corpus réel EDN : 0 `statut` null (était 14).
- Les **commentaires / PI XML** (`.tag` non-`str`) sont ignorés au parsing (sinon clé de
  dict non sérialisable au landing JSON).

## [3.0.0rc7] - 2026-06-16

Suivi opérationnel des **affaires SGE** (flux X12/X13) de bout en bout, parité de
formats de l'endpoint `/releves`, et durcissement du déploiement VPS.

### ✨ Nouveautés (ingestion — affaires SGE)

- **Flux X12/X13 → `flux_affaires`** ([#275](https://github.com/Energie-De-Nantes/electricore/issues/275)) :
  linéarisation des affaires SGE (cycle de vie des demandes de prestation). X12 (initiées)
  et X13 (reçues) partagent une source `raw_affaires` unique ; `origine` dérivée du nom de
  fichier. Grain = un *jalon*, dédupliqué sur la clé logique `(affaire_id, jalon_num)` — les
  flux quotidiens sont des snapshots cumulatifs ([ADR-0028](docs/adr/0028-identite-releve-cle-metier-priorite-sources.md)).
  Golden X12 + X13 + test de dédup + data tests `not_null`/`unique`.
- **`xml_vers_dict` capture désormais les attributs XML** (clés `@`) : X12/X13 est le premier
  flux à porter des données en attributs (id d'affaire, codes statut/objet/état) ; sans
  régression sur les flux element-only existants.

### ✨ Nouveautés (cockpit affaires — core/api/bot)

- **Cockpit des affaires non soldées** ([#276](https://github.com/Energie-De-Nantes/electricore/issues/276)) :
  loader `affaires()` + rollup **read-time** `affaires_ouvertes` (un statut COURS par affaire,
  dernier état, ancienneté = maintenant − 1ᵉʳ jalon, jamais matérialisée). `AME` (souscription
  de flux ≈ 45 % du volume) écartée par défaut. Endpoint `GET /perimetre/affaires`
  (JSON, `?origine`, `?inclure_ame`) + vue bot `/perimetre affaires`. **Lecture seule.**

### ✨ Nouveautés (api — relevés canoniques)

- **Parité de formats `/releves`** ([#263](https://github.com/Energie-De-Nantes/electricore/issues/263)–[#265](https://github.com/Energie-De-Nantes/electricore/issues/265),
  [ADR-0032](docs/adr/0032-modeles-marts-hors-flux-namespace.md)) : `GET /releves.arrow`
  + `client.releves()`, JSON enveloppé paginé + XLSX + `/releves/info`, et contrat de filtres
  (prm + source + fenêtre de dates). Mart servi hors du namespace `/flux/*`.

### 🏗️ Déploiement (durcissement VPS — [ADR-0031](docs/adr/0031-durcissement-ssh-vps-utilisateur-ops.md))

- Utilisateur ops + échafaudage `harden_vps` ([#258](https://github.com/Energie-De-Nantes/electricore/issues/258)),
  sshd root-off + garde-fou anti-verrouillage ([#259](https://github.com/Energie-De-Nantes/electricore/issues/259)),
  fail2ban ([#260](https://github.com/Energie-De-Nantes/electricore/issues/260)),
  unattended-upgrades + auto-reboot ([#261](https://github.com/Energie-De-Nantes/electricore/issues/261)),
  script autonome `harden.sh` + toggles ([#262](https://github.com/Energie-De-Nantes/electricore/issues/262)),
  et chemin de réversion `unharden.sh`.

### 🧹 Nettoyage (loaders)

- Factory `flux(name)` derrière l'interface du loader ([#272](https://github.com/Energie-De-Nantes/electricore/issues/272),
  [#273](https://github.com/Energie-De-Nantes/electricore/issues/273)) ; équivalence de requête
  testée plutôt qu'identité de config.

## [3.0.0rc6] - 2026-06-15

Calculateur TURPE variable (Odoo fournit l'assiette) et fin de la bascule relevés
canoniques (nettoyage de l'ancien chemin + garde de non-régression restaurée).

### ✨ Nouveautés (api)

- **`POST /facturation/turpe-variable`** ([ADR-0030](docs/adr/0030-calculateur-turpe-variable-odoo-fournit-assiette.md)) :
  calculateur **sans état** où Odoo POST l'assiette (énergies par cadran + FTA + `debut`)
  et electricore renvoie le **montant** € (`Σ energie_cadran × c_cadran(FTA, debut) / 100`).
  Lot + `id` opaque ré-émis, **succès partiel** par ligne (montant *xor* motif d'erreur :
  FTA inconnue / aucune règle pour la date), 7 cadrans passés et arbitrage par les zéros
  de la règle FTA. JSON enveloppé (`contract_version`/`results`), auth `X-API-Key`,
  ERP-agnostique. Contrat figé : [docs/contrat-turpe-variable.md](docs/contrat-turpe-variable.md).

### 🧹 Nettoyage (relevés canoniques, suite [ADR-0029](docs/adr/0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md))

- Retrait de l'ancien chemin relevés (zéro consommateur production) : `extraire_releves_evenements`,
  loaders `releves()` (R151+R15) / `releves_harmonises()` (R151+R64) + leurs requêtes SQL et
  transforms (cascade morte incluse).
- **Renommage `releves_canoniques()` → `releves()`** : une seule façon de lire les relevés
  (modèle dbt canonique), le nom court est libéré. `transform_releves` (endpoints `/flux/r151`,
  `/flux/r15`) conservé ; notebooks migrés.

### 🧪 Tests & docs

- Garde de l'**invariant FTA** load-bearing du calculateur (une seule granularité de cadrans
  non-nulle par règle `turpe_rules.csv` — sinon double comptage silencieux).
- Harnais snapshot `test_pipelines_snapshot.py` réparé (était *stale* + *skip*) : composition
  `pipeline_historique → abonnements/energie` sur fixtures conformes, horizon figé
  (déterminisme), agrégé en `energie_*_kwh` ; **8 snapshots** générés et actifs.
- `core/CONTEXT.md` rafraîchi (chronologie lit le modèle dbt, contexte mensuel 4 → 5 frames).

## [3.0.0rc5] - 2026-06-15

Correctif mémoire du build dbt sur VPS contraint — le rebuild rc4 OOMait encore sur
`flux_r64` (unnest des points R64), bloquant la matérialisation du mart `releves`.

### 🐛 Corrections (ingestion)

- **OOM `flux_r64` / `flux_r151`** : l'opérateur fautif (unnest + agrégation par hachage)
  ne déverse pas sur disque ; son pic mémoire est proportionnel au **nombre de threads**.
  → `threads: 1` dans le profil dbt (à mémoire machine pleine). Validé sur données
  réelles : la chaîne complète **+ le mart `releves`** se construisent, tous les data
  tests passent (`unique_releves_releve_id`, `not_null_releves_*`). Batch nocturne :
  la lenteur mono-thread est acceptable.
- `releves` apparaît désormais au `📊 Bilan` du runner d'ingestion.

## [3.0.0rc4] - 2026-06-15

Correctifs du build dbt **sur données réelles** : le rebuild rc3 échouait sur le
corpus de production (fixtures trop étroites pour reproduire ces cas).

### 🐛 Corrections (ingestion)

- **`flux_r64` Out of Memory (~6 GiB)** : le tri de la fenêtre `qualify` (dédoublonnage
  des fenêtres R64 chevauchantes) saturait la RAM. `preserve_insertion_order: false`
  dans le profil dbt — DuckDB déverse sur disque (l'ordre des lignes matérialisées est
  sans importance pour l'aval).
- **`flux_r15.nature_index` not_null** : certains relevés R15 réels n'ont pas de
  `Nature_Index` → la macro renvoyait `NULL`. Absent/`NULL` → `estimé` (défaut prudent,
  ADR-0028).
- **Mart `releves` jamais construit par le runner** : descendant des flux, il n'était
  pas atteint par la sélection `+flux_*` (ancêtres). Ajouté à la sélection dbt dès que
  C15 + R151 + R64 sont présents — sans quoi `flux_enedis.releves` n'existe pas et la
  facturation échoue. Le runner **surface désormais les nœuds dbt en échec**.

## [3.0.0rc3] - 2026-06-15

Incrément de la candidate : **modèle de relevés canonique** et **traçabilité des
index** de bout en bout. Pose les fondations du futur affichage des index sur la
facture (#180 → #232 → #244 → #233, ADR-0029).

### ✨ Modèle de relevés canonique en dbt (ADR-0029)

Nouveau modèle dbt `releves` (`models/marts/releves.sql`) : la **ligne de temps des
relevés** consommée par l'aval, assemblée à la source.

- **Union** des sources (1 ligne = 1 relevé) : R151 (harmonisé J→J+1, ADR-0003) + R64
  + **relevés contractuels C15 avant/après dépivotés** (#241, #242).
- **Mint uniforme** de `releve_id` (clé métier déterministe, ADR-0028) pour **toutes**
  les sources, C15 comprise — l'exception « en core pour c15 » disparaît. `nature_index`
  canonique (réel/estimé/corrigé), `id_releve` (provenance), `occurrence_id` (forensique).
- **Enrichissement contractuel piloté par C15** : forward-fill RSC/FTA sur les relevés
  périodiques, en remplacement du `join_asof` incident sur les événements FACTURATION (#243).
- **Dedup même-source** (re-livraison) par `releve_id`, livraison la plus récente.
- `flux_r64` : PIVOT → agrégation conditionnelle (contrat de colonnes stable).

### ✨ Bascule cœur + journal des relevés utilisés (#244, #233)

- La *Chronologie des relevés* consomme `releves` via `releves_canoniques()` ; elle
  arbitre la priorité des sources (C15 > R64 > R151), sélectionne les bornes de
  facturation et flag les manquants. **Énergies inchangées** (parité vérifiée).
- `ChronologieReleves` porte `releve_id` + `nature_index`.
- `ContexteMensuel.releves_utilises` : **journal des relevés effectivement consommés**
  (registres réels + identité + nature), conservé pour la traçabilité jusqu'à la facture ;
  un MCT en cours de mois y figure sans cas particulier.

### 🐛 Corrections

- `releve_id` **déterministe** : les dates `timestamptz` (R15/C15) sont normalisées en
  `Europe/Paris` au mint — sinon la clé variait selon le fuseau de session (#232).

### 🔭 Suites tracées

Cycle de vie des relevés (correction/annulation) #240 ; nettoyage du dead-code de
l'ancien chemin #248 ; doc CONTEXT #249 ; réparation du harnais snapshot #250.

> **⚠️ Migration de données** : cette candidate ajoute le modèle dbt `releves`, désormais
> requis par le chemin énergie. **Relancer `dbt build` (pipeline d'ingestion)** pour
> matérialiser `flux_enedis.releves` avant de calculer la facturation sur une base existante.

## [3.0.0rc2] - 2026-06-13

Incrément de la candidate : ajoute l'**endpoint de lecture des méta-périodes**,
prérequis de la migration Odoo 19 (Odoo *tire* d'electricore au lieu du write-back
notebook). N'ajoute aucune rupture par rapport à rc1.

### ✨ Endpoint de lecture des méta-périodes (#231, ADR-0027)

`GET /facturation/meta-periodes` — un ERP tire les *méta-périodes mensuelles*
d'electricore (Odoo construit ses `souscription.periode` à partir de ce flux), au
lieu du write-back notebook. Router **ERP-agnostique** (zéro import
`integrations/odoo`, ADR-0016), JSON enveloppé (`mois` / `contract_version` /
`filters` / `pagination` / `data`), filtres `mois` + `rsc`, pagination, sécurisé
`X-API-Key`. Calcul à la volée, read-only vers Odoo (ADR-0012).

- **Charge utile** non valorisée aux prix fournisseur : quantités physiques
  (énergies `base`/`hp`/`hc`, jours, puissance, FTA), montants réseau **finaux**
  (`turpe_fixe_eur`, `turpe_variable_eur`, `cta_eur`), et `taux_accise_eur_mwh`
  (**taux seul** — l'accise *facturée* est calculée côté ERP : assiette = le
  facturé ; règle « montant € si electricore possède l'assiette, taux sinon »).
- **`source_hash`** (empreinte de contenu déterministe) pour un upsert **non
  destructif** côté ERP : skip-si-inchangé, détection de dérive sous verrou.
- Contrat figé documenté : `docs/contrat-meta-periodes.md`.
- Glossaire core affiné : *Accise physique* vs *Accise de déclaration*.

Périmètre **C5** (le détail 4 cadrans C4 sera un ajout de colonnes versionné).
Découvertes accise ouvertes : #225 (comptage de l'assiette), #226 (catégories de
taux).

## [3.0.0rc1] - 2026-06-13

Release **majeure**, candidate. Deux ruptures structurelles à valider au
déploiement avant de promouvoir en stable : la configuration runtime passe par un
registre unique (#141), et le module `etl` devient `ingestion`.

### ⚙️ Configuration runtime centralisée (#141, ADR-0024/0025)

Un lecteur unique — `electricore/config/runtime.py` (pydantic-settings) — avec un
`BaseSettings` indépendant par domaine (`sftp`, `aes`, `duckdb`, `api`, `bot`,
`odoo`), précédence env-système > `.env`, et validation **fail-fast par point
d'entrée** (`runtime.valider(...)`) : une variable obligatoire manquante arrête le
service au démarrage avec la liste des manquantes, au lieu d'échouer plus tard.

#### 💥 Breaking (déploiement)

- **Clés AES en variables d'environnement uniquement** : le support
  `.dlt/secrets.toml` est **retiré**. Format à plat (`AES__KEY`/`AES__IV`) ou
  imbriqué pour la rotation (`AES__CURRENT__KEY`/`AES__CURRENT__IV`,
  `AES__PREVIOUS__*`).
- **`API_BASE_URL` retiré** : le bot tourne dans le processus de l'API et la joint
  sur `localhost:8001`.
- **`env.py` supprimé** ; `charger_config_odoo()` réduit à une façade sur
  `runtime.odoo()`.

### ♻️ Renommage — le module `etl` devient `ingestion`

Le nom « ETL » décrivait une technique abandonnée (le procédé est ELT depuis la
bascule dbt, ADR-0020/0021) ; « ingestion » nomme la fonction et était déjà le
terme canonique de la doc. Changements cassants et transitions :

- **Package** : `electricore/etl/` → `electricore/ingestion/` ; CLI :
  `uv run python -m electricore.ingestion <all|test|rebuild|resync|flux…>` ;
- **Extra** : `uv sync --extra etl` → `uv sync --extra ingestion` (**cassant**) ;
- **Routes API** : `/etl/run`, `/etl/jobs` → `/ingestion/run`, `/ingestion/jobs`
  (**cassant**) ; les alias de transition `/etl/*` sont **retirés** (#193) et
  répondent désormais `404` — la crontab doit appeler `/ingestion/run`
  (`deploy/docker/crontab` l'est déjà) ;
- **Bot** : `/ingestion` remplace `/etl`, **sans alias** (#193) ; les callbacks
  `etl:*` des claviers postés avant le renommage ne routent plus ;
- **Compose** : service `etl-scheduler` → `ingestion-scheduler`, conteneur
  `electricore-etl` → `electricore-ingestion` (recréation au prochain pull).

### ✨ Taxes : millésime des taux régulés (#185, #186, #187)

Les taux régulés (accise/TICFE, CTA) sont datés et versionnés en CSV de référence ;
le cœur dérive le taux en vigueur à la date, l'API et le bot l'exposent. Une
surveillance proactive alerte sur Telegram quand un taux est présumé périmé. Un
formulaire d'issue « Nouveau taux régulé » ouvre la contribution aux non-techniciens.

### 🧪 Socle property-based testing (#194–#197)

Stratégies Hypothesis dérivées des schémas Pandera + invariants de conservation
(TURPE, taxes, facturation, abonnements/énergie). Tests uniquement, profil CI borné.

### 💥 Autres ruptures

- **API loaders legacy retirée** (#181) : `load_historique` / `load_releves`
  disparaissent au profit des query builders DuckDB + `charger_*`.

## [2.1.1] - 2026-06-12

### 🐛 Corrections

- **Check Odoo figé sur ⏳** (incident prod 2026-06-12) : `KeyError` dans le
  rendu des blocs « factures draft » et « lissés » — colonnes réelles du check
  (`name`, `name_account_move`, `categ_names`) alignées. Bug hérité du bot v1,
  latent tant qu'aucune facture draft n'existait.
- **Filet anti-⏳** : toute erreur de formatage/édition du check s'affiche
  désormais en `❌ …` dans Telegram au lieu de figer le message.
- **Garde 4096 caractères** : un résumé de check trop long pour Telegram est
  tronqué aux frontières de lignes et bascule automatiquement sur le XLSX de
  détail.

---

## [2.1.0] - 2026-06-11

### 🤖 Bot Telegram : surface par domaines hybrides (ADR-0022)

Refonte complète de la surface du bot (issues #150–#160) : 11 commandes plates →
**5 domaines métier** (`/etl`, `/flux`, `/perimetre`, `/taxes`, `/facturation`).
Sans argument, un domaine ouvre un **clavier inline** ; avec arguments, raccourci
power-user. Guide : [electricore/bot/README.md](electricore/bot/README.md).

#### 💥 Breaking

- **Rupture du contrat de commandes, sans alias** (big bang, ADR-0022) :
  `/status`, `/stats`, `/export`, `/entrees`, `/sorties`, `/check` disparaissent —
  absorbées respectivement par `/etl statut`, `/flux stats`, `/flux export`,
  `/perimetre entrees|sorties`, `/facturation check`. L'ancien `/flux` plat et
  `/facturation [date]` changent de forme (`/flux`, `/facturation documents [date]`).
- **`/etl reset` retiré** de la surface du bot (déprécié côté runner — `resync`
  le remplace, derrière une **confirmation à deux taps**).

#### ✨ Nouveau

- **Menu natif Telegram** (`setMyCommands`) publié au démarrage, **adapté à
  l'instance** : sans ERP configuré, `/taxes` et `/facturation` sont masqués et
  répondent un message explicite (P2.5, traduction bot d'ADR-0016).
- **Modes ETL réels exposés** : `rebuild`, `resync`, sélection de flux arbitraire
  (`/etl r151 c15`) — l'API `POST /etl/run` accepte désormais les listes de flux.
- **Suivi de job par édition** : le message de lancement s'édite
  (`⏳ running` → `✅`/`❌` + sortie) au lieu d'un second message.
- **Alertes proactives** : `TELEGRAM_NOTIFY_CHAT_ID` — alerte 🚨 sur tout job ETL
  `failed`, y compris ceux du scheduler nocturne.
- **Fraîcheur des données** : `/flux` stats affiche la dernière date *métier* par
  table (`GET /flux/{table}/info` expose `derniere_date`).
- Rendu **HTML partout** (fin du mélange Markdown V1/V2), descriptions des tables
  dans le menu `/flux`, `/start` annonce l'instance servie
  (convention `@<slug>_electricore_bot`).
- API : `GET /facturation/check/odoo.xlsx` (détail du check pré-facturation) —
  le bot redevient strictement client HTTP (garde-fou d'architecture en CI).

#### 🔧 Interne

- `bot.py` (monolithe) supprimé : package par domaine (`handlers/`), allowlist
  factorisée en décorateur, fakes Telegram partagés côté tests (+39 tests).

---

## [2.0.0] - 2026-06-11

### 🏗️ Ingestion ELT : la linéarisation des flux vit en dbt (ADR-0020 → ADR-0021)

Release **majeure** : le chemin d'ingestion maison (parseur Python piloté par le DSL
`flux.yaml`) est remplacé par une architecture ELT — dlt dépose les documents Enedis
**intégraux** en colonne JSON (`flux_raw`), dbt les linéarise en SQL (`flux_enedis`).
Parité record-par-record prouvée 3× (golden, cache local 4 400 XML, corpus SFTP
complet ~700 k lignes, 7/7 tables). Guide : [docs/ingestion.md](docs/ingestion.md).

#### 💥 Breaking

- **Historique re-matérialisé ≠ legacy là où le legacy avait tort** : la validation a
  corrigé 5 défauts latents — relevés agrégés par PDL (chimères inter-événements),
  ~75 % des index R15 mélangeant index/consommation, relevés multiples perdus (jusqu'à
  20/PRM sur R151), re-livraisons F15 double-comptées (261 lignes), gagnant R64
  arbitraire. Le grain des tables R15/R151 devient **le relevé**.
- **`pipeline_production.py` supprimé** → `pipeline_dbt.py` (modes `test`/`all`/
  sélection/`rebuild`/`resync` ; `reset` déprécié, alias de `resync`).
- **`flux.yaml` réduit au mouvement** (`file_pattern`/`format`/`file_regex`) — le DSL
  de sélection (`row_level`/`data_fields`/`nested_fields`) disparaît avec le moteur.
- L'image Docker embarque l'extra `dbt` ; premier run : nouvel état incrémental
  (re-téléchargement complet, ~15 min), tables `flux_enedis` remplacées par les
  versions typées.

#### ✨ Nouveau

- **Mode `rebuild`** : re-matérialiser toutes les tables depuis le brut, zéro réseau
  (~13 s pour 700 k lignes) — le geste standard après un changement de modèle.
- **Types à la source** (XSD Enedis) : TIMESTAMPTZ/DATE/BIGINT/DOUBLE portés par les
  tables ; instants ancrés `Europe/Paris` indépendamment du fuseau de session ;
  domaine de cadrans fermé (contrat de colonnes stable).
- **Dédup par construction** : re-livraisons Enedis (merge `file_name`), R64
  multi-fenêtres (gagnant déterministe = livraison la plus récente).
- **Filet d'ingestion en CI** : golden générés par le chemin de production, fixtures
  XSD maximales, data tests dbt + contrat de types.
- `docs/ingestion.md` (schéma Excalidraw + recettes), `docs/configuration.md`
  (inventaire complet), ADR-0020/0021.

#### 🧱 Architecture (revue du 11/06, depuis la rc1 — issues #142–#146)

- **Le livrable facturation descend en core** : assemblage dans
  `core/builds/rapport_facturation.py` + `contexte_mensuel.py`, I/O Odoo dans
  `integrations/odoo/sources.py`, wire-up dans `api/services/facturation_service.py` —
  `integrations/odoo/facturation.py` supprimé. Garde-fou CI en whitelist (ADR-0019
  règle 4) : `integrations/` n'importe de `core` que `models` et `loaders`.
- **Vraie passe-plat dans `rapprocher()`** : sortie = colonnes d'entrée + colonnes
  calculées, plus aucune colonne ERP nommée en core. ⚠️ le détail rapprochement gagne
  `est_brouillon` et change d'ordre de colonnes (l'ordre facturiste est porté par
  `feuilles_rapport_*`) ; collision de noms → erreur claire au seam.
- **`chemin_base_duckdb()`** : résolution unique du chemin de la base
  (`electricore/config/`) — `DUCKDB_PATH` (`.env` compris) honoré par les loaders,
  l'API, le runner dbt **et** les tools ; défaut absolu indépendant du CWD.
- **`contexte_du_mois(mois)`** : entrée I/O du contexte mensuel ; `charger(frames)`
  reste la composition pure (scindage prévu par ADR-0019).

#### 🔧 Corrections

- `DUCKDB_PATH` honoré par le runner (volume Docker) ; smoke-test release réaligné.

---

## [1.7.0] - 2026-06-07

### 🏗️ core/ ERP-agnostique + déploiement script-first + API épaisse

Release majeure structurée autour de deux ADR architecturaux et de la finalisation de l'« API épaisse » (notebooks de prod migrés en HTTP).

#### Architecture — ADR-0016 (`core/` ERP-agnostique)

- **ADR-0016** ([`docs/adr/0016-core-erp-agnostique.md`](docs/adr/0016-core-erp-agnostique.md)) — `core/` ne dépend plus d'aucun ERP. Tout l'Odoo migre vers `electricore/integrations/odoo/` (lecteurs, query builder, modèles Pandera, helpers, orchestrations).
- **Refactor `core/` → `integrations/odoo/`** — `OdooReader`, `OdooQuery`, `OdooWriter`, helpers (`commandes`, `factures`, `lignes_factures_du_mois`…) et modèles (`FactureOdoo`, `CommandeVenteOdoo`…) sortis de `core/`. Imports `from electricore.integrations.odoo import …` (breaking).
- **Orchestrations Odoo** — `rapprocher_facturation_mensuelle`, `calculer_cta_detail`, `accise_par_contrat` déplacées dans `integrations/odoo/`. `core/pipelines/` reste strictement Polars/DuckDB.
- **Test architectural** ([`tests/architecture/test_core_erp_agnostique.py`](tests/architecture/test_core_erp_agnostique.py)) — verrouille le contrat : aucun import Odoo détectable dans `core/`.
- **`integrations/odoo/decorators.py`** — décorateur `@with_odoo` qui encapsule l'ouverture/fermeture d'`OdooReader` au call-site et l'injecte en 1er argument. Services `taxes_service` / `facturation_service` deviennent des pass-through purs de sérialisation.
- **`api/serializers/`** — extraction des sérialiseurs XLSX, Arrow, ZIP hors des services pour les rendre réutilisables.
- **Glossaire / ADR / README / CLAUDE alignés** sur la nouvelle frontière `core/` ↔ `integrations/`.

#### Déploiement — ADR-0017 (script-first)

- **ADR-0017** ([`docs/adr/0017-deploiement-script-first.md`](docs/adr/0017-deploiement-script-first.md)) — layout `/srv/<INSTANCE_SLUG>/`, user dédié, bind-mount backups, `INSTANCE_SLUG` au cœur du provisioning.
- **`deploy/install.sh`** — installateur idempotent (mode fresh + mode `reconfigure`) avec helpers Bash modulaires dans [`deploy/lib/`](deploy/lib/). Tests unitaires + e2e (`deploy/tests/`).
- **Auto-refresh `lib/` en mode reconfigure** (#62) — élimine le piège « stale lib » lors des migrations rcN → rcN+1. Refactor : `fetch_lib_files`, `main()` guard, `_source_lib` pour permettre le sourcing dans les tests.
- **`--version` pilote le ref Git** — `latest` → `main`, sinon le tag exact ; `ELECTRICORE_VERSION` et `nano` par défaut dans l'env généré.
- **Doc déploiement** ([`docs/deploiement.md`](docs/deploiement.md)) réécrite script-first ([#50](https://github.com/Energie-De-Nantes/electricore/issues/50)).

#### API épaisse — splits rapport / détail (convention `.extension`)

- **#56 — Accise** : split en `rapport_accise` (agrégé, métier) + `accise_par_contrat` (détail brut, audit). Endpoint `/taxes/accise/{rapport,detail}.{xlsx,arrow}`.
- **#63 — CTA** : split en `rapport_cta` (par contrat-mois) + `cta_par_contrat` (détail brut Odoo). Même convention d'endpoints.
- **#64 — Facturation** : split en `rapport_facturation` (proposition mensuelle) + `facturation_du_mois` (lignes brutes Odoo avec flags `a_facturer`/`a_supprimer`).
- **#65 — Convention `.extension` étendue à `/flux/*`** : `/flux/c15/entrees.xlsx`, `/flux/c15/sorties.xlsx`, `/flux/{table}.xlsx`, `/flux/{table}.arrow`. Anciens paths segmentés supprimés (404). Documentation inline sur l'ordre des routes FastAPI (les paths à extension doivent précéder le catch-all).
- **#67 — `@with_odoo`** : collapse des 7 services pass-through (4 taxes + 3 facturation), qui n'avaient plus qu'un rôle d'ouverture/fermeture d'`OdooReader`.

#### DuckDB query builder

- **#52 — Retry-on-lock baked in** — `DuckDBQuery` gère le retry transparent sur `IO Error: Could not set lock`, plus besoin de wrappers ad hoc.
- **#53 — `c15().entrees()` / `c15().sorties()`** — méthodes builder dédiées (typage + auto-complétion) qui factorisent les filtres `evenement_declencheur`.
- **#54 — SQL injection killée** — `query_table` valide les filtres via `FluxSchema` (whitelist colonnes + opérateurs).

#### Client HTTP & Notebooks

- **Notebooks de prod migrés vers `ElectricoreClient`** — le notebook `facturation.py` consomme exclusivement l'API HTTP (`client.facturation(mois)`, `client.lignes_factures_du_mois(...)`) au lieu d'instancier `OdooReader` localement. Sépare proprement le déploiement notebook du déploiement core/ETL.

#### Fixes

- **`api_version` dynamique** ([#73](https://github.com/Energie-De-Nantes/electricore/pull/73)) — `/health` retournait `0.1.0` quel que soit le tag. Lue maintenant via `importlib.metadata.version("electricore")`, override `API_VERSION` toujours possible mais retiré de `.env.example`.
- **`deploy/install.sh`** — `latest` → ref `main`, ownership préservé par `substitute_env`/`caddyfile`/prepend, `prepend_errors_to_env` ne réécrase plus le `.env`, auto-bootstrap de `lib/` si absent.

#### Sécurité / CI

- **Scan secrets en pre-commit + avant push GHCR** — TruffleHog ajouté localement et dans le job release.
- **Pre-commit pytest hook** sur stage `pre-push` — évite de pousser un main rouge.
- Bumps `actions/checkout@6`, `docker/setup-buildx-action@4`, `docker/build-push-action@7`, `docker/login-action@4`.

#### Breaking changes

- **Imports Odoo** : `from electricore.core.loaders.odoo import …` → `from electricore.integrations.odoo import …` (ADR-0016).
- **`/flux/*` endpoints** : `/flux/c15/entrees/xlsx` → `/flux/c15/entrees.xlsx` (et équivalents). Anciens paths renvoient 404 (#65).

#### Tests

- 455 passed, 35 skipped.

---

## [1.6.1] - 2026-06-05

### 🔒 Sécurité + multi-instance + ContexteFacturation

Release patch combinant la résolution de 28 alertes Dependabot, la mise en place du déploiement multi-instance (ADR-0015) et la refonte interne de `facturation_service` autour de `ContexteFacturation`. Aucune API publique cassée.

#### Sécurité

- **28 alertes Dependabot résolues** ([`.github/dependabot.yml`](.github/dependabot.yml)) — bump `marimo>=0.23.0` (CVE-2026-39987, pre-auth RCE WebSocket, **critical**) et `lxml>=6.1.0` (CVE-2026-41066, XXE, **high**) ; `uv lock --upgrade` propage les correctifs aux transitives (aiohttp 3.14.0, urllib3 2.7.0, gitpython 3.1.50, cryptography 48.0.0, idna 3.18, pymdown-extensions 10.21.3, pytest 9.0.3, pygments 2.20.0, requests 2.34.2). Ajout de `.github/dependabot.yml` (pip + github-actions, hebdo, PRs minor/patch groupées, `versioning-strategy: lockfile-only` côté pip pour ne pas élargir les bornes upper de `pyproject.toml`) pour prévenir la dérive. Alerte #34 paramiko ≤ 4.0.0 (SHA-1, low) laissée ouverte — pas de correctif amont.

#### Ajouts

- **ADR-0015** ([`docs/adr/0015-deploiement-multi-instance.md`](docs/adr/0015-deploiement-multi-instance.md)) — modèle multi-instance : une instance Electricore par fournisseur/client final, identifiée par `INSTANCE_SLUG`. Isolation données, secrets et URL ; même codebase.
- **`INSTANCE_SLUG` exposé** ([`electricore/api/main.py`](electricore/api/main.py)) — visible dans `/health`, dans le titre OpenAPI et dans le log de boot pour identifier sans ambiguïté l'instance courante.
- **Préfixage des backups DuckDB** ([`deploy/docker/`](deploy/docker/)) — backups nommés `{INSTANCE_SLUG}-flux_enedis-YYYYMMDD.duckdb` pour éviter les collisions multi-instances dans un même bucket S3.
- **Guide de provisioning** ([`docs/deploiement.md`](docs/deploiement.md)) — procédure pas-à-pas pour démarrer une nouvelle instance sur le VPS Docker (DNS, secrets, .env, premier déploiement).
- **`ContexteFacturation`** ([`electricore/core/pipelines/facturation.py`](electricore/core/pipelines/facturation.py)) — objet immuable portant `(historique, abonnements, energies, accise)` partagés entre `rapprocher_facturation_mensuelle` et `calculer_cta_detail`. Évite le rechargement double dans `facturation_service`.
- **Décorateurs `@xlsx_endpoint` / `@arrow_endpoint` / `@zip_endpoint`** ([`electricore/api/decorators.py`](electricore/api/decorators.py)) — factorise headers, content-type et streaming des réponses binaires de l'API. Suite de tests d'intégration ([`tests/integration/test_decorators.py`](tests/integration/test_decorators.py)).

#### Modifications

- **`facturation_service`** ([`electricore/api/services/facturation_service.py`](electricore/api/services/facturation_service.py)) — consomme `ContexteFacturation` au lieu de recharger Odoo+Enedis dans chaque sous-fonction. Migration de `calculer_cta_detail` en conséquence.
- **Suppression des helpers d'orchestration morts** ([`electricore/core/pipelines/orchestration.py`](electricore/core/pipelines/orchestration.py)) — code mort depuis la migration Polars de v1.4, nettoyé.

#### Tests

- 285 passed, 35 skipped. Nouveaux tests d'intégration `test_decorators.py` et `test_health_endpoint.py`.

---

## [1.6.0] - 2026-06-04

### 🧮 Facturation — flags d'état + glossaire Historique

Deuxième brique de l'API épaisse : refonte du chargement Odoo pour exposer toutes les lignes de factures du mois (avec flags), résolvant la douleur de test hors période de facturation. Renommage terminologique acté par ADR-0013 pour aligner code et glossaire.

#### Ajouts

- **ADR-0013** ([`docs/adr/0013-renommage-perimetre-historique.md`](docs/adr/0013-renommage-perimetre-historique.md)) — Renommage *Périmètre* → *Historique* dans le code et le glossaire. Deux concepts distincts : `Historique` (séquence temporelle enrichie produite par `pipeline_historique`) ; `Périmètre` (snapshot à une date, conservé au glossaire sans implémentation).
- **ADR-0014** ([`docs/adr/0014-lignes-factures-du-mois-avec-flags.md`](docs/adr/0014-lignes-factures-du-mois-avec-flags.md)) — `lignes_factures_du_mois` qui ne filtre Odoo que par `sale.order.state == 'sale'` et `invoice_date` du mois ; flags `a_facturer` / `a_supprimer` matérialisent les sous-ensembles métier. Résout : tester le notebook hors période, audit a posteriori, debug.
- **`lignes_factures_du_mois(odoo, mois)`** ([`electricore/core/loaders/odoo/helpers.py`](electricore/core/loaders/odoo/helpers.py)) — LazyFrame de toutes les lignes de factures du mois cible avec les colonnes `a_facturer` (drafts + qty > 0) et `a_supprimer` (drafts + qty == 0).
- **`flags_etat_facturation(lf)`** — helper pur testable séparément (`x_invoicing_state`, `state_account_move`, `quantity` → flags).
- **Modèle Pandera `Historique`** ([`electricore/core/models/historique.py`](electricore/core/models/historique.py)) — valide la sortie enrichie de `pipeline_historique` (impacte_*, resume_modification, événements FACTURATION).

#### Modifications

- **`LignesFactureRapprochees`** ([`electricore/core/models/lignes_facture_rapprochees.py`](electricore/core/models/lignes_facture_rapprochees.py)) — 9 → 20 colonnes nullable : méta-période Enedis (`debut`, `fin`, `data_complete`, `turpe_fixe_eur`, `turpe_variable_eur`, `ref_situation_contractuelle`, `pdl`), identifiants compteur (`num_compteur`, `type_compteur`) joints depuis l'Historique, flags d'état (`a_facturer`, `a_supprimer`). `x_lisse`, `x_pdl`, `name_product_category`, `name_product_product` rendus nullable.
- **`rapprocher_facturation_mensuelle`** ([`electricore/core/pipelines/facturation.py`](electricore/core/pipelines/facturation.py)) — nouveau paramètre `historique` (LazyFrame), join `.unique(keep='last')` sur `ref_situation_contractuelle` pour récupérer les identifiants compteur ; propage les flags ; ne projette plus aucune colonne méta.
- **`pipeline_perimetre`** → **`pipeline_historique`** ([`electricore/core/pipelines/historique.py`](electricore/core/pipelines/historique.py)) — décoré `@pa.check_types` validant le modèle `Historique`.
- **`facturation_service`** ([`electricore/api/services/facturation_service.py`](electricore/api/services/facturation_service.py)) — utilise `lignes_factures_du_mois`, résout le mois avant l'appel Odoo.
- **`notebooks/facturation.py`** — un seul call `client.facturation(mois)` côté Enedis + `lignes_factures_du_mois(odoo, mois)` côté Odoo. `lignes_a_facturer_df` et `lignes_a_supprimer` dérivés par `.filter()` sur les flags côté client. UI `mois_input` par défaut au 1er du mois courant. `sim_mode=True` + `run_button` gate préservés.

#### Suppressions (breaking)

- **`lignes_a_facturer`** et **`lignes_quantite_zero`** ([`electricore/core/loaders/odoo/helpers.py`](electricore/core/loaders/odoo/helpers.py)) — remplacés par `lignes_factures_du_mois(odoo, mois)` + filtre client `.filter(pl.col("a_facturer"))` ou `.filter(pl.col("a_supprimer"))`.
- **`pipeline_perimetre`** → renommée `pipeline_historique` (cf. ADR-0013).
- **`HistoriquePérimètre`** (modèle Pandera) supprimé — le brut C15 n'a plus de nom métier autonome, validation déplacée à la sortie de `pipeline_historique`.
- **`transform_historique_perimetre`** → renommée `transform_historique`.
- **`load_historique_perimetre`** → renommée `load_historique`.
- **`registry.py`** : `validator=None` pour C15 (validation maintenant à la sortie du pipeline).

#### Tests

- TDD complet : 4 cycles RED→GREEN sur `flags_etat_facturation` (table tests), 1 cycle sur la propagation des flags par `rapprocher_facturation_mensuelle`, fixtures unit+smoke adaptées.
- Adaptation `test_facturation_service_smoke.py` pour la nouvelle signature de `lignes_factures_du_mois` et les nouvelles colonnes.

---

## [1.5.0] - 2026-06-04

### 🧮 Facturation — API épaisse

Première brique de l'API épaisse v1.5 : extraction du rapprochement Odoo↔Enedis vers le domaine `core`, prérequis pour exposer les résultats structurés en Arrow IPC aux notebooks distants.

#### Ajouts

- **`rapprocher_facturation_mensuelle()`** ([`electricore/core/pipelines/facturation.py`](electricore/core/pipelines/facturation.py)) — fonction métier pure qui joint les lignes de facture Odoo (taggées `x_ref_situation_contractuelle`) avec la facturation Enedis mensuelle et calcule `quantite_enedis` selon la catégorie produit (HP/HC/Base/Abonnements).
- **Schéma Pandera `LignesFactureRapprochees`** ([`electricore/core/models/lignes_facture_rapprochees.py`](electricore/core/models/lignes_facture_rapprochees.py)) — validation stricte en sortie via `@pa.check_types(lazy=True)`.
- **ADR-0012** ([`docs/adr/0012-api-read-only-odoo.md`](docs/adr/0012-api-read-only-odoo.md)) — politique « API read-only sur Odoo ; écritures via notebook humain-dans-la-boucle » + règle nuancée pour OdooReader en notebook (autorisé pour enrichissement, interdit en amont d'une pipeline).
- **Glossaire** ([`electricore/core/CONTEXT.md`](electricore/core/CONTEXT.md)) — entrées « Rapprochement PDL ↔ RSC » (étape amont, notebook `injection_rsc.py`) et « Rapprochement facturation mensuelle » (étape aval, exposée par l'API) pour clarifier la distinction.

- **Endpoint `GET /facturation/arrow`** ([`electricore/api/main.py`](electricore/api/main.py)) — sérialise `lignes_facture_rapprochees` en flux Arrow IPC, lisible par `pl.read_ipc_stream`. Query param `mois=YYYY-MM-DD` ; sans paramètre, dernier mois disponible. Authentification API key, comme les autres endpoints data.
- **Endpoint `GET /taxes/accise/arrow`** ([`electricore/api/main.py`](electricore/api/main.py)) — détail Accise TICFE (table par PDL × mois × trimestre) sérialisé en Arrow IPC. Query param `trimestre=YYYY-TX` (sans : tout). Les agrégations « Par taux » et « Résumé » de l'XLSX restent à charge du notebook (group_by trivial).
- **Endpoint `GET /taxes/cta/arrow`** ([`electricore/api/main.py`](electricore/api/main.py)) — détail CTA mensuel (table par PDL × mois avec `cta_eur`, `taux_cta_pct`, `turpe_fixe_eur`, `trimestre`) sérialisé en Arrow IPC. Query param `trimestre=YYYY-TX`.
- **Extra optionnel `[viz]`** ([`pyproject.toml`](pyproject.toml)) — `altair`, `vegafusion`, `vl-convert-python`, `marimo`, `plotly[express]` quittent les dépendances obligatoires et passent en optional. L'image Docker production (`uv sync --extra etl --no-dev`) ne les inclut donc plus. Installer côté notebook avec `uv sync --extra viz`.
- **Module `electricore.client`** ([`electricore/client/__init__.py`](electricore/client/__init__.py)) — classe `ElectricoreClient(url, api_key)` avec méthodes `.facturation(mois)`, `.accise(trimestre)`, `.cta(trimestre)` retournant un `pl.DataFrame`. Extension point pour le HTTP transport DuckDBQuery prévu en v1.6.

#### Modifications

- **`api/services/facturation_service.generer_facturation_xlsx`** consomme désormais `rapprocher_facturation_mensuelle()` — comportement XLSX inchangé, logique métier simplifiée. Le chargement Odoo+Enedis est factorisé dans `calculer_lignes_facture_rapprochees()`, partagé entre les services XLSX et Arrow.

#### Correctifs

- **`generer_documents_facturation` régression #5** ([`electricore/api/services/facturation_service.py`](electricore/api/services/facturation_service.py)) — `MAPPING_CATEGORIE` avait été supprimé du module lors de l'extraction de `rapprocher_facturation_mensuelle`, laissant un `NameError` runtime dans `/facturation/documents` (déclenché par le bot Telegram). Refactor : la fonction consomme désormais `rapprocher_facturation_mensuelle` (single source of truth) + 3 tests smoke ([`tests/integration/test_facturation_service_smoke.py`](tests/integration/test_facturation_service_smoke.py)) qui catchent ce type de bug post-refactor.
- **Lignes draft incluses dans le calcul Accise** ([`electricore/core/pipelines/accise.py`](electricore/core/pipelines/accise.py)) — `agreger_consommations_mensuelles` agrégeait toutes les lignes énergie, y compris celles dont l'`account.move` est en draft (sans `invoice_date`). Conséquence : `mois_consommation = null` → ValueError "ligne(s) sans taux en vigueur" en sortie de `ajouter_taux_en_vigueur`. Fix : filtrer `invoice_date.is_not_null()` au début de l'agrégation — sémantique « assiette accise = factures validées ». Test unitaire dédié.

#### Tests

- Isolation des tests crypto ETL ([`tests/etl/test_crypto.py`](tests/etl/test_crypto.py)) — autouse fixture qui efface `AES__*` d'`os.environ` avant chaque test `load_key_chain`. Bug pré-existant exposé en important `electricore.api.main` au niveau test (lecture `.env` au chargement de l'API).

---

## [1.4.0] - 2026-06-02

### 🚀 Déployabilité — fondations VPS

Mise en place d'une stack Docker reproductible (`docker compose up -d`) avec ETL planifié et reverse-proxy TLS. La cible : un VPS unique qui exécute l'ETL à intervalle fixe, expose l'API + bot Telegram 24/7, et héberge optionnellement les fichiers Enedis collocés (pas de SFTP redondant). L'accès distant des notebooks via API HTTP est reporté à v1.5.

#### Ajouts

- **Image Docker** ([`deploy/docker/Dockerfile`](deploy/docker/Dockerfile)) — multi-stage avec `uv` au build, runtime minimal (`python:3.13-slim` + libxml2 + supercronic + duckdb CLI), utilisateur non-root, `tini` comme PID 1. Publiée sur `ghcr.io/energie-de-nantes/electricore` à chaque tag `vX.Y.Z`.
- **Stack docker-compose** ([`deploy/docker/docker-compose.yml`](deploy/docker/docker-compose.yml)) — services `api` (FastAPI + bot), `etl-scheduler` (supercronic), `caddy` (TLS automatique). Volumes nommés pour la base DuckDB et les sauvegardes.
- **ETL planifié** via supercronic — déclenche `POST /etl/run` toutes les nuits à 02:00. Les jobs scheduled apparaissent dans `/etl/jobs` aux côtés des runs manuels (bot, API), donc l'historique reste unifié.
- **Mode "fichiers collocés"** — pour le scénario où l'application tourne sur le même VPS qu'un dépôt Enedis. Aucune modification de code : `SFTP__URL=file:///var/enedis/` est nativement supporté par `dlt.sources.filesystem`. Les fichiers restent chiffrés AES sur disque, donc les clés AES restent obligatoires. Voir [`docs/deploiement.md`](docs/deploiement.md).
- **Sauvegardes DuckDB** ([`deploy/docker/backup_duckdb.sh`](deploy/docker/backup_duckdb.sh)) — `EXPORT DATABASE` + tar.gz quotidien, rétention 14 snapshots. Restauration documentée.
- **Endpoint `/health` enrichi** ([`electricore/api/main.py`](electricore/api/main.py)) — retourne désormais un payload structuré `{database: {accessible, last_write, tables}, bot: {running}, ...}` au lieu de juste `{status: "ok"}`. Toujours `200` (même base inaccessible), pour qu'ops puisse différencier un verrou ETL transitoire d'une panne réelle.
- **Retry-on-lock DuckDB** ([`electricore/api/services/duckdb_service.py`](electricore/api/services/duckdb_service.py)) — 3 tentatives × 1s pour absorber les fenêtres de checkpoint pendant que l'ETL écrit.
- **Documentation déploiement** ([`docs/deploiement.md`](docs/deploiement.md)) — guide complet : prérequis, installation initiale, modes SFTP vs collocé, rotation clés AES, sauvegarde/restauration, dépannage.

#### Modifications

- **Import package-style** ([`electricore/etl/pipeline_production.py:29`](electricore/etl/pipeline_production.py#L29)) — `from electricore.etl.sources.sftp_enedis import flux_enedis`. Permet d'invoquer le pipeline via `python -m electricore.etl.pipeline_production` depuis n'importe quel `cwd`. Service ETL côté API ajusté en conséquence.
- **Masquage URL généralisé** ([`electricore/etl/sources/sftp_enedis.py`](electricore/etl/sources/sftp_enedis.py)) — `mask_password_in_url()` gère désormais `file://` (no-op) en plus de `sftp://`.

#### Suppressions (breaking)

- **Unit systemd `electricore.service`** supprimée (ainsi que les fichiers `.example` annexes). Les utilisateurs sur déploiement bare-metal doivent rester sur v1.3.x ou migrer vers Docker. La doc Docker couvre tous les cas d'usage de l'ancienne unit.

#### Reportés à v1.5

- Transport HTTP pour `DuckDBQuery` builders — permettra aux notebooks marimo locaux de lire les données depuis l'API distante (sans nécessiter le fichier DuckDB local).
- Extra `[viz]` optionnel — marimo, altair, vegafusion, vl-convert-python, plotly retirés des dépendances par défaut. L'image production passerait de ~1.2 GB à ~400 MB.
- Image multi-arch (arm64) — actuellement amd64 uniquement.
- Observabilité : logging JSON structuré, intégration Sentry.

---

## [1.3.5] - 2026-06-01

### 🛠️ Hygiène d'ingénierie

Première itération de mise à niveau des conventions OSS standards (CI, lint, type-checking, workflow release). Pas de changement métier — l'ensemble du comportement runtime reste identique à v1.3.4.

- **CI GitHub Actions** ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) : exécute pytest sur chaque PR (Python 3.12 + 3.13), ruff lint + format, mypy. `uv sync --locked` détecte les dérives entre `pyproject.toml` et `uv.lock`.
- **Ruff lint + format** activés sur `electricore/` et `tests/` (ruleset E/F/I/UP/B, line-length 120, notebooks/scripts exclus). 536 violations auto-corrigées. 1 vrai bug surfacé et corrigé : `__all__` exportait `expr_data_complete` (symbole inexistant) dans `electricore/core/pipelines/facturation.py`.
- **Mypy strict** (scope étroit sur la surface d'API publique : `electricore.core.loaders.*`, `electricore.core.writers.*`, `electricore.core.pipelines.orchestration`). 85 erreurs → 0. Implicit-Optional éliminé, `QueryConfig.validator` correctement typé `type[pa.DataFrameModel] | None`, paramètre `how` du join typé `Literal`.
- **Pre-commit hooks** ([`.pre-commit-config.yaml`](.pre-commit-config.yaml)) : ruff lint + format à chaque commit local.
- **Workflow release** ([`.github/workflows/release.yml`](.github/workflows/release.yml)) : déclenché sur push de tag `vX.Y.Z`, publie sur PyPI via trusted publishing (OIDC, sans token API) et crée la release GitHub avec artefacts attachés + notes auto-générées.

### 🐛 Détecté latent — non corrigé dans cette release

- `json_to_dict_from_bytes` appelé mais non défini dans `electricore/etl/transformers/parsers.py` (chemin générique JSON jamais exercé en production ; tous les flux JSON utilisent `_json_r64_transformer_base`). Annoté `# noqa: F821` + TODO ; à implémenter avant d'ajouter un nouveau type de flux JSON.

---

## [1.0.0] - 2025-10-06

### 🎉 Version majeure - Architecture moderne Polars/DuckDB

Refonte complète du projet avec migration de pandas vers Polars, nouvelle architecture modulaire, et ajout de fonctionnalités majeures. **BREAKING CHANGES** : non rétrocompatible avec v0.x.

### ✨ Ajouts majeurs

#### Architecture & Performance
- **Migration Polars 100%** : Remplacement complet de pandas par Polars pour performances optimales
- **DuckDB integration** : Query builder fluide pour interroger la base de données DLT
- **LazyFrame pipelines** : Évaluation différée pour optimisation mémoire et calcul
- **Architecture modulaire** : Séparation claire ETL / Core / API

#### Module ETL (nouveau)
- **Pipeline DLT** : Extraction automatisée depuis SFTP Enedis
- **Transformers modulaires** : Crypto (AES-256-CBC), Archive (ZIP/TAR), Parsers (XML/CSV/JSON)
- **Configuration flux** : `flux.yaml` centralisé pour tous les flux Enedis
- **Modes exécution** : Test (2 fichiers, ~3s), Flux unique (~6s), Production complète
- **Support flux** : C15, F12, F15, R15, R151, R64, R401
- **Documentation** : Guide complet ETL + outils diagnostic

#### Module Core (refonte)
- **Query Builders**
  - `DuckDBQuery` : API fluide pour C15, R151, R15, F15, R64, relevés harmonisés
  - `OdooQuery` : Navigation relations avec `.follow()`, enrichissement `.enrich()`
  - `ParquetLoader` : Chargement direct fichiers Parquet
- **Pipelines Polars**
  - `pipeline_perimetre` : Détection périmètre actif avec 8 expressions composables
  - `pipeline_abonnements` : Calcul périodes abonnement avec bornes temporelles
  - `pipeline_energie` : Consommations par cadran (HPH/HCH/HPB/HCB/HP/HC/Base)
  - `pipeline_turpe` : TURPE fixe + variable + CMDPS
- **Modèles Pandera** : Validation stricte avec décorateurs `@pa.check_types`
- **Writers** : OdooWriter pour synchronisation ERP

#### Module API (nouveau)
- **FastAPI** : API REST sécurisée avec authentification API key
- **Endpoints** : `/flux/c15`, `/flux/r151`, `/flux/r15`, `/flux/f15`, `/health`
- **Services DuckDB** : Requêtage optimisé avec filtres et pagination
- **Documentation** : OpenAPI interactive sur `/docs`

#### TURPE - Implémentation complète
- **TURPE Fixe C5** (BT ≤ 36 kVA)
  - Formule : `(b × P) + cg + cc`
  - Calcul annuel, journalier, période avec prorata
- **TURPE Fixe C4** (BT > 36 kVA) - **NOUVEAU**
  - 4 puissances souscrites : P₁ (HPH), P₂ (HCH), P₃ (HPB), P₄ (HCB)
  - Formule progressive : `b₁×P₁ + b₂×(P₂-P₁) + b₃×(P₃-P₂) + b₄×(P₄-P₃) + cg + cc`
  - Validation contrainte réglementaire : P₁ ≤ P₂ ≤ P₃ ≤ P₄
  - Économies jusqu'à 20% via modulation saisonnière
  - Détection automatique C4/C5, rétrocompatibilité totale
- **TURPE Variable** : Calcul par cadran horaire (HPH/HCH/HPB/HCB ou HP/HC ou Base)
- **CMDPS** : Pénalités dépassement puissance
- **Configuration** : `turpe_rules.csv` avec tarifs officiels CRE
- **Nomenclature CRE** : `b_*` (puissance €/kVA/an), `c_*` (énergie c€/kWh)

#### Documentation
- **README** : Guide complet architecture + quickstart + exemples
- **CLAUDE.md** : Instructions projet pour IA avec patterns établis
- **Guides modules** : ETL, API, Query Builders, Conventions dates
- **Tutoriels TURPE** : Documentation technique C4 + usage standalone
- **Tests** : 167 tests unitaires + intégration avec fixtures métier

### 🔧 Améliorations

#### Code Quality
- **Type hints** : Annotations complètes pour meilleure maintenabilité
- **Functional programming** : Expressions pures `Fn(Series) -> Series`
- **Immutabilité** : Query builders fluides sans mutation d'état
- **Tests exhaustifs** : 167 passing avec Hypothesis property-based testing
- **Fixtures métier** : Cas réels (déménagement, changements contrat, etc.)

#### Performance
- **LazyFrame optimization** : Évaluation différée par Polars
- **DuckDB analytics** : Requêtes SQL optimisées sur données DLT
- **Vectorisation** : Élimination boucles Python au profit opérations Polars
- **Mémoire** : Réduction empreinte grâce lazy evaluation

#### Developer Experience
- **Notebooks Marimo** : Exploration interactive des pipelines
- **Outils diagnostic** : Scripts analyse flux, états incrémentaux
- **Configuration centralisée** : `database.yaml`, `flux.yaml`
- **Logs structurés** : Meilleure traçabilité ETL

### 🗑️ Suppressions (Breaking Changes)

#### Modules supprimés
- `electricore.core.énergies` → remplacé par `electricore.core.pipelines.energie`
- `electricore.core.périmètre` → remplacé par `electricore.core.pipelines.perimetre`
- `electricore.core.relevés` → intégré dans `electricore.core.pipelines.energie`
- `electricore.core.pipeline_*` (ancien format) → nouveaux pipelines dans `pipelines/`
- `electricore.core.orchestration` → remplacé par `pipelines/orchestration`
- `electricore.core.taxes` → remplacé par `pipelines/turpe`
- `electricore.core.services` → remplacé par `loaders/` et `writers/`
- `electricore.inputs.flux` → parsers intégrés dans `etl/transformers/parsers.py`

#### Dépendances supprimées
- `pandas` → migration complète vers Polars
- `toolz` → remplacement par opérations Polars natives
- Autres dépendances obsolètes nettoyées

### 📦 Changements techniques

#### Dependencies
- **Ajoutées**
  - `polars >=1.0.0` (remplacement pandas)
  - `pandera[polars] >=0.24.0` (validation)
  - `duckdb >=1.3.2` (analytics)
  - `dlt[workspace] >=1.16.0` (ETL)
  - `fastapi >=0.116.1` (API)
  - `uvicorn >=0.35.0` (serveur ASGI)
  - `pycryptodome >=3.23.0` (décryptage flux)
- **Supprimées**
  - `pandas` (migration Polars)
  - `toolz` (refactor architecture)

#### Configuration
- `pyproject.toml` : Configuration Poetry moderne avec groupes optionnels
- Python : `>=3.12,<3.15` (support Python 3.12+)
- Build : `poetry-core>=2.0.0`

#### Structure projet
```
electricore/
├── etl/              # Nouveau module ETL (DLT)
├── core/
│   ├── loaders/      # Query builders (nouveau)
│   ├── pipelines/    # Pipelines Polars (refonte)
│   ├── models/       # Modèles Pandera (refonte)
│   └── writers/      # Writers Odoo (nouveau)
├── api/              # Nouveau module API (FastAPI)
└── config/           # Configuration centralisée (nouveau)
```

### 🔄 Migration depuis v0.x

⚠️ **ATTENTION** : Cette version introduit des breaking changes majeurs.

#### Actions requises

1. **Mise à jour imports**
   ```python
   # Avant (v0.x)
   from electricore.core.énergies import calculer_energies
   from electricore.core.périmètre import detecter_perimetre

   # Après (v1.0.0)
   from electricore.core.pipelines.energie import pipeline_energie
   from electricore.core.pipelines.perimetre import pipeline_perimetre
   from electricore.core.loaders import c15, r151, charger_releves
   ```

2. **Migration pandas → Polars**
   ```python
   # Les DataFrames sont maintenant des Polars DataFrames/LazyFrames
   import polars as pl

   # Avant : pandas df
   df = pd.read_csv("data.csv")

   # Après : Polars LazyFrame (recommandé)
   lf = pl.scan_csv("data.csv")
   ```

3. **Configuration**
   - Créer `config/database.yaml` pour DuckDB
   - Créer `etl/config/flux.yaml` pour ETL
   - Migrer anciens paramètres vers nouvelle structure

4. **Tests**
   - Adapter fixtures pour Polars
   - Utiliser nouveaux modèles Pandera
   - Tester avec nouveaux query builders

#### Guide complet
Consultez [CLAUDE.md](CLAUDE.md) pour architecture détaillée et patterns établis.

### 🐛 Corrections

- Fix validation Pandera pour colonnes optionnelles C4
- Fix gestion dates timezone Europe/Paris
- Fix convention dates R151 (+1 jour pour harmonisation)
- Fix calcul TURPE avec règles temporelles multiples
- Fix tests paramétrés avec colonnes C4 NULL

### 🔒 Sécurité

- API authentification par clé API
- Décryptage sécurisé flux cryptés (AES-256-CBC)
- Validation inputs stricte avec Pandera
- Configuration sensible via variables environnement

### 📊 Métriques

- **Tests** : 167 passing, 29 skipped
- **Couverture** : ~49%
- **Fichiers modifiés** : 141 fichiers
- **Lignes** : +24,734 / -9,297
- **Commits** : 287 commits depuis v0.2.7

### 🙏 Contributeurs

- Virgile - Architecture, développement, migration Polars
- Claude Code (Anthropic) - Assistance développement et refactoring

---

## [0.2.7] - 2024-08-13

Dernière version avant migration majeure v1.0.0.

### Ajouts
- Amélioration visualisation pipeline
- Mise à jour toolz vers 1.0.0

### Corrections
- Correction FutureWarning pandas pour fillna

---

## [0.1.0] - 2024-01-01

Version initiale du projet ElectriCore.

### Ajouts
- Pipeline de base pour traitement flux Enedis
- Calculs énergétiques avec pandas
- Intégration Odoo basique

---

[1.0.0]: https://github.com/Energie-De-Nantes/electricore/compare/v0.2.7...v1.0.0
[0.2.7]: https://github.com/Energie-De-Nantes/electricore/compare/v0.1.0...v0.2.7
[0.1.0]: https://github.com/Energie-De-Nantes/electricore/releases/tag/v0.1.0
