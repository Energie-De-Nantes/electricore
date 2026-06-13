# Contexte — ingestion

Vocabulaire spécifique à l'ingestion des flux Enedis vers DuckDB. Les concepts métier portés par ces flux (PDL, événements contractuels, etc.) sont définis dans [`electricore/core/CONTEXT.md`](../core/CONTEXT.md).

## Flux Enedis

**Flux** :
Fichier de données émis périodiquement par Enedis aux fournisseurs, au format XML ou CSV. Chaque type a un code (C15, R151…) et un contenu spécifique.

**C15** :
Flux d'événements contractuels (MES, RES, MCT…). Source de l'historique du périmètre.

**R151** :
Flux de relevés périodiques mensuels (index Linky par cadran). Source principale du calcul de consommation. Convention de date « fin de journée » — voir [ADR-0003](../../docs/adr/0003-r151-date-harmonisation.md).

**R15** :
Flux de relevés à la demande + événements ponctuels (déplacements, contestations).

**R64** :
Flux de relevés au format JSON, séries temporelles plus granulaires.

**F12** :
Synthèse mensuelle de facturation distributeur (volumes agrégés).

**F15** :
Facturation distributeur détaillée, utilisée pour valider les calculs TURPE.

---

## Pipeline d'ingestion

**Mode d'ingestion** :
Paramètre du pipeline d'ingestion sélectionnant l'étendue de l'exécution :
- `test` : 2 fichiers (validation rapide)
- `r151` : flux R151 complet uniquement
- `all` : tous les flux
- `reset` : purge + ré-ingestion complète

**Job d'ingestion** :
Exécution asynchrone du pipeline d'ingestion déclenchée via l'API (`POST /ingestion/run`). Identifié par un UUID, doté d'un statut (`running`, `completed`, `failed`) et d'une sortie (`output` ou `error`).

**Scheduler** :
Conteneur Docker qui déclenche périodiquement l'ingestion via cron et un appel HTTP à `/ingestion/run` — pas de logique métier embarquée. Voir [ADR-0011](../../docs/adr/0011-deploiement-vps-docker.md).

**Transformer** :
Étape unitaire et composable de transformation d'un fichier, sous forme d'*adapter* DLT mince autour d'un noyau pur (`crypto.py` déchiffrement AES, `archive.py` extraction ZIP, `parsers.py` linéarisation). Les transformers se chaînent via le `|` de DLT : `encrypted | decrypt | unzip | parse`.

**Linéarisation** :
Transformation d'un document hiérarchique (XML, JSON) en lignes plates typées. Vit dans les **modèles dbt** (`ingestion/dbt/models/` : staging = éclatement en occurrences, flux_* = sélection WHERE + pivot cadrans + types XSD), cf. [ADR-0020](../../docs/adr/0020-linearisation-en-dbt.md). Le seul étage Python est `xml_vers_dict` (conversion générique XML → dict, politique « conteneur = liste »), avant dépôt du document intégral en colonne JSON (`flux_raw.raw_*`). L'ex-moteur Python piloté par `flux.yaml` (hérité de la lib `electriflux`) a été retiré (#138).
_Éviter_ : extraction (collision avec le E de ETL = récupération SFTP), parsing tout court (la sélection est constitutive).

**Configuration de flux** :
Entrée de `flux.yaml` réduite au **mouvement** : `file_pattern` (glob SFTP des zips), `format` (xml/json) et `file_regex` (fichiers à extraire). Le contrat de sélection vit dans les modèles dbt — ce qui n'est pas sélectionné par un modèle reste néanmoins disponible dans le brut (`flux_raw`), re-matérialisable à volonté (`ingestion.py rebuild`).

---

## Déchiffrement

**Trousseau de clés AES** :
Ensemble des clés AES qu'Enedis a successivement utilisées pour chiffrer les flux d'une instance. Une clé a une **fenêtre de validité** temporelle : les fichiers de l'archive historique sont chiffrés par la clé en vigueur à leur date d'émission. Le chiffrement lui-même a évolué dans le temps — bascule **AES-128 → AES-256 dans la nuit du 8 au 9 juin 2026** —, absorbée par la longueur de clé (16 vs 32 octets) sans branche de code distincte. Sélection **par essai** : on tente les clés du trousseau (de la plus récente à la plus ancienne), le déchiffrement est son propre oracle (padding PKCS7 + magic bytes ZIP). Les fenêtres de validité sont **dérivées** du corpus (min/max `modification_date` des fichiers qu'une clé déchiffre) et exposées pour l'observabilité — jamais le pilote de la sélection (parallèle au _millésime_ réglementaire d'[ADR-0024](../../docs/adr/0024-trois-registres-de-savoir.md)). Registre **runtime** (secret, par-instance, ajouté au déploiement), pas réglementaire. La rotation à deux clés ([ADR-0008](../../docs/adr/0008-rotation-cles-aes.md)) est le stopgap qui ne couvre qu'une rotation ; le trousseau N-clés est la cible.
_Éviter_ : « clé courante » au singulier (le trousseau est pluriel par construction).
