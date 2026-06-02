# Contexte — etl (ingestion)

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

**Mode ETL** :
Paramètre du pipeline d'ingestion sélectionnant l'étendue de l'exécution :
- `test` : 2 fichiers (validation rapide)
- `r151` : flux R151 complet uniquement
- `all` : tous les flux
- `reset` : purge + ré-ingestion complète

**Job ETL** :
Exécution asynchrone du pipeline d'ingestion déclenchée via l'API (`POST /etl/run`). Identifié par un UUID, doté d'un statut (`running`, `completed`, `failed`) et d'une sortie (`output` ou `error`).

**Scheduler** :
Conteneur Docker qui déclenche périodiquement l'ETL via cron et un appel HTTP à `/etl/run` — pas de logique métier embarquée. Voir [ADR-0011](../../docs/adr/0011-deploiement-vps-docker.md).

**Transformer** :
Étape unitaire et composable de transformation d'un fichier (`crypto.py` déchiffrement AES, `archive.py` extraction ZIP, `parsers.py` parsing XML/CSV). Les transformers se chaînent via le `|` de DLT : `encrypted | decrypt | unzip | parse`.
