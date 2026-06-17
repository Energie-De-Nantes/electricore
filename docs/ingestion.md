# Ingestion des flux Enedis — architecture ELT (dlt + dbt)

> **La thèse en une phrase : le mouvement ne casse jamais ; la transformation est rejouable.**

![Architecture de l'ingestion](ingestion.png)

*(source éditable : [ingestion.excalidraw](ingestion.excalidraw))*

## Vue d'ensemble

L'ingestion est en deux étages strictement séparés, articulés autour d'un pivot : le **brut**.

```
SFTP Enedis ──(dlt : decrypt AES, unzip, incrémental)──▶ flux_raw.raw_*   (documents JSON intégraux)
                                                              │
                                              (dbt : SQL pur, ~13 s) ──▶ flux_enedis.flux_*   (tables typées)
                                                                              │
                                                                  core/loaders (DuckDBQuery) ──▶ pipelines Polars
```

1. **MOUVEMENT — dlt** (`electricore/ingestion/sources/sftp_enedis_brut.py`)
   La chaîne `sftp | decrypt | unzip` ne connaît **rien** du contenu. Chaque fichier extrait est
   converti en dict générique — `xml_vers_dict` pour le XML (politique « **conteneur = liste** » :
   tout élément à enfants devient un tableau, même unique → les chemins `[0]` sont stables et les
   multiplicités Enedis sont absorbées), `json.loads` pour R64 — puis déposé **intégralement** en
   colonne JSON dans `flux_raw.raw_<flux>`. Clé `file_name` en merge : les re-livraisons Enedis
   (même fichier dans plusieurs zips) sont dédoublonnées par construction. L'incrémental dlt
   (`modification_date`) ne porte que sur le mouvement.

2. **TRANSFORMATION — dbt** (`electricore/ingestion/dbt/models/`)
   Un modèle `staging/stg_<flux>` éclate le document en occurrences (clé `prm_id` / `releve_id` /
   `mesure_id` = fichier + position — le **grain** est l'occurrence, jamais le PDL : un PDL revient
   dans N fichiers). Un modèle `flux/flux_<table>` sélectionne (`WHERE`), pivote les cadrans sur le
   **domaine fermé** (`base, hp, hc, hph, hpb, hch, hcb` — contrat de colonnes stable pour les
   loaders) et type selon les **XSD Enedis** (`dateTime`→TIMESTAMPTZ, `date`→DATE, `integer`→BIGINT,
   `decimal`→DOUBLE). Matérialisation dans `flux_enedis.flux_*`, schéma lu par les loaders core.

Le runner de production est `electricore/ingestion/runner.py` (lancé par l'API `/ingestion/run`, cron VPS) :

```bash
uv run python -m electricore.ingestion test      # smoke : 2 fichiers/flux
uv run python -m electricore.ingestion all       # tout (landing + dbt build)
uv run python -m electricore.ingestion r151 c15  # sélection de flux
uv run python -m electricore.ingestion rebuild   # dbt seul — zéro réseau, ~13 s
uv run python -m electricore.ingestion resync    # re-télécharge tout (brut perdu)
uv run python -m electricore.ingestion all --db /tmp/essai.duckdb   # base jetable
```

## Le filet

Trois étages, tous joués en CI (`--extra dbt` installé par le job test) :

| filet | ce qu'il prouve |
|---|---|
| **golden fixtures réelles** (`tests/fixtures/flux/*.xml`, anonymisées) | la linéarisation d'échantillons réels est figée au record près |
| **golden fixtures XSD** (`*_xsd.xml`, générées des schémas Enedis) | les optionnels et les enums que les échantillons réels n'exercent pas |
| **data tests dbt + contrat de types** (`schema.yml`, `test_dbt_flux_golden`) | not_null sur les colonnes critiques, type DuckDB exact dicté par le XSD |

Les golden sont générés **par le chemin de production lui-même** (`generer_golden.py` : landing →
`dbt build` → capture) : tout changement de comportement = diff git des golden, revu en PR.

## Recettes

### Ajouter un champ à un flux

Le champ est **déjà dans le brut** (le landing capture le document intégral). Il suffit de l'exposer :

1. Ajouter la ligne dans le modèle (`ingestion/dbt/models/flux/flux_<table>.sql`) :
   `prm ->> '$.Chemin.Vers.Le.Champ' as mon_champ,` (penser `[0]` pour chaque conteneur traversé) ;
2. `uv run python -m electricore.ingestion rebuild` — **l'historique entier est backfillé**
   (~13 s), zéro re-téléchargement ;
3. Régénérer les golden (`uv run python tests/fixtures/flux/generer_golden.py`), relire le diff,
   committer.

### Le jour où Enedis change un flux (nouvelle version XSD)

**Rien ne casse à l'ingestion** : le brut n'a pas de schéma, les nouveaux documents atterrissent
dès le premier jour. Ensuite, à froid :

1. Récupérer le nouveau XSD (`~/Documents/guides_flux/`) ;
2. **Champ ajouté** → recette ci-dessus. **Champ déplacé/renommé** → le brut contient les deux
   versions mélangées pendant la transition Enedis : `coalesce(nouveau_chemin, ancien_chemin)`
   dans le modèle ;
3. Régénérer la fixture XSD maximale (`uv run python tests/fixtures/flux/generer_fixtures_xsd.py`,
   exige les XSD en local) — elle valide contre le nouveau schéma **par construction** ;
4. `rebuild` + golden + diff en PR.

Garde-fou : un champ critique qui disparaît casse les data tests `not_null` au `dbt build` ;
pour les colonnes non testées, étoffer `schema.yml` au fil de l'eau.

### Ajouter un nouveau flux

1. `flux.yaml` : `file_pattern` (glob SFTP), `format` (xml/json), `file_regex` — c'est tout, le
   mouvement est générique ;
2. `models/sources.yml` : déclarer `raw_<flux>` ;
3. Écrire `staging/stg_<flux>.sql` (éclatement en occurrences) + `flux/flux_<table>.sql`
   (sélection + pivot + types XSD) + entrée `schema.yml` (not_null) ;
4. `MODELES_PAR_RAW` dans `runner.py` ;
5. Fixture anonymisée (`anonymiser.py`) et/ou XSD, golden, spec dans `test_dbt_flux_golden.py`.

### Les trois pièges DuckDB (appris sur données réelles, encodés dans les modèles)

1. **Pushdown sous unnest** : un `WHERE` sur un chemin JSON d'un élément unnesté peut être poussé
   sous l'unnest et casté contre le mauvais objet → **extraire en colonnes nommées dans un CTE,
   filtrer dans le suivant** (cf. `flux_c15.sql`).
2. **PIVOT dynamique** : ne crée que les colonnes rencontrées dans le corpus → binder error aval
   sur les absentes → **agrégation conditionnelle sur le domaine fermé des cadrans**.
3. **Cast struct strict** : `CAST(json AS STRUCT(...))` casse sur toute clé inattendue/absente
   (4 formes de `classeTemporelle` observées sur le corpus R64 réel) → **accès JSON tolérant**
   (`->>` / `unnest(cast(... as json[]))`).

## Décisions et histoire

- [ADR-0020](adr/0020-linearisation-en-dbt.md) — la linéarisation vit en dbt (prototype, fork α).
- [ADR-0021](adr/0021-bascule-production-dbt.md) — bascule production actée : parité totale
  legacy/dbt prouvée 3× (golden, 4 400 XML du cache local, corpus SFTP complet ~700 k lignes),
  5 défauts du legacy corrigés en route (grain PDL, index/conso R15 ~75 % faux, chimères
  multi-relevés, double-comptage des re-livraisons F15, gagnant R64 arbitraire).
- Conventions de dates : [conventions-dates-enedis.md](conventions-dates-enedis.md) (ADR-0003
  amendé #294 : R151 J → J+1 portée par le **mart `releves`** uniquement ; l'endpoint brut
  `/flux/r151` sert la date nue, fidèle source, dépréciable).
