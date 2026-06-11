# Linéarisation des flux Enedis en dbt (prototype)

## Statut

**Prototype** (branche `dbt`, issue #123). Décision conditionnelle : si le prototype
reproduit les 6 golden (#123 R64, #124 C15, #125 les autres), la bascule production
sera actée séparément (suite conditionnelle de #55). Tant que le prototype n'a pas
convaincu, le chemin de production reste `etl/parsing/` (ADR ETL existante, DLT).

## Contexte

La discovery #55 a établi que le chemin XML/JSON → DuckDB → DataFrame typé traverse
deux *seams* qui se compensent : `etl/parsing/` émet des dicts non typés (tout en
`str`), et `core/loaders/duckdb/sql.py` rattrape les types par 50 `CAST` à la lecture.
S'y ajoute une **asymétrie** (relevée pendant la discovery) : le XML est linéarisé par
un moteur générique piloté par `flux.yaml`, le JSON R64 par ~250 lignes Python
hardcodées (sélection *et* pivot wide).

Trois prototypes ont été comparés contre le **filet golden** (#121/#122) sur les flux
extrêmes C15 (conditions + axe parent) et R64 (pivot) :

- **A** — moteur dict unifié Python : reproduit C15, mais le pivot R64 reste hors du
  langage de config ;
- **B (dbt-duckdb)** — `read_json`/structs + SQL : reproduit C15 *et* R64, le pivot
  tient dans un `PIVOT`, le typage est gratuit (structs inférés), et l'axe parent XPath
  disparaît (les champs du parent sont en scope après `unnest`, les conditions
  deviennent des `WHERE`).

dbt est le standard de transformation SQL (runner natif dans DLT, dbt Core reste
Apache 2.0). Inconvénient assumé : changer le modèle d'un flux exige SQL + l'outillage
dbt — mais `flux.yaml` n'a pas été édité depuis 8 mois (config écrite-une-fois), et un
modèle dbt est du `SELECT` standard là où `flux.yaml` est un dialecte maison.

## Décision

**La linéarisation (sélection + pivot + typage) vit dans des modèles dbt, pas en Python.**

### Fork raw-landing : α — colonne JSON en DuckDB

DLT garde `decrypt | unzip` puis dépose **une ligne par document**, contenu intégral
préservé en **colonne JSON** (`columns={"content": {"data_type": "json"}}` — vérifié :
pas d'explosion en tables filles). Un modèle dbt `staging` par flux fait un unique
`CAST(content AS <struct>)` qui récupère l'ergonomie de `read_json` ; le modèle `flux_`
aval reste propre (`unnest` + `WHERE` + `PIVOT`).

Alternative écartée — **β, fichiers staging + `read_json` glob** : ergonomie maximale
(zéro cast) mais **deux stockages** (DuckDB + fichiers) et cycle incrémental à outiller
à la main. α garde **un seul store et l'incrémental DLT** (la raison d'être du choix DLT),
au prix d'un cast staging par flux — et les types sont de toute façon souhaités
(dérivables des XSD Enedis, `Documents/guides_flux/`).

### Périmètre

- `core/` reste Polars + Pandera (calculs purs) — **ADR Polars-only intacte** : dbt ne
  touche que l'ingestion/linéarisation, pas les calculs métier.
- DLT reste la couche de mouvement (SFTP, déchiffrement, incrémental) — **ADR DLT
  amendée**, pas remplacée : DLT dépose le brut, dbt linéarise.
- Le **filet golden** est le critère d'acceptation : chaque table `flux_*` matérialisée
  par dbt doit reproduire `tests/fixtures/flux/golden/<table>.json`, modulo traçabilité.

### Politique timezone

Les timestamps sont matérialisés en instant-correct (`TIMESTAMP`/`TIMESTAMP WITH TIME
ZONE` selon la source). R64 porte des dates sans offset — sujet non bloquant ici. Le cas
des offsets Europe/Paris (`+02:00` du C15, où DuckDB normalise en UTC à l'inférence) est
tranché en #124 : la parité golden compare l'**instant**, pas la représentation string.

## Conséquences

- Nouveau projet dbt `electricore/etl/dbt/` (extra `[dbt]`), un modèle `staging` + un
  `flux_` par type de flux. Le SQL R64 remplace `etl/parsing/r64.py`.
- Primitive `etl/raw_landing.py` (`lander_documents_bruts`) : dépose un document en
  colonne JSON ; réutilisée par les tests, par le chemin XML (#124, après conversion
  XML→dict) et par le câblage production (suite conditionnelle).
- Tant que le prototype tourne en parallèle, `etl/parsing/` n'est **pas** retiré ; sa
  suppression + le runner DLT→dbt + le déploiement VPS sont la dernière étape, conditionnée
  à la parité sur les 6 flux.

## Limites à connaître avant un changement d'échelle

- **Le type struct du staging est aujourd'hui capturé par inférence** sur un échantillon.
  Pour la production, le dériver des XSD Enedis (autoritatifs) plutôt que d'un fichier —
  sinon un flux dont l'échantillon ne couvre pas un champ optionnel produit un cast
  incomplet.
- **La parité golden est testée via `dbtRunner` programmatique** (skip si l'extra `dbt`
  est absent). Le test landé la fixture en colonne JSON et compare la table matérialisée —
  fidèle au chemin production, mais dépend de dbt installé.
- **Un seul flux JSON (R64) aujourd'hui.** Si Enedis généralise le JSON, la primitive de
  landing et le motif staging/flux absorbent les nouveaux flux sans changement d'archi —
  c'est précisément ce que ce prototype valide.
