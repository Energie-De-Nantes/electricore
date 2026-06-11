# Bascule production de l'ingestion sur dbt

## Statut

**Acté** (juin 2026). Suite conditionnelle de l'[ADR-0020](0020-linearisation-en-dbt.md) :
la condition (parité sur les 6 flux golden) a été remplie puis dépassée, le prototype est
promu chemin de production et le legacy retiré. Issues #134–#138, release cible 2.0.0 (#139).

## Contexte

L'ADR-0020 conditionnait la bascule à la reproduction des golden. La validation est allée
plus loin, en trois étages de preuve :

1. **golden** : 6 flux reproduits (fixtures réelles anonymisées + fixtures XSD maximales) ;
2. **cache local** : ~4 400 XML réels re-parsés par les deux chemins, parité record-par-record ;
3. **corpus SFTP complet** : run réel des deux pipelines (~700 k lignes chacun), parité
   **totale 7/7 tables** par empreintes canoniques (PR #133).

La campagne a en outre révélé **5 défauts du chemin legacy**, invisibles depuis des mois,
tous corrigés en route : relevés agrégés par PDL au lieu de l'occurrence (chimères
inter-événements), ~75 % des index R15 mélangeant index et consommation (condition
`Classe_Mesure` absente), relevés multiples perdus/fusionnés (jusqu'à 20 par PRM sur R151),
re-livraisons F15 double-comptées (261 lignes de facture), gagnant R64 arbitraire sous
fenêtres chevauchantes. Un moteur non observé pendant 8 mois n'était pas stable — il était
non audité ; la réécriture a payé d'abord comme audit.

## Décision

**Le chemin dbt est le chemin de production ; le legacy est retiré.**

- dbt matérialise dans le schéma **`flux_enedis`** de la base de production, mêmes noms de
  tables que l'ex-legacy → les loaders core et l'API sont inchangés au jour de la bascule.
  Le brut vit dans `flux_raw` de la même base.
- `/etl/run` (API, cron VPS, bot) lance **`pipeline_dbt`** ; modes `test` / `all` / sélection /
  `rebuild` (re-matérialisation depuis le brut, zéro réseau, ~13 s pour 700 k lignes) /
  `resync` (re-téléchargement complet, exceptionnel) — `reset` déprécié, alias de `resync`.
- `flux.yaml` est réduit au **mouvement** (`file_pattern`, `format`, `file_regex`) ; le contrat
  de sélection vit dans les modèles dbt. Le DSL de linéarisation, le moteur `parser_flux_xml`,
  le parseur R64, `pipeline_production` et le harnais de comparaison (mort avec son oracle)
  sont supprimés (−1 600 lignes).
- L'**oracle golden** est le chemin de production lui-même (`generer_golden.py` : landing →
  `dbt build` → capture) — tout changement de contrat est un diff git de golden revu en PR.
- Contrats durcis au passage : grain = l'occurrence (relevé), domaine de cadrans **fermé**
  (colonnes stables pour l'aval), types XSD à la source, instants ancrés `Europe/Paris`
  indépendamment du fuseau de session, dédup déterministe (re-livraisons par `file_name`,
  R64 par `(pdl, type_releve, date_releve)`, livraison la plus récente gagne).

## Conséquences

- **Breaking (2.0.0)** : l'historique re-matérialisé diffère du legacy là où celui-ci avait
  tort (chimères, doublons, index/conso mélangés) ; `pipeline_production` disparaît ; le format
  de `flux.yaml` change.
- Le brut (~1 Go pour 2 ans de flux) est la police d'assurance : toute correction de modèle est
  un `rebuild` de quelques secondes, l'historique entier est backfillé sans re-téléchargement.
- Une évolution de flux Enedis ne casse plus l'ingestion (brut sans schéma) ; l'adaptation est
  du SQL (`coalesce(nouveau, ancien)`) + une régénération de fixtures XSD — voir le guide
  [docs/ingestion.md](../ingestion.md).
- SQLMesh a été évalué et écarté (#55) ; à réévaluer uniquement en cas de migration vers un
  warehouse cloud.
