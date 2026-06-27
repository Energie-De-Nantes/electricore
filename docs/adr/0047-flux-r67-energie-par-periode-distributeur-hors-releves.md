# Flux R67 : énergie par période livrée par le distributeur — asset parallèle, hors union `releves`

## Statut

Accepté — grill `/grill-with-docs` du 27/06/2026 (sur [#214](https://github.com/Energie-De-Nantes/electricore/issues/214), brique de l'épique [#191](https://github.com/Energie-De-Nantes/electricore/issues/191)), adossé à 4 demandes M023 R67 réelles (pilotes Linky/non-Linky/MES/PMES).

- **Prolonge** [ADR-0020](0020-linearisation-en-dbt.md) (landing JSON-colonne + motif staging/flux), [ADR-0034](0034-index-kwh-entiers-floor-au-boundary-dbt.md) (Wh→kWh floor au boundary), [ADR-0042](0042-convention-date-instant-jour-civil-boundary-flux.md) (instant `TIMESTAMPTZ` vs jour civil `DATE`), [ADR-0035](0035-typage-chaine-ingestion-coeur-proprietaire-par-fait.md) (cadrans SSOT, parité de frontière).
- **Amende** [ADR-0029](0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md) (R67 **exclu** de l'union des relevés index) et [ADR-0028](0028-identite-releve-cle-metier-priorite-sources.md) (R67 **hors** de l'espace de nommage `releve_id` et de la table de priorité C15>R64>R151).
- **Respecte sans réserve** [ADR-0016](0016-core-erp-agnostique.md) (cœur ERP-agnostique) / [ADR-0027](0027-endpoint-lecture-meta-periodes-odoo-tire.md) (electricore = physique/réseau, Odoo = comptable) : R67 est de la donnée distributeur pure.

## Contexte

Le flux **R67 (« mesures facturantes »)** est publié par Enedis **à la demande** (prestation M023, ponctuelle, JSON zippé sur le même SFTP que R64). Il porte l'**historique des consommations retenues pour la facturation**, par cadran, sur un horizon glissant `max(aujourd'hui − 36 mois, dernière MES)`. Son débouché tranché en amont (#214) : **amorcer (cold-start) la provision d'un mensualisé** dès le mois 1, avant qu'EDN ait accumulé ~12 mois de relevés propres (R151/R64). Rôle mince, pas un flux du quotidien.

La tentation est de le traiter « comme R64 » (autre flux JSON timeseries à cadrans). **C'est un piège.** La dissection des 4 fichiers réels établit une différence de nature, pas de surface :

| | R64 (et R151, R15, C15) | **R67** |
|---|---|---|
| Feuille JSON | `valeur[]` = un **index cumulé** à un **instant** (`d`, `v`, `iv`) | `quantite[]` = une **énergie consommée** sur une **période** (`dbtMesure`, `finMesure`, `codeNature`, `codeStatut`, `quantite` en Wh) |
| Grain | 1 relevé = 1 instant | 1 ligne = 1 période `[debut, fin)` |
| Grandeur | odomètre (cumul) → l'énergie se calcule **en différenciant** deux index (`pipeline_energie`) | énergie **déjà différenciée** par le distributeur |

Autrement dit R67 arrive **en aval** du travail que `pipeline_energie` fait sur les index : c'est de l'énergie pré-calculée, parallèle au chemin index→énergie, **pas un 5ᵉ relevé**. Le forcer dans la chaîne des relevés (mart `releves`, ADR-0029) casserait cette chaîne (cf. *Raison*).

La dissection a aussi remonté quatre faits structurants que le modèle doit encaisser :

1. **Les `contexte[]` partitionnent le temps par motif** (`CYCL` cyclique, `CFNS` entrée/sortie fournisseur, `MCT` changement FTA/puissance, `AUTRE` régularisation). Chaque motif a des trous que les autres comblent ; il faut **unioner tous les contextes** pour une couverture continue. **Zéro recouvrement temporel inter-motif** (vérifié 35/35, 23/23, 8/8 adjacences `fin[i]==dbt[i+1]`, 0 doublon de clé) → l'union ne double-compte jamais.
2. **La grille distributeur peut être absente** (pilote non-Linky : seulement `F/HoroF1`) **ou dégénérée** (`DI000001` BASE seul, en fenêtre d'entrée/MCT, avant bascule `DI000003` 4 cadrans — qui peut survenir **en cours de mesure**).
3. **Somme 4-cadrans distributeur ≠ total fournisseur** de ~1 kWh sur la moitié des intervalles (arrondi par cadran ; reconstructions indépendantes). Mais `D/DI000003` est un **raffinement strict** du fournisseur : `F-BASE = Σ4 cadrans`, `F-HP/HC = HPB+HPH / HCB+HCH` (vérifié **exact** sur intervalle propre).
4. **Quantités négatives légitimes** : `codeNature=C` « Régularisé » est une **régularisation physique** (révision d'une conso souvent estimée), `quantite < 0` quand elle réduit une sur-estimation. Donnée réseau valide — **sans rapport** avec les conventions de signe comptables.

## Décision

`flux_r67` est un **asset parallèle** : il réutilise toute la *plomberie* JSON de R64 (ADR-0020) mais sa *linéarisation* est propre, et il **ne rejoint jamais** la chaîne des relevés index.

1. **Landing α réutilisé (ADR-0020).** Bloc `R67` dans `config/flux.yaml` (`format: json`, glob `**/R63_R64_R65_R66_R67_C68/*_R67_*.zip`), table `raw_r67` (JSON-colonne), modèles `stg_r67` + `flux_r67`. `raw_r67 → flux_r67` dans `MODELES_PAR_RAW` ; **jamais** ajouté aux gardes des marts périodiques (`releves`, `chronologie_releves`).

2. **Grille coalescée — 1 grille par période, priorité `D/DI000003 ≻ D/DI000001 ≻ F`.** `energie_<cadran>_kwh` est rempli depuis la grille **la plus fine disponible** pour chaque période ; `code_grille`/`code_calendrier` gardés en provenance. Justification : `DI000003` étant un raffinement strict, le total fournisseur est **re-dérivable** par agrégation (« on ne perd rien avec le DI ») ; le repli `F` n'intervient que quand le distributeur manque (non-Linky) ou est dégénéré.

3. **Union de tous les `contexte[]`.** Concaténation des quantités de tous les motifs (l'union tuile sans trou ni recouvrement) ; `id_motif_releve` **gardé en annotation** (le grain `(pdl, debut, fin)` est préservé — motif 1:1 par intervalle, zéro overlap). Le motif est le *pourquoi* d'un segment (entrée/sortie/changement/cyclique/régul), pas une partition à sommer.

4. **Format wide `energie_<cadran>_kwh`** via `pivot_cadrans` **paramétré** : la macro gagne un argument `grandeur='index'` (défaut inchangé) ; R67 l'appelle avec `grandeur='energie'` sur `quantite`. Réutilise le SSOT cadrans (`cadrans_releve`/`CADRANS`, ADR-0035) — **pas** de re-triplication de la liste des 7 cadrans.

5. **Wh→kWh par `floor` explicite** (`cast(floor(quantite / 1000.0) as bigint)`, ADR-0034) — **pas** `// 1000` : l'opérateur `//` de DuckDB **tronque vers zéro** (≠ floor) et R67 porte des **négatifs**, donc `floor` explicite (vers −∞). Lossless sur la donnée observée (249/249 quantités multiples de 1000 Wh → résultat identique) ; le floor explicite honore le contrat si une régul négative non-multiple survient un jour. **Négatif préservé** (régul physique).

6. **Bornes `debut`/`fin` en jour civil `DATE`, intervalle demi-ouvert `[debut, fin)`** (ADR-0042 : ce sont des bornes de période, pas des instants). **Aucune** harmonisation +1j (R67 est un asset autonome, jamais joint à R151/R64). La `periode` (fenêtre d'éligibilité M023, plus large que la couverture data) est gardée **à part** et n'est **jamais** utilisée comme borne d'énergie.

7. **Nature depuis `codeNature`** (`R/E/C → réel/estimé/régularisé`, défaut prudent `estimé`) — **pas** `nature_depuis_etape_metier` (l'`etapeMetier` est toujours `FACT`). `code_statut` conservé en passthrough.

8. **Clé propre `periode_id = md5(flux_R67|pdl|debut|fin)[:16]`**, mintée sur dates **naïves** (stabilité tz, cf. ADR-0028), dans un **espace de nommage distinct** de `releve_id`. **Dédup** par `qualify row_number() over (partition by pdl, debut, fin, grille order by modification_date desc, date_creation desc) = 1` (patron R64 : la livraison la plus récente gagne ; protège une éventuelle re-publication restatée même si l'échantillon est 100 % `Initial`).

9. **`flux_r67` n'est jamais unioné dans `releves`/`chronologie_releves`.** Il est requêtable directement (loader `r67()`). Sa frontière Pandera (grain énergie-par-période, **pas** `RelevéIndex`) est différée hors du présent périmètre d'ingestion (le loader expose un LazyFrame `SELECT *`, `validator=None`, comme R64 aujourd'hui).

10. **La provision reste hors `core`/hors ce flux.** `flux_r67` livre l'énergie physique par cadran ; la dérivation de provision (estimation annuelle/cadran, total ÷ 12 plate) et son écriture sont une brique séparée de #191, côté core→Odoo (ADR-0016/0027). Même partage que l'accise : electricore = grandeur physique, Odoo = politique.

## Raison

**Pourquoi R67 ne peut pas entrer dans `releves` (les 5 ruptures).** Le contrat `releves` (`contrat_releve()`, ADR-0029/0045) est *1 ligne = 1 index relevé à un instant* :

1. **Grain** : `releves` est clé sur un `date_releve` unique (`TIMESTAMPTZ`) ; R67 a **deux** bornes (`debut`, `fin`). Fabriquer un instant unique jette une borne et la sémantique de période.
2. **Nature de la valeur** : `releves` porte des colonnes **`index_<cadran>_kwh`** (cumul/odomètre) ; R67 porte de l'**`energie_<cadran>_kwh`** (déjà différenciée). Verser l'une dans l'autre fait calculer à `pipeline_energie` une « énergie d'énergie » — du faux.
3. **Identité & priorité** : R67 n'a pas de place dans l'espace `releve_id` ni dans le `CASE` de priorité `C15→R64→R151` (ADR-0028) ; il tomberait au bucket `else` ou **entrerait en collision** sur `(rsc, date_releve, ordre_index)` avec un vrai relevé index — exactement l'« accident lexical » qu'ADR-0028 tue.
4. **SSOT index** : `releves` est la source unique des valeurs d'index (ADR-0045) ; y injecter du non-index casse l'invariant `releve_id` UNIQUE et le star-join 1:1 de `chronologie_releves`.
5. **Frontière Pandera** : `RelevéIndex` devrait s'élargir pour admettre bornes de période + énergie — ce qui ferait sauter le test de parité ADR-0035, justement le garde-fou contre cette dérive.

**Pourquoi coalescer plutôt que garder les deux grilles.** `D/DI000003` est un raffinement strict de `F` (re-dérivation vérifiée exacte) : garder `F` en plus serait redondant *et* un footgun (sommer sans filtrer `code_grille` double-compte). L'écart `D`-vs-`F` (~1 kWh/période, arrondi) est du bruit immatériel sur une provision ÷12 (déjà réconcilié au spike : `FC022034` BASE 2513 kWh/an ↔ détail 4 cadrans).

**Pourquoi `floor` explicite (et pas `// 1000`).** Cohérence avec la conversion Wh→kWh de tout le boundary dbt (ADR-0034). Mais `//` de DuckDB **tronque vers zéro** (≠ floor), et R67 porte des **négatifs** : on écrit donc `floor(x / 1000.0)` (vers −∞), pas `// 1000` comme R64 (qui n'a que des cumuls ≥ 0). À noter (seule nuance vs ADR-0034) : la preuve « l'erreur de troncature télescope » vaut pour des **index cumulés** ; sur des **énergies par période** chaque ligne tronque indépendamment, l'erreur ne s'annule pas d'une période à l'autre. Sans objet ici (data lossless : multiples de 1000 → `//` et `floor` coïncident), mais on ne s'appuie pas sur la preuve de télescopage.

## Alternatives écartées

- **Unioner R67 dans `releves` (le « 5ᵉ relevé »).** Casse grain + valeur + priorité + SSOT + frontière (les 5 ruptures). C'est la confusion centrale que cet ADR existe pour interdire.
- **Garder les deux grilles `D` et `F` en lignes séparées (`code_grille` dimension).** Servirait la provision « au kWh facturé près » et l'optim tarifaire sans re-dériver — mais footgun de double-compte et choix de grille remonté chez chaque consommateur ; rejeté car le raffinement strict de `DI000003` rend `F` re-dérivable et l'écart d'arrondi est immatériel.
- **Round / float pour Wh→kWh.** Plus « précis » par période — rejeté pour cohérence maison (ADR-0034 floor) ; lossless de toute façon.
- **Minter un `releve_id` pour R67.** Pollue l'identité des relevés index (ADR-0028).
- **`ge ≥ 0` sur l'énergie.** Rejetterait les régularisations physiques négatives (donnée prod légitime) — même *classe* de bug que les schémas trop étroits (accise rc14), mais ici la cause est physique, pas comptable.
- **CAR (Consommation Annuelle de Référence) au lieu de R67.** Source idéale (auto-recalculée par Enedis) mais **absente de tout flux ingéré** ; exigerait l'API SGE web-service (#207) — plus gros chantier que R67.

## Conséquences

- **Issues à cuter (une PR, brique de #191 / sous-tâche de #214)** : (1) `config/flux.yaml` `R67` + `raw_r67` + `MODELES_PAR_RAW` ; (2) `stg_r67` + `flux_r67.sql` (linéarisation énergie-par-période) + `schema.yml` ; (3) golden depuis échantillon réel anonymisé (`tests/fixtures/flux/r67.json` + `golden/flux_r67.json` + `test_dbt_r67_golden.py`) ; (4) loader `r67()` (`helpers.py` + `DESCRIPTOR_R67` + export) ; (5) doc recette `docs/ingestion.md` + procédure M023.
- **ADR amendés** : ajouter à [ADR-0029](0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md) une note « R67 intentionnellement exclu (énergie-par-période) » ; à [ADR-0028](0028-identite-releve-cle-metier-priorite-sources.md) « R67 hors espace `releve_id` » ; à [ADR-0035](0035-typage-chaine-ingestion-coeur-proprietaire-par-fait.md) « `pivot_cadrans` paramétré `grandeur` ».
- **Macro** : `pivot_cadrans` (et `cadrans_index_noms/_cols/_rename` si un contrat les exige) gagnent un argument `grandeur='index'` ; le test de parité `cadrans_releve`/`CADRANS` reste couvrant.
- **Hors périmètre** : la dérivation de provision (#191) ; la frontière Pandera propre de R67 ; l'automatisation de la demande M023 (portail manuel, [#216](https://github.com/Energie-De-Nantes/electricore/issues/216) wontfix). La voie M023 batch est recevable **aussi** pour les non-communicants (démenti du préjugé « niveau 0 → rejet »), mais le contenu y est grossier (BASE, bimestriel, estimé).
- **Limite à connaître avant un changement d'échelle** : la dédup `code_statut` n'est pas exercée (échantillon 100 % `Initial`) ; la clé `(pdl, debut, fin, grille)` + `modification_date`/`date_creation` est posée par précaution pour la re-publication. Ne pas supposer un statut unique.
- **Glossaire** : `electricore/ingestion/CONTEXT.md` — entrées *Mesures facturantes (R67)* (préciser : énergie par période, hors `releves` ; régul `C` = physique, peut être négative) + *CAR* (déjà esquissées en [#481](https://github.com/Energie-De-Nantes/electricore/pull/481), à consolider avec cette nuance).
