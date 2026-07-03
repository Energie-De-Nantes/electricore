# Axe 2 — couverture data par NC → source de vérité moisniversaire

Spike #545 (sous-issue du PRD #542, Observabilité des non-communicants). Couvre **R15 /
F15 / C15 / R67** : un premier lot de la campagne M023
[#543](https://github.com/Energie-De-Nantes/electricore/issues/543) a été landé (22/25 NC),
ce qui permet le **verdict V2 définitif** (section 7).

Script rejouable : [`axe2_couverture.py`](axe2_couverture.py) (`uv run python
docs/spikes/nc/axe2_couverture.py`), lecture seule DuckDB, invariants inline. Il s'adapte à
la présence du R67 : verdict définitif si `flux_r67` est landé, provisoire sinon. Le landing
d'un ZIP M023 se fait via [`lander_r67_m023.py`](lander_r67_m023.py) puis `uv run python -m
electricore.ingestion rebuild`. Base locale au 2026-07-03 : données jusqu'au 2026-06-17.
Détail par PDL en sortie locale non committée (`sorties-locales/axe2_couverture_nc.csv`) —
RGPD : ce rapport ne contient que des agrégats.

## Cohorte

25 PDL non-communicants (présents au périmètre selon [ADR-0052](../../adr/0052-presence-perimetre-spans-rsc-fermeture-code-sortie.md),
`niveau_ouverture_services = '0'` sur le dernier fait connu de leur RSC).

## 1. R15 cyclique

- Couverture : 25/25 PDL ont au moins un relevé R15 cyclique (`Motif_Releve = 'CYCL'`).
- Fréquence : écart médian de **61 jours** entre deux relevés cycliques (min 5, max 123,
  105 intervalles observés) — **88 % des intervalles dépassent 40 jours** : chez les NC,
  le rythme de relève dominant est bimestriel/trimestriel, pas mensuel.
- Nature d'index : **100 % des relevés cycliques NC sont marqués `estimé`** (0 `réel`
  parmi les relevés `CYCL` — la relève réelle existe mais est portée par d'autres motifs,
  cf. section 5).
- Régularité du moisniversaire : sur les 19/25 PDL avec ≥3 relevés cycliques, l'écart-type
  du jour du mois de relevé est **médian 0,3 jour** (max 6,3 j) — le jour de relève
  cyclique est très stable une fois établi.
- **Écart d'ingestion découvert** : la colonne d'index canonique de `flux_r15` (dbt) est
  **NULLE à 100 %** pour les 25 PDL NC (0 valeur sur l'ensemble de leurs relevés, tous
  motifs confondus). Cause : le modèle dbt n'extrait que le bloc JSON
  `Classe_Temporelle_Distributeur`, absent des relevés de ces PDL (compteurs
  électromécaniques / non-Linky). L'index existe pourtant, sous un bloc frère
  `Classe_Temporelle` (singulier), universellement présent dans le flux — l'écart est une
  lacune d'ingestion, pas une absence Enedis. Hors scope de correction pour ce spike ;
  signalé comme piste de suivi.

## 2. F15

- Couverture : 24/25 PDL ont au moins une ligne F15.
- 200 fenêtres de facturation cyclique valorisées en kWh (`type_facturation = 'CYCL'`,
  `nature_ev = '01'`) sur 24 PDL.
- **Invariant — absence de recouvrement** : 0 chevauchement détecté sur les 177 paires de
  fenêtres consécutives (139 strictement adjacentes, 38 avec un trou entre deux fenêtres).
- **Invariant — réconciliation somme-des-fenêtres vs total** : pour les 23 PDL avec ≥2
  fenêtres, la somme des durées de fenêtres ne dépasse jamais l'amplitude totale observée
  (23/23 conformes) — la partition temporelle est saine.
- Durée des fenêtres : médiane **50 jours** (min 1, max 89), cohérente avec la cadence
  bimestrielle observée côté R15.

## 3. C15 — index aux événements

- 33 événements C15 sur les 25/25 PDL NC : `CFNE` (21), `MDPRM` bascule de niveau (6),
  `MES` (4), `MDACT` (1), `AUTRE` (1).
- **0/25 PDL avec un index avant/après porté par un de ces événements.** Sur ce corpus,
  les types d'événements observés chez les NC (entrée en portefeuille, bascule de niveau
  d'ouverture) ne matérialisent structurellement pas de relevé d'index — contrairement à
  un `MES`/`MCT` classique côté communicant. Le C15 n'est donc **pas** une source d'index
  exploitable pour les NC sur cette base.

## 4. R67 — mesures facturantes (campagne M023 #543 landée)

Un lot M023 a été landé (via [`lander_r67_m023.py`](lander_r67_m023.py) → `flux_r67`,
[ADR-0047](../../adr/0047-flux-r67-energie-par-periode-distributeur-hors-releves.md)).

- **Couverture : 22/25 PDL NC**, 403 fenêtres (une mesure d'énergie par période, déjà
  différenciée par cadran et par période par Enedis). Les 3 NC manquants ne sont pas dans
  ce lot (vraisemblablement définitivement absents côté SGE).
- **Nature : 65 % `estimé`, 27 % `régularisé`, 8 % `réel`.** R67 apporte surtout de
  l'*estimation Enedis* — il ne résout **pas** la rareté du relevé réel (cf. section 5),
  cohérent avec le parc pré-Linky de l'axe 1.
- Motifs : `CYCL` 246, `AUTRE` 132, `CFNS` 25.
- **Fenêtres** : taille médiane **56 j** (moisniversaire, bimestriel). Le pavage est propre
  à un recouvrement résiduel près (1 recouvrement inter-motifs — CFNS/CYCL — toléré par
  l'ADR-0047, contrairement à F15 dont l'unicité de motif garantit le non-recouvrement).
- **Profondeur** : R67 remonte à **2023-06-27** contre **2024-04-09** pour le F15 en base —
  ~9 mois de mesures moisniversaire de plus.
- **Invariant — cohérence R67↔F15** : sur les **84 fenêtres à bornes strictement identiques**
  (CYCL des deux côtés), R67 et F15 donnent **exactement** la même énergie (84/84 à ±1 kWh,
  écart moyen 0,00 kWh). R67 ne contredit jamais le facturé : il l'**étend** (246 fenêtres
  CYCL vs 200 pour F15, et plus profond).

## 5. Fréquence des relevés Réels (relève physique) chez les NC

Seuls **7/25 PDL** portent au moins un relevé de nature `réel` sur toute la fenêtre
d'observation (18/25 n'en ont aucun) — et aucun PDL n'en a plus d'un. Fenêtre
d'observation médiane par PDL : ~405 jours. Autrement dit, **une relève physique
"propre" est un événement rare chez les NC** (à peine plus d'un tiers des PDL en ont vu
une seule en plus d'un an) : un solde basé sur des bornes réelles n'est praticable qu'à
une cadence largement supra-annuelle, jamais mensuelle. Donnée directement consommée par
la praticabilité « bornes réelles » de l'axe 4.

## 6. Cohérence R15 vs F15

En s'appuyant sur l'extraction de secours (`Classe_Temporelle`, cf. section 1 — lecture
brute hors dbt, propre à ce spike, aucune modification de production), 53 fenêtres ont
des bornes strictement identiques entre R15 (diff d'index entre deux relevés cycliques)
et F15 (période facturée). Sur ces fenêtres communes :

| Cadran | Fenêtres comparées | Écart moyen | Écart max | Exactes (±1 kWh) |
|---|---|---|---|---|
| base | 43 | 0,00 kWh | 0 kWh | 43/43 |
| hp | 2 | 0,00 kWh | 0 kWh | 2/2 |
| hc | 2 | 0,00 kWh | 0 kWh | 2/2 |

**Concordance parfaite** sur les 47 fenêtres comparables : une fois l'index R15 extrait
correctement (bloc `Classe_Temporelle`), il redonne exactement l'énergie facturée en F15.
R15 et F15 ne sont donc pas deux vérités concurrentes mais **la même énergie**, vue depuis
deux flux différents — F15 l'agrège déjà par fenêtre facturée, R15 exige un diff
d'index entre deux relevés.

## 7. V2 (définitif) — désignation de la source de vérité moisniversaire

> **Verdict V2 définitif** — un lot R67 de la campagne M023 #543 est landé.

Confrontation des quatre sources chez les NC :

- **C15 écarté** : 0 % de couverture en index chez les NC sur ce corpus.
- **R15 seul écarté comme source d'énergie** : son index canonique est nul à 100 % pour les
  NC (lacune d'ingestion, section 1) ; seules ses *dates* de relevé sont exploitables.
- **F15** : source facturée fiable (fenêtres propres, 24/25), mais moins profonde que R67 et
  strictement corroborante (identique à R67 sur bornes communes).
- **R67** : énergie moisniversaire déjà différenciée par cadran ET par période, 22/25 NC,
  profondeur ~3 ans (9 mois de plus que F15), nature étiquetée, **identique à F15 là où les
  bornes coïncident**.

**Source de vérité désignée : R67**, avec F15 en corroboration et garde-fou. R67 est le
seul flux qui donne, pour les NC, une énergie déjà ventilée par période moisniversaire sur
une profondeur exploitable, sans jamais contredire le facturé.

**Réserve assumée** : R67 est **majoritairement `estimé`** (8 % de lignes réelles) — c'est
la meilleure estimation Enedis disponible, pas du relevé physique. Le solde « propre » sur
relevé réel reste hors de portée à cadence utile (section 5). Une régularisation NC
s'appuie donc sur R67 en assumant son caractère estimé. Ce verdict alimente l'ADR final
[#548](https://github.com/Energie-De-Nantes/electricore/issues/548).
