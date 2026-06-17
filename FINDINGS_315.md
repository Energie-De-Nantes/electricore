# Spike #315 — statut de communication sur données prod (findings)

Spike **non mergé**, produit les chiffres pour l'ADR-0036 (#316). DB prod locale
(`flux_enedis_pipeline.duckdb`, 2204 événements C15, 1372 PDL, données du 2024-04-03
au 2026-06-07). Reproductible : `uv run python spike_315.py`.

On mesure l'**alignement structurel** (pas un délai de flux) : le modèle « tient »-il ?

## 1. Distribution du niveau d'ouverture sur le parc

Niveau courant (dernier événement par PDL), sur **1372 PDL** :

| niveau | PDL | part |
|--------|-----|------|
| 0 (non-communicant) | 25 | **1.8 %** |
| 1 | 0 | **0.0 %** |
| 2 (communicant complet) | 1347 | **98.2 %** |

Événements bruts (toutes lignes C15) : niveau 0 = 99, niveau 1 = **27**, niveau 2 = 2078.
→ Le niveau 1 existe comme état **transitoire** (27 événements) mais **aucun PDL n'y
stationne** : tous finissent à 0 ou 2.

## 2. Bascules et monotonie

- PDL dont le niveau change ≥ 1 fois : **26 (1.9 %)**
- PDL à **bascule 0 → ≥1** : **25 (1.8 %)**
- PDL **non-monotones** (niveau qui redescend, communicant → non → …) : **0 (0.0 %)**

→ **Écarter les non-monotones en P1 coûte 0 %** sur ce parc. Hypothèse de l'épique validée.

## 3. Alignement présence-d'un-relevé-au-1er ⇆ niveau

Grain : (PDL × 1er du mois dans la fenêtre active). **15 426 bornes**.

| niveau | relevé au 1er présent | absent |
|--------|----------------------|--------|
| 0 | 3 | **421** (99.3 % absent — attendu) |
| 1 | 2 | 6 |
| 2 | **11 225** | 3 769 |

Source du relevé à la borne (priorité C15 > R64 > R151) : **R64 = 7338, R151 = 3607,
C15 = 285**. → Le **R64-pull est la source majoritaire à la borne** (≈ 2/3), conforme à
« R64 demandé à la borne, source maîtrisée » (CONTEXT).

## 4. Anomalies (communicant ∧ relevé absent) et seuil

| seuil | bornes communicantes | avec relevé au 1er | sans relevé (anomalie brute) |
|-------|---------------------|--------------------|------------------------------|
| ≥ 1 | 15 002 | 74.8 % | **25.2 % (3775)** |
| ≥ 2 | 14 994 | 74.9 % | 25.1 % (3769) |

**Seuil ≥1 vs ≥2 : empiriquement identique** (le niveau 1 ne pèse que 8 bornes). On
retient **≥1** : sémantiquement correct (niveau 1 ouvre la collecte quotidienne d'index,
suffisante à la facturation calendaire) et sans coût face à ≥2.

### Le « 25 % » n'est PAS un défaut du modèle — c'est la couverture de pull/extraction

Caractérisation des 3775 bornes communicantes sans relevé au 1er :
- décalage de date (relevé ailleurs dans le mois) : 358
- **aucun relevé du mois (trou réel) : 3417 (22.8 %)**

Mais ce trou est **concentré par mois, pas par PDL** :
- PDL communicants jamais couverts (« noirs ») : **7 / 1324 (0.5 %)**
- toujours couverts : 842 (63.6 %) · partiellement : 475 (35.9 %)
- taux de trou **par mois de borne** très volatil : **2026-05 = 0.4 %**, 2026-06 = 35.7 %,
  2026-02/03 ≈ 32 %, 2025-12 = 13.9 %…

Un mois à 0.4 % de trou prouve que **quand le pull R64 est fait, l'alignement communicant
est quasi-total**. Le « 25 % » reflète donc la **politique de pull R64 / la fenêtre
d'extraction** de cette DB (quels mois ont été tirés), pas une défaillance de collecte.
La vraie population « communicant ∧ incalculable » à usage opérationnel se réduit aux
**~7 PDL noirs** + les trous ponctuels — à confirmer avec la politique de pull réelle.

→ **Le modèle tient** : détection directe par le niveau C15, alignement structurel sain
quand la donnée est tirée.

## 5. ⚠️ Caveat #314 INFIRMÉ sur données prod : le relevé CMAT n'entre PAS via `impacte_energie`

`detecter_points_de_rupture` exécuté sur les vrais événements : **8/61 CMAT seulement**
ont `impacte_energie=True`. Ventilation par forme du calendrier avant→après :

| avant | après | impacte_energie | n | interprétation |
|-------|-------|-----------------|---|----------------|
| null | présent | **False** | **27** | activation pure (calendrier *apparaît*) — **non détecté** |
| null | null | False | 15 | pas de calendrier (niveau 0) |
| présent | présent | False | 11 | calendrier inchangé, index inchangé |
| présent | présent | **True** | 8 | index change dans la ligne → seul cas qui entre |

**Les 27 CMAT d'activation (`avant=null`) n'entrent PAS** : `expr_changement_avant_apres`
exige deux valeurs non-nulles, donc une transition *absent → présent* du calendrier n'est
pas vue comme un changement. La prémisse #314/CONTEXT « le relevé CMAT entre déjà via
impacte_energie par changement de calendrier » est **fausse en prod**.

**Mais c'est probablement inoffensif** : la règle « pas de demi-mois » (CONTEXT *Voie
communicante*) place le **mois de bascule en responsabilité non-communicante** ; le premier
mois pleinement communicant commence au 1er *suivant*, **borné par le périodique (R64/R151),
pas par le relevé CMAT**. Le relevé CMAT exact n'est donc pas requis pour le rollup au grain
méta.

**Décision à acter en ADR-0036 (#316)** — choisir :
1. **(recommandé)** Corriger la CONTEXT : retirer le claim « CMAT déjà impacte_energie » ;
   assumer que le mois de bascule est non-communicant et que le 1er communicant est borné
   par le périodique. Coût : 0 code.
2. Si on veut le relevé de bascule exact dans la chronologie : coder la détection
   *null → valeur* du calendrier distributeur (ou router sur le niveau). Coût : du code +
   re-tests #314.

## Synthèse pour l'ADR-0036

- Détection du statut **directe et fiable** via `Niveau_Ouverture_Services` (C15).
- Parc : 98.2 % communicants, 1.8 % non-communicants, **0 au niveau 1** → **seuil ≥1**
  (équivalent à ≥2 ici, mais correct sémantiquement).
- **0 % non-monotone** → écarter en P1 sans coût.
- Anomalie communicant∧incalculable mesurable mais **dominée par la couverture de pull**,
  pas par des défaillances ; cœur dur ≈ 7 PDL noirs.
- **Rollup pire-gagne au grain méta validé** ; **ne pas s'appuyer** sur l'entrée du relevé
  CMAT via `impacte_energie` (infirmé) — la règle « pas de demi-mois » couvre le cas.
