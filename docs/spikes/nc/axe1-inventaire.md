# Axe 1 — inventaire du parc non-communicant

> Généré par `docs/spikes/nc/axe1_inventaire.py` le 2026-07-03 — issue [#544](https://github.com/Energie-De-Nantes/electricore/issues/544), PRD [#542](https://github.com/Energie-De-Nantes/electricore/issues/542).
>
> Uniquement des agrégats anonymes — aucun PDL, RSC ni num_compteur. Le détail par PDL est une sortie locale non committée (`sorties-locales/axe1_parc_nc.csv`).

## Synthèse

- **25 PDL** sont non-communicants (NC) aujourd'hui : présents au périmètre et au niveau d'ouverture 0, sur 1225 RSC présentes (2.0 %).
- **Palier technologique** : 0/25 sont sur le type de compteur communicant dominant (`CCB`, Linky à collecte fermée/refusée — action client envisageable) ; 25/25 sont sur un autre type (matériel pré-Linky).
- **Ancienneté** : le champ natif Enedis de bascule est vide sur 25/25 RSC NC actuelles ; ancienneté dérivée de l'historique C15 pour celles-ci (détail : axe 3).
- **Bascules** : 25 bascules ont quitté le niveau 0 sur tout l'historique, **0** y sont revenues (aucune bascule 2→0 ni 1→0 observée à ce jour).
- **Mouvements du parc NC** : 51 entrées / 26 sorties historiques (solde 25, cohérent avec le NC actuel).

## 0. Volumétrie

- lignes `flux_c15` : 2204
- RSC distinctes : 1413
- PDL distincts : 1372

## 1. Présence au périmètre

- RSC présentes (ADR-0052) : 1225 / 1413 (sorties : 188)
- Répartition par niveau d'ouverture des services (RSC présentes) :
    - niveau 0 : 25 (2.0 %)
    - niveau 2 : 1200 (98.0 %)

→ **NC actuel (présent + niveau 0) : 25 PDL**

## 2. Palier technologique

- `type_compteur`, parc présent (tous niveaux) :
    - 'CCB' : 1200
    - 'CFB' : 15
    - 'CEB' : 8
    - None : 2

- `type_compteur`, NC (niveau 0) :
    - 'CFB' : 15
    - 'CEB' : 8
    - None : 2

- `categorie`, NC (niveau 0) :
    - 'RES' : 23
    - 'PRO' : 2

- Type de compteur dominant côté communicant (niveau ≥ 1) : `CCB` (hypothèse de travail : Linky).
- NC sur ce même type (**Linky à collecte fermée/refusée, action client envisageable**) : 0
- NC sur un autre type (**matériel pré-Linky, ouverture non pertinente**) : 25

## 3. Ancienneté au niveau 0

- Champ natif `date_changement_niveau_ouverture_services` renseigné pour 0/25 RSC NC actuelles (vide à 100 % → repli sur l’historique C15).

- Détail par PDL exporté (sortie locale, non committée) : `sorties-locales/axe1_parc_nc.csv`

- Distribution d'ancienneté au niveau 0 (RSC NC présentes) :
    - ≤3 mois : 5
    - 6-12 mois : 5
    - 12-24 mois : 11
    - >24 mois : 4

## 4. Bascules de niveau (tout l'historique C15)

- Matrice des bascules observées (niveau précédent → nouveau niveau) :
    - 0 → 1 : 23
    - 0 → 2 : 2
    - 1 → 0 : 0
    - 1 → 2 : 24
    - 2 → 0 : 0
    - 2 → 1 : 0

- Total bascules **quittant** le niveau 0 (0→1 ou 0→2) : 25
- Total bascules **revenant** au niveau 0 (1→0 ou 2→0) : 0

## 5. Entrées / sorties du parc NC par année

- Entrées dans le parc NC (nouvelle RSC ou bascule vers niveau 0), par année :
    - 2024 : 22
    - 2025 : 17
    - 2026 : 12

- Sorties du parc NC, par année et par cause :
    - 2024 (bascule vers un niveau supérieur) : 5
    - 2025 (bascule vers un niveau supérieur) : 15
    - 2025 (sortie du périmètre (RES/CFNS)) : 1
    - 2026 (bascule vers un niveau supérieur) : 5

- Total entrées historique : 51 — total sorties historique : 26 — solde : 25
