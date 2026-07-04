---
fraicheur: 2026-07-04
---

# Gouvernance du registre réglementaire

Le **registre réglementaire** est l'un des [trois registres de savoir](../adr/0024-trois-registres-de-savoir.md)
du dépôt (avec le runtime et le structurel) : les **taux régulés** — TURPE (fixe et
variable), Accise (TICFE), CTA — versionnés dans des CSV du dépôt et **appliqués par date
d'entrée en vigueur**, pas lus à runtime depuis un système externe.

## Ce que contient le registre

Trois fichiers dans `electricore/config/` :

| fichier | grille | lu par |
|---|---|---|
| `turpe_rules.csv` | 2D — `Formule_Tarifaire_Acheminement` (FTA) × fenêtre `start`/`end` | lecteur dédié dans `core/pipelines/turpe.py` (grille propre, hors périmètre du lecteur partagé) |
| `accise_rules.csv` | 1D — historique `start` → `taux_accise_eur_mwh` | `charger_regles_taux()` (`core/pipelines/taux.py`) |
| `cta_rules.csv` | 1D — historique `start` → `taux_cta_pct` | `charger_regles_taux()` (`core/pipelines/taux.py`) |

Chaque ligne porte une colonne **`reference`** — la délibération CRE ou l'article de loi de
finances qui la justifie, texte libre + URL. `ajouter_taux_en_vigueur()` attache à chaque
période de consommation le taux applicable à sa date par une jointure `join_asof`
(stratégie `backward` : chaque ligne du registre remplace la précédente jusqu'à la
suivante). Le **millésime** d'un fichier — sa dernière ligne entrée en vigueur, avec sa
référence — se dérive de ce même historique et ne se déclare jamais à part
(`core/millesimes.py`, exposé via `GET /taxes/millesimes`).

## Qui met à jour, et comment

**La lib — le commun ElectriCore — est l'autorité sur le contenu des taux régulés**, pas
une instance ni un ERP particulier :

1. Un taux change (délibération CRE, loi de finances) → une **PR** qui édite la ou les
   lignes concernées des CSV, avec sa `reference` renseignée.
2. Cette PR se relit **comme du code** — même circuit que n'importe quelle contribution
   ([CONTRIBUTING.md](https://github.com/Energie-De-Nantes/electricore/blob/main/CONTRIBUTING.md)) :
   revue humaine, merge humain.
3. La **distribution** est une [release](release.md) — les instances récupèrent le
   nouveau taux à leur prochaine mise à jour d'image, pas avant.

La **surveillance** (repérer qu'une réglementation a changé) est déléguée — un membre non
technique de la fédération, une alerte automatisée, peu importe le canal : le point
d'entrée reste toujours la contribution, jamais un flux de données lu en direct par les
pipelines. Un **check de péremption** heuristique existe déjà comme filet
(`core/peremption.py`, exposé via `GET /taxes/peremption`) : il compare la dernière ligne
connue de chaque registre à un rythme attendu (TURPE ~1ᵉʳ août, Accise ~1ᵉʳ janvier ; la
CTA n'a pas de rythme connu, donc pas de check) et **avertit sans jamais corriger** —
un changement hors calendrier reste invisible tant qu'un humain ne le signale pas.

Un ERP peut offrir un **chemin de saisie** qui aboutit à une contribution (une PR
proposée), mais jamais une source de taux lue à runtime par les pipelines — sans quoi une
facture ne serait plus rejouable depuis une version donnée de la lib, et l'instance
no-ERP se retrouverait sans taux.

## Où vivent les exceptions déclaratives

Le registre réglementaire ne connaît qu'un **taux national standard** par taxe et par
date. Les **catégories d'accise réduites ou exonérées** (certains usages professionnels,
certaines puissances) ne sont **pas** modélisées dans les CSV du dépôt : elles vivent
**côté Odoo**, de façon déclarative — portées par la catégorie de produit facturée
(`agreger_consommations_mensuelles()` dans `core/pipelines/accise.py` filtre et agrège les
lignes de factures telles qu'Odoo les a déjà qualifiées ; le pipeline n'encode lui-même
aucune règle de taux réduit). C'est un choix cohérent avec la frontière ERP-agnostique du
cœur ([ADR-0016](../adr/0016-core-erp-agnostique.md)) : le taux national est un savoir du
commun, l'éligibilité d'un client donné à une exception est un savoir de l'instance.

## Pour aller plus loin

[ADR-0024 — Trois registres de savoir](../adr/0024-trois-registres-de-savoir.md) ·
[Configuration — inventaire complet](https://github.com/Energie-De-Nantes/electricore/blob/main/docs/configuration.md) ·
[Carte des domaines](../contribuer/carte-domaines.md).

[Retour à Maintenir](index.md).
