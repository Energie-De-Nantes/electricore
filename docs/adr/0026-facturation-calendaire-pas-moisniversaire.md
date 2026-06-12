# Facturation calendaire du 1ᵉʳ au 1ᵉʳ, pas au moisniversaire Enedis

## Contexte

Enedis facture l'acheminement au **moisniversaire** : la période de facturation de
chaque contrat est ancrée au jour de souscription (souscrit un 12, facturé du 12 au 12).
Chaque souscripteurice a donc sa propre période, et c'est sur ce découpage que sont
construits les agrégats des flux de facturation Enedis (R15 notamment : énergies,
nombres de jours, TURPE par période moisniversaire).

Un fournisseur qui facture lui aussi au moisniversaire peut réutiliser ces agrégats
tels quels — c'est le chemin « zéro calcul ».

## Décision

**Facturer les souscripteurices en calendaire : du 1ᵉʳ du mois au 1ᵉʳ du mois suivant.**
Aux bornes de vie du contrat, la première et la dernière période sont simplement
tronquées :

- entrée : date d'entrée → 1ᵉʳ du mois suivant ;
- sortie : 1ᵉʳ du mois → date de sortie.

Ce n'est **pas un prorata** : rien n'est réparti proportionnellement. L'énergie, les
jours et le TURPE de ces périodes tronquées sont calculés réellement — différences
d'index aux dates exactes, comptage des jours effectifs — comme pour n'importe quelle
période pleine.

## Raison

Simplicité pour les souscripteurices : toutes les factures couvrent le même mois
calendaire — lisibles, comparables entre elles et d'un souscripteur à l'autre,
alignées sur le rythme des budgets des ménages.

## Conséquences

- **Les agrégats de facturation Enedis sont inexploitables tels quels** : leur
  découpage (moisniversaire) ne coïncide jamais avec le nôtre (calendaire). Le R15
  ne peut pas servir de source de facturation.
- **Énergie, nombres de jours et TURPE doivent être recalculés** sur le découpage
  calendaire, à partir des relevés d'index et des événements contractuels. C'est la
  raison d'être des pipelines de `core/` (historique → abonnements / énergie →
  facturation) et des événements FACTURATION générés au 1ᵉʳ de chaque mois par
  `pipeline_historique`.
- Toute proposition du type « brancher la facturation directement sur les données
  Enedis » doit d'abord lever ce choix de mode — il est structurel, pas un détail
  d'implémentation.
- Voir aussi [ADR-0023](0023-periodisations-separees-abonnement-energie.md) : au-delà
  du découpage, Enedis sépare aussi abonnement (à échoir) et énergie (à échue) — deux
  raisons indépendantes pour lesquelles la facture réseau ne se superpose pas à la
  facture client.

### Alternative écartée

- **Facturer au moisniversaire** : réutilisation directe des agrégats Enedis, pipelines
  de calcul presque inutiles. Écarté au profit de l'expérience souscripteurice — le
  coût (recalcul complet) est porté par ElectriCore, pas par les client·es.
