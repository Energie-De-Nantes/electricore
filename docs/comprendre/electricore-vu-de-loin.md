---
fraicheur: 2026-07-04
---

# ElectriCore vu de loin

Tu es peut-être dans un collectif énergie, une coopérative, un groupe qui se demande
s'il pourrait s'approprier cet outil. Avant de passer la main à une personne technique,
tu veux une image claire de **ce que fait ElectriCore**, sans jargon et sans code.

La voici.

## En une phrase

**Des fichiers de données brutes arrivent d'Enedis ; ElectriCore les range et refait
tous les calculs ; il en sort des factures qu'on peut vérifier ligne à ligne.**

C'est tout. Le reste n'est que le détail de ces trois temps.

## Les trois temps

### 1. Des fichiers arrivent

Enedis ne t'envoie pas une jolie facture : il dépose, régulièrement, des **fichiers
techniques** — des relevés d'index, des changements de contrat, des informations de
facturation du réseau. Illisibles tels quels pour un humain, et déposés en vrac.

ElectriCore va **chercher ces fichiers automatiquement** et les **déchiffre** (Enedis les
protège pour la confidentialité).

### 2. L'outil range et recalcule

Une fois les fichiers récupérés, ElectriCore fait deux choses.

- **Il range.** Le fouillis brut devient des tables propres et cohérentes : chaque relevé
  à sa place, chaque contrat suivi dans le temps, les doublons et corrections démêlés.
- **Il recalcule.** À partir de ces données rangées, il refait **tout** le calcul de
  facturation vu dans les pages précédentes : l'énergie par cadran (différences d'index),
  l'abonnement au prorata des jours, le TURPE, les taxes. Sur des **mois calendaires nets**,
  pas sur les découpages d'Enedis.

Point important : le calcul est **rejouable**. Repars des mêmes fichiers de base, tu
obtiens exactement le même résultat. Rien n'est bricolé à la main dans un coin.

### 3. Des factures vérifiables sortent

Le résultat n'est pas juste un montant. Pour chaque mois et chaque point de livraison,
ElectriCore produit une facturation **rattachée à ses preuves** :

- le montant, décomposé par étage (énergie, abonnement, TURPE, taxes) ;
- **les relevés exacts** qui l'ont justifié — quelles dates, quels index, mesurés ou estimés ;
- un **verdict de confiance** : ce mois s'appuie-t-il sur des mesures réelles, ou sur des
  estimations à surveiller ?

Autrement dit, chaque facture arrive avec le « pourquoi » attaché. C'est ce qui la rend
**vérifiable**, et c'est la promesse de la page précédente tenue concrètement.

## Ce qu'ElectriCore n'est pas

Pour évaluer justement, autant savoir où sont les bords.

- **Ce n'est pas un fournisseur.** ElectriCore ne vend pas d'électricité et n'a pas de
  client à lui. C'est un **moteur de calcul** au service de qui facture.
- **Ce n'est pas magique.** Il recalcule à partir des données d'Enedis ; si une mesure
  manque à la source, il le **signale** plutôt que d'inventer.
- **Ça ne décide pas à ta place.** Aujourd'hui, l'outil **propose** une facturation ;
  c'est un·e humain·e qui la valide avant qu'elle parte dans le logiciel de gestion. Rien
  n'est envoyé sans qu'une personne ait regardé.

## Qui met les mains dedans ?

Trois cercles, du plus large au plus technique :

- **Toi, qui évalues** : cette page suffit. Tu sais maintenant ce que fait l'outil et ce
  qu'il ne fait pas.
- **La personne qui facture** au quotidien : elle pilote l'outil via un bot de messagerie
  et des feuilles de calcul interactives, sans coder — c'est la section *Facturer*.
- **La personne technique** qui installe, adapte, ou contribue au code — c'est la section
  *Développer*.

Passer d'un cercle à l'autre, c'est le principe de cette documentation : chacun·e monte
d'une marche quand iel est prêt·e.

## Pour aller plus loin

Deux marches au-dessus, selon qui prend le relais :

- Pour voir l'outil **au travail** au quotidien :
  [la section Facturer](../facturiste/changelog-facturiste.md).
- Pour la **mécanique technique** (architecture, code, données) :
  [Développer](../contribuer/developper.md).

[Retour à l'accueil](../index.md).
