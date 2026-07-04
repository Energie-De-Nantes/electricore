---
fraicheur: 2026-07-04
---

# L'anatomie d'une facture

Une facture d'électricité paraît opaque parce qu'elle empile plusieurs choses qui
n'ont rien à voir entre elles. Une fois que tu sais les séparer, elle devient lisible.
Prenons une facture fictive et décortiquons-la, ligne par ligne.

## Les quatre étages de ta facture

Ce que tu paies se range en quatre grandes familles :

1. **L'abonnement** — un forfait fixe, que tu consommes ou non.
2. **L'énergie** — ce que tu as réellement consommé, en kWh.
3. **Le TURPE** — le péage du réseau.
4. **Les taxes** — la part qui va à l'État et à des fonds publics.

Reprenons chacune.

## 1. L'abonnement : le forfait d'accès

L'abonnement est un montant **fixe par mois** (ou par an), indépendant de ta
consommation. Tu le paies même en partant en vacances.

Il dépend d'un paramètre : ta **puissance souscrite**, exprimée en **kVA** (une mesure
de « débit » électrique maximal). Plus tu peux tirer de puissance en même temps —
four, plaques, chauffe-eau, voiture qui charge — plus ta puissance souscrite est
élevée, et plus ton abonnement coûte cher. Un petit studio n'a pas la même puissance
qu'une maison tout-électrique.

## 2. L'énergie : ce que tu as consommé

C'est le cœur variable de la facture, et c'est là que reviennent les **index** vus à la
page précédente : ton énergie du mois, c'est la **différence entre deux relevés**,
multipliée par un prix au kWh.

### Base, ou heures pleines / heures creuses ?

Selon ton contrat, l'énergie est comptée dans un ou plusieurs **compteurs internes**,
qu'on appelle des **cadrans** :

- **Base** : un seul prix, 24 h/24. Simple. Tu consommes, c'est le même tarif jour et nuit.
- **Heures pleines / heures creuses (HP/HC)** : deux prix. La nuit et à certaines heures
  creuses, le kWh est **moins cher** ; le reste du temps (heures pleines), il est plus
  cher. L'idée : t'inciter à décaler certains usages (chauffe-eau, lave-linge) vers les
  heures où le réseau est moins sollicité.

Concrètement, ton compteur tient un total **séparé** pour chaque cadran :

> Heures creuses : 180 kWh × prix bas
> Heures pleines : 160 kWh × prix haut
> → deux lignes, une seule consommation de 340 kWh.

Il existe des découpages plus fins encore (par saison, par jour de pointe), mais le
principe ne change pas : **un cadran = un compteur interne avec son propre prix.**

## 3. Le TURPE : le péage du réseau

Le **TURPE** (Tarif d'Utilisation des Réseaux Publics d'Électricité) est le **péage**
que tu paies pour faire passer ton électricité sur les fils d'Enedis et de RTE. Comme
un péage d'autoroute : peu importe qui t'a vendu la voiture, tu paies la route.

- Il ne va **pas** à ton fournisseur, mais au **réseau**.
- Il est **fixé par la CRE**, le régulateur — ton fournisseur ne le décide pas, il ne
  fait que le collecter pour le reverser.
- Il a lui-même une part **fixe** (liée à ta puissance souscrite) et une part
  **variable** (liée à tes kWh consommés).

En général, le TURPE n'apparaît pas en ligne séparée sur ta facture : il est **fondu**
dans le prix de l'abonnement et de l'énergie que ton fournisseur t'affiche. Il est bien
là, mais caché. Le sortir de l'ombre est l'une des choses qu'ElectriCore sait faire.

## 4. Les taxes : la part publique

Enfin, l'État et des fonds publics prélèvent plusieurs taxes :

- **L'accise sur l'électricité** (anciennement appelée TICFE) : une taxe sur chaque kWh
  consommé. Elle a absorbé l'ancienne CSPE. C'est le gros de la fiscalité sur l'énergie.
- **La CTA** (Contribution Tarifaire d'Acheminement) : une contribution calculée sur la
  **part fixe du réseau**, qui finance les retraites des salarié·es des industries
  électriques et gazières.
- **La TVA** : comme sur tout achat. Particularité de l'électricité, elle s'applique à
  **deux taux** — un taux réduit sur l'abonnement, un taux plein sur l'énergie.

Les montants de ces taxes changent par décision publique (loi de finances, arrêtés).
On ne les fige donc pas ici : retiens leur **rôle**, pas un chiffre qui périmerait.

## Le récapitulatif

| Étage | Ce que c'est | À qui ça va | Ça dépend de |
|---|---|---|---|
| Abonnement | Forfait d'accès | Ton fournisseur | Ta puissance souscrite |
| Énergie | Tes kWh consommés | Ton fournisseur | Tes relevés d'index |
| TURPE | Le péage du réseau | Le réseau (Enedis/RTE) | Puissance **et** kWh |
| Taxes | Accise, CTA, TVA | L'État / fonds publics | kWh, part fixe, tout |

La leçon : **ton fournisseur ne garde qu'une partie** de ce que tu paies. Le reste
transite par lui vers le réseau et vers l'État. Une facture, c'est un empilement — et
chaque étage se calcule séparément.

## Pour aller plus loin

Tu sais lire une facture. L'étape suivante, c'est de voir comment on la **fabrique**
et on la **vérifie**, mois après mois, côté métier :
[la section Facturer](../facturiste/changelog-facturiste.md) raconte le cycle de
facturation tel qu'il tourne réellement avec ElectriCore.

[Retour à l'accueil](../index.md).
