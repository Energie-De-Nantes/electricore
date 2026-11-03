---
fraicheur: 2026-11-03
---

# Trouver la donnée

Cette page s'adresse à toi si tu es **opérateur·ice** et qu'on te pose une **question de
données** : « il nous faudrait les consommations mensuelles de ces 30 PDL sur trois ans »,
« on doit faire la déclaration CTA du trimestre », « combien d'acheminement dans le budget
de ce site ? ». La réponse est rarement une simple requête : la donnée existe souvent dans
**plusieurs sources** qui ne sont pas synchronisées, chacune avec sa propre convention de
période, et une partie n'est tout simplement pas du ressort d'ElectriCore.

Ces pages cartographient ces chemins. Elles ne réexpliquent pas les concepts — le
[glossaire métier](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/core/CONTEXT.md)
le fait — elles disent **où regarder et comment y accéder**.

- **[Catalogue des besoins](besoins.md)** — une fiche par besoin courant : statut,
  profondeur, où regarder, comment accéder, pièges.
- **[Obtenir les données Enedis](sources-enedis.md)** — flux et services Enedis,
  profondeurs d'historique, conditions, et quoi faire quand l'historique manque.
- **[Flux Enedis](flux/index.md)** — une fiche par guide Enedis : contenu, obtention,
  pièges, ce qu'ElectriCore en extrait et ce qu'il laisse dans le brut.

## La méthode en cinq questions

Une demande se traduit en **chemin d'action** en répondant, dans l'ordre :

1. **Quoi, exactement ?** Grandeur (consommation, index, courbe de charge, puissance,
   montant), **grain** (jour, mois civil, période de relevé, année scolaire…),
   **profondeur** (combien de mois en arrière), **périmètre** (quels PDL, C4 ou C5).
   Une demande « croisée » (consos + TURPE + CTA + part fournisseur) se découpe en
   plusieurs besoins du [catalogue](besoins.md).
2. **Est-ce du ressort d'ElectriCore ?** Chaque fiche porte un statut :

    | Statut | Sens |
    |---|---|
    | **calculé** | ElectriCore dérive la valeur (énergies, TURPE, CTA, périmètre…). |
    | **lu** | ElectriCore sert la donnée Enedis telle quelle, linéarisée. |
    | **à construire** | Du ressort d'ElectriCore, mais pas encore supporté. |
    | **ailleurs** | Pas son rôle : ERP du fournisseur, portail Enedis, client lui-même. |

    « Ailleurs » n'est pas une impasse : la fiche dit **où** et **qui** détient
    l'information. Exemple type : l'accise se déclare sur ce qui a été **facturé**, donc
    l'assiette vit dans l'ERP ; ElectriCore ne fournit que le taux
    ([ADR-0027](../adr/0027-endpoint-lecture-meta-periodes-odoo-tire.md)).
3. **La donnée est-elle présente ?** Le type peut être supporté sans que la donnée soit
   là : instance déployée récemment, PDL entré tard au périmètre, flux non souscrit.
   Vérifie avant de promettre — voir [Vérifier la disponibilité](#verifier-la-disponibilite).
4. **Sinon, comment l'obtenir ?** Demande ponctuelle Enedis (service M023 sur le portail
   SGE), rejeu d'archives, ou source tierce — voir
   [Obtenir un historique manquant](sources-enedis.md#obtenir-un-historique-manquant),
   qui dit aussi **où la donnée atterrira** dans ElectriCore, donc ce qu'on pourra
   calculer ensuite.
5. **Quels pièges de période et de synchronisation ?** Voir ci-dessous — c'est là que
   se logent la plupart des erreurs.

## Les pièges transverses

**Les sources ne parlent pas du même mois.**

- ElectriCore facture au **mois civil**, du 1er au 1er, bornes tronquées aux entrées et
  sorties ([ADR-0026](../adr/0026-facturation-calendaire-pas-moisniversaire.md)).
- Enedis publie R15 et F15 au **moisniversaire** (date de relève du compteur) ; les
  mesures facturantes R67 sont par **période de relevé** (bimestrielle pour un compteur
  non communicant).
- Un logiciel de facturation fournisseur peut facturer au mois civil, à la date
  anniversaire du contrat, ou mélanger les deux selon la ligne (énergie vs abonnement).
- Une demande au grain « année scolaire » ou « année budgétaire » se reconstruit par
  sommes de mois civils, **ou** par prorata des jours quand la source est en périodes
  de relevé. Dis-le dans la réponse.

**Les bornes et les dates bougent.** Intervalles `[début, fin)` partout dans ElectriCore ;
R151 décalé de +1 jour pour s'harmoniser avec les autres flux
([conventions de dates](../conventions-dates-enedis.md)) ; certains exports tiers ont des
bornes inclusives ou une fin à J+1.

**Les flux ne sont pas synchronisés entre eux.** Un F15 arrive en retard et daté dans le
passé ; un C15 peut être republié (la **première** publication fait foi, l'inverse des
X12 et R64) ; plusieurs sources peuvent porter un relevé le même jour ; un trimestre
« terminé » n'est pas forcément « frais »
([ADR-0058](../adr/0058-declaration-taxe-nouvelle-architecture.md)). Date toujours ta
lecture : « état de la base au JJ/MM ».

**Les identités ne se recouvrent pas.** Le grain d'ElectriCore est la **RSC** (référence
de situation contractuelle), pas le PDL : un PDL peut avoir deux lignes dans le même
mois. Le C15 porte le **titulaire** (nom ou raison sociale, SIRET), mais ElectriCore
ne l'extrait pas encore ; le payeur et le groupement de facturation, eux, n'existent que
dans l'ERP.

**Le périmètre Enedis n'est pas forcément « le nôtre ».** Le périmètre est une donnée
réseau ; il peut être partagé entre entités, et savoir si un client est « chez nous »
relève de l'ERP.

## Vérifier la disponibilité

| Question | Où |
|---|---|
| Quelles tables de flux existent ? | `GET /` → `available_tables` |
| Combien de lignes, jusqu'à quand ? | `GET /flux/{table}/info` (`count`, `derniere_date`) — bot `/flux stats <table>` |
| Relevés canoniques disponibles ? | `GET /releves/info` |
| Base accessible, ingestion en cours ? | `GET /health`, `GET /ingestion/jobs` — bot `/ingestion` |
| Une archive Enedis a-t-elle été manquée ? | vue `audit_sequences` (DuckDB direct, pas d'API) — [ingestion](../ingestion.md) |
| Ce PDL a-t-il des relevés sur la période ? | `GET /releves?prm=…&debut=…&fin=…` |

!!! warning "Profondeur d'historique"
    `/flux/{table}/info` donne la **dernière** date, pas la **première**, et `GET /releves`
    n'est ni trié ni complet au-delà de 1000 lignes. Pour savoir jusqu'où remonte un PDL,
    télécharge `/releves.arrow?prm=…` et prends le minimum de `date_releve`, ou interroge
    DuckDB en lecture seule. La date de déploiement d'une instance borne son historique :
    avant, il n'y a que ce qu'on a rejoué ou importé.

## Pour aller plus loin

Les chemins d'accès en détail : [contrats d'intégration](../contrat-meta-periodes.md),
[ingestion des flux](../ingestion.md), [index des ADRs](../maintenir/index-adr.md).

[Retour à Facturer](../facturiste/index.md).
