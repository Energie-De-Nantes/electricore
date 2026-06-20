# Chronologie du contrat : substrat consolidé, attributs de situation hors du mart relevés

## Statut

accepted — supersède l'**attachement d'attributs** de [ADR-0029](0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md) (pas son modèle canonique de relevés). Préserve [ADR-0023](0023-periodisations-separees-abonnement-energie.md).

## Contexte

Le mart dbt `releves` **recopiait** (forward-fill par PDL) les attributs de *situation* — `niveau_ouverture_services`, `ref_situation_contractuelle` (RSC), `formule_tarifaire_acheminement` (FTA) — sur les relevés périodiques, depuis le dernier relevé **C15 porteur d'index**. Or un attribut qui change sur un événement C15 **sans index** ne devient jamais un relevé (`int_releves__c15` filtre `index is not null`) : la recopie traîne alors une valeur **périmée**.

- **Niveau** : *systématique* — les `MDPRM` (bascule de niveau) sont toujours sans index. Confirmé en prod (RSC `834877952` : la borne du 01/04 héritait niveau 0 du `CMAT` du 11/03 alors que le PDL était niveau 2 depuis le `MDPRM` du 16/03 → mois `réelle` faussement `non_communicante`).
- **RSC/FTA** : *rare mais réel* — `CFNE`/`MES`/`PMES` sans index changent RSC/FTA.

Le cœur, lui, dérive **déjà** le niveau correctement : `pipeline_historique` forward-fille les attributs de situation sur le flux C15 **complet** (MDPRM compris). Et dans la chronologie énergie, RSC/FTA des relevés périodiques viennent **déjà** de la requête FACTURATION (historique), pas du mart — le `_right` du `join_asof` montre que la version mart ne fait que rider, ignorée. Le niveau avait simplement pris la mauvaise porte ([#324](https://github.com/Energie-De-Nantes/electricore/issues/324)).

## Décision

**Les attributs de situation appartiennent au substrat d'événements consolidé — la *Chronologie du contrat* (`pipeline_historique`) — pas au relevé.** Le mart `releves` ne porte plus d'attribut de situation **recopié** : il garde la valeur **native** d'un relevé C15 (le fait propre de l'événement), `null` sur les périodiques. La chronologie source les trois étiquettes depuis la situation : le niveau rejoint RSC/FTA dans la requête FACTURATION.

La *Chronologie du contrat* (grain RSC, bornée entrée→sortie) et la *Chronologie du point* (grain PDL, qui enjambe les RSC successives) sont **deux grains de filtre d'un même substrat**, pas deux pipelines. On dérive de ce substrat les périodisations énergie / abonnement / non-communicant — chacune avec **son propre découpage** : partager le substrat n'est pas unifier les découpages (ADR-0023 préservé).

## Alternatives écartées

- **Corriger le forward-fill dans dbt** (reconstruire une ligne de temps de situation en SQL pour joindre la bonne valeur) — rejeté : duplique l'historique que le cœur dérive déjà, et continue de **coller** la situation sur des lectures.
- **Ne corriger que le niveau** (laisser RSC/FTA recopiés à la `0029`) — rejeté : laisse dormir le même bug à un attribut près, alors que la solution s'applique à la ligne d'à côté. Scan des notebooks : aucun ne lit RSC/FTA sur la sortie brute de `releves()`, donc rien ne s'oppose au retrait des trois.

## Conséquences

- **Increment 1 (prod)** : niveau sourcé depuis la requête FACTURATION + retrait du forward-fill niveau dans `releves.sql`. Corrige le faux-`non_communicante` des compteurs récemment activés.
- **Increment 2** : retrait du forward-fill RSC/FTA du même endroit. `/releves` sert alors des **lectures pures** (+ attribution native C15) ; l'attribution par-contrat, quand un consommateur la voudra, viendra de la *Chronologie du contrat*.
- **Filtrabilité en amont** : un filtre PDL/RSC posé **au loader** (`charger(c15().filter(…), releves().filter(…))`) reconstruit la chronologie d'un seul point/contrat sans changer le pipeline — partition-local + horizon paramétrique (#179) rendent le résultat filtré **identique** au run plein restreint. À garder par un **test de parité** (`filtré sur X ≡ plein ∩ X`), car l'équivalence repose sur « horizon = paramètre ».
- **Consommateurs moteurs différés**, sur le même substrat : timeline non-communicante via R15 ([#322](https://github.com/Energie-De-Nantes/electricore/issues/322)), et vue Odoo facturiste (lecture par-point à la demande, pattern ADR-0027).
