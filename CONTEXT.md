# Contexte — ElectriCore

ElectriCore traite les flux de données du distributeur Enedis pour le marché français de l'électricité. Ce document est le glossaire canonique : tout terme métier utilisé dans le code, la documentation ou les commits doit avoir sa définition ici.

## Langue

Voir [ADR-0004](docs/adr/0004-langue-francaise.md) — l'intégralité du code, des colonnes et de la documentation est en français.

---

## Acteurs

**Enedis** :
Gestionnaire du réseau de distribution d'électricité en France métropolitaine (filiale d'EDF). Émet les flux de données contractuels et de mesure consommés par ElectriCore.

**CRE** (Commission de Régulation de l'Énergie) :
Régulateur français qui fixe les tarifs d'acheminement (TURPE) et arbitre les évolutions réglementaires.

**Fournisseur** :
Acteur commercial qui vend l'électricité au consommateur final. Utilise les flux Enedis pour facturer ses clients.
_Éviter_ : vendeur, opérateur (ambigus).

**Gestionnaire de réseau** :
Synonyme métier d'Enedis dans la majeure partie du territoire. À distinguer du *transporteur* (RTE, haute tension).

---

## Points de livraison et segments

**PDL** (Point De Livraison) :
Identifiant unique d'un point de raccordement physique au réseau, sur 14 chiffres. Un compteur Linky correspond à un PDL.
_Éviter_ : compteur (le compteur est l'appareil, le PDL est l'emplacement), point de comptage.

**FTA** (Formule Tarifaire d'Acheminement) :
Choix tarifaire contractuel qui détermine la grille TURPE applicable (ex : `BTINFCUST`, `BTSUPCU4`). Conditionne aussi le nombre de cadrans facturés.

**Segment C5** :
Sites BT (Basse Tension) de puissance souscrite ≤ 36 kVA. Marché résidentiel et petit tertiaire ; une seule puissance souscrite.

**Segment C4** :
Sites BT > 36 kVA. Tertiaire et industriel léger ; quatre puissances souscrites distinctes (une par cadran), avec contrainte réglementaire P₁ ≤ P₂ ≤ P₃ ≤ P₄.

---

## Cadrans temporels

**Cadran** :
Plage tarifaire temporelle (heures × saison) groupant des périodes au même tarif. Le découpage en cadrans est fixé par le *calendrier distributeur*.
_Éviter_ : créneau, plage tarifaire.

**Base** :
Tarif unique 24h/24, sans découpage temporel.

**HP / HC** (Heures Pleines / Heures Creuses) :
Découpage en deux cadrans, 16 h pleines et 8 h creuses.

**HPH / HCH / HPB / HCB** :
Découpage en quatre cadrans croisant saison (**H**aute = nov-mars, **B**asse = avr-oct) et plage horaire (**P**leines / **C**reuses). Obligatoire en C4 ; utilisé aussi en Tempo et EJP.

**EJP** (Effacement Jour de Pointe) :
Offre historique avec ~22 jours de pointe annuels à tarif majoré. Maintenue pour les contrats existants.

**Tempo** :
Offre avec jours rouges / blancs / bleus à tarifs différenciés. Découpage temporel HPH/HCH/HPB/HCB.

---

## Calendriers distributeur

**DI000001** :
Calendrier Base (tarif unique).

**DI000002** :
Calendrier HP / HC (deux cadrans).

**DI000003** :
Calendrier 4 cadrans (HPH / HCH / HPB / HCB).

---

## Tarification réseau et taxes

**TURPE** (Tarif d'Utilisation des Réseaux Publics d'Électricité) :
Tarif réglementé payé au gestionnaire de réseau, indépendamment du fournisseur. Se décompose en :
- **TURPE fixe** : part puissance, en €/kVA/an, calculée au prorata des jours d'abonnement.
- **TURPE variable** : part énergie, en c€/kWh, appliquée par cadran sur la consommation.

**CMDPS** (Composante Mensuelle de Dépassement de Puissance Souscrite) :
Pénalité TURPE spécifique aux sites C4 dépassant leur puissance souscrite, exprimée en €/h de dépassement.

**Accise** :
Taxe intérieure sur la consommation finale d'électricité (TICFE), exprimée en €/MWh. Intègre depuis 2022 l'ancienne CSPE.
_Éviter_ : TICFE (renommée Accise), CSPE (fusionnée dans l'accise).

---

## Flux Enedis

**Flux** :
Fichier de données émis périodiquement par Enedis aux fournisseurs, au format XML ou CSV. Chaque type a un code (C15, R151…) et un contenu spécifique.

**C15** :
Flux d'événements contractuels (MES, RES, MCT…). Source de l'historique du périmètre.

**R151** :
Flux de relevés périodiques mensuels (index Linky par cadran). Source principale du calcul de consommation. Convention de date « fin de journée » — voir [ADR-0003](docs/adr/0003-r151-date-harmonisation.md).

**R15** :
Flux de relevés à la demande + événements ponctuels (déplacements, contestations).

**R64** :
Flux de relevés au format JSON, séries temporelles plus granulaires.

**F12** :
Synthèse mensuelle de facturation distributeur (volumes agrégés).

**F15** :
Facturation distributeur détaillée, utilisée pour valider les calculs TURPE.

---

## Événements contractuels (codes C15)

**MES** (Mise En Service) :
Activation initiale d'un PDL.

**RES** (Résiliation) :
Clôture d'un contrat sur un PDL.

**MCT** (Modification Contractuelle Tarifaire) :
Changement de FTA, de puissance souscrite ou de calendrier sur un PDL actif.

---

## Mesures et énergie

**Index** :
Valeur cumulée affichée par le compteur à une date donnée, en kWh. Un index par cadran (`index_hp_kwh`, `index_hph_kwh`…).
_Éviter_ : mesure (trop générique), valeur.

**Relevé** :
Événement de lecture d'index à une date donnée, contenant un ou plusieurs index selon le calendrier. Source : flux R151 (périodique) ou R15 (ponctuel).
_Éviter_ : lecture, mesure.

**Énergie** :
Consommation calculée entre deux relevés (différence d'index), par cadran (`energie_hp_kwh`…). Distincte de l'index.

**Puissance souscrite** :
Limite contractuelle en kVA. Un seul champ en C5 (`puissance_souscrite_kva`), quatre en C4 (`puissance_souscrite_hph_kva`…).

---

## Concepts pipeline (spécifiques ElectriCore)

**Périmètre** :
Vue chronologique des événements contractuels d'un ou plusieurs PDL, enrichie pour la facturation. Produit par `pipeline_perimetre()` à partir du flux C15.

**Abonnement** :
Période continue entre deux événements contractuels où la configuration (FTA, puissance, calendrier) ne change pas. Unité de calcul du TURPE fixe.

**Rupture de période** :
Événement contractuel (MES, MCT, RES) qui découpe une période d'abonnement en deux.

**Période d'énergie** :
Intervalle entre deux relevés d'index, support du calcul de consommation et du TURPE variable.

**Méta-période mensuelle** :
Agrégation d'abonnements + périodes d'énergie sur un mois calendaire, unité de la facturation client mensuelle.

**Harmonisation des relevés** :
Alignement des sources de relevés (R151, R15, R64) sur la convention de date « début de journée ». Implémenté dans `releves_harmonises()` — voir [ADR-0003](docs/adr/0003-r151-date-harmonisation.md).

**`date_ajustee`** (champ) :
Booléen marquant les relevés dont la date a été décalée pendant l'harmonisation (R151 +1 jour).
