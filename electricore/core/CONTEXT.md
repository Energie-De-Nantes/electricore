# Contexte — core (métier)

Vocabulaire du domaine métier ElectriCore : acteurs, points de livraison, tarification, événements contractuels, calculs, intégration Odoo. C'est le contexte canonique référencé par les autres modules.

## Langue

Voir [ADR-0004](../../docs/adr/0004-langue-francaise.md) — l'intégralité du code, des colonnes et de la documentation est en français.

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

**CTA** (Contribution Tarifaire d'Acheminement) :
Taxe assise sur la part fixe du TURPE (puissance), reversée à la CNIEG pour financer les retraites des industries électriques et gazières.

**Taux en vigueur** :
Valeur d'un taux réglementé (Accise, CTA, TURPE…) applicable à une date donnée. Les taux changent par arrêté ou décret CRE : les fichiers `*_rules.csv` versionnent ces changements en stockant seulement la date d'**entrée en vigueur** (`start`). Chaque ligne remplace la précédente jusqu'à la ligne suivante (ou indéfiniment pour la dernière) — il n'y a pas de colonne `end`, car les taux régulés sont continus dans le temps. La sélection « taux en vigueur à la date X » est implémentée par `ajouter_taux_en_vigueur()` dans `core/pipelines/taux.py`.
_Éviter_ : barème (gradué par montant, pas par date), grille tarifaire.

**Trimestre fiscal** :
Unité de déclaration des taxes (Accise, CTA), notée `YYYY-TN` (ex : `2025-T1` pour janvier-mars 2025).

---

## Événements contractuels (codes C15)

Les *entrées* (le PDL devient actif chez nous) sont `PMES`, `MES`, `CFNE`. Les *sorties* (le PDL nous quitte) sont `RES`, `CFNS`. Les modifications en cours de vie utilisent `MCT`. Les flux C15 qui transportent ces événements sont décrits dans `electricore/etl/CONTEXT.md`.

**PMES** (Première Mise En Service) :
Variante de MES correspondant à une première activation du PDL. Distinction exacte avec MES à préciser (probablement liée à l'absence d'historique contractuel antérieur).

**MES** (Mise En Service) :
Activation d'un PDL chez nous.

**CFNE** (Changement de Fournisseur Nouvelle Entrée) :
Entrée d'un PDL chez nous en provenance d'un autre fournisseur ; le PDL était déjà actif côté Enedis.

**RES** (Résiliation) :
Clôture d'un contrat sur un PDL.

**CFNS** (Changement de Fournisseur Sortie) :
Sortie d'un PDL vers un autre fournisseur ; symétrique de CFNE côté sortant.

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

## Concepts pipeline

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
Alignement des sources de relevés (R151, R15, R64) sur la convention de date « début de journée ». Implémenté dans `releves_harmonises()` — voir [ADR-0003](../../docs/adr/0003-r151-date-harmonisation.md).

**`date_ajustee`** (champ) :
Booléen marquant les relevés dont la date a été décalée pendant l'harmonisation (R151 +1 jour).

**Contrat lissé** :
Modalité de facturation où le client paie un montant mensuel constant basé sur une estimation annuelle, plutôt qu'au prorata de la consommation réelle du mois. Régularisation annuelle. S'oppose au contrat *réel* (facturation au prorata).

---

## Intégration Odoo

**RSC** (Référence Situation Contractuelle) :
Identifiant Enedis d'une situation contractuelle d'un PDL — un PDL peut avoir plusieurs RSC successives. Côté Odoo, portée par `x_ref_situation_contractuelle` sur `sale.order`.

**`x_invoicing_state`** :
Champ Odoo qui matérialise l'avancement d'un `sale.order` dans le cycle de facturation mensuel. Les vérifications pré-facturation (`/check odoo` côté bot) reposent en partie sur sa répartition.
