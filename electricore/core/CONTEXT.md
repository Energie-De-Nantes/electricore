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
_Note_ : « Fournisseur » est générique — il désigne aussi bien le fournisseur opérant une *instance* ElectriCore (ex : EDN, Enargia) que les autres fournisseurs cités dans les événements C15 (CFNE/CFNS). Préférer *instance* quand on parle du fournisseur opérant.

**Instance** :
Déploiement ElectriCore dédié à un *fournisseur* particulier (EDN, Enargia…). Chaque instance a sa propre base DuckDB, ses clés AES, sa config Odoo, sa source SFTP, son sous-domaine. Identifiée par un slug court (`edn`, `enargia`).
_Éviter_ : tenant (anglicisme), déploiement (confus avec l'opération de release).

**Gestionnaire de réseau** :
Synonyme métier d'Enedis dans la majeure partie du territoire. À distinguer du *transporteur* (RTE, haute tension).

---

## Points de livraison et segments

**PDL** (Point De Livraison) :
Identifiant unique d'un point de raccordement physique au réseau, sur 14 chiffres. Un compteur Linky correspond à un PDL.
_Éviter_ : compteur (le compteur est l'appareil, le PDL est l'emplacement), point de comptage.

**RSC** (Référence Situation Contractuelle) :
Identifiant Enedis d'une situation contractuelle d'un PDL — un PDL peut avoir plusieurs RSC successives. Côté ERP, chaque adaptateur décide comment la porter (cf. [`integrations/odoo/CONTEXT.md`](../integrations/odoo/CONTEXT.md) pour la représentation Odoo).

**FTA** (Formule Tarifaire d'Acheminement) :
Choix tarifaire contractuel qui détermine la grille TURPE applicable (ex : `BTINFCUST`, `BTSUPCU4`). Conditionne aussi le nombre de cadrans facturés.

**Segment C5** :
Sites BT (Basse Tension) de puissance souscrite ≤ 36 kVA. Marché résidentiel et petit tertiaire ; une seule puissance souscrite.

**Segment C4** :
Sites BT > 36 kVA. Tertiaire et industriel léger ; quatre puissances souscrites distinctes (une par cadran), avec contrainte réglementaire P₁ ≤ P₂ ≤ P₃ ≤ P₄.

---

## Cadrans temporels

**Cadran** :
Plage tarifaire temporelle (heures × saison) groupant des périodes au même tarif. Le découpage en cadrans est fixé par le *calendrier distributeur*. La liste canonique des 7 cadrans, la relation de synthèse (hp ← hph+hpb, hc ← hch+hcb) et les constructeurs de noms de colonnes (`grandeur_cadran_unité`) vivent dans `core/models/cadrans.py`.
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

**Accise physique** vs **Accise de déclaration** (deux assiettes à ne pas confondre) :
- **Accise physique** : assise sur l'énergie *mesurée* du mois (calculable depuis la *méta-période mensuelle*), au taux standard en vigueur. Vérité physique d'electricore : sert l'analytique et la *référence de régularisation* des lissés. N'est **pas** exposée comme montant dans le contrat de l'endpoint méta-périodes — qui ne livre que le *taux* accise (l'assiette = le facturé appartient à l'ERP).
- **Accise de déclaration** : assise sur l'énergie *facturée* (le facturé = les *provisions* pour un lissé, quantité qui vit côté ERP), au taux applicable. C'est ce qu'on déclare à l'État (`/taxes/accise/*`) — on déclare ce qu'on a facturé, pas ce qu'on a mesuré. Calculée par `pipeline_accise` depuis les lignes facturées.

electricore est l'**autorité du taux** accise ; la valorisation de l'accise *facturée* (assiette = facturé) peut vivre côté ERP. Le taux n'est pas uniforme : il dépend d'une **catégorie** de consommateur — enhancement ouvert : la catégorie est-elle dérivable du flux Enedis, ou saisie côté ERP ?
_Éviter_ : parler d'« assiette accise » sans préciser physique ou déclaration (elles divergent dès qu'un contrat est lissé).

**CTA** (Contribution Tarifaire d'Acheminement) :
Taxe assise sur la part fixe du TURPE (puissance), reversée à la CNIEG pour financer les retraites des industries électriques et gazières.

**Taux en vigueur** :
Valeur d'un taux réglementé (Accise, CTA, TURPE…) applicable à une date donnée. Les taux changent par arrêté ou décret CRE : les fichiers `*_rules.csv` versionnent ces changements en stockant la date d'**entrée en vigueur** (`start`). Pour l'Accise et la CTA, chaque ligne remplace la précédente jusqu'à la ligne suivante (ou indéfiniment pour la dernière) — pas de colonne `end`, les taux régulés sont continus dans le temps ; `turpe_rules.csv` porte en revanche des fenêtres `start`/`end` par grille tarifaire. La sélection « taux en vigueur à la date X » est implémentée par `ajouter_taux_en_vigueur()` dans `core/pipelines/taux.py`.
_Éviter_ : barème (gradué par montant, pas par date), grille tarifaire.

**Référence réglementaire** :
Citation du texte qui fonde un taux régulé (délibération CRE, article de loi de finances). Portée ligne à ligne par la colonne `reference` des fichiers `*_rules.csv` ([ADR-0024](../../docs/adr/0024-trois-registres-de-savoir.md), #185) : chaque changement de taux devient auditable en revue de contribution comme en contrôle a posteriori.
_Éviter_ : source (collision avec les sources de données), justificatif.

**Millésime** :
Dernier changement réglementaire intégré dans un fichier de taux régulés — dérivé, pas déclaré : dernière ligne entrée en vigueur + sa *référence réglementaire*. Dit ce que la lib « sait » de la réglementation ; dérivé par `core/millesimes.py` et exposé via `GET /taxes/millesimes` et `/taxes millesimes` (bot) pour vérifier la fraîcheur d'une instance ([ADR-0024](../../docs/adr/0024-trois-registres-de-savoir.md)).
_Éviter_ : version (réservé aux releases de la lib), tag (réservé à git).

**Trimestre fiscal** :
Unité de déclaration des taxes (Accise, CTA), notée `YYYY-TN` (ex : `2025-T1` pour janvier-mars 2025).

---

## Événements contractuels (codes C15)

Les *entrées* (le PDL devient actif chez nous) sont `PMES`, `MES`, `CFNE`. Les *sorties* (le PDL nous quitte) sont `RES`, `CFNS`. Les modifications en cours de vie utilisent `MCT`. Les flux C15 qui transportent ces événements sont décrits dans `electricore/ingestion/CONTEXT.md`.

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

## Affaires SGE (suivi opérationnel)

Le cycle de vie des demandes de prestation déposées au portail SGE, rendu requêtable (cockpit read-only). Source : flux X12/X13 (décrits dans `electricore/ingestion/CONTEXT.md`). ElectriCore *observe* l'avancement (read-only) ; il n'agit jamais sur le SGE.

**Affaire** :
Dossier de suivi d'une demande de prestation auprès d'Enedis, identifié par un code à 8 caractères (ex : `G08TJ7VC`). Porte un cycle de vie (*jalons*) du dépôt à la clôture. Son `id` est **le même identifiant** que l'`Id_Affaire` porté par les *événements contractuels* C15 : une affaire est le **précurseur opérationnel** d'un événement C15 (MES, CFN…), traçable **avant** que le C15 ne matérialise son issue contractuelle.
_Éviter_ : dossier, demande (la demande n'est qu'une *partie* de l'affaire), commande (terme ERP/Odoo).

**Prestation** :
Type de service demandé à Enedis, porté par le code `objet` de la demande. Catalogue Enedis ~30 types. Les prestations qui **façonnent le périmètre** : `CFN` (changement de fournisseur), `MES` (mise en service), `PMS` (première MES), `CST` (changement de structure tarifaire), `RIC` (résiliation initiative client). À distinguer de **`AME`** (« transmission récurrente des index quotidiens et Pmax ») qui domine le volume (≈ 45 % des affaires EDN observées) mais n'est qu'une *souscription de flux de données* (R63B/R64/R66), pas une intervention de périmètre — le cockpit opérationnel l'écarte par défaut.
_Éviter_ : demande (l'acte de dépôt) employé pour prestation (le type).

**Jalon** :
Étape datée de l'avancement d'une affaire (`num` d'ordre, `dateHeure`, *état d'affaire*). Une affaire accumule ses jalons (cas nominal : DMTR → DMREC → INPL → CPRE). Les flux X12/X13 sont des **snapshots cumulatifs quotidiens** : chaque livraison reprend toute la liste de jalons → la clé logique d'un jalon est `(affaire, num)`, jamais la position-fichier (même raisonnement que l'*identité de relevé*, [ADR-0028](../../docs/adr/0028-identite-releve-cle-metier-priorite-sources.md)).
_Éviter_ : étape (vague), événement (réservé aux *événements contractuels* C15).

**Statut d'affaire** vs **État d'affaire** (deux granularités, à ne pas confondre) :
- **Statut** : cycle de vie grossier de l'affaire, 3 valeurs — `COURS` (en cours), `TERMN` (terminé), `ANNUL` (annulée). Sur le corpus EDN observé : ≈ 94 % TERMN, 5 % ANNUL, < 1 % COURS.
- **État** (`affaireEtat`, porté par *chaque jalon*) : étape fine, ~16 valeurs observées — DMTR « demande transmise », DMREC « demande recevable », INPL « intervention planifiée », CPRE « close, prestation réalisée »… Les états d'**échec/blocage** sont `CPNR` (close, prestation non réalisée), `CNRE` (close, non recevable), et les re-planifications `INRP` — signaux exploitables pour une future alerte (faible volume : quelques-uns par mois).
_Éviter_ : confondre *statut* (3 valeurs) et *état* (fin, ~16 valeurs).

---

## Conventions de date

**Instant** :
Un moment précis et univoque sur la ligne du temps mondiale (l'horodatage d'un *événement* C15, le moment d'un *relevé*). Le même instant partout, quel que soit le lieu où on le lit. Convention de stockage et d'harmonisation : voir [ADR-0042](../../docs/adr/0042-convention-date-instant-jour-civil-boundary-flux.md).
_Éviter_ : timestamp, horodatage, date (un instant n'est pas un jour).

**Jour civil** :
Une journée du calendrier, **sans** heure ni fuseau (une *date de facture*, une borne de période, une date de bascule de niveau). « Le 4 octobre » désigne le même jour partout — lui coller un fuseau (« minuit Paris ») est une erreur de modélisation qui rouvre des bugs de bord.
_Éviter_ : date, jour (seuls) quand la distinction instant / jour civil importe.

**Convention début / fin de journée** :
La borne du jour à laquelle un flux Enedis attribue son *index*. R64 / R15 / C15 : « début de journée » (index au début du jour J). R151 : « fin de journée » (index à la fin du jour J = début de J+1). Aligner R151 sur les autres avance son label d'un jour (le « +1 jour ») — conversion **native** de R151, appliquée à l'ingestion (boundary `flux_r151`), pas une harmonisation portée en aval.
_Éviter_ : « décalage » (ce n'est pas un bug à corriger mais la convention propre de R151).

## Mesures et énergie

**Index** :
Valeur cumulée affichée par le compteur à une date donnée, en **kWh entiers** — le grain facturable. Un index par cadran (`index_hp_kwh`, `index_hph_kwh`…). Enedis livre les index en Wh (R151/R64) ; la résolution sub-kWh est délibérément abandonnée au boundary d'ingestion (l'analyse fine relève des *courbes de charge*, pas des différences d'index), la normalisation Wh→kWh par `floor` est actée par [ADR-0034](../../docs/adr/0034-index-kwh-entiers-floor-au-boundary-dbt.md).
_Éviter_ : mesure (trop générique), valeur ; un `index_*_kwh` en Wh (le suffixe `_kwh` est un contrat).

**Relevé** :
Événement de lecture d'index à une date donnée, contenant un ou plusieurs index selon le calendrier. Source : flux R151 (périodique) ou R15 (ponctuel).
_Éviter_ : lecture, mesure.

**Énergie** :
Consommation calculée entre deux relevés (différence d'index), par cadran (`energie_hp_kwh`…). Distincte de l'index. Ce qu'elle vaut — sa fiabilité — est porté à part par la *qualité de période d'énergie* (réelle / estimée / incalculable). La *provision d'énergie* n'entre pas dans cette échelle : quantité conventionnelle facturée en attente de solde.
_Note d'axe_ : « énergie **mesurée** » employé ailleurs (*Part variable*, *Contrat lissé*, *Accise physique*, *Régularisation*) désigne la consommation **réelle opposée à la provision** (axe *lissé*), pas la *qualité de période* — une période de qualité estimée fait quand même partie de l'énergie mesurée du mois. Les deux axes sont orthogonaux ; « réelle/estimée » est réservé à la qualité, « mesurée » au lissé.

**Identité de relevé** (clé métier) :
Handle stable d'un relevé, dérivé de la lecture *logique* (`pdl`, `date_releve`, source, et le discriminant de la source — `ordre_index` pour les avant/après C15, `type_releve` pour R64). **Pas** la position-fichier (l'`id` d'occurrence dbt `fichier#position` est instable : les fenêtres R64 se chevauchent et la livraison gagnante change à chaque correction) ni un id Enedis natif (absent de R151 *et* de R64, les sources périodiques qui bornent la facturation calendaire ; présent seulement en R15/F15/C15-événements via `Id_Releve`). L'`Id_Releve` natif, quand il existe, est conservé comme *provenance* additionnelle, pas comme clé. Support du lien d'audit entre une *période d'énergie* et les relevés qui l'ont bornée. La priorité des sources qui tranche les doublons (`flux_C15 > flux_R64 > flux_R151`) et le rejet des id natif/fichier comme clé sont actés par [ADR-0028](../../docs/adr/0028-identite-releve-cle-metier-priorite-sources.md).
_Éviter_ : id fichier (c'est de la provenance, pas une identité), Id_Releve (natif et partiel — ne couvre pas le cas dominant).

**Nature d'index** :
Qualité d'une **lecture** d'index, normalisée (réel / estimé / corrigé) depuis des champs source hétérogènes : `Nature_Index` (R15/C15 : `REEL`→réel, sinon estimé par défaut prudent), `etape_metier` (R64 : BRUT/VALID→réel, CORR→corrigé), réel par construction pour les télérelevés périodiques R151. Mention légale obligatoire sur la facture. Qualifie la lecture, **pas la période** : son rollup sur les deux relevés bornants donne la *qualité de période d'énergie* (toutes bornes `réel`/`corrigé` → réelle ; une borne `estimé` → estimée).

**Qualité de période d'énergie** :
Verdict à trois états sur ce que vaut l'énergie d'une *période d'énergie*, porté par le champ `qualite` de `PeriodeEnergie` — **réelle**, **estimée**, **incalculable**. C'est le **rollup** de la *nature d'index* des deux relevés qui bornent la période :
- **incalculable** : un relevé bornant manque (`releve_manquant_debut` / `releve_manquant_fin` disent lequel) — l'énergie n'est pas calculable, ses cadrans sont nuls. Strictement plus fin que `data_complete` (qui valait `False` ici) ;
- **réelle** : les deux bornes sont présentes et de nature `réel` *ou* `corrigé` — un index corrigé reste une vraie mesure (R64 CORR), jamais une estimation ;
- **estimée** : les deux bornes sont présentes mais au moins une est de nature `estimé`.

Vocabulaire aligné sur la *nature d'index* Enedis (le domaine dit *réel*, pas *mesuré*) : le relevé est *réel* (m.), la période est *réelle* (f.) — le genre suffit à séparer les deux grains. Réserver « réelle/estimée » à la qualité libère « mesurée » pour son sens *lissé* (cf. *Énergie*). Voie d'évolution assumée : une estimation ML des relevés manquants ferait passer une période d'*incalculable* à *estimée* — la trichotomie l'absorbe sans changement de modèle.

Remonte au grain *méta-période mensuelle* par **pire-gagne** (`incalculable > estimée > réelle`) : un mois est réel ssi toutes ses sous-périodes le sont. Cette qualité **remplace** ([ADR-0033](../../docs/adr/0033-qualite-periode-remplace-data-complete-coverage.md)) l'ancien appareillage `data_complete` + `coverage_abo`/`coverage_energie` — le booléen ne distinguait pas réel d'estimé, `coverage_abo` était un placeholder figé à `1.0`, et la fraction `coverage_energie` n'était pas un *gate* de facturation actionnable.
_Éviter_ : « énergie mesurée/estimée » comme binaire (il manquait l'état *incalculable*), confondre avec la *nature d'index* (qui qualifie la lecture), ou avec l'« énergie mesurée » au sens lissé (axe orthogonal) ; `data_complete` / `coverage_*` (retirés, ADR-0033).

**Statut de communication** ([ADR-0036](../../docs/adr/0036-statut-communication-routage-energie-grain-meta.md)) :
Capacité **effective** d'un PDL à transmettre ses relevés au fournisseur, dérivée du *niveau d'ouverture aux services* Enedis (balise C15 `Niveau_Ouverture_Services` ∈ {0, 1, 2}, **toujours transmise**, portée par la situation contractuelle ; date de bascule `Date_Changement_Niveau_Ouverture_Services`, événement déclencheur `MDPRM`, précédé de `CMAT` à l'activation du calendrier Distributeur). **Communicant ⇔ niveau ≥ 1** (seuil de *travail*, à arbitrer sur données réelles : niveau 1 ouvre la collecte quotidienne d'index — ce qui suffit à la facturation calendaire — niveau 2 y ajoute l'infra-journalier / courbe de charge ; le bloc d'index `Classe_Temporelle_Distributeur` du C15 est d'ailleurs absent à niveau 0). C'est la **cause** déclarative qui *route* un contrat vers la voie communicante ou non-communicante — à distinguer de la *qualité de période d'énergie*, qui en est l'**effet** mesuré période par période (un mois communicant peut produire une période *estimée*). Un seul champ couvre les **deux** populations de l'issue #189 (compteur non communicant *et* Linky à collecte non ouverte/refusée — cf. aussi `Refus_Pose_AMM`, `Teleoperable`) : la détection se réduit à la lecture du niveau, invalidant la crainte « bien plus difficile à savoir ». **Attention** : niveau ≥ 1 ouvre l'**éligibilité** aux flux périodiques, pas leur *réception* — le **R151 n'est pas éligible** tant que le compteur est NC et exige une souscription **après** la bascule (action manuelle), tandis que le **R64 est demandé à la borne** — *pull* ponctuel en début de mois ciblé sur le 1er, juste avant la facturation —, donc disponible dès que le point est communicant. Trois couches à ne pas confondre : *capacité matérielle* (Linky) → *ouverture aux services* (niveau, = éligibilité) → *réception effective* (**pull R64** sur la borne calendaire ; R151 = flux récurrent à souscrire). D'où R64 prioritaire sur R151 (`PRIORITE_SOURCES`) : c'est la source **maîtrisée**.
_Éviter_ : « communicant » comme synonyme de « qualité réelle » (cause vs effet) ; confondre avec la *capacité matérielle* (`Palier_Technologique` = LINKY) qui ne dit pas si la collecte est ouverte ; « réel/estimation » pour nommer les voies (réserver réelle/estimée à la qualité).

**Voie communicante** vs **Voie non-communicante** ([ADR-0036](../../docs/adr/0036-statut-communication-routage-energie-grain-meta.md) — routage de l'énergie, pas de l'abonnement) :
Les deux trajets de calcul d'énergie d'un contrat-mois, séparés par le *statut de communication*. La part fixe (*abonnement*) est calculée pour **tous** — le routage ne porte que sur l'*énergie*; le statut est un **filtre en amont de l'axe énergie**, pas un troisième axe pair de l'abonnement.
Mécanique **miroir de la qualité de période** (même portage, même rollup) : le `niveau_ouverture` est reporté du C15 sur chaque relevé de la *chronologie des relevés* ; une *période d'énergie* est **communicante** ssi ses deux bornes sont à niveau ≥ 1 (comme une période *réelle* a ses deux bornes `réel`/`corrigé`) ; le verdict mensuel est le **rollup pire-gagne** au grain *méta-période*. Un `(situation contractuelle, mois)` part en **voie communicante** (facturable en réel) ssi **toutes** ses sous-périodes du segment actif sont communicantes — la troncature calendaire d'entrée/sortie reste éligible, seule une *bascule* de niveau **en cours** de mois le rend partiel (« reste en responsabilité non-communicante » : pas de demi-mois).
**Pas de gate au grain relevé** : le pipeline énergie calcule *toutes* les sous-périodes ; la sélection vit **au grain méta**, pas en filtrage fragile au grain relevé. Une bascule en cours de mois rend le mois non-communicant par sa seule borne à niveau 0 (« pas de demi-mois ») — le relevé d'activation du calendrier Distributeur (`CMAT`) **n'a pas besoin** d'entrer dans la chronologie pour ça. C'est heureux, car il **n'y entre pas** : `impacte_energie` est un détecteur de *delta avant/après* aveugle aux apparitions `null→valeur` (le calendrier qui *apparaît* à la bascule — chiffré par le spike #315 : 27/61 `CMAT` d'activation, `avant=null`, non captés). Matérialiser ce relevé pour *salvager* la sous-période communicante partielle est **déféré au chantier #322** (« Gestion/estimation des NC »).
**Voie non-communicante** : aujourd'hui un *seam* différé (rien / pas d'estimation), demain une estimation (ML des relevés manquants, ou données estimées Enedis du *moisniversaire*). Les contrat-mois non communicants n'ont pas d'énergie mesurée et relèvent du *lissé* (provisions manuelles).
_Éviter_ : « pipeline réel » / « pipeline d'estimation » (collision *réelle/estimée* avec la qualité) ; décrire la sélection comme un *gate au grain relevé* (c'est un *rollup* au grain méta) ; router l'*abonnement* sur ce statut (il ne dépend pas du niveau d'ouverture).

**Anomalie de communication** (issue #189) :
Discordance entre le *statut de communication* déclaré et la réalité observée. Deux formes : (1) **communicant ∧ incalculable** — niveau ≥ 1 mais relevé manquant sur un mois éligible : le **pull R64** du 1er ne ramène rien malgré niveau ≥ 1 (collecte réellement défaillante ; cas résiduel : R151 récurrent non souscrit). L'anomalie a un usage opérationnel : investiguer la collecte / relancer la demande ; (2) **non-communicant ∧ non-lissé** — niveau 0 mais contrat facturé en réel côté ERP (croisement avec `x_lisse`, cf. *Compteur non communicant*). Le `statut` porté en champ **orthogonal** de la *qualité de période* sur la méta-période est ce qui rend la forme (1) distinguable d'un non-communicant attendu (même `incalculable`, cause différente).
_Éviter_ : traiter tout `incalculable` comme une anomalie (un non-communicant l'est par nature, sans alerte).

**Traçabilité des index** (le *besoin*) :
Conservation, jusqu'à la facture, des relevés effectivement consommés par le calcul d'une période : valeurs d'index en **registres réels** (jamais de cadran synthétisé — on n'additionne pas deux registres cumulés pour fabriquer un « index HP » qu'aucun compteur n'affiche), `date_releve`, *nature d'index*, et *identité de relevé*. Exigence légale : les index doivent figurer sur la facture, dont **Odoo est le système de référence** du « facturé » (le core reste sans état et recalcule ; une correction postérieure relève de la *régularisation*, pas d'une réécriture). C'est une **propriété** exigée du livrable, réalisée par l'artefact *Relevés utilisés*.
_Éviter_ : audit des index (anglicisme mou), historique d'index (collision avec *Historique* contractuel).

**Relevés utilisés** (l'*artefact*) :
Matérialisation de la *Traçabilité des index* : la liste des relevés effectivement consommés par le calcul d'énergie d'un contrat-mois, au grain **1 ligne par `(ref_situation_contractuelle, date_releve, ordre_index)`**, portée par le frame `releves_utilises` du *Contexte mensuel*. Depuis #377 ([ADR-0041](../../docs/adr/0041-chronologie-contrat-spine-relationnelle-dbt.md)), c'est **exactement la même frame** que l'entrée du pipeline énergie — la *Chronologie des relevés* (vue `chronologie_releves`) filtrée à l'horizon : « relevés tracés = relevés utilisés » est garanti **par construction** (fin de la double-assemblée — plus de second calcul à maintenir convergent). Conforme au schéma *Chronologie des relevés* (registres réels d'index + *identité de relevé* `releve_id` + *nature d'index*). Un changement de configuration en cours de mois (MCT) y apparaît sans cas particulier : les relevés intermédiaires utilisés s'ajoutent simplement à la liste. Distinct du *modèle de relevés canonique* `releves` (qui réunit *tous* les relevés disponibles, exposé par `/releves`) — *Relevés utilisés* en est le **sous-ensemble bornant** réellement consommé.
**Exposé à l'ERP** imbriqué dans chaque *méta-période* (l'ERP tire et stocke les relevés bornants pour satisfaire l'exigence légale d'index sur la facture et l'affichage en espace usager) : chaque relevé porte son *identité* (`releve_id`, **handle de reprise** pour la *régularisation*), sa *date*, sa *nature d'index* (mention légale), son *origine de relevé* et ses registres réels (les 7 cadrans canoniques, registres présents seulement). Le cœur reste **sans état** — il ne persiste rien : l'ERP stocke une copie de travail, le gel légal vit dans la facture postée ; une dérive ultérieure d'un index est un signal de *régularisation*, pas une réécriture.
_Éviter_ : journal des relevés (connote un log append-only / event-sourcing, alors que le cœur est sans état et **recalcule**).

**Origine de relevé** :
Axe qui distingue **comment** un relevé a été pris : *périodique* (télérelevé automatique R151/R64, à cadence régulière) ou *événementiel* (relevé pris à un *événement* contractuel C15 — `evenement_declencheur` en précise la cause : `MES` mise en service, `MCT` modification/changement, `RES` résiliation…). Porté aux champs `origine_releve` (+ `evenement` si événementiel) des *Relevés utilisés* exposés ; dérivé à l'exposition de la `source` du relevé et de l'`evenement_declencheur` que le mart `releves` porte **nativement** depuis les relevés C15 (non forward-fillé — un télérelevé n'est déclenché par aucun événement ; comme RSC/FTA/niveau, désormais natifs eux aussi, plus recopiés, [ADR-0039](../../docs/adr/0039-chronologie-substrat-attributs-situation-hors-mart.md)). Axe **orthogonal** à la *nature d'index* (réel/estimé/corrigé, qui qualifie la *valeur* lue, pas l'origine).
_Éviter_ : « quotidien » (un R151 est périodique sans être quotidien), confondre avec la *nature d'index*.

**Puissance souscrite** :
Limite contractuelle en kVA. Un seul champ en C5 (`puissance_souscrite_kva`), quatre en C4 (`puissance_souscrite_hph_kva`…).

---

## Modes de facturation

**Part fixe** :
Composantes de la facture client indépendantes de la consommation : prix de l'abonnement fournisseur, TURPE fixe, CTA. Assise sur les *périodes d'abonnement* (jours × puissance souscrite). Identique en contrat réel et lissé — c'est pourquoi le mode lissé n'a aucun impact dessus.
_Éviter_ : « abonnement » tout court pour cette composante — triple collision : *Abonnement* (période, terme pipeline), « Abonnements » (catégorie produit ERP, cf. *Ligne de facture*), et le module Odoo de facturation récurrente nommé Abonnement.

**Part variable** :
Composantes de la facture client assises sur l'énergie : prix de l'énergie fournisseur (€/kWh), TURPE variable, Accise. **L'assiette est le facturé** : pour un contrat réel, le facturé est le mesuré (périodes d'énergie) ; pour un lissé, c'est la mensualité estimée. L'accise suit cette assiette dans les deux cas — c'est ce qui la rend cohérente pour les lissés en attendant la régularisation.

**Moisniversaire** :
Mode de facturation où la période de chaque contrat est ancrée au jour de souscription (souscrit un 12 → facturé du 12 au 12). Chaque souscripteurice a sa propre période. C'est le découpage des données de facturation Enedis : les agrégats du R15 (énergies, jours, TURPE) sont calculés sur ces périodes-là.
_Éviter_ : mois anniversaire (deux mots — le portmanteau est le terme maison), période glissante.

**Facturation calendaire** :
Le mode ElectriCore : du 1ᵉʳ du mois au 1ᵉʳ du mois suivant, identique pour tout le monde ; aux bornes de vie du contrat, la première et la dernière période sont tronquées (entrée : date d'entrée → 1ᵉʳ suivant ; sortie : 1ᵉʳ → date de sortie) et calculées réellement — différences d'index aux dates exactes, jours effectifs — comme une période pleine. Choisi pour la simplicité des souscripteurices (cf. [ADR-0026](../../docs/adr/0026-facturation-calendaire-pas-moisniversaire.md)). Conséquence structurelle : les agrégats Enedis (découpés au *moisniversaire*) sont inexploitables — énergie, jours et TURPE sont **recalculés** sur le découpage calendaire depuis les relevés et les événements contractuels. C'est la raison d'être des pipelines de `core/` et des événements FACTURATION au 1ᵉʳ de chaque mois.
_Éviter_ : facturation mensuelle (ambigu — le moisniversaire est mensuel aussi), prorata (suggère une quote-part d'un montant mensuel — les périodes tronquées sont mesurées, pas réparties).

**Contrat lissé** :
Modalité de facturation articulée en triptyque : le client paie chaque mois une *provision d'énergie* constante ; l'*énergie mesurée* court en parallèle (rejouable depuis les données Enedis pour les compteurs communicants) ; la *régularisation* solde l'écart entre les deux. S'oppose au contrat *réel* (facturé directement sur le mesuré). Orthogonal au mode calendaire : un contrat lissé est aussi facturé du 1ᵉʳ au 1ᵉʳ, seule la quantité d'énergie diffère. Marqué `x_lisse` côté Odoo, traverse le rapprochement en passe-plat (`est_lisse`).

**Provision d'énergie** :
Quantité d'énergie (kWh) facturée chaque mois à un contrat *lissé*, en attendant la régularisation. Fixée manuellement à la souscription par estimation de la consommation, portée par les lignes énergie de la commande ERP — une par **cadran facturé** (Base, ou HP et HC, selon la *formule tarifaire fournisseur*, pas la FTA). Le prix fournisseur, le TURPE variable et l'Accise s'y appliquent comme à de l'énergie mesurée (cf. *Part variable*, assiette facturé).
_Éviter_ : mensualité (vocabulaire courant qui agrège tout le paiement mensuel en euros, parts fixe et variable confondues), estimation (l'estimation est l'acte ; la provision est la quantité retenue).

**Estimation de provision** :
Dérivation *cœur-pure* et *data-driven* de la *provision d'énergie* : d'un historique d'énergie par cadran, une estimation de consommation **annuelle par cadran** (tranche de 12 mois glissants) puis une provision mensuelle **`/12` plate**, assortie de métadonnées de **couverture** (fenêtre, mois disponibles) et de **qualité** (mix réel/estimé/corrigé). N'émet que des **kWh** — la valorisation appartient à l'ERP. Premier régime livré : l'**amorçage (cold-start)** d'un nouvel entrant *CFNE* depuis `flux_r67` (*mesures facturantes* M023, ~36 mois), quand EDN n'a pas encore d'historique propre ; le régime *établi* (réévaluation depuis l'historique R151/R64) se branchera dessus. En **amont** de la *régularisation* (le thermostat, pas le solde — cf. #191) : l'estimation ne calcule jamais un solde. Voir ADR-0048.
_Éviter_ : la confondre avec la *provision d'énergie* (la quantité effectivement retenue/facturée côté ERP, que cette estimation alimente).

**Formule tarifaire fournisseur** :
Structure tarifaire du contrat client : Base, ou HP/HC (à terme, possiblement les 4 cadrans). Distincte de la *FTA* : le TURPE n'est **pas refacturé ligne à ligne** au client — il entre dans les calculs macro du fournisseur pour fixer ses tarifs. Détermine les **cadrans facturés**, c'est-à-dire les catégories produit (Base/HP/HC) des lignes de facture — l'« homonymie » entre catégories produit et cadrans réseau vient de là.
_Éviter_ : FTA (c'est le choix d'acheminement réseau, qui peut différer), option tarifaire.

**Compteur non communicant** :
Compteur sans télérelevé quotidien — pas de R151/R64, donc pas d'index au 1ᵉʳ du mois : infacturable en *réel* sous le mode calendaire. Règle d'usage (respectée à 100 % en pratique, sans contrainte logicielle) : ces contrats sont passés en *lissé*. Même situation côté données pour un compteur communicant dont la **collecte est refusée** par le client — mais **directement lisible** dans le C15 via le *statut de communication* (`Niveau_Ouverture_Services`, cf. cette entrée), issue #189.
_Éviter_ : non-Linky (la marque n'est pas le critère — c'est le *niveau d'ouverture aux services*, pas le palier technologique).

**Régularisation** :
Solde d'un contrat *lissé* : différence, par cadran facturé, entre l'*énergie mesurée* et les *provisions d'énergie* facturées, ventilée — en cible — par sous-période mensuelle (le réglementaire ne change qu'en début de mois). Le périmètre ElectriCore s'arrête aux **quantités** : soldes en kWh ventilés, et correction d'assiette Accise des trimestres concernés. La **valorisation aux tarifs fournisseur** est déférée à l'ERP et au process — la lib ne connaît pas les prix fournisseur, par principe (calculs métier énergie déférés, pas la compta ni les tarifs). Aujourd'hui : process entièrement manuel — présentation technique du chantier dans l'issue épique #191. Le retour à l'équilibre repose d'abord sur l'**adaptation des provisions** (réévaluation régulière de la consommation) ; la régularisation est le solde, pas le thermostat. Déclencheur actuel (~6 mois à date anniversaire, et solde de tout compte à la sortie) : décision de process interne, non arbitrée par la lib.
_Éviter_ : rattrapage, remise à zéro.

---

## Rôles des dossiers

Voir [ADR-0019](../../docs/adr/0019-roles-loaders-pipelines-builds-integrations.md) pour le cadre complet (règles d'import, garde-fou, alternatives écartées). Vocabulaire canonique :

**Loader** :
Source interne (DuckDB, fichiers, configs). `core/loaders/`. Lit une table, un fichier ou un query builder et retourne un `LazyFrame[Schema core]`. Aucune logique métier.

**Pipeline** :
Transformation pure. `core/pipelines/`. Prend N frames Pandera-typés (souvent du même domaine métier, mais multi-sources autorisé tant que le domaine reste cohérent) et retourne 1 frame Pandera-typé. **Aucune I/O, aucun import d'`integrations/`.** Le `.collect()` est porté par le build ou le caller, pas par le pipeline.
_Éviter_ : confondre avec « pipeline » au sens DAG scheduling (Airflow, Dagster jobs) — ici un pipeline est une *suite d'opérations composables*, pas un graphe planifié.

**Build** :
Producteur de livrable. `core/builds/`. Compose pipelines + loaders pour produire un *bundle dataclass* (`RapportTaxe`, `ContexteMensuel`) ou un side-effect via writer. **Ne peut pas importer `integrations/`** — les sources ERP sont injectées par le caller (typiquement `api/services/`). Anciennement `core/orchestrations/` (renommé pour éviter la collision avec le sens industriel de « orchestration » = scheduling).
_Éviter_ : orchestration (mot réservé pour le jour où un vrai scheduler est ajouté).

**Writer** :
Sink interne (DuckDB, fichiers). `core/writers/`. Symétrique de loader côté écriture. Aucune logique métier.

**Integration** (`integrations/<erp>/`) :
Source et sink ERP. Expose des fonctions qui retournent des `LazyFrame[Schema core]` (lecture) ou consomment des `DataFrame` (écriture). **Pas d'assemblage de livrable, pas d'orchestration métier** — un fichier comme `integrations/odoo/taxes.py` qui produirait un `RapportTaxe` est une violation : l'assemblage descend en `core/builds/`.

**Service API** (`api/services/`) :
Wire-up non-trivial ou stateful entre sources (loaders + integrations) et builds. Lieu où les sources ERP rencontrent les builds core. Routeur reste pur transport.

**Config partagée** (`electricore/config/`) :
Socle transverse : le module canonique du registre runtime ([ADR-0024](../../docs/adr/0024-trois-registres-de-savoir.md), issue #141) — domaines de config typés, fail-fast par point d'entrée —, la façade `.env` → `os.environ` pour dlt (`charger_env`), la config Odoo, et les CSV de taux régulés. Importable par **tous** les modules (`core/` compris — compatible ADR-0016 : aucune dépendance ERP ; pydantic-settings est la seule lib externe admise ici). C'est le seul module hors `core/models/` que `core/loaders/` peut importer.

---

## Concepts pipeline

**Historique** :
Séquence temporelle ordonnée des faits contractuels d'une situation (RSC), enrichie pour la **branche abonnement** : annotation de l'impact abonnement (`impacte_abonnement`) et résumé des modifications. Depuis [ADR-0041](../../docs/adr/0041-chronologie-contrat-spine-relationnelle-dbt.md) (#378), c'est la **spine** de la *Chronologie du contrat* (événements C15 ∪ grille FACTURATION mensuelle, situation forward-fillée **en dbt**) filtrée à l'horizon — `pipeline_historique` n'y ajoute plus que la détection de ruptures d'abonnement (`detecter_points_de_rupture`). La branche **énergie** a quitté ce concept (plus d'`impacte_energie` ni de colonnes d'index/calendrier) : elle vit dans la *Chronologie des relevés*. Sortie validée par le schéma `Historique`.
_Éviter_ : journal, log contractuel, périmètre (le périmètre est l'état à une date, pas la séquence) ; croire qu'il porte encore la branche énergie (`impacte_energie`, index — partis en dbt).

**Périmètre** :
Ensemble des PDLs actifs (et de leur configuration contractuelle) à une date donnée. Snapshot dérivé de l'Historique par filtrage temporel. Répond à la question « qui était dans notre portefeuille à la date X ? ».
_Éviter_ : portefeuille (terme commercial), parc.

**Abonnement** :
Période continue entre deux événements contractuels où la configuration (FTA, puissance, calendrier) ne change pas. Unité de calcul du TURPE fixe.

**Rupture de période** :
Événement contractuel (MES, MCT, RES) qui découpe une période d'abonnement en deux. Toute rupture d'abonnement n'est **pas** une rupture d'énergie : Enedis ne fournit un relevé d'index qu'aux événements impactant le comptage (un MCT puissance seule n'en a pas) — les deux lignes de temps ne sont pas synchronisables (cf. [ADR-0023](../../docs/adr/0023-periodisations-separees-abonnement-energie.md)).

**Chronologie des relevés** :
Ligne de temps **énergie** d'un contrat : tous les relevés d'index aux dates qui bornent ses périodes — relevés C15 aux événements qui **impactent l'énergie** + relevés périodiques (R151/R64) appariés aux **bornes FACTURATION**. **Assemblée entièrement en dbt** (loader `chronologie_releves()`, [ADR-0041](../../docs/adr/0041-chronologie-contrat-spine-relationnelle-dbt.md)) : c'est la **projection énergie de la *spine*** (filtre `impacte_energie` + bornes FACTURATION héritées de la spine), où l'appariement relevé↔borne est un **equi-join au grain JOUR** — l'asof « ±4 h » (`TOLERANCE_APPARIEMENT_RELEVES`) qui vivait au cœur **a disparu** —, dédoublonné par la **table de priorité explicite** `flux_C15 > flux_R64 > flux_R151` via `QUALIFY` (ADR-0028 — fin du tri qui faisait gagner R151 sur R64). Entrée pure de `calculer_periodes_energie` / `pipeline_energie`, qui depuis #377 ne fait plus que le **découpage** (shift/diff, verdicts qualité/communication) — l'ex-`_assembler_chronologie` du cœur est retiré. Validée par le schéma `ChronologieReleves` : grain d'**1 ligne par `(ref_situation_contractuelle, date_releve, ordre_index)`**, RSC **non-null** (attribution garantie en amont — valeur **native** des relevés C15 + situation portée par la spine pour les périodiques, ADR-0039/[ADR-0041](../../docs/adr/0041-chronologie-contrat-spine-relationnelle-dbt.md)), `source` dans une **énumération fermée** (nullable : une borne FACTURATION sans relevé apparié — `releve_manquant` — n'en porte pas), `ordre_index` **booléen** (`False` = avant / relevé périodique, `True` = après C15). **Normalisée** ([ADR-0045](../../docs/adr/0045-relations-spine-reference-cle-normalisation-chronologie-releves.md), #431) : `chronologie_releves` est une **vue** (star-join sur `releve_id`) qui **ré-attache** le payload d'index (`index_*`/`nature_index`/`id_calendrier_distributeur`) au **mart mince** `chronologie_releves_situation` (identité + situation + `releve_id` + attributs de slot) depuis `releves` — **source de vérité unique** des index (fin de la recopie, ADR-0029/0038) ; contrat `ChronologieReleves` inchangé.
_Éviter_ : reconstitution des relevés (ancien nom de la fonction, gardé comme alias de compat), timeline (anglicisme).

**Chronologie du contrat** :
Ligne de temps **complète** d'une *situation contractuelle* (RSC) : **tous** ses faits ordonnés — événements C15 (y compris hors-comptage : `MDPRM` de niveau, `MCT` puissance seule), relevés d'index, **événements FACTURATION** (1ᵉʳ de chaque mois), demain les *jalons* d'affaire — chacun porteur des attributs de situation (FTA, *niveau d'ouverture*…) **au moment du fait**. Bornée entrée→sortie de la RSC. **Forme : une *spine* relationnelle assemblée en dbt** ([ADR-0041](../../docs/adr/0041-chronologie-contrat-spine-relationnelle-dbt.md)) — une épine commune `(pdl, ref_situation_contractuelle, date, source, type_fait)` + situation forward-fillée **en SQL** sur la timeline d'événements **complète** (`last_value … IGNORE NULLS OVER (PARTITION BY rsc ORDER BY date_evenement)`), à laquelle se rattachent les **relations** par source (événements, relevés, jalons). Deux rôles distincts du temps : le **timestamp** ordonne le forward-fill (ordre total vérifié sur prod — séquence `CMAT` niveau 0 puis `MDPRM` niveau 1 le même jour), le **jour** apparie relevé↔borne FACTURATION (equi-join, plus d'`asof` — ADR-0041). C'est du *Class-Table Inheritance*, **pas** une frame large nullable (STI) ni un payload imbriqué (rejetés, ADR-0041). C'est le **substrat** dont on *dérive* les périodisations (abonnement, énergie, NC), chacune `filtre(spine) ⨝ sa relation` avec son propre découpage — partager le substrat n'est **pas** unifier les découpages, qui restent séparés ([ADR-0023](../../docs/adr/0023-periodisations-separees-abonnement-energie.md)). **Extensible** : une nouvelle source de faits entre comme une **relation** rattachée à la spine, sans élargir l'existant — les *affaires* (`type_fait = jalon`, rattachées par `id_affaire`) en sont la prochaine ; R15 n'en est *pas* une, il enrichit la relation *relevés* (déjà multi-sources). Les attributs de situation appartiennent au substrat, **pas** au relevé ([ADR-0039](../../docs/adr/0039-chronologie-substrat-attributs-situation-hors-mart.md) — fin de la recopie sur le mart `releves`, qu'ADR-0041 prolonge en descendant l'attribution **et** la génération de la grille FACTURATION en dbt ; le cœur ne fait plus que *filtrer* par horizon et dériver les ruptures par branche). La *Chronologie des relevés* en est la **projection énergie** (restreinte aux relevés + événements impactant le comptage). **Règle d'intégration** ([ADR-0045](../../docs/adr/0045-relations-spine-reference-cle-normalisation-chronologie-releves.md)) : une colonne entre dans la spine **⟺** c'est un attribut de *situation* (état contractuel à sémantique **forward-fill**, nécessaire à des points qui ne sont **pas** son événement-source — bornes FACTURATION, relevés périodiques) ; tout *payload* propre à un fait (index, nature, calendrier ; demain le détail d'une affaire) est une **relation**, **référencée par clé** (`releve_id`…), **pas** inlinée. « Utile à ≥1 pipeline » n'est **pas** le critère (il inlinerait *tout* → STI, écartée). Le mart de spine matérialisé est `spine_contrat` (loader `spine_contrat()`).
_Éviter_ : timeline (anglicisme) ; confondre le *substrat* (cette chronologie) avec une *périodisation* (un découpage qui en dérive) ; décrire la spine comme une frame large (STI) ou un payload imbriqué (rejetés, ADR-0041).

**Chronologie du périmètre** :
Le **concept parapluie** ([ADR-0045](../../docs/adr/0045-relations-spine-reference-cle-normalisation-chronologie-releves.md)) : la *spine* relationnelle **+ ses relations** rattachées par clé (relevés ; demain les affaires). *« Du contrat »* (grain **RSC**) et *« du point »* (grain **PDL**) en sont des **grains de coupe** — des *vues* du même substrat, **pas** des marts distincts. Le seul mart de spine matérialisé est `spine_contrat` (loader `spine_contrat()`), dont le grain de forward-fill **est** le contrat/RSC : « spine » nomme la *forme* (l'épine de colonnes communes), « périmètre » le *concept*. Le diagramme [`chronologie-spine-modele`](../../docs/chronologie-spine-modele.png) ([source](../../docs/chronologie-spine-modele.excalidraw)) argumente l'ensemble : substrat → périodisations homogènes, règle d'intégration, dimension/fait/vue, « pourquoi N frises ».
_Éviter_ : parler d'un « mart chronologie du périmètre » (c'est un **concept**, pas une table) ; confondre la *forme* (`spine_contrat`) et le *concept* (périmètre).

**Chronologie du point** :
La même ligne de temps au grain **PDL** : elle *contient* les *Chronologies du contrat* successives du point, plus les événements-charnières (sortie d'un occupant → entrée du suivant). Vue du **point physique** à travers ses RSC successives — pour suivre un changement de main (déménagement, reprise) ou un relevé-orphelin entre deux contrats, que la vue contrat masque. Containment : **point ⊇ contrat**. Grain plus large, usage plus rare ; le grain contrat sert la facturation et le gros des vues.
_Éviter_ : timeline (anglicisme).

**Période d'énergie** :
Intervalle entre deux relevés d'index, support du calcul de consommation et du TURPE variable. Sa ligne de temps (relevés — la *Chronologie des relevés*) est distincte de celle des abonnements (événements contractuels) et le reste délibérément — Enedis facture d'ailleurs séparément l'abonnement (à échoir) et l'énergie (à échue) (cf. [ADR-0023](../../docs/adr/0023-periodisations-separees-abonnement-energie.md)).

**Méta-période mensuelle** :
Agrégation d'abonnements + périodes d'énergie sur un mois calendaire, unité de la facturation client mensuelle — l'incarnation du mode *facturation calendaire* (cf. [ADR-0026](../../docs/adr/0026-facturation-calendaire-pas-moisniversaire.md)). Grain : une ligne par **(situation contractuelle, mois)** — pas par PDL : un PDL qui change de RSC en cours de mois porte deux méta-périodes ce mois-là (le TURPE fixe est facturé par situation). Porte la *qualité de période d'énergie* du mois, rollup **pire-gagne** de ses sous-périodes (réelle / estimée / incalculable, [ADR-0033](../../docs/adr/0033-qualite-periode-remplace-data-complete-coverage.md)).

**Horizon de facturation** :
Borne temporelle unique (`datetime` en Europe/Paris) appliquée comme **filtre** au boundary : `date_evenement <= horizon`. Elle borne *à la fois* les faits retenus de la *Chronologie du contrat* *et*, par voie de conséquence, la fin par défaut des périodes ouvertes — la dernière FACTURATION retenue (= 1ᵉʳ du mois de l'horizon) ferme la période des PDL non résiliés. La **grille FACTURATION elle-même est pré-générée dans la spine (dbt)**, calendaire et déterministe, jusqu'à une borne généreuse ([ADR-0041](../../docs/adr/0041-chronologie-contrat-spine-relationnelle-dbt.md)) : l'horizon n'est **plus un input de génération** mais un simple filtre. Ce qui **préserve la pureté** (#179) : à horizon fixé, deux exécutions filtrent à l'identique, indépendamment de l'heure d'exécution *et* de la borne dbt (tant qu'elle ≥ l'horizon — toujours vrai, on ne facture pas un mois futur sans données). Le défaut « 1ᵉʳ du mois courant » est résolu en **vraie heure de Paris** (`horizon_par_defaut()`) une seule fois, au boundary I/O — `charger()` / `contexte_du_mois()` dans `core/builds/contexte_mensuel.py` — puis propagé. Unifie et remplace l'ancien `date_limite` (qui ne bornait que les événements) ; corrige le bug à deux fuseaux d'#179.
_Éviter_ : `date_limite` (ancien nom, ne couvrait que le filtrage des événements) ; « date du jour » / `now()` dans un pipeline (rompt la pureté) ; croire que l'horizon *génère* les FACTURATION (il les *filtre* — la génération calendaire vit en dbt, ADR-0041).

**`mois_annee`** (champ) :
Mois calendaire d'une période (abonnement, énergie, méta-période, taxes), clé calculable et triable au format `"YYYY-MM"`, garanti par `str_matches` dans les schémas Pandera (issue #115). Côté Accise, dérivé de la date de facture (M − 1 : la facture du mois M porte la consommation de M−1 ; anciennement `mois_consommation`). Les libellés français restent portés par les champs d'affichage dédiés `debut_lisible` / `fin_lisible`.
_Éviter_ : mois_consommation (fusionné), libellé `"mars 2025"` comme clé (trie `août < avril`).

**Contexte mensuel de facturation** :
Bundle immutable des éléments dérivés une seule fois pour produire la facturation d'un mois donné : `historique_enrichi`, `abonnements`, `energie`, `releves_utilises` (journal des relevés effectivement consommés par la chronologie — traçabilité des index, ADR-0029) et `facturation_mensuelle` (méta-périodes du mois) — **cinq** frames. Deux entrées dans `core/builds/contexte_mensuel.py` (cf. [ADR-0019](../../docs/adr/0019-roles-loaders-pipelines-builds-integrations.md)) : `charger(historique, releves, mois)` — composition pure, frames fournis par l'appelant (tests, notebooks, source alternative) — et `contexte_du_mois(mois)` — entrée I/O qui résout les sources par défaut (loaders DuckDB) puis délègue à `charger()` (issue #145, scindage pur/I-O prévu par les « Limites » d'ADR-0019). Partagé par les builds qui touchent au même mois (rapprochement, documents de campagne, taxes mensuelles) pour ne déclencher le pipeline `facturation()` qu'une seule fois.
_Éviter_ : résultat de facturation (trop vague), contexte tout court (sans qualificatif).

**Rapprochement facturation mensuelle** :
Jointure des *lignes de facture* (côté ERP) avec la méta-période mensuelle Enedis du même mois, enrichie des identifiants compteur (`num_compteur`, `type_compteur`) et des flags `a_facturer` / `a_supprimer` (cf. [ADR-0014](../../docs/adr/0014-lignes-factures-du-mois-avec-flags.md)). Implémenté par `rapprocher()` dans `core/builds/contexte_mensuel.py`. Produit une `LignesFactureRapprochees` en **vraie passe-plat** (issue #142) : sortie = colonnes d'entrée (`est_brouillon` compris, auditable à côté des flags qu'il dérive) + colonnes calculées, sans nommer aucune colonne ERP en core. Ordre déterministe — contrat, calculées, puis passe-plat en ordre d'entrée — mais **sans promesse pour les livrables** (l'ordre facturiste vit dans `feuilles_rapport_*`). Une colonne d'entrée homonyme d'une calculée échoue au seam (`ValueError` nommant la collision).
_Éviter_ : matching, reconciliation (anglicisme).

**Ligne de facture** :
Unité de facturation côté ERP : un produit, une période, une quantité, un PDL. Schéma agnostique `LignesFacture` (`core/models/`) — **contrat minimal** de ce sur quoi `rapprocher()` branche : `ref_situation_contractuelle`, `categorie_produit ∈ {Base, HP, HC, Abonnements}`, `quantite`, `est_brouillon`. Toute autre colonne (`pdl`, `est_lisse`, identifiants ERP `invoice_line_ids` / `name_account_move` / `name_product_product`, etc.) est acceptée en passe-plat (`strict=False`) et préservée dans la sortie. Chaque adaptateur ERP renomme ses clés métier vers ces 4 noms ; les flags ADR-0014 (`a_facturer` / `a_supprimer`) sont dérivés en core depuis `est_brouillon` + `quantite`. Le catalogue produit de l'ERP déborde ce contrat : `categorie_produit` y prend aussi des valeurs hors scope de la facturation calendaire — refacturation de prestations Enedis (`Prestation-Enedis`), produits non catégorisés (catégorie racine Odoo « `All` ») — qui ne sont pas des *lignes de facture* au sens retenu ici. L'enum du contrat reste donc le sous-ensemble **facturable** (cadrans facturés + abonnement), pas le catalogue ERP ; le consommateur (la facturation legacy) écarte les autres lignes en amont du rapprochement.
_Éviter_ : ligne Odoo (le concept est agnostique), ligne facture client (`account.move` est la facture entière).

**Harmonisation des relevés** :
Alignement des sources de relevés (R151, R64) sur la convention de date « début de journée » (R151 +1 jour, cf. *Convention début / fin de journée*). Le +1 jour R151 et l'ancrage en *instant* sont portés à l'**ingestion**, dans le modèle `flux_r151` (boundary), comme la conversion native de chaque source — plus dans le mart ([ADR-0042](../../docs/adr/0042-convention-date-instant-jour-civil-boundary-flux.md) révise l'amendement #294 d'[ADR-0003](../../docs/adr/0003-r151-date-harmonisation.md)). L'ancien `releves_harmonises()` (union SQL R151+R64 côté loader) a été retiré (#248).

**`date_ajustee`** (champ) :
Booléen marquant les relevés dont la date a été décalée pendant l'harmonisation (R151 +1 jour).

**Contrat de pipeline** :
Tout `pipeline_*` (et la fonction de premier niveau qu'il compose, ex : `generer_periodes_abonnement`, `calculer_periodes_energie`) est décoré `@pa.check_types(lazy=True)` avec entrée *et* sortie typées par un schéma Pandera. Les violations de contrat apparaissent au *seam* (avec un message nommant la colonne fautive) plutôt qu'au fond d'une stack-trace. Discipline héritée de l'ère pandas (commit `b066b98`, août 2025) et restaurée pour Polars après le creux laissé par la migration `052c99f` ; la version *eff3d19* n'avait restauré que les agrégateurs de `pipeline_facturation`. Conséquence directe : aucun *self-repair silencieux* dans un pipeline — si l'entrée n'est pas conforme, on échoue au seam, pas en se rattrapant.

---

## Exports et livrables

**Rapport** :
Structure de données (`@dataclass(frozen=True, slots=True)`, cf. [ADR-0018](../../docs/adr/0018-classes-justifiees-par-l-etat.md)) regroupant les DataFrames qu'un *build* (cf. [ADR-0019](../../docs/adr/0019-roles-loaders-pipelines-builds-integrations.md)) produit pour un domaine (Accise, CTA, facturation). Exemples : `RapportAccise(resume, par_taux, detail)` (`core/builds/rapport_taxe.py`), `RapportFacturation(resume, lignes, changements_puissance)` (`core/builds/rapport_facturation.py`, issue #143 — assemblé en core sur le shape agnostique `LignesFacture`). Pré-trié et prêt à consommer — l'ordre des lignes est porté par la fonction `rapport_*` elle-même, pas par les consommateurs ; l'ordre des **colonnes** d'un livrable facturiste est porté par sa fonction `feuilles_rapport_*`.

**Livrable** :
Export structuré destiné à un consommateur métier (facturiste, audit). Un livrable matérialise un *Rapport* sous une forme attendue par son destinataire — typiquement un XLSX multi-onglets (`Résumé` / `Par taux` / `Détail`). Distinct du *détail brut* (table unique exportée en XLSX mono-onglet ou Arrow IPC, format technique).
_Éviter_ : export (trop générique), fichier.

**Feuilles** :
Onglets d'un *Livrable* XLSX, matérialisés par un `dict[str, DataFrame]` consommable par `xlsx_multi_sheet`. Les clés du dict sont les libellés d'onglets affichés à l'utilisateur (FR : `Résumé`, `Par taux`, `Détail`, `F15 complet`, `Changements puissance`…). Trois formes de production coexistent :

- **À partir d'un `Rapport*`** : `feuilles_rapport_*(r) -> dict[str, DataFrame]` co-localisée avec le `rapport_*` correspondant (cas Accise, CTA, facturation rapport).
- **Directement par un build** : `documents(ctx, lignes, f15, c15) -> tuple[dict[str, DataFrame], str]` dans `core/builds/contexte_mensuel.py` (cas `/facturation/documents.xlsx` — pas de dataclass intermédiaire, la forme du livrable est directement assemblée par le build ; le wire-up `documents_facturation_du_mois(odoo, mois)` vit en `api/services/facturation_service.py`, #144).
- **Livrable ERP-spécifique, côté service** : quand les données sont intrinsèquement ERP (cas check Odoo — `sale.order`, champs `x_*`), la projection ne peut ni descendre en `core/builds/` (ADR-0016) ni vivre en `integrations/` (ADR-0019 règle 3, lecture stricte : source-only) ; elle vit en `api/services/`, à côté de la sérialisation (`feuilles_check_odoo` dans `check_facturation_service.py`, issue #173). Forme stable : les onglets sont fixes, vides quand rien à signaler.

_Éviter_ : onglets (anglicisme), sheets.
