---
fraicheur: 2026-11-03
---

# Mesures à la demande : R63 à R67, compte rendu M023, IFJ

Les mesures commandées sur le portail SGE (service M023) et leurs variantes récurrentes, le compte rendu qui clôt chaque demande, et les données infra-journalières. Règles d'accès et arbre de décision : [Obtenir les données Enedis](../sources-enedis.md). Conventions de lecture : [index des flux](index.md#lire-une-fiche).

## R63 (+ R63A, R63B) — courbe de charge

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0503 Flux R6X v1.5.3 (13/05/2026), § 1, 1.2.1, 2, 4.2 ; schémas JSON R63 v1.2.0 (M023) et R63X v1.1.0 (récurrent) |
| **Segments** | R63 : C1-C5, P1-P4 · R63A : C1-C4, P1-P3 · R63B : C5, P4 |
| **Obtention** | **Ponctuel R63** : M023 (portail SGE ou webservice *CommandeHistoriqueDonneesMesuresFines*), publié dès production avec un compte rendu M023 (GUI.0505). **Récurrent R63A** (quotidien, hebdomadaire, mensuel) / **R63B** (quotidien, mensuel) : abonnement (formulaires F-P300/F-P305 ou page « services de données » du PDL ; webservice *CommandeServicesAccesDonnees*), sans compte rendu. Le quotidien est publié « au fil de l'eau » entre 2 h et 18 h (dernières 24 h en date transactionnelle) ; l'hebdomadaire le lundi après 18 h ; l'horaire du mensuel n'est pas encore fixé par le guide |
| **Prérequis** | C5 : **consentement** du client déclaré, et collecte de la courbe **active** dans le SI Enedis (prestation F300C, 36 mois renouvelables) ; historique ponctuel sur **24 mois**, coupé à la dernière mise en service (NMO-CF_016E V2 § 4.2.7-4.2.9). C2-C4 : **autorisation expresse** du client, PDL en service ; historique ponctuel 12 mois portés à **24 mois** chez Enedis (NMO-CF_015E V2 § 4.2.6, note 4). Abonnement récurrent : 36 mois renouvelables, délai standard 3 jours (C5) |
| **Format** | JSON zippé (CSV possible pour une M023 saisie sur le portail) ; chiffré sur le SFTP, en clair au téléchargement depuis le portail. Archive M023 *ENEDIS_R63_P_CdC_<idDemande>_<numSequence>_<horodate>.zip* ; récurrent *ENEDIS_R63A_Q_CdC_<idDemande>_<codeContrat>_<numSequence>_<horodate>.zip* (*Q*/*H*/*M* selon la fréquence). Au plus 100 Mo zippé, 3 Go décompressé, par fichier |
| **Dans ElectriCore** | non ingéré — abonnement R63B : issue #212. Les M023 arrivent dans le même répertoire SFTP que R64/R67 mais aucun motif de fichier ne les prend |

**Contenu.**
- Par PDL (*idPrm*) et par grandeur (*grandeurMetier* CONS/PROD × *grandeurPhysique*) : une série de points *v* (valeur arrondie à l'entier), *d* (horodate), *p* (pas), *n* (nature).
- Grandeurs : puissance active *PA* en **W** (tous segments) ; puissance réactive inductive *PRI* / capacitive *PRC* en **VAr** et tension *E* en **V** (C1-C4 seulement, la tension ne dépend pas de CONS/PROD).
- Pas : *PT5M* (C1-C4 seulement), *PT10M*, *PT15M* / *PT30M* / *PT60M* (C5/P4 seulement). En C5, c'est le pas enregistré par le compteur : 60 min par défaut, 30 min une fois la collecte activée, 15 ou 10 min selon les services souscrits (NMO-CF_016E § 4.2.7).
- *etapeMetier* : *BRUT* (données compteur) ou *BEST* (avec estimations et corrections, puissance active C1-C4 seulement).
- Qualité par point : *n* parmi B, C, R, D, H, P, S, T, F, G, E (brut, corrigé, réel > 36 kVA, importé, changement d'heure, coupures secteur, estimé) ; pour le Linky, indice de vraisemblance *iv* (0, 1, 2) et état complémentaire *ec* (coupure, reprise, correction d'heure…) ; en *BEST*, type de complétion *tc* (interpolation, recopie J-7, valeur forcée à 0…).
- Récurrent seulement : bloc *echeances* (*filEau* pour le quotidien, *Immediat* pour l'hebdomadaire et le mensuel) et *idPublication* dans l'en-tête.

**À quoi ça sert.**
- Optimiser une puissance souscrite ou détecter des dépassements avec une vraie résolution infra-horaire.
- Analyser un profil de consommation pour le conseil, l'approvisionnement ou le prix d'une offre (pointe, talon, effacement).
- Estimer une production ou une autoconsommation (grandeur PROD, P4).
- Répondre à une demande de « courbe de charge sur N mois » : M023 R63 pour l'historique, R63A/R63B pour la suite.

**Pièges.**
- **Convention d'horodate opposée selon le segment** : horodate de *début* de pas en C1-C4/P1-P3, de *fin* de pas en C5/P4 (§ 4.2.1). Un point C5 daté 15:30 couvre 15:00-15:30.
- Les valeurs sont des **puissances moyennes** sur le pas, pas des énergies : pour obtenir des Wh, multiplier par la durée du pas, puis vérifier la cohérence avec les index.
- Pas d'historique si la collecte n'était pas active : le compteur Linky ne garde en local qu'environ 5 mois au pas 60 min (NMO-CF_016E § 3.3, note 2). Le pas peut aussi changer au cours d'une même période.
- En récurrent, la sélection se fait en **date transactionnelle** : un même point peut arriver dans plusieurs versions. La dernière publiée fait foi, d'où l'obligation d'intégrer les fichiers par *numSequence* croissant (§ 1.1.1, 2.1.1).
- C'est de loin le flux le plus volumineux (288 points par jour et par grandeur au pas 5 min) : la limite de 100 Mo par zip découpe une demande en plusieurs archives numérotées.
- En hebdomadaire et en mensuel, la nature (brute ou corrigée) est un critère de regroupement des fichiers : une souscription par nature.

---

## R64 (+ R64A, R64B) — index quotidiens

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0503 Flux R6X v1.5.3 (13/05/2026), § 1.2.2, 2, 4.3 ; schémas JSON R64 v1.2.1 (M023) et R64X v1.1.0 (récurrent) ; correspondance avec les anciens flux : Enedis.SGE.CAR.0549 |
| **Segments** | R64 : C1-C5, P1-P4 · R64A : C1-C4, P1-P3 · R64B : C5, P4 |
| **Obtention** | **Ponctuel R64** : M023 (portail ou *CommandeHistoriqueDonneesMesuresFines*), avec compte rendu. **Récurrent R64A** (quotidien seulement) / **R64B** (quotidien ou mensuel) : abonnement F305 / F305A, ou *CommandeServicesAccesDonnees*. D'après le fichier de correspondance CAR.0549, R64B remplace le R151 et R64A le R171 |
| **Prérequis** | C5 : **consentement** déclaré ; historique ponctuel sur **36 mois**, coupé à la dernière mise en service (NMO-CF_016E § 4.2.4). Abonnement : 36 mois renouvelables, délai 3 jours ; depuis le 08/12/2025, index et Pmax font l'objet de **deux souscriptions distinctes** (§ 4.2.5). C2-C4 : autorisation expresse (NMO-CF_015E § 4.2.2) ; profondeur ponctuelle non précisée (à confirmer) |
| **Format** | JSON zippé (CSV possible au portail). Archive M023 *ENEDIS_R64_P_INDEX_<idDemande>_<numSequence>_<horodate>.zip* ; récurrent *ENEDIS_R64B_Q_INDEX_<idDemande>_<codeContrat>_<numSequence>_<horodate>.zip* (ou *_M_*). Constaté en production : le JSON interne a une extension en majuscules (*.JSON*), contrairement au guide |
| **Dans ElectriCore** | ingéré (M023 seulement) → `flux_r64` → `releves` → `chronologie_releves`. Le motif `**/R63_R64_R65_R66_R67_C68/*_R64_*.zip` **ne capte pas** les récurrents : *_R64B_* et *_R64A_* ne contiennent pas la chaîne *_R64_*, et leur répertoire SFTP n'est pas connu (à confirmer). Aucune issue identifiée pour R64B |

**Contenu.**
- Par PDL : des blocs *contexte* (*etapeMetier* *BRUT*, *contexteReleve*, *typeReleve*, *motifReleve*) contenant des *grandeur* (*grandeurMetier* × *grandeurPhysique*, *unite*), puis des *calendrier* (*idCalendrier*, *libelleGrille* distributeur ou fournisseur), puis des *classeTemporelle* (*idClasseTemporelle*, *codeCadran*), enfin des valeurs (*d*, *v* entier, *iv*).
- **C5 (Linky)** : jusqu'à 20 index par relevé, soit 10 cadrans fournisseur (*IDX_EAS_F1* à *F10*), 4 distributeur (*IDX_EAS_D1* à *D4*), le totalisateur soutirage *IDX_EAS_T* (bloc *cadranTotalisateur*, sans calendrier), et en mode production l'injection *IDX_EAI_T* et 4 index réactifs.
- **C1-C4** : pas de totalisateur ; énergies active et réactive (*EA*, *ER*, *ERC*, *ERI*), plus les dépassements (*DD* durée en s, *DE*, *DQ*), la puissance maximale atteinte (*PMA*) et le temps de fonctionnement (*TF*) par classe temporelle.
- Unités : **Wh** pour l'énergie active, VArh pour la réactive, s, W, VA pour le reste.
- Contexte de relève : *COL* (collecte), *TOP* (télé-opération), *CRI* (intervention) en C5 ; *FMR*, *CRD*, *CRI* et *RHF* (relevé à heure fixe proche de minuit, à partir d'une date D2) en C1-C4. Type de relève : *AQ* (arrêté quotidien, normalement à minuit), *AV*/*AS*/*AP* (avant, pendant, après une reprogrammation), *LC* (lecture courante) en C5 ; *LC*, *RM*, *RC* en C1-C4.
- Indice de vraisemblance Linky *iv* de 0 à 15, codé sur 4 bits : index non utile qui évolue, dépassement de la consommation maximale, index décroissant, message incohérent. Il est nul hors énergie active Linky.

**À quoi ça sert.**
- Obtenir les index passés d'un PDL pour calculer des consommations mensuelles par cadran, sur un PDL au périmètre (CFNE : on remonte avant l'entrée chez le fournisseur).
- Obtenir un index à une date précise (contestation, résiliation, changement d'offre).
- Contrôler la cohérence grille distributeur / grille fournisseur / totalisateur.
- En récurrent (R64B), assurer le suivi quotidien des index C5 à la place du R151.

**Pièges.**
- Les horodates *d* sont **naïves** (sans fuseau, supposées heure de Paris) ; ElectriCore les ancre au boundary. R64 n'a pas le décalage d'un jour du R151.
- Les fenêtres de plusieurs demandes M023 se chevauchent : le même relevé revient dans plusieurs fichiers.
- En récurrent, sélection en date transactionnelle : plusieurs versions d'un index pour une même date, à intégrer dans l'ordre des *numSequence* (§ 1.1.1).
- Un PDL peut porter plusieurs contextes le même jour (*AQ* et *AV*/*AS*/*AP* lors d'une reprogrammation, ou *CRD*/*RHF*/collecte qui coexistent en C1-C4).
- *idCalendrier* vaut parfois juste « D » ou « F » quand aucun code calendrier n'existe.
- Le numéro de séquence s'incrémente pour chaque fichier d'une demande (§ 2.1.1). Une grosse demande M023 donne donc plusieurs archives : le compte rendu indique combien il faut en attendre.

**Non extrait aujourd'hui** (`flux_r64` ne garde que CONS × EA, grille distributeur, points d'*iv* = 0) :
- la grille **fournisseur** (*IDX_EAS_F1* à *F10*) et les **totalisateurs** (*IDX_EAS_T*, *IDX_EAI_T*) ;
- toute la grandeur **PROD** (injection) et les index réactifs ;
- les grandeurs C1-C4 *DD*, *DE*, *DQ*, *PMA* et *TF* ;
- *motifReleve*, *codeCadran*, les libellés de calendrier et de classe ;
- les points d'*iv* ≠ 0, écartés sans trace. Comme l'*iv* n'est renseigné que pour l'énergie active Linky, les index C1-C4 sont probablement **tous** écartés (à confirmer sur un échantillon C4).

---

## R65 — énergies globales quotidiennes

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0503 Flux R6X v1.5.3 (13/05/2026), § 1.2.3, 4.4 ; schéma JSON R65 v1.2.0 |
| **Segments** | C1-C5, P1-P4 |
| **Obtention** | ponctuel seulement : M023 (portail ou *CommandeHistoriqueDonneesMesuresFines*), avec compte rendu. Pas de variante récurrente |
| **Prérequis** | consentement (donnée fine : NMO-CF_016E § 3.2.2 cite les « consommations quotidiennes » parmi les données à autorisation spécifique) ; PDL au périmètre, coupé à la dernière mise en service. Profondeur M023 non donnée par les notes NMO (36 mois annoncés, non confirmé en M023) |
| **Format** | JSON zippé (CSV possible au portail) ; *ENEDIS_R65_P_Energie_<idDemande>_<numSequence>_<horodate>.zip* |
| **Dans ElectriCore** | non ingéré — issue #215, oracle de validation du pipeline énergie (énergie globale, sans cadran) |

**Contenu.**
- Par PDL et par grandeur : une énergie par jour, avec *d* (date fonctionnelle, sans heure) et *v* ; *pas* fixé à *P1D* et *typeValeur* à *GLOBALE*.
- **Aucune répartition** par grille ni par classe temporelle.
- C5/P4 : *modeCalcul* *DIFF.INDEX*, différence des index **totalisateurs** relevés à minuit ; énergie active seulement, en **Wh**.
- C1-C4/P1-P3 : *modeCalcul* *INTEG.COURBE*, intégrale de la courbe de charge ; active (Wh) ou réactive inductive/capacitive (*ERI*/*ERC*, VArh).
- *etapeMetier* fixé à *BRUT*.

**À quoi ça sert.**
- Oracle indépendant : comparer la somme des énergies par cadran calculées par `pipeline_energie` avec l'énergie globale du distributeur, jour par jour ou sur un mois.
- Fournir un profil journalier (saisonnalité, jours ouvrés, vacances) sans le volume d'une courbe de charge.
- Servir une demande de « consommation quotidienne » quand le cadran n'importe pas.

**Pièges.**
- La source dépend du segment (totalisateur en C5, courbe en C4) : deux méthodes pour une même colonne.
- Le totalisateur C5 compte aussi les évolutions d'index « non utiles ». L'écart avec la somme des cadrans du calendrier distributeur n'est donc pas forcément une erreur de calcul (à confirmer).
- En C4, l'énergie quotidienne n'existe que si la courbe a été collectée ; un trou de courbe donne un trou ou une sous-estimation (à confirmer).
- La date *d* est un jour civil, à ne pas confondre avec l'horodate d'un index.

---

## R66 (+ R66B) — puissance maximale quotidienne

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0503 Flux R6X v1.5.3 (13/05/2026), § 1.2.4, 4.5, 6.1 ; schémas JSON R66 v1.2.0 (M023) et R66X v1.1.0 (récurrent) |
| **Segments** | C5, P4 (Linky seulement) |
| **Obtention** | **Ponctuel R66** : M023 (portail ou *CommandeHistoriqueDonneesMesuresFines*), avec compte rendu. **Récurrent R66B** : quotidien ou mensuel, abonnement F305 ou *CommandeServicesAccesDonnees*. D'après CAR.0549, R66B reprend la Pmax du R151 |
| **Prérequis** | **consentement** ; historique ponctuel sur **36 mois**, coupé à la dernière mise en service (NMO-CF_016E § 4.2.4) ; abonnement de 36 mois renouvelables, **souscription distincte** de celle des index depuis le 08/12/2025 (§ 4.2.5) |
| **Format** | JSON zippé (CSV possible au portail) ; *ENEDIS_R66_P_Pmax_<idDemande>_<numSequence>_<horodate>.zip*, récurrent *ENEDIS_R66B_Q_Pmax_<idDemande>_<codeContrat>_<numSequence>_<horodate>.zip* |
| **Dans ElectriCore** | non ingéré — abonnement R66B : issue #213 |

**Contenu.**
- Par PDL et par jour : la puissance maximale atteinte *v*, et *d*, l'**horodate précise** (à la seconde) où elle a été atteinte ; *pas* fixé à *P1D*, *modeCalcul* à *MESURE*.
- Puissance **apparente**, en **VA**.
- Monophasé : *PMA*. Triphasé : *PMA* (maximum de la somme des trois phases, « équivalent monophasé ») plus *PMA1*, *PMA2*, *PMA3* par phase (annexe 6.1).
- Soutirage (CONS) ; injection (PROD, compteurs P4) à partir d'une date D1 fixée après la v1.3.0 du guide.

**À quoi ça sert.**
- Vérifier ou optimiser la puissance souscrite d'un C5 en comparant la Pmax journalière au kVA souscrit.
- Repérer un déséquilibre de phases sur un triphasé.
- Répondre à une demande de « puissance maximale atteinte » sans courbe de charge, donc sans collecte à activer.

**Pièges.**
- VA (apparent), et non W : à comparer à une puissance souscrite en kVA, pas à une courbe en W.
- Le maximum journalier ne dit pas combien de temps a duré la pointe : un seul pic suffit.
- Le guide se contredit sur le segment : le tableau général (corrigé en v1.5.3) indique C5, P4, mais l'en-tête du § 4.5 ne parle que du C5.
- En récurrent, versions multiples d'une même journée (date transactionnelle) : intégrer par *numSequence* croissant.

---

## R67 — mesures facturantes

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0503 Flux R6X v1.5.3 (13/05/2026), § 1.2.5, 5 ; schéma JSON R67 v1.3.0 |
| **Segments** | C1-C5, P1-P4 |
| **Obtention** | ponctuel seulement : M023 (portail, ou webservice *CommandeHistoriqueDonneesMesuresFacturantes*), avec compte rendu ; publié sur le portail et sur le SFTP. Pas de variante récurrente |
| **Prérequis** | être le **fournisseur titulaire actif** du PDL (constat d'usage d'un fournisseur : une demande sur un PDL qui n'est plus au périmètre est refusée). C5 : NMO-CF_016E § 4.2.3 demande de **déclarer l'autorisation** du client et donne 24 mois, portés à **36 mois** chez Enedis (36 mois constatés en M023 ; l'API Mesures en annonce 60). C2-C4 : autorisation expresse, 12 mois portés à **24 mois** chez Enedis (NMO-CF_015E § 4.2.4). Dans tous les cas, coupé à la dernière mise en service |
| **Format** | JSON zippé (CSV possible au portail, mais non ingéré) ; *ENEDIS_R67_P_Mfact_<idDemande>_<numSequence>_<horodate>.zip* ; JSON interne en minuscules (*.json*) |
| **Dans ElectriCore** | ingéré → `flux_r67`, **hors** `releves` (ADR-0047) ; lecture par le loader `r67()` ou `GET /flux/r67` |

**Contenu.**
- *etapeMetier* fixé à *FACT* : les mesures transmises dans les flux de relevé du titulaire (§ 1.2.5), c'est-à-dire les consommations retenues pour la facturation.
- Par PDL : des *contexte* par motif de relève (*idMotifReleve* / *libelleMotifReleve*). Le guide ne donne que des exemples (*CYCL* relevé cyclique…) ; motifs constatés par ElectriCore : *CYCL*, *CFNS* (entrée ou sortie fournisseur), *MCT* (changement de FTA ou de puissance), *AUTRE* (régularisation). Ils se succèdent dans le temps sans se recouvrir.
- Puis *grandeur* → *calendrier* (*codeGrille* D ou F, *codeCalendrier*) → *classeTemporelle* → *quantite* : **énergie sur une période** *dbtMesure* / *finMesure* (jours), avec *dateCreation*, *codeNature* et *codeStatut*.
- *codeNature* : E estimé, I inconnu, C régularisé, R réel. *codeStatut* : I initial, A annulé, R rectificatif.
- Grandeurs : *EA* (Wh) pour tous les segments ; en C1-C4, aussi *ER*, *ERN* (réactive négative, ajoutée en v1.5.3 pour le TURPE 7 CER HTA), *PA* (puissance atteinte, VA ou W), *DD* et *TF* (en **heures**), *DE*, *DQ*.
- Deux grilles parallèles : distributeur (jusqu'à 4 cadrans saisonniers) et fournisseur (Base ou HP/HC).

**À quoi ça sert.**
- Amorcer la provision d'un mensualisé sans attendre 12 mois de relevés (ADR-0047 et ADR-0048), quand le client est resté sur place (CFNE).
- Reconstituer des consommations mensuelles par cadran sur 36 mois, en proratisant les périodes de relevé au mois civil.
- Seule voie M023 recevable aussi pour les **non-communicants**, mais le contenu y est grossier (Base, bimestriel, estimé).
- Contrôler une facture passée face à ce que le distributeur a retenu.

**Pièges.**
- C'est de l'**énergie déjà différenciée** par période, pas un index : ne jamais la verser dans une chaîne d'index.
- Les périodes suivent les relevés (souvent bimestriels ou mensuels), pas le mois civil : il faut proratiser.
- **Régularisations négatives** légitimes (*codeNature* C) : une révision physique d'une estimation, à ne pas filtrer comme une anomalie.
- La grille distributeur peut manquer (compteur non communicant, grille F seule) ou être dégénérée (Base seule en début de période, puis passage à 4 cadrans en cours de mesure).
- La somme des cadrans distributeur diffère du total fournisseur d'environ 1 kWh sur la moitié des périodes (arrondi par cadran).
- Après un changement d'occupant (MES/PMES), l'historique s'arrête à l'entrée et la demande n'apporte rien. La *periode* de l'en-tête est la fenêtre d'éligibilité, pas la couverture réelle des données.

**Non extrait aujourd'hui** (`flux_r67` ne garde que CONS × EA, une grille par période) :
- la grille **fournisseur** quand une grille distributeur existe : elle se re-dérive par somme des cadrans, à l'arrondi près ;
- la grandeur PROD, et toutes les grandeurs C2-C4 hors énergie active : *ER*, *ERN*, *PA*, *DD*, *DE*, *DQ*, *TF* ;
- les libellés (*libelleMotifReleve*, *libelleCalendrier*, *libelleClasseTemporelle*, *libelleNature*, *libelleStatut*) ;
- *codeStatut* est conservé (`code_statut`) mais **pas interprété** : une ligne *A* (annulé) ou *R* (rectificatif) republiée gagnerait la déduplication comme n'importe quelle livraison plus récente (cas non exercé, échantillon 100 % *Initial*).

---

## CR M023 — Compte rendu des demandes M023

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0505 v1.2.4 — document classé « usage restreint » : seul son rôle est résumé ici |
| **Segments** | C1 à C5, P1 à P4 |
| **Obtention** | automatique : un compte rendu clôt chaque demande M023 (R63 à R67, C68), publié comme les données (portail et SFTP) |
| **Prérequis** | avoir passé la demande M023 correspondante |
| **Format** | JSON ou CSV |
| **Dans ElectriCore** | non ingéré : aucune entrée dédiée dans la configuration de flux (un compte rendu déposé à côté des R64/R67 pourrait être capté par leurs motifs, à confirmer) |

**Rôle.** Clôt chaque demande M023 et dit, PDL par PDL, ce qui a été publié. À lire avant d'exploiter un lot R64, R67 ou C68 : après le compte rendu, plus rien n'arrive pour cette demande. Le détail du format et des motifs relève d'un document à diffusion restreinte.

## IFJ — données infra-journalières C1-C4

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0553 Flux IFJ v1.0.0 (07/05/2026) ; schéma Enedis.SGE.JSON.0554 v1.0.0 |
| **Segments** | C1-C4, P1-P3 équipés d'un **boîtier IP** (compteurs PME-PMI, ICE, SAPHIR, Linky Pro) |
| **Obtention** | ponctuel, sur demande, par webservice *CommandeTransmissionDonneesInfraJ* v1.1 (prestation F375A, API seulement) ; une lecture est lancée sur le compteur, et le résultat arrive en quelques minutes. Une seule demande en cours par acteur et par PDL (NMO-CF_015E § 4.2.8) |
| **Prérequis** | **autorisation expresse** du client déclarée, PDL en service (NMO-CF_015E § 4.2.8) |
| **Format** | JSON UTF-8 zippé, **un fichier par PDL** : *ENEDIS_IFJ_<affaireId>_<idPRM>_<horodatage>.zip* |
| **Dans ElectriCore** | non ingéré — reporté (issue #219) |

**Contenu.**
- Les **24 dernières heures glissantes** lues sur le compteur, en données brutes.
- Courbe de charge en soutirage et en injection : puissance active et puissances réactives positive et négative, au pas de 5 ou 10 min (*pasIntegrationCdc* 300 ou 600 s), avec des marqueurs d'événement (coupure, changement de période tarifaire, changement de pas…).
- Courbe de tension efficace moyenne (non disponible sur PME-PMI et ICE2Q).
- Index courants de la période contractuelle en cours : énergies active et réactives, puissances maximales, durée et dépassement quadratique, temps de fonctionnement.
- Paramètres de tarification dynamique (pas pour les compteurs ICE).
- Statut global *REALISEE*, *ECHEC*, *NON_RECEVABLE* ou *PARTIEL*, avec un code et un motif de rejet (PDL inconnu, fictif ou non téléopérable…) ; *dateReleve* et *dateCompteur* pour mesurer la dérive de l'horloge du compteur.

**À quoi ça sert.**
- Vérifier sur-le-champ une situation de comptage C4 (après une intervention, un changement de puissance, une coupure).
- Suivre une pointe ou un effacement en quasi temps réel, sans attendre la publication quotidienne.

**Pièges.**
- Unités en **kilo** (kW, kWh, kvarh, kVA, durées en minutes), contrairement aux R6X (W, Wh).
- Horodates ISO **avec décalage horaire**, alors que les R6X sont naïfs.
- Aucun historique au-delà de 24 h ; un statut *PARTIEL* peut ne couvrir qu'une partie des blocs demandés.

## Pour aller plus loin

Comment les flux sont ingérés : [ingestion des flux](../../ingestion.md) · [conventions de dates](../../conventions-dates-enedis.md).

[Retour aux flux Enedis](index.md).
