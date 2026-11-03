---
fraicheur: 2026-11-03
---

# Flux contractuels : C15, C12, C68

Ce que les flux contractuels décrivent du point et de son contrat, et ce qu'ElectriCore en extrait. Conventions de lecture : [index des flux](index.md#lire-une-fiche).

## C15 — Événements contractuels et techniques des points C5

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0300 v5.1.3 (19/03/2026), XSD *GRD.XSD.0301.Flux_C15_v5.2.0* |
| **Segments** | C5 (BT ≤ 36 kVA, soutirage) ; événements du contrat d'injection P4 surplus portés sur le même point (*Origine_Evenement*) |
| **Obtention** | récurrent SFTP, quotidien y compris week-ends et jours fériés (§1), un flux par direction interrégionale (8 *Instance_GRD*) et par contrat GRD-F ; aucun flux un jour sans événement ; délai de publication non précisé par le guide |
| **Prérequis** | contrat GRD-F : le fournisseur reçoit les seuls points de son périmètre ayant subi un événement ; aucun consentement client. Rien avant l'entrée au périmètre, pas de rattrapage d'historique |
| **Format** | XML UTF-8 + XSD ; archive *<Emetteur>_C15_<destinataire>_<num_contrat>_<Instance_GRD>_<num_seq>_<horodatage>.zip* contenant un ou plusieurs *…_<num_seq>_XXXXX_YYYYY.xml* (§4.1) ; chiffré AES à la livraison SFTP (constat ElectriCore, ADR-0037 ; le guide n'en parle pas) |
| **Dans ElectriCore** | ingéré → `flux_c15` (+ `releves` pour les index avant/après, `spine_contrat`, périmètre `presence_perimetre` / `pdls_actifs_a`) |

**Contenu.**

- **Événement déclencheur** (*Evenement_Declencheur*, §3.2.3.1 et annexe 5.2.1) : *Type_Evenement* CONTRAT ou TECHNIQUE, *Date_Evenement* (date et heure : date d'effet de l'affaire ou date de l'intervention), *Nature_Evenement*, *Id_Affaire*, *Ref_Demandeur*, *Origine_Evenement* (0 soutirage, 1 injection). Natures **contrat** : entrées (*PMES*, *MES*, *CFNE*), sorties (*CFNS*, *RES*), modifications (*MCT* FTA, puissance ou autoconsommation collective ; *MDCTR* correction ; *MDACT* titulaire ou interlocuteur ; *AUTRE*). Natures **techniques** : branchement, coupure et rétablissement, changement de compteur (*CMAT*), données du point (*MDPRM*, dont le niveau d'ouverture), calendrier fournisseur (*MCF*), reprogrammation des heures creuses par Enedis (*PHC*). Liste complète : annexe 5.2.1 du guide. Le bloc *Operation* détaille les gestes réalisés (pose, dépose, programmation, limitation…).
- **Relevés avant/après** (*Releves* / *Donnees_Releve*) : *Code_Qualification* 1 = avant, 2 = après l'opération (index de dépose et de pose lors d'un changement de compteur), *Date_Releve*, *Nature_Index* (REEL, ESTIME, AUTO-RELEVE) ; index par cadran du calendrier distributeur (*Classe_Temporelle_Distributeur* : BASE, HP/HC, HPH/HCH/HPB/HCB) et du calendrier fournisseur (*Classe_Temporelle*), consommation seule, en kWh.
- **Situation contractuelle à l'issue de l'événement** (§3.2.3.3) : *Etat_Contractuel* (EN SERVICE / RESILIE), *Ref_Situation_Contractuelle* (change à chaque changement de client ou de fournisseur), *Date_Mise_En_Service*, *Date_Resiliation*, *Num_Sequence* (1 à la MES ou au CFNE, incrémenté à chaque changement de FTA ou de puissance, 0 = contrat annulé), branchement provisoire.
- **Structure tarifaire** : *Formule_Tarifaire_Acheminement*, *Puissance_Souscrite* et son unité, *Contexte* d'utilisation, *Forfait* (points sans comptage), *Id_Calendrier_Distributeur* (DI000001 sans différenciation temporelle, DI000002 avec, DI000003 avec différenciation temporelle et saisonnière), *Id_Calendrier* fournisseur, *Id_Structure_Horosaisonniere* (compteurs non Linky), groupe de période mobile.
- **Plages heures creuses** (*Plages_Heures_Creuses*, §3.2.3.3.1.2, depuis la v5.1.1 du 20/10/2025, TURPE 7) : deux plages, *SAISON_HAUTE* (1er novembre → 31 mars) et *SAISON_BASSE* (1er avril → fin octobre), chacune avec *Id_Plage_Heures_Creuses* et un libellé qui porte les horaires, du type « HC (2H00-7H00;13H00-16H00) » (liste en annexe 5.5.3). Le C15 porte donc les horaires, et pas seulement un identifiant de calendrier.
- **Titulaire** (*Titulaire_Contrat*, toujours présent malgré la XSD) : *Categorie* (PRO / RES), *Residence_Principale*, *Ref_Externe* (référence du fournisseur), *Personne_Physique* (civilité, nom, prénom) ou *Personne_Morale* (raison sociale, mono ou multi-site, nom commercial, activité, secteur, SIRET de l'établissement principal), *Coordonnees_Contact* (deux téléphones, fax, courriel), *Adresse_Postale* sur 7 lignes. **Interlocuteur** (*Interlocuteur_Contrat*) : mêmes sous-blocs.
- **Point, alimentation, comptage** : *Niveau_Ouverture_Services* (0, 1, 2) et sa date de changement, jour fixe, périodicité et rang de relève, *Date_Derniere_Modification_FTA*, dernières augmentation et diminution de puissance, première pose Linky, refus de pose, autoproducteur, autoconsommation collective, hébergeur / décomptant ; *Adresse_Installation* ; *Alimentation* (état, coupure, limitation et puissance limitée, puissance de raccordement, mono / tri) ; *Dispositif_De_Comptage* (compteur : type, numéro de série, palier, TIC, pas de courbe de charge ; disjoncteur : calibre, réglage).

**À quoi ça sert.**

- Reconstituer le périmètre et ses mouvements : entrées (MES, PMES, CFNE), sorties (RES, CFNS), changements d'occupant. C'est la source de `presence_perimetre` et de la liste à déposer pour une demande M023 (`/perimetre/pdls.csv`).
- Suivre la situation tarifaire jour par jour (FTA, puissance, calendrier) pour les abonnements et le TURPE, et borner la facturation avec les index avant/après des changements de compteur, de FTA ou de puissance (`releves`).
- Savoir si un compteur est communicant (niveau d'ouverture aux services) pour aiguiller le calcul de l'énergie.
- Identité du client (titulaire, SIRET, adresse de correspondance) et plages heures creuses saisonnalisées par point : demandes servables depuis le brut, pas encore exposées.

**Pièges.**

- **Renvoi** : Enedis ré-émet parfois un événement déjà publié avec la situation au moment de l'extraction (une MES renvoyée « RESILIE »). C'est la **première** publication qui fait foi (glossaire ingestion, #740). Le guide n'évoque pas ce cas. Vague observée du 15 au 18/10/2026 avec les nouvelles plages heures creuses, probablement liée aux natures *PHC* (à confirmer).
- Plusieurs affaires terminées le même jour : le point revient autant de fois. Plusieurs opérations dans une même affaire : un seul événement, choisi par prépondérance (CONTRAT l'emporte sur TECHNIQUE ; MES et CFNE l'emportent sur MCT, §2.2). Le détail reste dans *Operation*.
- Passage d'un compteur non communicant à communicant : deux événements successifs, *CMAT* (activation du calendrier distributeur, le point reste au niveau 0) puis *MDPRM* (niveau cible). *Date_Changement_Niveau_Ouverture_Services* n'a pas été initialisée à la migration Ginko.
- Index au niveau 0 : le bloc *Classe_Temporelle_Distributeur* est absent (sauf activation de la grille distributeur), seul le calendrier fournisseur peut porter des index. Liste des classes fournisseur non exhaustive (Tempo, EJP, classes propres au fournisseur).
- *Num_Sequence* est le rang de la situation contractuelle, sans rapport avec le numéro de séquence du nom d'archive. Ce dernier est continu par couple (contrat, DIR).
- Saison basse annoncée « du 1er avril au 30 octobre » au §3.2.3.3.1.3 : coquille probable pour le 31 octobre (à confirmer).

**Non extrait aujourd'hui** (`flux_c15` sur origin/main) :

- Titulaire : tout sauf `categorie`, donc *Residence_Principale*, *Ref_Externe*, *Personne_Physique*, *Personne_Morale* (dont SIRET), *Coordonnees_Contact*, *Adresse_Postale* ; tout le bloc *Interlocuteur_Contrat*.
- Situation contractuelle : *Date_Mise_En_Service*, *Date_Resiliation*, *Num_Sequence* et *Date_Debut_Num_Sequence*, *Type_Branchement_Provisoire*.
- Structure tarifaire : *Plages_Heures_Creuses* (saison haute et basse), *Contexte*, *Forfait*, *Unite_Puissance_Souscrite*, groupe de période mobile ; les calendriers de la structure tarifaire (seuls ceux des relevés sont extraits).
- Point : dernières augmentation et diminution de puissance, jour fixe, périodicité et rang de relève, première pose et refus de pose Linky, télé-opérabilité, autoproducteur, autoconsommation collective, hébergeur / décomptant, *Id_PRM_Rattache* ; *Adresse_Installation* ; *Alimentation* (état, coupure, limitation) ; disjoncteur et détails du compteur.
- Événement : *Origine_Evenement* (soutirage ou injection), *Ref_Regroupement_Demandeur*, bloc *Operation*.
- Relevés : les index du calendrier fournisseur (*Classe_Temporelle*). Seuls les index du calendrier distributeur sont pivotés, si bien qu'un point au niveau 0 peut ressortir sans index avant/après alors que le brut en porte (à confirmer sur données réelles). Ni *Id_Structure_Horosaisonniere*, ni *Coefficient_Lecture*, ni *Indicateur_Passage_A_Zero*.

## C12 — Description contractuelle des points C2-C4

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0129 v1.12.4 (25/03/2026), XSD *Enedis.SGE.XSD.0130.C12_v1.12.1* |
| **Segments** | C2, C3, C4 (HTA et BT > 36 kVA) |
| **Obtention** | récurrent SFTP, quotidien, un flux par contrat GRD-F (sans découpage par DIR) ; aucun flux un jour sans événement ; délai de publication non précisé par le guide |
| **Prérequis** | contrat GRD-F : les seuls points du périmètre de facturation du fournisseur ; aucun consentement client |
| **Format** | XML UTF-8 + XSD ; archive *ENEDIS_C12_<destinataire>_<num_contrat>_<num_seq>_<horodatage>.zip*, fichiers *…_<num_seq>_XXXXX_YYYYY.xml* de 100 Mo au plus (§4.1) ; *Mode_Generation* toujours DELTA (seuls les points ayant eu un événement) ; chiffré AES à la livraison SFTP (constat ElectriCore) |
| **Dans ElectriCore** | ingéré → `flux_c12` (ADR-0051) ; n'alimente pas encore la chaîne de facturation ni `spine_contrat` |

**Contenu.**

- **Type d'événement** (*Type_Evenement*, §2.1 et §3.2.3.3) : quatre booléens *Contractuel*, *Comptage*, *Reseau*, *Administratif* et une *Date_Evenement*. Tous les événements d'une même date sont regroupés dans un bloc, un bloc par date distincte. Chaque envoi transmet la totalité des données du point.
- **Natures d'événement contractuel** (annexe 5.1), éventuellement cumulées dans un même événement : *PMES*, *MES*, *CFN* (changement de fournisseur, entrant comme sortant), *RES*, *CTF* (formule tarifaire), *CPS* (puissance souscrite), *CCF* (calendrier fournisseur), *CGPM* (groupe de période mobile), *CTU* (changement de TURPE), *RELSPE* (relevé spécial), *PROV* / *PROVCD* (raccordement provisoire), *INIT* (photo initiale à la mise en place du flux), *ENT_ACC* / *SOR_ACC* (autoconsommation collective), *CGF* (offre historique) ; *Numero_Affaire*.
- **Situation contractuelle** : *Date_Debut*, *Date_Fin*, *Profil* (ENT1…ENT7), *Option_Tarifaire_TURPE* (BTSUPCU4, BTSUPLU4, HTACU5, HTALU5, HTACUPM5, HTALUPM5, HTAST5 stockeur depuis la v1.12.4…) avec la **puissance souscrite par classe temporelle** (*Classe_Temporelle_TURPE_Souscrite* : Pointe, HPH, HCH, HPE, HCE, PM…), calendrier fournisseur souscrit, options contractuelles (bilans de continuité et de qualité de fourniture, période d'observation, télécommande, protection de chantier…).
- **Synthèse contractuelle** : *Date_De_Premiere_Mise_En_Service* (fixe pour toute la vie du point), *Date_Der_Modif_PS* (sert aux pénalités après changement de puissance), dates des derniers changements de tarif, de calendrier fournisseur et de groupe de période mobile.
- **Utilisateur du réseau** : *Personne_Morale* (*Raison_Sociale*, *Code_APE*) et *Interlocuteur_Technique* (nom, téléphone, courriel). L'interlocuteur client n'est pas transmis.
- **Réseau et comptage** : adresse du point, alimentation principale (*Domaine_De_Tension*, *Tension_De_Livraison*, puissances de raccordement et limite, zone de qualité de desserte), alimentations de secours et complémentaires ; modalités de relève (*Plage_De_Releve*, *Mode_De_Traitement_Du_Compteur* : courbe de charge, index ou mixte), compteur, transformateurs de courant et de tension, facteurs de correction des pertes. **Aucun index** : les relevés C2-C4 passent par le R17.

**À quoi ça sert.**

- Situation tarifaire des points C4 et HTA : option TURPE et puissances par classe, base du TURPE fixe et du contrôle de la facture distributeur (F12).
- Catégorie d'accise : la raison sociale et le code APE servent de proxy de la nature économique (#226).
- Mouvements de périmètre C2-C4 : mises en service, changements de fournisseur, résiliations, avec leurs dates de début et de fin.
- Préparer les pénalités de dépassement et les changements de puissance (*Date_Der_Modif_PS*, période d'observation).

**Pièges.**

- Changement de fournisseur côté sortant : *Date_Fin* = date d'effet − 1 jour, soit une borne incluse, alors qu'ElectriCore raisonne en intervalles demi-ouverts [début, fin). Côté entrant : *Date_Debut* = date d'effet. Sur une résiliation, *Date_Fin* est la date d'effet (§2.2.1).
- Une affaire SGE en cours met en attente tous les autres événements (comptage, réseau, options) jusqu'à son solde, puis tout arrive d'un bloc, sur plusieurs *Type_Evenement* si les dates diffèrent (§2.2.5).
- Données non gérées à date : l'adresse et les transformateurs de courant et de tension valent au jour de l'émission, pas à la date d'événement.
- Un changement de l'interlocuteur client déclenche un C12 qui peut être identique au précédent (seule *Date_Evenement* change).
- Pas de *Segment_Clientele* : le segment se déduit du domaine de tension et de l'option tarifaire.
- *Numero_Sequence* est réservé et vide : seul le numéro de séquence du nom d'archive (continu par contrat) prouve la complétude.

**Non extrait aujourd'hui** (`flux_c12` sur origin/main) :

- Seuls le premier *Type_Evenement* et la première *Nature_Evenement* sont lus : les cumuls (MES + CTF + CPS, CFN + RELSPE…) et les blocs aux dates ultérieures sont perdus, de même que les quatre booléens de type.
- Puissance de la classe *Pointe* et des classes HTA (PM, demi-saison, JA) : non pivotées. D'après l'annexe 5.2, BTSUPLU4 (C4 longue utilisation) porte aussi une classe Pointe (à confirmer sur données réelles).
- *Date_Der_Modif_PS*, *Date_Der_Modif_Tarif*, *Date_Der_Modif_CCF*, *Date_Der_Modif_CGPM* ; options contractuelles ; calendrier fournisseur souscrit (*Structure_Fournisseur_Souscrite*).
- *Interlocuteur_Technique*, adresse du point, *Type* (hébergeur, autoconsommation collective), *Autoproducteur*.
- Alimentation hors domaine de tension (tension de livraison, puissances de raccordement et limite, secours, complémentaire) ; tout le comptage (mode de traitement, plage de relève, compteur, transformateurs, pertes).

## C68 — Photo technique et contractuelle d'un point, à la demande

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0504 v2.1 (30/06/2026 ; la v2.0 du 29/05/2026 apporte les évolutions TURPE 7), exemple *Enedis.SGE.JSON.0514.Flux.C68_v1.2.1* |
| **Segments** | tous : C1 à C5 et P1 à P4 (installation de production pour P1 à P4, options contractuelles pour C1/C4 et P1/P3) |
| **Obtention** | ponctuel : demande M023 « informations techniques et contractuelles » (ITC), au portail SGE ou par webservice (*commandeInformationsTechniquesEtContractuelles*) ; livré sur le portail et sur le SFTP (répertoire *R63_R64_R65_R66_R67_C68*), clos par un compte rendu CR M023 |
| **Prérequis** | être fournisseur titulaire de **tous** les points de la demande, ou déclarer l'accord du client ; une demande porte sur le soutirage **ou** sur l'injection, jamais les deux (§3.1). Hors titulaire, l'accès se limite aux colonnes ouvertes « accord soutirage » ou « accord injection » |
| **Format** | JSON ou CSV, au choix de la demande ; archive primaire *ENEDIS_C68_P_ITC_<idDemande>_00001_<horodate>.zip* contenant de 1 à 10 archives secondaires de 1 000 points au plus (soit 10 000 points au plus par demande, déduit) ; la CSV de la maquette est en point-virgule, Windows-1252, fins de ligne CRLF (constat sur la maquette) |
| **Dans ElectriCore** | non ingéré : aucune entrée dans la configuration de flux (cadrage dans #216, close sans livraison) |

**Contenu.**

- **Données générales** : adresse d'installation non normalisée, typages (alimentation complémentaire ou de secours, borne poste, borne fixe, hébergeur / décomptant, regroupement TURPE), instance SI.
- **Synthèse contractuelle** : dernières mises en service en soutirage et en injection, derniers changements de FTA, de puissance (augmentation et diminution), de calendrier fournisseur et de groupe de période mobile, première pose Linky, niveau d'ouverture aux services, autoconsommation collective et individuelle, type d'injection (totalité ou surplus).
- **Situation contractuelle courante** (*situationsContractuelles*) : date de début de la situation en cours, contrat GRD-F, *etatContractuel*, *segment* ; structure tarifaire (FTA, *puissanceSouscrite*, contexte d'utilisation, forfait, grille fournisseur et grille TURPE avec calendrier et classes temporelles, type d'offre) ; client final (catégorie, référence fournisseur, type de résidence, adresse, courriel et téléphones, personne physique ou morale avec dénomination, nom commercial, secteur, code NAF, SIRET, SIREN) ; interlocuteur client et interlocuteur technique ; refus de pose Linky et sa date.
- **Situation d'alimentation** : état, domaine de tension, tension de livraison, puissances de raccordement (soutirage, injection) et limite, coupure et limitation en cours, alimentations complémentaires et de secours, production autonome.
- **Situation de comptage** : dispositif (type, télé-opérabilité, mode de relève, média), caractéristiques de relève (périodicité, mode de traitement, plage), **plages heures creuses saison haute et saison basse** (points C5), compteur (numéro de série, sous-type, TIC, programmation horaire), disjoncteur, transformateurs, pertes.
- **Rattachements** (regroupement TURPE, hébergeur / décomptant, secours comptée, autoconsommation totale AO CRE…), **installation de production** (filière, technologie), **options contractuelles** (bilans de continuité et de qualité de fourniture, période d'observation, télécommande…).

**À quoi ça sert.**

- Photo du périmètre : une demande réelle porte sur jusqu'à 10 000 points, liste fournie par le demandeur. Elle permet de confronter l'état dérivé du C15 à l'état de référence Enedis (événements manqués, dérives), et d'initialiser les points arrivés avant le déploiement de l'instance, que le C15 ne décrit pas.
- Identité et contact du client (SIRET, SIREN, NAF, secteur) pour l'accise, le recouvrement et la mise à jour de l'ERP.
- Plages heures creuses saisonnalisées et calendriers de chaque point, pour les offres heures creuses et le conseil.
- Données techniques absentes du C15 : production, rattachements, puissance limite, options C4.

**Pièges.**

- Photo à la date de traitement, sans historique : seules les dates de dernière modification renseignent sur le passé. D'après l'usage réel, les dates de modification de puissance sont peu fiables.
- Les points résiliés ne sont pas restitués (constat d'usage) : le compte rendu les marque non publiés, sans doute au motif « état contractuel » (à confirmer).
- Archive dans l'archive (primaire puis secondaires) : l'ingestion actuelle ne dézippe qu'un niveau (à confirmer avant tout landing).
- Le schéma bouge avec le TURPE 7 : la v2.0 supprime une trentaine de champs (dont le code tarif d'acheminement et le code du calendrier fournisseur) et remplace la plage heures creuses unique par deux saisons. Le tableau du guide nomme *plageHeuresCreusesSaisonHaute* / *SaisonBasse*, alors que le fichier exemple porte *plageHeuresCreuses.saisonHaute* / *saisonBasse* : se fier au fichier reçu.
- Demande soutirage **ou** injection : un autoproducteur en surplus demande deux commandes pour tout couvrir.
- Contient des données personnelles (nom, coordonnées des interlocuteurs) : mêmes précautions que le titulaire du C15.

## Pour aller plus loin

Comment les flux sont ingérés : [ingestion des flux](../../ingestion.md) · [conventions de dates](../../conventions-dates-enedis.md).

[Retour aux flux Enedis](index.md).
