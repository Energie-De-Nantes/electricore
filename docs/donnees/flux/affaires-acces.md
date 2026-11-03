---
fraicheur: 2026-11-03
---

# Affaires, réclamations et accès : X12-X15, API, chiffrement

Le suivi des demandes faites à Enedis, les API REST en préparation, et ce qu'il faut pour lire les flux. Conventions de lecture : [index des flux](index.md#lire-une-fiche).

## X12 / X13 — suivi des affaires SGE

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0322 v2.5.0 (07/05/2026) ; XSD Enedis.SGE.XSD.0352 v2.0.0 |
| **Segments** | C2, C3, C4, C5 (§4.2.1) |
| **Obtention** | récurrent SFTP, quotidien, par contrat GRD-F ; un flux « sans affaires » est émis les jours sans mouvement (§1) |
| **Prérequis** | contrat GRD-F ; couvre les PRM au périmètre **et** les PRM entrants ou sortants (§1) ; aucune souscription dédiée citée par le guide (à confirmer) |
| **Format** | XML + XSD, UTF-8 ; ZIP de 1 à 20 XML de 1 000 affaires au plus ; *ENEDIS_<X12\|X13>_<EIC>_<num_contrat>_<num_seq>_<horodatage>.zip*, fichiers *…_XXXXX_YYYYY.xml* (§5.1) ; chiffré AES comme tout flux SFTP |
| **Dans ElectriCore** | ingéré → `flux_affaires` (grain jalon, colonne `origine` initiee/recue) → `affaires_ouvertes` ; exposé par `GET /perimetre/affaires` (bot `/perimetre affaires`) et `POST /facturation/rsc` |

**Contenu.**

- **X12** : avancement des affaires **à l'initiative du fournisseur** destinataire. **X13** : affaires
  **non initiées par lui**, dites « reçues » : un autre fournisseur sur un PRM entrant ou sortant, ou le
  distributeur (§1). En X13, la demande est filtrée des informations commercialement sensibles (ICS) et
  l'*initiateur* n'est fourni que pour les affaires du distributeur (§4.2, §4.2.2.1.3).
- Identité : *id* d'affaire (8 caractères, le même que l'*Id_Affaire* des événements C15),
  *referenceDemandeur* / *referenceDemandeurGroupee* (référence du fournisseur), PRM
  (*donneesPoint/id*), *segment*.
- *statut* (cycle grossier : COURS, TERMN, ANNUL) et liste de *jalons* : *num*, *dateHeure*,
  *affaireDateEffet* recalculée à chaque étape, *affaireEtat* (DMTR, DMREC, INPL, INRP, CPRE, CPNR, CNRE,
  CLAA…). Les listes de valeurs sont dans l'annexe ListeDeValeurs du guide chapeau B2B
  (Enedis.SGE.AX.0355), pas dans ce guide (§4.2.1).
- Demande initiale : *objet* (code de prestation : CFN, MES, PMS, CST, RIC, AME, RET, RPI…),
  *prestationPrincipale* (fiche et option du catalogue), *dateEffetSouhaitee*, *contratId*,
  *affaireLieeId*, client final, structure tarifaire souhaitée, plus un bloc spécifique par type de
  demande (une trentaine, §4.2.2).
- Blocs ajoutés au fil de l'avancement : *recevabilite* (motif de refus), *faisabilite* (réserves,
  devis), *retourFrn*, *interventions* (planification, replanification, compte rendu), *solde*
  (*dateEffetReelle*, prestations réalisées ou facturées, *frais*, structure tarifaire après
  traitement, *releveUtilise*) et *annulation* (§4.2.4 à §4.2.9).

**À quoi ça sert.**

- Suivre une demande déposée au SGE (MES, CFN, résiliation, changement de puissance ou de FTA) **avant**
  que le C15 ne publie l'événement : cockpit des affaires ouvertes, qui écarte AME par défaut
  (`inclure_ame`).
- Retrouver la situation contractuelle issue d'une affaire : `POST /facturation/rsc`, appariement par
  *Id_Affaire* C15.
- Repérer les échecs et retards : clôtures CPNR (prestation non réalisée) et CNRE (non recevable),
  re-planifications INRP.
- Voir venir les sorties de périmètre via X13 (CFN ou MES d'un autre fournisseur sur un PDL du
  périmètre).
- Une demande M023 ponctuelle **ne se suit pas ici** : aucune affaire de ce type n'apparaît dans le
  corpus EDN (à confirmer). Son suivi passe par le compte rendu M023 (GUI.0505). En revanche, les
  souscriptions récurrentes de données (objet AME : index quotidiens et Pmax, courbe de charge)
  apparaissent bien.

**Pièges.**

- **Snapshots cumulatifs** : chaque livraison reprend tous les jalons d'une affaire qui a bougé. La clé
  est le couple (affaire, `jalon_num`) et **la plus récente publication gagne**, à l'inverse des renvois
  C15 où la première fait foi.
- *affaireDateEffet* change d'un jalon à l'autre. La date d'effet constatée est *dateEffetReelle*, dans
  le *solde*.
- Les **réclamations** (M009) passent aussi par X12/X13, mais sans bloc *demande*. Depuis SGE 7.1.5,
  seules leurs données générales sont transmises (§4.2, note 1). Résultat : `prestation` est vide et les
  états sont ceux des réclamations (CRAP, ENCO, CCIN), comme constaté sur le corpus EDN.
- La v2.5.0 (07/05/2026) ajoute *num_contrat* au nom d'archive et agrège les flux par contrat. Le motif
  d'ingestion `*_X1[23]_*` reste compatible et écarte bien X14/X15. Le numéro de séquence reste
  propre au couple destinataire × flux selon §5.1.1 : à vérifier sur les archives postérieures à mai
  2026.
- X13 est presque vide en C5 (quelques dizaines d'affaires par an).

**Non extrait aujourd'hui** (brut conservé dans `raw_affaires`) :

- *referenceDemandeur* : la référence du fournisseur, utile pour un rapprochement ERP.
- Champs de la demande : *dateEffetSouhaitee*, *contratId*, *affaireLieeId*.
- Motif de refus de recevabilité.
- Planification et compte rendu d'intervention, motifs de replanification et de non-réalisation.
- Tout le *solde* : *dateEffetReelle*, prestations réalisées et facturées, *frais*, structure tarifaire
  après traitement, *releveUtilise*.
- Bloc *annulation*.

---

## X14 / X15 — suivi des réclamations (M009)

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0501 v1.0.4 (18/07/2024) ; schéma Enedis.SGE.JSON.0506_X1415 v1.0.4, tableaux Enedis.SGE.GUI.0506 |
| **Segments** | C2-C4, C5 (§1) |
| **Obtention** | récurrent SFTP, quotidien, **seulement les jours avec un événement** (pas de flux vide) ; plusieurs flux par jour possibles si le volume l'exige (§1) |
| **Prérequis** | contrat GRD-F ; réclamations M009 déposées par le fournisseur (X14) ou saisies par un agent Enedis et concernant le fournisseur (X15) |
| **Format** | JSON (un seul fichier par ZIP), UTF-8 ; *ENEDIS_<X14\|X15>_<GRD-FXXX>_<num_seq>_<horodatage>.zip* et *.json* (§6.1) ; chiffrement : non précisé par le guide (à confirmer) |
| **Dans ElectriCore** | non ingéré : #218 ouverte, reportée ; le motif d'ingestion des affaires écarte volontairement X14/X15 |

**Contenu.**

- **X14** : réclamations à l'initiative du fournisseur. **X15** : réclamations initiées par un agent
  Enedis (§1).
- Une affaire figure dans le flux seulement après un événement déclencheur : création, précision
  demandée, précision apportée, indemnisation en instruction ou clôture (§2).
- Données générales : *statut*, *segment*, *jalons* (*numero*, *dateHeure*, *affaireEtatExterne* :
  ENCO, PRAF, CRAP, CCIN, CHMD, CINR, IDEC, CINA, CIAC, CIRC, CLAA), et le point (*pointId*,
  *domaineTension*, adresse normalisée) (§5.2.1).
- Demande : *dateReception*, puis *prestationPrincipale*, qui porte la *fiche* M009 ainsi que le
  *type*, le *sousType* et le *motif* de la réclamation. S'y ajoutent *typeReclamant*, le client final
  et *objetReclamation*. Ce dernier contient le *detail*, le *prejudiceClient* (nature : BECHP, DMATE,
  FACEC…, incidents, *typeEngagement*) et les noms des pièces jointes (§5.2.2).
- X14 seulement : *suiviDemande* (*saisineMediateur*, *reponseDirecteReclamant*), *precisionDemandee*
  et *precisionRetour* (§5.2.3 à §5.2.5).
- *solde* : *commentaireReponseDistributeur*, *versementIndemnite* et, en cas de transfert à l'Agence
  Responsabilité Civile Générale, la date d'envoi et la référence du dossier. *annulation* : *motif*
  DOUBL, PDNTR ou ERREUR (§5.2.6, §5.2.7).

**À quoi ça sert.**

- Suivre les réclamations transmises à Enedis (index contestés, facturation erronée, dommages) et leur
  délai de traitement.
- Alerter sur l'état PRAF (précision à fournir) : sans réponse, la réclamation est annulée (motif
  PDNTR).
- Savoir si une indemnité a été versée, pour répondre au client ou au médiateur.
- Repérer les réclamations saisies par Enedis (X15) qui touchent un client du périmètre.

**Pièges.**

- Aucun flux n'est émis les jours sans événement, à la différence de X12/X13. Un jour sans fichier
  n'est donc pas un trou. Seule la séquence par contrat × flux permet le contrôle (§6.1.1).
- Format JSON, pas XML : l'étage XML → dict de l'ingestion ne s'applique pas tel quel (déduit).
- Les réclamations apparaissent aussi dans X12/X13, mais réduites à leurs données générales. Le détail
  est ici.
- Les codes d'état (*affaireEtatExterne*) et le numéro de jalon (*numero*) diffèrent de X12/X13
  (*affaireEtat*, *num*).
- Texte libre jusqu'à 2 100 caractères et données personnelles du réclamant : à traiter comme des
  données sensibles (RGPD).
- Le §6.2 cite le schéma v1.0.1, alors que l'historique du guide le porte en v1.0.4.

---

## API Affaires / Mesures / Point — services REST SGE (v0)

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0560 (Affaires), GUI.0561 (Mesures), GUI.0562 (Point), tous en v0.0.0 du 06/05/2026, publiés « à titre d'information ». Les informations techniques exactes sont annoncées pour la v1.0.0 |
| **Segments** | Mesures : C1-C4, P1-P3, et C5 / P4 Linky au niveau d'ouverture aux services 2. Point : C1-C5, P1-P4. Affaires : tous |
| **Obtention** | API REST B2B (*sge-ws.enedis.fr*), appel synchrone, un PRM et une ressource par appel |
| **Prérequis** | contrat GRD-F (fournisseur) ou habilitation (tiers) ; *initiateurLogin* (e-mail), *contratId* ; cadre d'accès déclaré (*cadreAcces*, voir Contenu) ; authentification **non décrite** en v0 (renvoi au guide chapeau B2B GUI.0355, à confirmer) |
| **Format** | JSON (schéma JSON, YAML pour Point) ; codes HTTP accompagnés des codes retour métier B2B (ex. *SGT401*) |
| **Dans ElectriCore** | aucun client API Enedis ; en veille (#220) |

**Contenu.**

- **Affaires** (*/api/v1/affaires*, GUI.0560 §4) : affaires commandées par le demandeur sur un PRM,
  qu'elles soient en cours, terminées ou annulées. Champs : *affaireId*, *prestation*, *etat*, *statut*,
  *motifCloture*, *dateEnvoi*, *dateEffet*, *interventionEnCours*. Un fournisseur sortant n'en voit
  qu'une version filtrée des ICS. Le détail reste dans le service ConsultationAffaire (GUI.0353).
- **Mesures** (*/api/v4/mesures/…*, GUI.0561 §4) : courbes au pas enregistré (PA, PRI, PRC, E),
  énergies globales quotidiennes, index quotidiens avec indice de vraisemblance, Pmax quotidiennes et
  mensuelles (par phase en triphasé). Les mesures facturantes sont exposées en *api/v7* ; elles portent
  *etapeMetier* FACT, la grille distributeur ou fournisseur, le calendrier, la classe temporelle, la
  nature (E, I, C, R) et le statut (I, A, R).
- **Profondeurs** (GUI.0561 §2.2.2), toutes coupées à la dernière mise en service :
  - 36 mois pour les énergies quotidiennes, les Pmax et les index quotidiens ;
  - **7 jours consécutifs au plus par appel**, dans les 24 derniers mois, pour les courbes ;
  - **60 mois** pour les mesures facturantes et leurs index.
- ***cadreAcces*** (Mesures) :
  - courbes, énergies et index : *SERVICE_ACCES* (service d'accès aux données souscrit) ou
    *ACCORD_CLIENT* ;
  - Pmax : *EST_TITULAIRE* en plus ; la Pmax mensuelle est accessible au titulaire sans accord du
    client ;
  - mesures facturantes : *ACCORD_CLIENT*, *CONTRAT_CONCLU* ou *EST_TITULAIRE*.
- **Point** (*/api/v1/points/{pointId}* et 4 sous-ressources, GUI.0562 §5.1) : situations
  contractuelles, situation d'alimentation, situation de comptage, installations clients ; 5 appels
  pour tout récupérer. Le point porte notamment *niveauOuvertureServices*, la FTA, les calendriers, les
  postes et plages horaires (y compris les *futuresProgrammationsHoraires*), les puissances et les
  équipements. Un demandeur non titulaire déclare son droit d'accès par des booléens (autorisation ou
  contrat conclu, en soutirage et en injection).

**À quoi ça sert.**

- Remplacer une demande M023 par un appel unitaire immédiat : historique de 36 mois d'un PDL, mesures
  facturantes sur 60 mois annoncés par la v0 (36 constatés en M023 pour le R67, à confirmer).
- En C2-C4, obtenir la courbe de charge sur 24 mois en un appel par semaine de données.
- Prendre une photo à jour d'un point sans C68 : FTA, niveau d'ouverture, **plages d'heures creuses
  détaillées** (aussi dans le C15 depuis TURPE 7, mais pas encore extraites), changements de programmation à venir.
- Lister les affaires d'un PDL sans attendre le X12 du lendemain.

**Pièges.**

- Statut **v0 informatif** : les chemins se contredisent d'une section à l'autre (*courbes?{pointId}*
  en query, *{pointId}/energie* en path ; titres V3 ou V4 ; mesures facturantes en v4 ou en v7). Les
  paramètres d'appel de l'API Point sont renvoyés à la v1.0.0. Rien n'est implémentable en l'état.
- Aucun des trois guides ne décrit l'authentification. Quotas et volumétrie ne sont pas documentés non
  plus.
- Les appels sont unitaires : un appel par PRM et par ressource, et 7 jours de courbe par appel. Servir
  un périmètre entier suppose beaucoup d'appels.
- Les 60 mois de mesures facturantes sont annoncés ; la M023 R67 n'en rend que 36 constatés. Même mur
  occupant : dernière mise en service (F120, ou changement de SIREN).
- *dateDebut* est incluse et *dateFin* exclue. En C5/P4, un appel avec *mesuresCorrigees*=true est
  rejeté.
- API Affaires : *dateEffet* y est définie comme « date de création », probablement une coquille.

---

## Déchiffrement — chiffrement AES des flux SGE (transverse)

| | |
|---|---|
| **Guide** | Enedis.SGE.NOTE.0525 V2.0.0 (26/03/2025) |
| **Segments** | tous : s'applique à tout flux émis vers un acteur de marché (SFTP, FTP, e-mail) |
| **Format** | AES-256-CBC, padding PKCS5, IV de 128 bits aléatoire par fichier, porté en clair dans les 16 premiers octets (« AES256 IV dyn », §3.1) ; clé de 64 caractères hexadécimaux |
| **Dans ElectriCore** | trousseau `AES__TROUSSEAU__` (ADR-0037) : sélection de la clé par essai dans `decrypt_with_key_chain` ; deux schémas, IV-préfixé pour AES-256 et IV-fixe pour l'AES-128 historique (ADR-0040) ; un flux qui ne déchiffre rien fait échouer le job (`echecs_dechiffrement`) et le bot alerte |

**Contenu.**

- Une clé de déchiffrement **par code contrat**. Elle est envoyée par e-mail au référent SI désigné
  dans l'annexe SI du contrat, qui fournit pour cela son e-mail et son numéro de mobile (§2.1).
- Pour déchiffrer, on lit l'IV dans les 16 premiers octets du fichier, puis on déchiffre le reste
  (§3.1.2).
- Enedis fournit un outil Java (scripts *dechiffrement-client* pour Windows et Linux) et un exemple de
  code (§3.1.3, §3.1.4).

**Pièges.**

- La note ne décrit plus que l'AES-256. Or les archives antérieures à la bascule des **8-9 juin 2026**
  restent en AES-128 avec IV fixe, la clé et l'IV ayant été livrés par Enedis. Garder ces anciennes
  clés dans le trousseau tant qu'un rejeu est possible.
- Un fournisseur qui a plusieurs contrats a plusieurs clés.
- Les ZIP téléchargés depuis le portail SGE sont en clair : ils ne passent pas par cette étape.

---

## MFA — accès au portail SGE (fiche courte)

| | |
|---|---|
| **Guide** | ENEDIS.SGE.GUI.0538 v2.0.0 (19/08/2025) |
| **Dans ElectriCore** | hors périmètre : concerne les personnes qui utilisent le portail (commande M023, dépôt d'affaires), pas l'ingestion SFTP (à confirmer pour les webservices B2B) |

- Depuis le 03/06/2025, l'accès au portail se fait par authentification multifacteur sur
  *https://sge.enedis.fr*. Les clés PKI et l'accès par reverse-proxy deviennent obsolètes pour les
  utilisateur·ices habilité·es au MFA (§1.1).
- La connexion demande l'identifiant et le mot de passe GARDIAN, puis une validation sur un canal
  enrôlé au préalable : l'application *TrustBuilder Authenticator* sur le téléphone, ou l'extension
  *TrustBuilder Backup* dans le navigateur (§1.2, §2.2).
- Chaque utilisateur·ice doit avoir un e-mail fonctionnel rattaché à son pseudo-NNI. L'extension
  navigateur peut nécessiter l'accord du service informatique.

## Pour aller plus loin

Comment les flux sont ingérés : [ingestion des flux](../../ingestion.md) · [conventions de dates](../../conventions-dates-enedis.md).

[Retour aux flux Enedis](index.md).
