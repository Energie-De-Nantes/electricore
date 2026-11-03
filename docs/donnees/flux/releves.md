---
fraicheur: 2026-11-03
---

# Flux de relevés : R15, R151, R17, R171, R10, P15

Les relevés reçus en continu, par segment. Conventions de lecture : [index des flux](index.md#lire-une-fiche).

## R15 — Index et consommations C5 à chaque relevé

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0292 v3.0.4 (08/12/2021, document daté du 25/01/2022), XSD Enedis.SGE.XSD.0293 v2.3.2 |
| **Segments** | C5 (contrat GRD-F) ; le même format sert au contrat unique en injection (GRD-A), hors périmètre ici |
| **Obtention** | récurrent SFTP, **quotidien** (week-end et jours fériés compris) : une archive par contrat GRD-F et par direction interrégionale (DIR), qui regroupe les PRM ayant eu un événement déclencheur dans la journée ; aucune archive si aucun PRM de la DIR n'a été relevé. Pour un PRM donné : **au moins une publication tous les deux mois** (guide § 1). Le délai exact entre relevé et publication n'est pas chiffré par le guide |
| **Prérequis** | être fournisseur titulaire du PRM (contrat GRD-F). Aucune souscription ni consentement : le flux est émis d'office pour les PRM relevés (déduit du § 1, à confirmer) |
| **Format** | XML + XSD, UTF-8, archive zip chiffrée (comme tous les flux SFTP SGE). Archive *<EIC Enedis>_R15_<destinataire>_<contrat>_<DIR>_<séquence>_<horodatage>.zip*, fichiers *…_XXXXX_YYYYY.xml* ; séquence propre au couple contrat × DIR |
| **Dans ElectriCore** | ingéré → `flux_r15` (index distributeur, *Classe_Mesure* 1) et `flux_r15_acc` (énergies d'autoconsommation collective) ; extractions `r15` et `r15_acc` via `/flux/{table_name}`, loaders `r15()` et `r15_acc()`. **Exclu du mart** `releves` (erreurs TURPE constatées sur des clients professionnels, voir les conventions de dates) |

**Contenu.**

- Un bloc *Donnees_Releve* par relevé et par PRM, parfois plusieurs pour un même PRM dans un fichier : annulation puis rectificatif, ou bien index de fiabilisation suivis d'index estimés (§ 4.2.3).
- **Motif** du relevé (*Motif_Releve*, annexe 6.2) : *CYCL* (relevé cyclique), *MES*, *CFNE*, *CFNS*, *RES*, *MCT* (changement de FTA ou de puissance), *MCF*, *FIAB* (index réel qui fiabilise une estimation), *RECT*, *CMAT* (changement de compteur), *AUTRE*.
- **Nature des index** (*Nature_Index*) : *REEL*, *ESTIME*, *AUTO-RELEVE* ; **nature de la consommation** (*Nature_Consommation*) : *REEL*, *ESTIME*, *REGULARISE* (index nouveaux réels, précédents estimés : la consommation peut alors être négative).
- **Statut** (*Statut_Releve*) : *INITIAL*, *ANNULE*, *RECTIFICATIF*. Le motif de correction (*Motif_Rectif* : dysfonctionnement, fraude, correction d'index…) n'est porté que par la ligne *ANNULE*.
- Valeurs par cadran dans deux grilles : *Classe_Temporelle_Distributeur* (BASE, HP/HC, HPH/HCH/HPB/HCB selon *Id_Calendrier_Distributeur* DI000001/2/3) et *Classe_Temporelle* (grille fournisseur, codes libres). Chaque valeur est typée par *Classe_Mesure* : 1 index, 2 consommation, 3 énergie autoproduite, 4 énergie alloproduite (5 et 6 existent dans la XSD mais ne sont pas utilisées, annexe 6.9). Unité : kWh.
- Le **relevé précédent** (*Id_Releve_Precedent*, *Date_Releve_Precedent*, *Valeur_Precedent*) est donné sauf en début de situation : MES, CFNE, pose de compteur, changement de calendrier (annexe 6.3).
- Contexte du point : *Type_Compteur* (CCB Linky, CEB, CFB, PSC sans comptage), *Niveau_Ouverture_Services* (0/1/2), *Type_Client* (0 professionnel, 1 résidentiel), *Autoconsommation_Collective*, *Ref_Situation_Contractuelle*, *Id_Affaire* et *Ref_Demandeur* (pour les affaires du fournisseur), *Date_Theorique_Prochaine_Releve*.
- *Id_Releve* fait le lien avec les lignes du F15 qui facturent ce relevé (§ 3.1).

**À quoi ça sert.**

- C'est le relevé sur lequel Enedis facture le TURPE (F15). Il sert à rapprocher le F15 relevé par relevé, et à contrôler ce que le distributeur a retenu (index réel ou estimé, régularisation).
- C'est la **seule source d'index récurrente pour les compteurs non communicants** (CEB, CFB, Linky de niveau 0), qui n'ont ni R151 ni R64. Voir le premier piège.
- Volumes d'autoconsommation collective C5 : énergies autoproduite et alloproduite par cadran et par période de relevé, dans `flux_r15_acc`.
- Suivi des événements : relevés de MES, CFNE, RES, CMAT, rectifications et leurs motifs.

**Pièges.**

- **Moisniversaire, pas mois civil.** Les relevés cycliques ont lieu entre le 1er et le 28 du mois (§ 3.1), à la date de relevé propre au PRM, et ce sont eux que le F15 facture. Les énergies du R15 couvrent donc la période entre deux relevés, pas le mois civil : elles sont inexploitables telles quelles pour une facturation calendaire. C'est la raison d'être du R151 et du R64 (index journaliers).
- **Compteurs non communicants.** Le bloc distributeur est absent quand *Niveau_Ouverture_Services* vaut 0 (§ 4.2.4). Or `flux_r15` ne lit que ce bloc : pour ces PRM, les index par cadran sortent vides, alors qu'ils figurent dans la grille fournisseur (déduit du modèle, à vérifier sur données).
- **Annulations non distinguées.** `flux_r15` ne porte ni *Statut_Releve* ni *Motif_Rectif*. Une ligne *ANNULE* et son *RECTIFICATIF* y cohabitent avec la même date, donc le même `releve_id` (déduit du modèle, à confirmer). Pour les distinguer, passer par `flux_r15_acc`, qui porte `statut_releve`.
- **Calendrier mal étiqueté.** Dans `flux_r15`, la colonne `id_calendrier_distributeur` est alimentée par *Id_Calendrier*, qui désigne le calendrier **fournisseur** depuis Ginko v2. *Id_Calendrier_Distributeur* n'y est pas lu (à vérifier sur données ; `flux_r15_acc` lit bien les deux).
- `nature_index` : *AUTO-RELEVE* est ramené à « estimé » par prudence, alors que le guide le traite comme un index réel pour *Nature_Consommation*.
- **Autoconsommation collective.** *Autoconsommation_Collective* = 0 signifie « participant **consommateur** », pas « hors opération » : c'est l'absence de la balise qui signifie la non-participation. Les classes 3 et 4 sont des **énergies** par période de relevé, pas des index, malgré le préfixe `ea_autoproduite_` / `ea_alloproduite_` suivi de « index » dans les noms de colonnes. Elles sont calculées à partir des courbes de charge de l'opération.

**Non extrait aujourd'hui.**

- *Classe_Mesure* 2 (consommation par cadran), absente des deux modèles : c'est pourtant le total à comparer à autoproduit + alloproduit.
- Le relevé précédent : *Id_Releve_Precedent*, *Date_Releve_Precedent*, *Motif_Releve_Precedent*, *Nature_Index_Precedent*, *Valeur_Precedent*.
- *Nature_Consommation* ; *Statut_Releve* et *Motif_Rectif* dans `flux_r15`.
- La grille fournisseur *Classe_Temporelle* (index des compteurs non communicants) et *Id_Structure_Horosaisonniere*.
- *Type_Client* (professionnel ou résidentiel, piste possible pour les catégories d'accise, à confirmer), *Niveau_Ouverture_Services*, *Num_Sequence*, *Origine_Evenement*, *Date_Theorique_Prochaine_Releve*, *Ref_Regroupement_Demandeur*.
- Le détail compteur : *Num_Serie*, *Indicateur_Passage_A_Zero*, *Coefficient_Lecture*, *Rang_Cadran*.

## R151 — Index périodiques (et Pmax) des C5 communicants, sur abonnement

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0317 v2.3.1 (28/02/2023), XSD Enedis.SGE.XSD.0315 v1.2.0 |
| **Segments** | C5 équipés d'un Linky communicant |
| **Obtention** | récurrent SFTP, **sur abonnement**, à fréquence **quotidienne** ou **mensuelle** (§ 1). En quotidien, la journée J est publiée dans la nuit de J+1 à J+2 (vers 3 h à J+2). Une donnée absente lors de cette publication est rattrapée par un R151 ultérieur, jusqu'à 20 jours après J et tant que la souscription est active. En mensuel, la publication intervient au plus tard le 3e jour ouvré après le dernier jour de collecte, à un jour du mois choisi entre 1 et 28 |
| **Prérequis** | fournisseur titulaire et « demande de prestations sur le portail SGE » (§ 1). Le guide ne nomme pas le code : F305 selon les sources ElectriCore, à confirmer au catalogue. Consentement du client exigé pour des index quotidiens (règle Enedis, absente du guide) |
| **Format** | XML + XSD, UTF-8, archive chiffrée. **Un XML par zip** ; archive *…_R151_<destinataire>_<contrat>_<id_abonnement>_<séquence>_<Q\|M>_<XXXXX>_<YYYYY>_<horodatage>.zip* (le préfixe *ERDF_* du guide a pu évoluer). Le compteur XXXXX/YYYYY porte **entre zips** d'une même séquence et d'un même abonnement. Index en Wh, puissance en VA |
| **Dans ElectriCore** | ingéré → `flux_r151` → mart `releves` (puis `chronologie_releves`) ; extraction `r151`, loader `r151()` |

**Contenu.**

- Un bloc *Donnees_Releve* par journée publiée et par PRM. *Date_Releve* désigne la **journée mesurée** (de 0 h à 23 h 59), pas la date de transmission (§ 3.2.2.1, note 1).
- Index **grille distributeur** (*Classe_Temporelle_Distributeur*, 0 à 4 cadrans) et **grille fournisseur** (*Classe_Temporelle*, 0 à 10 cadrans), avec *Id_Calendrier_Distributeur* et *Id_Calendrier_Fournisseur*.
- *Indice_Vraisemblance* par index : 0 valeur correcte, 1 valeur sujette à caution.
- *Puissance_Maximale* : puissance apparente maximale de la journée, en VA, quand elle est souscrite et disponible.
- *Id_Affaire*, *Numero_Abonnement* (en-tête).
- Données **brutes** : aucune estimation, aucun remplacement d'une valeur manquante (§ 1).

**À quoi ça sert.**

- Borner la facturation calendaire : index au 1er du mois et aux dates exactes d'entrée et de sortie, sans attendre le relevé cyclique du moisniversaire.
- Reconstituer des consommations mensuelles ou journalières par cadran, et contrôler les estimations du R15 et du F15.
- La Pmax quotidienne permet de vérifier la puissance souscrite (sous réserve de la souscription).

**Pièges.**

- **Convention « fin de journée ».** L'index daté J est celui de la fin de J. `flux_r151` sert directement l'instant J+1 à 0 h (heure de Paris), conversion posée à l'ingestion (ADR-0042), tandis que `releve_id` reste calculé sur la date brute.
- **Livraisons tardives.** Une même journée peut arriver plusieurs fois, ou jusqu'à 20 jours plus tard dans un autre fichier. La déduplication par `releve_id` dans `releves` garde la livraison la plus récente.
- **Journée sans mesure.** Le bloc *Donnees_Releve* est publié vide, avec des calendriers à *INCONNU* et sans cadran. Ces lignes existent dans `flux_r151`, mais le loader `r151()` les écarte (filtre sur DI000001 à DI000003).
- `nature_index` vaut toujours « réel » : le flux n'a pas de *Nature_Index*, et une valeur marquée *Indice_Vraisemblance* = 1 n'est pas signalée.
- **Pas d'historique.** Seuls les jours postérieurs à la souscription sont publiés. Pour le passé, voir R64 (M023).
- **Unité.** Wh, ramenés en kWh entiers par troncature (ADR-0034).

**Non extrait aujourd'hui.**

- *Puissance_Maximale* (Pmax quotidienne, VA) — #761.
- *Indice_Vraisemblance*.
- Les index de la **grille fournisseur** (*Classe_Temporelle*) : seul l'identifiant du calendrier fournisseur est lu.
- *Rang_Cadran* et les libellés.
- *Numero_Abonnement* et *Identifiant_Contrat* (en-tête).

## R17 — Index et consommations C2-C4 à chaque relevé facturé

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0131 v1.11.4 (19/12/2025), XSD Enedis.SGE.XSD.0132 v1.9.1 ; annexe Enedis.SGE.GUI.0256 « Détail des publications par tarif-compteur » v1.3.0 (xls, présent dans le dossier) |
| **Segments** | C2, C3, C4 |
| **Obtention** | récurrent SFTP, **quotidien** par contrat GRD-F, regroupant les PRM ayant fait l'objet d'une facturation (cyclique ou liée à une intervention) dans la journée. Pour un PRM donné : **au moins une publication par mois**. Après une interruption de service, une archive est émise par journée manquée, donc plusieurs le même jour |
| **Prérequis** | être fournisseur titulaire (contrat GRD-F) ; émis d'office (déduit du § 1, à confirmer) |
| **Format** | XML + XSD, archive chiffrée *ENEDIS_R17_<destinataire>_<contrat>_<séquence>_<horodatage>.zip*, fichiers *…_XXXXX_YYYYY.xml* d'environ 100 Mo au plus ; séquence propre au contrat |
| **Dans ElectriCore** | **non ingéré** (issue #210 ouverte). Les zips sont vus par le relais (`relais_livraisons`, statut « vu ») et la macro `audit_sequences` connaît déjà leur nomenclature |

**Contenu.**

- Un bloc *Corps_PRM* par événement facturé (sous-lot), avec *Segment*, *Type_PRM* (hébergeur, décomptant, regroupement, variantes d'autoconsommation collective…), *Num_Sous_Lot* et *Date_Sous_Lot* : la clé de rapprochement avec le F12.
- *Donnees_Releve* : *Tarif_Souscrit* (FTA), *Code_Structure_Fournisseur*, *Type_Programmation_Compteur* (4, 5, 6 ou 8 classes), période *Date_Debut_Mesure* → *Date_Fin_Mesure*.
- Typage : *Statut_Mesure* (*INITIAL*, *ANNULE*, *RECTIFICATIF*) ; *Nature_Mesure* (*REEL*, *ESTIME*, *REGULARISE*, consommation négative possible) ; *Nature_Index_Precedents* et *Nature_Index_Nouveaux* (*REEL* ou *ESTIME*) ; *Motif_Rectif*.
- Motifs : *FACTURATION*, *MAINTENANCE* ou code de prestation du catalogue (*F130* changement de fournisseur, *F160_…* changement de FTA, *F170_…* changement de puissance…), concaténés par « + » quand plusieurs prestations sont synchronisées (annexe 7.1).
- *Donnees_Par_Type_Mesure* (grille distributeur) : index précédents et nouveaux selon la programmation du compteur, et consommations (*Quantite_Mesure*) **selon les classes TURPE** (Pointe, HPH, HCH, HPE, HCE…), celles que facture le F12.
- Types de mesure (*Type_Mesure*) : EA (énergie active), ER et ERN (énergie réactive, ERN ajoutée pour le TURPE 7), DD (durée de dépassement), TF (temps de fonctionnement), DQ (dépassement quadratique), PA (puissance atteinte), DP (dépassement de puissance), **EAAUTO et EAALLO** (énergies autoproduite et alloproduite).
- *Donnees_Par_Type_Mesure_Fournisseur* : index et consommations du calendrier fournisseur (EA seulement en Nouvelle Offre), avec *Correspondance_Index*. Les forfaits sont portés par *Valeur_Forfait*.

**À quoi ça sert.**

- Facturer et contrôler un C4 : consommations par classe TURPE et dépassements de puissance, rapprochables du F12 par le sous-lot.
- **Volumes d'autoconsommation collective d'un C4** : EAAUTO et EAALLO par classe temporelle (annexe 7.5). Aujourd'hui, ElectriCore ne les a qu'au travers de `flux_f12_detail`.
- Énergie réactive et dépassements, pour du conseil (optimisation de puissance, compensation).

**Pièges.**

- **Deux grilles.** Les classes des index (programmation du compteur) peuvent différer de celles des consommations (TURPE), et la grille fournisseur en ajoute une troisième. Le détail par couple compteur × tarif figure dans GUI.0256.
- **Libellés de saison.** Le R17 parle de HPE/HCE (« été »), là où ElectriCore attend hpb/hcb : la correspondance est à confirmer avant toute ingestion.
- **Autoconsommation collective.** EAAUTO et EAALLO sont calculés à partir des courbes de charge et peuvent s'écarter de la différence d'index (§ 7.5). Les index fournisseur des PRM en autoconsommation collective sont annoncés « à partir de début 2025 » ; jusque-là, seule la grille distributeur était transmise (§ 3.1).
- **Rectifications.** Elles réémettent l'original en *ANNULE*, en conservant ses motifs d'origine.
- **Période.** Les bornes suivent la facturation (moisniversaire), pas le mois civil. Leur convention « début » ou « fin de journée » n'est pas documentée (à confirmer).

## R171 — Index quotidiens C1-C4 et P1-P3, sur abonnement

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0479 v1.3.0 (14/03/2023) ; le guide cite la XSD Enedis.SGE.XSD.0478 v1.2.0, livrée dans le dossier sous le nom *ENEDIS.SGE.XSD.0315.Flux_R171_v1.2.0.xsd* |
| **Segments** | C1-C4 (soutirage) et P1-P3 (injection), compteur communicant |
| **Obtention** | récurrent SFTP, **quotidien** : index de la journée J relevés, **datés** et envoyés à J+1. Publication « multi-guichet » : plusieurs fichiers possibles dans la journée, à partir de 2 h, selon l'heure de collecte ; au plus 1 000 PRM par zip |
| **Prérequis** | prestations **F305A** (soutirage) ou **P305A** (injection), une demande par sens ; **autorisation du client** recueillie au préalable pour toute la durée du service (§ 1) |
| **Format** | XML + XSD, UTF-8, un XML par zip *ENEDIS_R171_<C\|P>_<id_demande_publication>_<GRDF\|GRDT>_<id_destinataire>_<horodatage>.zip* ; horodatage en heure locale |
| **Dans ElectriCore** | **non ingéré** : écarté (issue #211 close, « sera remplacé par R64 ») |

**Contenu.**

- Une série (*serieMesuresDatees*) par PRM × *grandeurMetier* (CONS ou PROD) × *grandeurPhysique* × *typeCalendrier* (D distributeur, F fournisseur) × classe temporelle (*codeClasseTemporelle*).
- Grandeurs physiques : EA (énergie active), PMA (puissance maximale atteinte), ER, ERC, ERI (énergie réactive), TF, DD, DE, DQ ; unités Wh, W, VA, VARh, s.
- Mesures datées (*mesureDatee*) : *dateFin* (fin de la période de relevé) et *valeur*. *type* vaut toujours IDX (index).
- Données brutes, sans estimation des trous.

**À quoi ça sert.**

- Index journaliers d'un C2-C4, pour une facturation calendaire ou un suivi de consommation hors moisniversaire, et Pmax quotidienne.
- Sert les mêmes demandes que R151 pour le C5, du côté des segments supérieurs.

**Pièges.**

- **Dates.** Datation à J+1 de l'index de fin de J, donc une convention « début de journée » comme R64 (déduit, à confirmer).
- **Fichiers.** Un PRM qui soutire et injecte donne deux fichiers distincts ; avec le multi-guichet, une même *id_demande_publication* peut couvrir plusieurs fichiers.
- **Pas d'historique** au-delà de la date de souscription.

## R10 / R10A — Courbe de charge mensuelle C2-C4

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0120 v1.3.3 (12/05/2022), XSD Enedis.SGE.XSD.0133 v1.3.1 |
| **Segments** | C2, et C4 (BT > 36 kVA) traités en courbe de charge dans la Recoflux |
| **Obtention** | récurrent SFTP, **mensuel** : la courbe du mois M-1 |
| **Prérequis** | depuis janvier 2021, envoi **sur demande** dans l'Annexe 9 du contrat GRD-F, avec un seuil de puissance au choix ; il n'est plus systématique |
| **Format** | XML + XSD, archives *GRD_<EIC>_<R10\|R10A>_CDC_<séquence>_<nb fichiers>_<aammjj>_<hhmm>.zip* (au plus 100 Mo) |
| **Dans ElectriCore** | **non ingéré** (aucune issue connue) |

**Contenu.**

- R10 : courbe de soutirage en énergie active, pertes incluses, par PRM ; R10A : consommation ajustée (adhésions avec décomptant, sites en vente totale).
- Points en kW (puissance moyenne) au pas de 10 min, puis de 5 min après une date de bascule non fixée dans le guide.
- Statut par point (*Statut_Point* : R relevé, C corrigé, S coupure secteur, E estimé), statut de courbe validée ou estimée (*Statut_Courbe*), flux d'origine ou rectificatif (*Evenement_Declencheur_Flux* : O ou R).

**À quoi ça sert.**

- Source **récurrente** de courbe de charge C2-C4, alternative à la commande ponctuelle R63 (M023) : approvisionnement, conseil de puissance, contrôle des dépassements.

**Pièges.**

- Horodatages *AAAA-MM-JJThh:mm:ssZ*, en UTC quand le « Z » est présent ; ce « Z » est facultatif.
- Plus systématique depuis 2021 : à demander dans l'Annexe 9, avec un seuil de puissance.

## P15 — Compte rendu d'activation de période mobile (C5)

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0416 v1.2.0 (06/04/2023), XSD Enedis.SGE.XSD.0415 v1.0.0 |
| **Segments** | C5 (équivalent C2-C4 : P12, autre guide) |
| **Obtention** | SFTP, **événementiel** : au plus tard 5 jours calendaires après une demande recevable d'activation de période mobile |
| **Prérequis** | avoir fait la demande sur SGE (B2B ou portail) pour un groupe de PRM, ce qui suppose une offre à calendrier fournisseur avec pointe mobile |
| **Format** | XML + XSD, *ENEDIS_P15_<EIC>_<séquence>_<horodatage>.zip* (préfixe *ERDF_* avant septembre 2023) |
| **Dans ElectriCore** | **non ingéré** |

**Contenu.**

- Récapitulatif de l'ordre : *affaireId*, *refFrn*, *groupePeriodeMobileCode*, *rang*, *dateHeureDebut*, *dateHeureFin*.
- Pour chaque PRM du groupe : résultat OK ou KO et date de diffusion effective.

**À quoi ça sert.**

- Vérifier qu'une période mobile (pointe) a bien été activée sur chaque point concerné, pour une offre à pointe mobile.

**Pièges.**

- Ce n'est pas un flux de relevé : aucun index.
- Préfixe de fichier *ERDF_* avant septembre 2023.

## Pour aller plus loin

Comment les flux sont ingérés : [ingestion des flux](../../ingestion.md) · [conventions de dates](../../conventions-dates-enedis.md).

[Retour aux flux Enedis](index.md).
