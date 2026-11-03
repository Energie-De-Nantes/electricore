---
fraicheur: 2026-11-03
---

# Flux de facturation : F15, F12, TURPE7HC

Les factures d'acheminement d'Enedis au fournisseur, et le flux ponctuel de reprogrammation des heures creuses. Conventions de lecture : [index des flux](index.md#lire-une-fiche).

## F15 — Facture d'acheminement Enedis détaillée (BT ≤ 36 kVA)

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0298 v4.1.3 (10/03/2026), XSD 4.0.4 |
| **Segments** | C5 (soutirage, contrat GRD-F) ; P4 en contrat unique d'injection (contrat GRD-A) |
| **Obtention** | récurrent SFTP, émission quotidienne (week-ends et fériés compris) dès qu'un élément est à facturer ; aucun fichier les jours sans facture (guide § 1, § 2.1) |
| **Prérequis** | contrat GRD-F (ou GRD-A) avec Enedis ; aucune souscription particulière |
| **Format** | ZIP chiffré = une facture groupée : un fichier de données générales *…_FA.xml* + un ou plusieurs fichiers de détail *…_FL_XXXXX_YYYYY.xml* (XML + XSD, UTF-8). Archive : *<EIC Enedis>_F15_<destinataire>_<contrat>_<DIR>_<type_facture>_<fréquence>_<type_client>_<démat>_<num_seq>_<horodatage>.zip* (§ 4.1) |
| **Dans ElectriCore** | ingéré → `flux_f15_detail` (une ligne par élément valorisé) ; prestations exposées par `GET /facturation/prestations` ; loader `f15_detail()` |

**Contenu.**

- Structure à trois niveaux : facture (*Num_Facture*, *Date_Facture*) → bloc de valorisation par événement et par PRM (*Donnees_Valorisation* : *Num_Valorisation*, *Type_Facturation*, *Donnees_PRM*) → éléments valorisés regroupés par nature (*Groupe_Valorise* / *Element_Valorise*) (§ 3.2).
- Quatre natures (*Nature_EV*) : 01 acheminement, 02 prestations et frais, 03 dispositions contractuelles, 04 contributions (§ 3.2.2.3.2).
- Acheminement = composantes TURPE : gestion (*ASG-…*), comptage (*ASCL/ASCP/ASCEL…*), soutirage part fixe (*ASSF…*) et part variable par cadran (*ASSV…*), avec *Formule_Tarifaire_Acheminement* et *Puissance_Souscrite* sur les lignes de soutirage (§ 5.6.1 à 5.6.7).
- Depuis TURPE 7 (1ᵉʳ août 2025) : composante socle compteur ancienne génération (*ASCS-…*) et composante additionnelle de relevé pour comptage non communicant (*ASCA*), justifiées par *Date_Dernier_Releve_Reel* et *Motif_Exemption_Releve_Residuel* au niveau PRM (historique v4.0.2, § 3.2.2.3.1, § 5.6.5).
- Prestations du catalogue (*Id_Affaire*, *Ref_Fournisseur*, *Date_Effet*), frais (déplacement vain, dédit, duplicata…), dispositions contractuelles (pénalités de coupure *DCOUP_PEN*, abattements *DCOUP_AB*, indemnités Linky *DIND…*), contribution *CSPE* ; factures d'intérêts de retard à part (*Detail_Interets_Retard*). Pas de CTA ni d'accise : le F15 ne facture que l'acheminement.
- Pour chaque élément : *Id_EV*, *Libelle_EV*, *Date_Debut*, *Date_Fin*, *Quantite*, *Unite_Quantite* (j, j.kVA, kWh, UNITE…), *Prix_Unitaire*, *Montant_HT*, *Taux_TVA_Applicable* (§ 3.2.2.3.2.1).
- Lien vers le relevé : bloc *Releve* / *Id_Releve*, identifiant du relevé du R15 associé, renseigné seulement pour l'acheminement en soutirage (§ 3.2.2.3.5).

**À quoi ça sert.**

- Contrôler le TURPE recalculé contre le TURPE réellement facturé par Enedis, PDL par PDL (rapprochement non construit, #468).
- Refacturer au client les prestations et indemnités ponctuelles (lignes `unite` = UNITE) : file « à refacturer » côté ERP via `GET /facturation/prestations`.
- Comptabilité : extraits du facturé Enedis par facture et par mois pour la compta fournisseur.
- Rapprocher facture et relevé par *Id_Releve* avec le R15 (`flux_r15` porte `id_releve`) ; certains logiciels tiers mettent en quarantaine un F15 de consommation tant que le R15 du même *Id_Releve* n'est pas arrivé.

**Pièges.**

- Découpage au moisniversaire du PRM (calendrier de facturation cyclique, § 2.2), pas au mois civil : le F15 ne se compare au mois facturé par ElectriCore qu'après reproratisation.
- Arrivée tardive : une facture d'aujourd'hui porte des périodes passées (rectifications, soldes de résiliation, prestations soldées) ; filtrer par *Date_Facture* ne donne pas « la consommation du mois ».
- Corrections : un bloc *ANNUL* annule tout le bloc d'origine (montants négatifs), suivi d'un bloc *RECT* qui refacture ; le lien passe par *Origine_Facture* / *Origine_Valorisation*, avec *Motif_Rectif_Facture* (§ 2.2.4). Additionner sans tenir compte du couple ANNUL/RECT compte en double.
- Suffixes d'*Id_EV* : *-E* à échoir (part fixe facturée d'avance), *-U* échu, *-R* reprise d'une avance, *-A* / *-F* annulation et refacturation des factures manuelles (§ 5.6, déduit des libellés pour *-E/-U/-R*).
- Pas d'identifiant de ligne : *Num_Sequence* n'est pas une clé (le guide le définit comme numéro de la dernière situation contractuelle) ; la dédup se fait par empreinte de contenu (voir le contrat des prestations).
- *Date_Fin* semble inclusive (quantité en jours = fin − début + 1 sur un export d'avril 2026, à confirmer), à l'inverse des périodes [début, fin) d'ElectriCore. Le numéro de séquence est propre au sextuplet du nom d'archive et a redémarré à 00001 à la bascule Ginko v2 (`audit_sequences`).

**Non extrait aujourd'hui** :

- *Id_Releve* (lien R15) et *Facture_Origine* (*Origine_Facture*, *Date_Origine_Facture*, *Origine_Valorisation*), *Motif_Rectif_Facture*, *Origine_Rectif*, *Num_Valorisation*.
- Au niveau PRM : *Date_Dernier_Releve_Reel*, *Motif_Exemption_Releve_Residuel*, *Code_Commune*.
- Au niveau élément : *Puissance_Souscrite*, *Rupture*, *Controle_Puissance*, *Dispositif_Comptage*, *Regime_Compteur*, *Num_Sequence* (jeté volontairement).
- Côté prestations : *Id_Affaire*, *Ref_Fournisseur*, *Date_Effet*, *Date_Demande* ; les bornes de bloc *Date_Debut_Part_Fixe* / *Date_Fin_Part_Variable*.
- Fichier de données générales (*_FA*) : totaux de facture, *Frequence_Facturation*, *Date_Debut_Facture* / *Date_Fin_Facture* ; et le détail des intérêts de retard.

---

## F12 — Facture d'acheminement Enedis C2-C4 (BT > 36 kVA et HTA)

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0124 v1.14.3 (27/02/2026, document daté 31/03/2026), XSD 1.20.3 |
| **Segments** | C2, C3, C4 |
| **Obtention** | récurrent SFTP, une facture agrégée par jour, par fournisseur et par région Enedis dès qu'un élément est à facturer (§ 1, § 3.1) |
| **Prérequis** | contrat GRD-F |
| **Format** | ZIP chiffré : *_FA* (données générales), *_FL_XXXXX_YYYYY* (détail, 100 Mo max par fichier), *_FR* (récapitulatif), *_FX* (données de la réforme de la facturation électronique) ; XML + XSD. Archive : *Enedis_F12_<dest>_<contrat>_<DIR>_<num_seq>_<type_facture>_<délai_paiement>_<horodatage>.zip* (§ 6.1) |
| **Dans ElectriCore** | ingéré → `flux_f12_detail` (une ligne par élément valorisé d'un sous-lot) ; loader `f12_detail()` |

**Contenu.**

- Structure : facture agrégée (*Num_Facture*, *Date_Facture*, *Agence_ERDF*) → sous-lot par événement et par PRM (*Sous_Lot* : *Num_Sous_Lot*, *Date_Sous_Lot*, *Type_Facturation*, *Id_PRM*) → éléments valorisés (§ 5.2).
- Contexte du PRM sur le sous-lot : *Code_Segmentation_ERDF*, *Tarif_Souscrit* (BTSUPCU4, HTACU5…, HTAST5 stockeur au 1ᵉʳ août 2026), *Puissance_Ponderee*, puissances par poste horosaisonnier (*PS_Poste_Horosaisonnier*), *Type_PRM* (dont autoconsommation collective), *Autoproducteur*, *Date_Anniversaire*.
- Chaque élément porte une seule des quatre classes : *Acheminement* (TURPE), *Prestations* (catalogue, avec *Num_Dossier* = affaire SGE), *Option* (options contractuelles périodiques), *Frais_Divers* (hors PRM, rectifications particulières, CSPE) (§ 5.2.2.6).
- Composantes TURPE : gestion, comptage, soutirage part fixe (*FSOU…*) et part énergie par poste (*VEASP*, *VEASHPH*, *VEASHCH*, *VEASHPE*, *VEASHCE*…), dépassements de puissance, énergie réactive, alimentations complémentaires et de secours, regroupement (§ 7.6).
- Autoconsommation collective : soutirage allo-produit (*VEASALP…*) et auto-produit (*VEASAUP…*) séparés par poste (§ 2.1.1, § 7.6.6).
- *Code_Recapitulatif* (CGEST, CCOMP, CSF, CSV, CMDPS, CREAC, PREST, FRAIS, REM, CSPE…) qui alimente le récapitulatif *_FR* ; *Donnee_Sup* donne les justificatifs de calcul (nombre de jours, puissance pondérée, coefficients) (§ 7.4, § 5.2.2.10).
- Pas de CTA ni d'accise : seulement l'acheminement, les prestations et l'élément *CSPE* (historique).

**À quoi ça sert.**

- Contrôler et refacturer l'acheminement des sites C2-C4 ; base du futur TURPE C4 recalculé.
- Volumes d'autoconsommation collective C4, auto-produit et allo-produit séparés.
- Comptabilité : extraits du facturé Enedis.
- Lien avec le relevé R17 par *Num_Sous_Lot* + *Date_Sous_Lot*, données partagées entre les deux flux (§ 3.2.1) ; R17 non ingéré.

**Pièges.**

- « Facture » = agrégat journalier de tous les PRM d'une région : un PRM est un sous-lot, pas une facture. Le *Num_Sous_Lot* identifie le sous-lot ; un élément valorisé n'a pas d'identifiant propre.
- Rectification : sous-lot *ANNUL* puis sous-lot *RECT* (part fixe échue et part variable), reliés par *Orig_Facture*, *Orig_Sous_Lot*, *Date_Orig_Sous_Lot*, avec *Motif_Rectif* ; les rectifications d'avant la migration du point ou sans annulation passent par *Donnees_Rectif* (identifiant PRM technique, non significatif) (§ 3.2.5, § 5.2.2.5).
- Débit ou crédit : porté par *Code_Debit_Credit* (Y / Z), pas forcément par le signe du montant (à confirmer sur flux réel).
- Cadrans C2-C4 : P, HPH, HCH, HPE, HCE, demi-saison, JA, PM. Ils ne se ramènent pas aux quatre cadrans C5.
- Les valeurs d'autoconsommation collective n'apparaissent qu'après la bascule de ces PRM vers les flux C/R/F (§ 2.1.1, date non donnée : à confirmer). Le numéro de séquence est propre au triplet destinataire × contrat × DIR.

**Non extrait aujourd'hui** :

- Classes *Prestations*, *Option*, *Frais_Divers* : `flux_f12_detail` ne lit que *Acheminement*. Pour ces lignes, dates, quantité, prix et `montant_ht` sont vides, ainsi que *Num_Dossier*, *Date_Effet*, *Date_Souscription*.
- *Date_Sous_Lot* (clé de lien R17 avec `num_sous_lot`), *Orig_Facture*, *Orig_Sous_Lot*, *Date_Orig_Facture*, *Date_Orig_Sous_Lot*, *Donnees_Rectif*.
- *Date_Anniversaire*, *PS_Poste_Horosaisonnier* (puissances par poste), *Type_Utilisateur_Reseau*, *Code_Commune*, *Donnee_Sup* (justificatifs de calcul), *Sous_Total*.
- Fichiers *_FA* (totaux, TVA), *_FR* (récapitulatif par composante), *_FX* (réforme de la facturation électronique).

---

## D020 (TURPE7HC) — Reprogrammation des heures creuses TURPE 7

| | |
|---|---|
| **Guide** | Enedis.SGE.GUI.0535 v2.0.1 (21/04/2026) |
| **Segments** | C5 Linky communicant (niveau de service 1 ou 2) ; HTA et BT > 36 kVA annoncés ultérieurement |
| **Obtention** | phase 1 par courriel ; phase 2 par flux D020 (SFTP, selon l'annexe SI du contrat GRD-F) et sur le portail SGE. Fichiers : *PHOTO* (ponctuel, mi-mai 2026), *M-6* (fin du mois M-7), *M-2* (fin du mois M-3), *CR-M* (début du mois M+2). Pas de fichier = aucun PRM concerné ce mois-là |
| **Prérequis** | être fournisseur titulaire de PRM concernés ; paramétrage du flux dans l'annexe SI du contrat GRD-F |
| **Format** | ZIP chiffré contenant un CSV UTF-8 séparé par « ; », 500 000 PRM max par CSV. Nom : *ENEDIS_D020_TURPE7HC-<type>-<MMAA>_<contrat>_<N>_<i>_<horodate>.zip* (ex. *TURPE7HC-M-6-1226*) |
| **Dans ElectriCore** | non ingéré — #209 close wontfix (flux ponctuel, déjà traité par ailleurs) |

**Contenu.**

- Une ligne par PRM concerné par la reprogrammation des heures creuses induite par TURPE 7.
- Plages actuelles et cibles, par saison depuis la v2.0 (heures creuses saisonnalisées) : *CODE_HC_SH_COURANTE* / *LIBELLE_HC_SH_COURANTE*, idem *SB* (saison basse), puis *…_FUTURE* ; le libellé détaille les créneaux (ex. « HC (2H00-7H00;14H00-17H00) »).
- *DATE_BASCULE* : date de bascule prévue ; *STATUT* : « A traiter », « Traite », « Abandon », « En cours » selon le fichier.
- Contexte client : *CODE_GRDF*, *OFFRE_FRN_HC* (le calendrier fournisseur suit-il les HC du TURPE, 0/1), *CATEGORIE_CLIENT* (PRO / RES), *MOIS_ANNIV* (mois du moisniversaire, issu de la dernière F100, F120 ou F130).

**À quoi ça sert.**

- Prévenir les clients d'un changement de plages heures creuses et adapter les offres HP/HC indexées sur les HC du TURPE.
- Anticiper l'effet sur la facturation par cadran : un même calendrier distributeur change de plages à une date connue.
- Contrôler au *CR-M* que la reprogrammation a bien eu lieu (ou a été abandonnée).
- Donner les plages HC futures et la date de bascule de chaque point ; les plages courantes sont aussi dans le C15 depuis sa v5.1.1 (non extraites, #756).

**Pièges.**

- *DATE_BASCULE* est figée : elle n'est pas mise à jour avec la date effective de reprogrammation. La date réelle se lit ailleurs, par exemple dans les événements C15 renvoyés avec les nouvelles plages saisonnalisées en octobre 2026 (déduit, à confirmer).
- Un PRM déjà traité par une mise en service (F120) disparaît des fichiers *M-6* et *M-2* mais reste dans le *CR-M* : le régime cible peut s'appliquer avant la date prévue.
- Échec de la reprogrammation à distance : passé J+30 jours calendaires, le PRM est mis de côté, avec un traitement éventuel à partir du second semestre 2027.
- Le champ *STATUT* est vide dans la *PHOTO*. Le nom de fichier porte le mois de reprogrammation (*MMAA*), pas le mois de publication. Les PRM du contrat PROTOC-501 sont rattachés au contrat GRD-C501.

## Pour aller plus loin

Comment les flux sont ingérés : [ingestion des flux](../../ingestion.md) · [conventions de dates](../../conventions-dates-enedis.md).

[Retour aux flux Enedis](index.md).
