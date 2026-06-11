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
Socle transverse, stdlib-only : chargement `.env` (`charger_env`), résolution du chemin de la base DuckDB (`chemin_base_duckdb`, issue #146), config Odoo, CSV de taux régulés. Importable par **tous** les modules (`core/` compris — compatible ADR-0016 : aucune dépendance ERP ni lib externe). C'est le seul module hors `core/models/` que `core/loaders/` peut importer.

---

## Concepts pipeline

**Historique** :
Séquence temporelle ordonnée des événements contractuels d'un PDL (entrées, sorties, modifications), enrichie pour la facturation : annotation des impacts métier (abonnement, énergie), résumé des modifications, et frontières mensuelles de facturation. Source : flux C15. C'est le concept canonique — la version brute (sortie directe de C15) n'a pas de nom métier autonome.
_Éviter_ : journal, log contractuel, périmètre (le périmètre est l'état à une date, pas la séquence).

**Périmètre** :
Ensemble des PDLs actifs (et de leur configuration contractuelle) à une date donnée. Snapshot dérivé de l'Historique par filtrage temporel. Répond à la question « qui était dans notre portefeuille à la date X ? ».
_Éviter_ : portefeuille (terme commercial), parc.

**Abonnement** :
Période continue entre deux événements contractuels où la configuration (FTA, puissance, calendrier) ne change pas. Unité de calcul du TURPE fixe.

**Rupture de période** :
Événement contractuel (MES, MCT, RES) qui découpe une période d'abonnement en deux.

**Période d'énergie** :
Intervalle entre deux relevés d'index, support du calcul de consommation et du TURPE variable.

**Méta-période mensuelle** :
Agrégation d'abonnements + périodes d'énergie sur un mois calendaire, unité de la facturation client mensuelle. Grain : une ligne par **(situation contractuelle, mois)** — pas par PDL : un PDL qui change de RSC en cours de mois porte deux méta-périodes ce mois-là (le TURPE fixe est facturé par situation).

**`mois_annee`** (champ) :
Mois calendaire d'une période (abonnement, énergie, méta-période, taxes), clé calculable et triable au format `"YYYY-MM"`, garanti par `str_matches` dans les schémas Pandera (issue #115). Côté Accise, dérivé de la date de facture (M − 1 : la facture du mois M porte la consommation de M−1 ; anciennement `mois_consommation`). Les libellés français restent portés par les champs d'affichage dédiés `debut_lisible` / `fin_lisible`.
_Éviter_ : mois_consommation (fusionné), libellé `"mars 2025"` comme clé (trie `août < avril`).

**Contexte mensuel de facturation** :
Bundle immutable des éléments dérivés une seule fois pour produire la facturation d'un mois donné : `historique_enrichi`, `abonnements`, `energie`, `facturation_mensuelle` (méta-périodes du mois). Construit par `charger()` dans `core/builds/contexte_mensuel.py` (cf. [ADR-0019](../../docs/adr/0019-roles-loaders-pipelines-builds-integrations.md)) ; partagé par les builds qui touchent au même mois (rapprochement, documents de campagne, taxes mensuelles) pour ne déclencher le pipeline `facturation()` qu'une seule fois.
_Éviter_ : résultat de facturation (trop vague), contexte tout court (sans qualificatif).

**Rapprochement facturation mensuelle** :
Jointure des *lignes de facture* (côté ERP) avec la méta-période mensuelle Enedis du même mois, enrichie des identifiants compteur (`num_compteur`, `type_compteur`) et des flags `a_facturer` / `a_supprimer` (cf. [ADR-0014](../../docs/adr/0014-lignes-factures-du-mois-avec-flags.md)). Implémenté par `rapprocher()` dans `core/builds/contexte_mensuel.py`. Produit une `LignesFactureRapprochees` en **vraie passe-plat** (issue #142) : sortie = colonnes d'entrée (`est_brouillon` compris, auditable à côté des flags qu'il dérive) + colonnes calculées, sans nommer aucune colonne ERP en core. Ordre déterministe — contrat, calculées, puis passe-plat en ordre d'entrée — mais **sans promesse pour les livrables** (l'ordre facturiste vit dans `feuilles_rapport_*`). Une colonne d'entrée homonyme d'une calculée échoue au seam (`ValueError` nommant la collision).
_Éviter_ : matching, reconciliation (anglicisme).

**Ligne de facture** :
Unité de facturation côté ERP : un produit, une période, une quantité, un PDL. Schéma agnostique `LignesFacture` (`core/models/`) — **contrat minimal** de ce sur quoi `rapprocher()` branche : `ref_situation_contractuelle`, `categorie_produit ∈ {Base, HP, HC, Abonnements}`, `quantite`, `est_brouillon`. Toute autre colonne (`pdl`, `est_lisse`, identifiants ERP `invoice_line_ids` / `name_account_move` / `name_product_product`, etc.) est acceptée en passe-plat (`strict=False`) et préservée dans la sortie. Chaque adaptateur ERP renomme ses clés métier vers ces 4 noms ; les flags ADR-0014 (`a_facturer` / `a_supprimer`) sont dérivés en core depuis `est_brouillon` + `quantite`.
_Éviter_ : ligne Odoo (le concept est agnostique), ligne facture client (`account.move` est la facture entière).

**Harmonisation des relevés** :
Alignement des sources de relevés (R151, R15, R64) sur la convention de date « début de journée ». Implémenté dans `releves_harmonises()` — voir [ADR-0003](../../docs/adr/0003-r151-date-harmonisation.md).

**`date_ajustee`** (champ) :
Booléen marquant les relevés dont la date a été décalée pendant l'harmonisation (R151 +1 jour).

**Contrat lissé** :
Modalité de facturation où le client paie un montant mensuel constant basé sur une estimation annuelle, plutôt qu'au prorata de la consommation réelle du mois. Régularisation annuelle. S'oppose au contrat *réel* (facturation au prorata).

**Contrat de pipeline** :
Tout `pipeline_*` (et la fonction de premier niveau qu'il compose, ex : `generer_periodes_abonnement`, `calculer_periodes_energie`) est décoré `@pa.check_types(lazy=True)` avec entrée *et* sortie typées par un schéma Pandera. Les violations de contrat apparaissent au *seam* (avec un message nommant la colonne fautive) plutôt qu'au fond d'une stack-trace. Discipline héritée de l'ère pandas (commit `b066b98`, août 2025) et restaurée pour Polars après le creux laissé par la migration `052c99f` ; la version *eff3d19* n'avait restauré que les agrégateurs de `pipeline_facturation`. Conséquence directe : aucun *self-repair silencieux* dans un pipeline — si l'entrée n'est pas conforme, on échoue au seam, pas en se rattrapant.

---

## Exports et livrables

**Rapport** :
Structure de données (`@dataclass(frozen=True, slots=True)`, cf. [ADR-0018](../../docs/adr/0018-classes-justifiees-par-l-etat.md)) regroupant les DataFrames qu'un *build* (cf. [ADR-0019](../../docs/adr/0019-roles-loaders-pipelines-builds-integrations.md)) produit pour un domaine (Accise, CTA, facturation). Exemple : `RapportAccise(resume, par_taux, detail)`. Pré-trié et prêt à consommer — l'ordre des lignes est porté par la fonction `rapport_*` elle-même, pas par les consommateurs.

**Livrable** :
Export structuré destiné à un consommateur métier (facturiste, audit). Un livrable matérialise un *Rapport* sous une forme attendue par son destinataire — typiquement un XLSX multi-onglets (`Résumé` / `Par taux` / `Détail`). Distinct du *détail brut* (table unique exportée en XLSX mono-onglet ou Arrow IPC, format technique).
_Éviter_ : export (trop générique), fichier.

**Feuilles** :
Onglets d'un *Livrable* XLSX, matérialisés par un `dict[str, DataFrame]` consommable par `xlsx_multi_sheet`. Les clés du dict sont les libellés d'onglets affichés à l'utilisateur (FR : `Résumé`, `Par taux`, `Détail`, `F15 complet`, `Changements puissance`…). Deux formes de production coexistent :

- **À partir d'un `Rapport*`** : `feuilles_rapport_*(r) -> dict[str, DataFrame]` co-localisée avec le `rapport_*` correspondant (cas Accise, CTA, facturation rapport).
- **Directement par un build** : `documents_facturation_du_mois(odoo, mois) -> tuple[dict[str, DataFrame], str]` (cas `/facturation/documents.xlsx` — pas de dataclass intermédiaire, la forme du livrable est directement assemblée par le build).

_Éviter_ : onglets (anglicisme), sheets.
