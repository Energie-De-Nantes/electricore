# Identité de relevé : clé métier stable et priorité des sources explicite

> **Superseded en partie par [ADR-0029](0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md)** — la
> *localisation* du mint de l'identité C15 (« dérivée de l'événement + marqueur avant/après **en core**
> pour c15 », ci-dessous) est remplacée : le mint devient **uniforme au seam dbt pour toutes les sources**
> (modèle de relevés canonique). Le reste de cet ADR — identité = clé métier déterministe, priorité des
> sources `flux_C15 > flux_R64 > flux_R151`, nature canonique — reste en vigueur.

## Contexte

La traçabilité légale des index — faire figurer sur la facture les relevés réellement
utilisés par le calcul — exige un **handle stable** reliant une *période d'énergie* aux
relevés qui l'ont bornée, qui survive jusqu'à la facture. Odoo, qui **tire** la donnée et
n'écrit pas ([ADR-0012](0012-api-read-only-odoo.md), [ADR-0027](0027-endpoint-lecture-meta-periodes-odoo-tire.md)),
consomme ce handle en lecture.

Deux faits, vérifiés sur les fixtures Enedis, contraignent le choix :

- **Aucun id Enedis natif pour les sources périodiques.** R151 (le workhorse actuel) *et*
  R64 (la **référence cible** — R151 fusionne à terme dans le R64 récurrent) ne portent
  **pas** d'`Id_Releve`. Seuls R15/F15/C15-événements en ont un (`Id_Releve`,
  `Id_Releve_Precedent`, `Num_Sequence`).
- **L'id d'occurrence fichier est instable.** L'id minté au staging dbt (`fichier#position`,
  cf. [ADR-0020](0020-linearisation-en-dbt.md)) est jeté au seam flux (`select * exclude(...)`).
  Surtout, pour R64 les fenêtres se **chevauchent** : un même relevé logique
  `(pdl, type_releve, date_releve)` arrive dans plusieurs fichiers et la linéarisation garde
  la **livraison la plus récente** — la position-fichier gagnante change donc à chaque
  correction.

Par ailleurs, la chronologie des relevés ([#180](https://github.com/Energie-De-Nantes/electricore/issues/180))
dédoublonne sur le triplet `(ref_situation_contractuelle, date_releve, ordre_index)` et choisit
une source gagnante **par tri alphabétique** : `flux_C15 < flux_R151 < flux_R64`. C15 prioritaire
est voulu ; **R151 qui bat R64 est un accident lexical**, jamais décidé. La traçabilité rend cet
accident décisif : il détermine *quelle source* fournit le handle et la valeur enregistrés.

## Décision

Deux décisions indissociables.

1. **L'identité de relevé est une clé métier déterministe**, dérivée de la lecture *logique* :
   `(pdl, date_releve, source, discriminant)` — discriminant = `ordre_index` (avant/après C15)
   ou `type_releve` (R64).
   - **Pas** d'id Enedis natif comme clé (absent du cas dominant R151/R64). L'`Id_Releve` natif
     (R15/F15/C15) est conservé comme **provenance** additionnelle, jamais comme clé.
   - **Pas** l'id d'occurrence fichier comme clé (provenance forensique, instable pour R64).
   - Mintée à la source : au seam dbt pour r151/r15/r64 (1 ligne = 1 relevé) ; dérivée de
     l'événement + marqueur avant/après en core pour c15.
   - Le type d'`ordre_index` (le discriminant) est **unifié** (un seul type, fin de la
     divergence Boolean-au-modèle / Int32-au-code).

2. **La priorité des sources est explicite : `flux_C15 > flux_R64 > flux_R151`** (table de
   priorité, plus de tri alphabétique). C15 reste prioritaire (relevés contractuels aux bornes
   de vie) ; **R64 passe devant R151** car c'est la source de référence et elle porte la donnée
   corrigée (`etape_metier` BRUT/CORR/VALID).

## Raison

L'identité doit exister pour le **cas dominant** (R151/R64 périodiques, qui bornent la
facturation calendaire — [ADR-0026](0026-facturation-calendaire-pas-moisniversaire.md)) et
**survivre aux re-livraisons et corrections** : seule une clé métier le garantit. La provenance
(id natif, id fichier) reste utile en forensique mais ne peut pas porter l'identité. Quand un même
relevé logique existe dans deux sources, le handle *et* la valeur retenus doivent venir de la
source de référence (R64), pas du hasard alphabétique. Le handle est un identifiant de **lecture**
stable, exposé et tiré par Odoo — jamais une clé d'écriture (ADR-0012/0027).

## Conséquences

- Le seam dbt cesse de jeter l'id ; `RelevéIndex` et `ChronologieReleves` portent `releve_id`
  + nature + provenance. Tracé : [#232](https://github.com/Energie-De-Nantes/electricore/issues/232)
  (identité + nature de bout en bout) puis [#233](https://github.com/Energie-De-Nantes/electricore/issues/233)
  (journal des relevés utilisés gardé dans `ContexteMensuel`), sous [#180](https://github.com/Energie-De-Nantes/electricore/issues/180).
- La clé métier est **déterministe et rejouable** : le core reste sans état, le « facturé » figé
  vit côté Odoo (système de référence, ADR-0027) ; une correction Enedis postérieure relève de la
  *régularisation*, pas d'une réécriture.
- Inverser `R64 > R151` plus tard = changer la table de priorité (localisée), mais re-jouerait des
  handles/valeurs différents sur l'historique — à éviter une fois en production.
- Cohérent avec [ADR-0023](0023-periodisations-separees-abonnement-energie.md) : la trace vit sur
  la ligne de temps **énergie** (relevés).

## Alternatives écartées

- **Id Enedis natif comme clé** — impossible pour le cas dominant : R151 et R64 n'en ont pas.
- **Id d'occurrence fichier (`fichier#position`)** — instable pour R64 (fenêtres chevauchantes,
  livraison gagnante mouvante). Bon comme provenance, pas comme identité.
- **Tri alphabétique des sources (statu quo)** — fait gagner R151 sur R64 par accident lexical,
  incompatible avec R64 comme référence cible.
- **Écriture du handle dans Odoo par electricore** — recouple au schéma Odoo et contredit
  ADR-0012/0027 ; la trace est **exposée** sur le feed de lecture et tirée par Odoo.
