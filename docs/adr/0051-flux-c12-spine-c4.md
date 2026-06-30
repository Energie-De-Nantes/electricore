# ADR-0051 — Ingestion flux C12 : spine contractuelle C4

## Statut

Accepté (#344).

## Contexte

Le flux **C12** (SGE GUI 0129, XSD 0130 v1.12.1) porte la *description contractuelle des PRM
du segment C2-C4* (>36 kVA) : options tarifaires TURPE, puissances souscrites par classe
temporelle, utilisateur réseau (personne morale, code APE), informations d'alimentation et de
comptage. Prérequis tracé en #344 comme condition du calcul d'accise (#226) : le **Code_APE**
et le domaine de tension sont nécessaires pour déterminer la catégorie d'accise d'un PRM
C2-C4.

Structure XSD notable : les puissances souscrites ne sont **pas** un champ scalaire mais une
liste de `Classe_Temporelle_TURPE_Souscrite` (Libelle + Puissance_Souscrite). Il n'y a **pas**
de `Segment_Clientele` natif (absent du XSD C12 — le segment est inféré à l'usage).

## Décision

C12 est ingéré comme une **spine contractuelle C4** minimale, suivant exactement la même
plomberie que C15 (xml_vers_dict → landing JSON → dbt stg_c12 + flux_c12).

**Grain** : une ligne par `Type_Evenement[0].Situation_Contractuelle[0]` (un événement
contractuel par fichier C12 ; grain identique à C15).

**Pivot des puissances** : les `Classe_Temporelle_TURPE_Souscrite` sont pivotés en colonnes
par agrégation conditionnelle sur le vocabulaire TURPE :

| Libelle XSD | colonne produite               |
|-------------|-------------------------------|
| Base        | puissance_souscrite_kva        |
| HP          | puissance_souscrite_hp_kva     |
| HC          | puissance_souscrite_hc_kva     |
| HPH         | puissance_souscrite_hph_kva    |
| HCH         | puissance_souscrite_hch_kva    |
| HPE         | puissance_souscrite_hpb_kva    |
| HCE         | puissance_souscrite_hcb_kva    |

Le mapping HPE→hpb / HCE→hcb suit la convention de nommage des cadrans (ADR-0035, §1) :
« hpb » = Heures Pleines Basse saison, « hcb » = Heures Creuses Basse saison.

**Classes HTA non mappées** (Pointe, HPDemiSaison, HCDemiSaison, JA, PM) : présentes dans le
XSD pour les tarifs HTA5/HTA8 ; produisent NULL dans le pivot. Gardées en blanc pour l'instant
(aucun cas de production observé dans le périmètre C2-C4 BT courant). Un test singulier dbt
(`assert_flux_c12_libelle_classe_dans_enum_xsd`) attrape toute valeur hors de l'enum XSD
complet (dérive de schéma Enedis) ; il ne fait **pas** échouer les classes HTA connues mais
non mappées.

**Pas de colonne `segment_clientele`** : absent du XSD C12. Le segment est inféré en aval
depuis le domaine de tension et l'option tarifaire.

**Golden sur données réelles différé** : nécessite le trousseau AES de prod. La fixture XSD
maximale (`c12_xsd.xml`) et son golden servent de filet de régression en attendant.

## Raison

- Même plomberie que C15 : réutilise le générateur `generer_fixtures_xsd.py`, le harness
  `test_dbt_flux_golden.py`, le loader SELECT * (ADR-0042), la factory `c12()` calquée sur
  `c15()`.
- Scope minimal : uniquement les champs nécessaires à l'accise (#226) et à la spine
  contractuelle. Les classes HTA et la structure d'alimentation détaillée sont délibérément
  hors périmètre pour l'instant.
- `c12()` ne porte pas de validateur Pandera : pas de seam de calcul consommateur établi
  à ce stade (ADR-0035 §5 — même raisonnement que F15/R64).
