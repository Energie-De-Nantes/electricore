---
fraicheur: 2026-11-03
---

# Flux Enedis

La carte des données qu'Enedis met à disposition d'un fournisseur : ce que transporte chaque
flux, comment l'obtenir, ce qu'ElectriCore en fait, et ce qu'il laisse encore de côté. Une
fiche par guide Enedis, rangée par famille.

Pour partir d'un **besoin** plutôt que d'un flux, va au [catalogue des besoins](../besoins.md) ;
pour les **règles d'accès** (titulaire, profondeur, accord du client) et l'arbre « il me manque
de l'historique », va à [Obtenir les données Enedis](../sources-enedis.md).

## Lire une fiche

Chaque fiche suit le même plan : un tableau (guide et version, segments, obtention, prérequis,
format, statut dans ElectriCore), puis **Contenu**, **À quoi ça sert**, **Pièges** et, pour un
flux ingéré, **Non extrait aujourd'hui** — les champs présents dans le brut mais pas encore
exposés, donc servables par une évolution du modèle dbt sans rien redemander à Enedis.

- Les noms *en italique* sont ceux des guides Enedis (balises, champs, fichiers) ; les noms
  `en code` existent dans ElectriCore et un test vérifie qu'ils existent dans le code.
- « (à confirmer) » marque ce qui est déduit et non vérifié sur données réelles.
- Les guides SGE sont diffusés aux acteurs du marché et ne sont pas versionnés ici : les
  fiches les résument et citent la référence et la version lues. Un document classé à
  diffusion restreinte n'est résumé que dans son rôle.

## Vue d'ensemble

| Flux | Ce qu'il porte | Segments | Obtention | Dans ElectriCore |
|---|---|---|---|---|
| [C15](contractuel.md#c15-evenements-contractuels-et-techniques-des-points-c5) | Événements contractuels et techniques, index avant/après, titulaire, plages HC | C5 | continu, quotidien | `flux_c15` |
| [C12](contractuel.md#c12-description-contractuelle-des-points-c2-c4) | Situation contractuelle, puissances par classe | C2-C4 | continu, quotidien | `flux_c12` (hors chaîne de facturation) |
| [C68](contractuel.md#c68-photo-technique-et-contractuelle-dun-point-a-la-demande) | Photo technique et contractuelle d'un point | tous | M023 | non ingéré |
| [R15](releves.md) | Index et consommations à chaque relevé, autoconsommation collective | C5 | continu, quotidien | `flux_r15`, `flux_r15_acc` |
| [R151](releves.md) | Index périodiques (quotidiens ou mensuels) et Pmax des Linky | C5 | abonnement | `flux_r151` → `releves` |
| [R17](releves.md) | Index et consommations à chaque relevé facturé | C2-C4 | continu, quotidien | non ingéré (#210) |
| [R171](releves.md) | Index quotidiens | C1-C4 | abonnement | écarté (#211, remplacé par R64) |
| [R10 / R10A](releves.md) | Courbe de charge du mois écoulé | C2-C4 | continu, mensuel | non ingéré |
| [P15](releves.md) | Compte rendu d'activation de période mobile | C5 | événementiel | non ingéré |
| [R63](mesures-m023.md) | Courbe de charge | tous | M023, ou abonnement R63A/B | non ingéré (#212) |
| [R64](mesures-m023.md) | Index quotidiens | tous | M023, ou abonnement R64A/B | `flux_r64` → `releves` (M023 seulement) |
| [R65](mesures-m023.md) | Énergie globale quotidienne, sans cadran | tous | M023 | non ingéré (#215) |
| [R66](mesures-m023.md) | Puissance maximale quotidienne | C5, P4 | M023, ou abonnement R66B | non ingéré (#213) |
| [R67](mesures-m023.md) | Énergie facturée par période de relevé et cadran | tous | M023 | `flux_r67` (hors `releves`) |
| [CR M023](mesures-m023.md) | Compte rendu de chaque demande M023 | tous | automatique | non ingéré |
| [IFJ](mesures-m023.md) | Données infra-journalières | C1-C4 avec boîtier IP | webservice | non ingéré (#219) |
| [F15](facturation.md) | Facture d'acheminement détaillée | C5, P4 | continu, quotidien | `flux_f15_detail` |
| [F12](facturation.md) | Facture d'acheminement détaillée par sous-lot | C2-C4 | continu, quotidien | `flux_f12_detail` (acheminement seulement) |
| [TURPE7HC](facturation.md) | Reprogrammation des heures creuses TURPE 7 | C5 Linky | ponctuel (2026-2027) | écarté (#209) |
| [X12 / X13](affaires-acces.md) | Suivi des affaires SGE | C2-C5 | continu, quotidien | `flux_affaires` |
| [X14 / X15](affaires-acces.md) | Suivi des réclamations | C2-C5 | continu, quotidien | non ingéré (#218) |
| [API REST v0](affaires-acces.md) | Affaires, mesures, données du point | tous | appel synchrone | veille (#220) |
| [Déchiffrement](affaires-acces.md) | Chiffrement AES des flux | tous | — | trousseau AES (ADR-0037) |

## Candidats à l'intégration

Repris de la revue des flux de juin 2026 (#207), mise à jour :

- **Intégrés depuis** : C12 (#208), X12-X13 (#217), R67 (#214).
- **Écartés** : TURPE7HC (#209, flux ponctuel traité hors ElectriCore), R171 (#211, remplacé
  par R64), C68 (#216).
- **Ouverts** : R17 (#210, ferme la boucle C12 → relevés → F12 pour le C2-C4), R63B (#212,
  courbe de charge pour l'approvisionnement), R66B (#213, conseil de puissance souscrite),
  R65 (#215, oracle de validation du pipeline énergie), X14-X15 (#218), IFJ (#219), API REST
  (#220).
- **Nouveau** : la table de correspondance Enedis (CAR.0549) présente R64B et R66B comme
  successeurs du R151, et le motif d'ingestion du R64 ne capte pas les *R64B* (#752).

## Écarts relevés entre les guides et ElectriCore

La rédaction de ces fiches a fait remonter des écarts entre les guides et les modèles
d'ingestion, chacun suivi par une issue (à confirmer sur données réelles avant correction) :

- R64B récurrents non captés, successeurs annoncés du R151 — #752
- `flux_r15` : calendrier, index des non-communicants, relevés annulés — #753
- `flux_r15_acc` : classe de mesure 2 — #754
- `flux_c15` : index du calendrier fournisseur — #755 ; plages heures creuses — #756 ;
  titulaire — #757
- `flux_c12` : cumuls d'événements, classe Pointe — #758
- `flux_f12_detail` : prestations et frais divers — #759
- `flux_r64` : index C1-C4 — #760
- `flux_r151` : Pmax — #761
- `flux_affaires` : réclamations, séquence X12 v2.5.0 — #762

## Pour aller plus loin

Comment les flux sont ingérés : [ingestion des flux](../../ingestion.md) ·
[index des ADRs](../../maintenir/index-adr.md).

[Retour à Trouver la donnée](../index.md).
