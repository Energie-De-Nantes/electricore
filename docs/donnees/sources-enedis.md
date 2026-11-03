---
fraicheur: 2026-11-03
---

# Obtenir les données Enedis

ElectriCore ne voit que ce qu'Enedis lui a envoyé **depuis son déploiement** et pour les
PDL **au périmètre** du fournisseur. Pour aller plus loin en arrière, il faut commander la
donnée — et seulement pour un PDL encore au périmètre. Cette page dit quoi commander, sous
quelles conditions, et où le résultat atterrit.

!!! note "Sources"
    Notes publiques Enedis
    [NMO-CF_016E](https://www.enedis.fr/media/2127/download) (BT ≤ 36 kVA communicant)
    et [NMO-CF_015E](https://www.enedis.fr/media/2128/download) (HTA / BT > 36 kVA),
    guides SGE des flux (diffusés aux acteurs du marché, non versionnés ici : seule leur
    référence GUI est citée, les fiches détaillées par guide restent à écrire), et
    constats d'ElectriCore
    ([ADR-0047](../adr/0047-flux-r67-energie-par-periode-distributeur-hors-releves.md)).
    Ce qui est **non confirmé** est signalé comme tel.

## Trois règles qui bornent tout

1. **Seulement les PDL au périmètre.** Une demande M023 de mesures pour un PDL qui n'est
   plus (ou n'a jamais été) chez le fournisseur est refusée par Enedis : le demandeur doit
   être le **fournisseur titulaire actif**. Exception documentée : la photo C68, qu'un
   fournisseur non titulaire peut commander en déclarant l'accord du client, avec un accès
   partiel ([fiche C68](flux/contractuel.md)).
2. **36 mois glissants au plus**, et **coupés à la dernière mise en service** du PDL :
   un nouvel occupant efface l'accès à tout ce qui précède son arrivée (le « mur
   occupant »). Un client arrivé il y a deux ans rendra deux ans. Un client resté sur
   place qui a changé de fournisseur (CFNE) garde en revanche son historique antérieur.
3. **Consentement du client pour les données fines** (index quotidiens, Pmax, courbe
   de charge) ; une simple autorisation suffit pour les données mensuelles et
   contractuelles. La demande se fait en **déclarant** disposer de l'accord ; Enedis
   contrôle a posteriori.

## Les flux et services

### Reçus en continu (abonnement SFTP)

Seulement à partir de l'entrée au périmètre, sans rattrapage d'historique.

| Flux | Contenu | Guide | Dans ElectriCore |
|---|---|---|---|
| C15 | Événements contractuels C5, index avant/après | GUI.0300 | `flux_c15` → `releves`, `spine_contrat` |
| C12 | Description contractuelle C2-C4 | GUI.0129 | `flux_c12` |
| R15 | Index et consommations C5 à chaque relevé | GUI.0292 | `flux_r15`, `flux_r15_acc` (hors `releves`) |
| R151 | Index quotidiens C5 communicants (abonnement, consentement) | GUI.0317 | `flux_r151` → `releves` ; successeurs annoncés R64B / R66B, non captés (à confirmer) |
| F12 | Facturation distributeur C2-C4 | GUI.0124 | `flux_f12_detail` |
| F15 | Facturation TURPE C5 détaillée (et P4 en injection) | GUI.0298 | `flux_f15_detail` |
| X12 / X13 | Suivi des affaires SGE | GUI.0322 | `flux_affaires` |
| R17 | Index et consommations C2-C4 | GUI.0131 | **non ingéré** |

### À la demande (service M023 sur le portail SGE)

Toutes ces demandes supposent un PDL au périmètre (règle 1).

| Flux | Contenu | Profondeur | Accord du client | Dans ElectriCore |
|---|---|---|---|---|
| **R67** mesures facturantes | Énergie par période de relevé et par cadran | 36 mois constatés | autorisation | `flux_r67` (hors `releves`) — [recette](../ingestion.md) § R67 |
| **R64** index quotidiens | Index par cadran, compteur communicant | 36 mois | consentement | `flux_r64` → `releves` |
| R63 | Courbe de charge | 24 mois, si la collecte était active | consentement | non ingéré |
| R65 | Énergie quotidienne, sans cadran | 36 mois (non confirmé en M023) | consentement | non ingéré |
| R66 | Pmax quotidienne | 36 mois | consentement | non ingéré |
| C68 | Données techniques et contractuelles du point, à la date de la commande | situation courante | selon la donnée | non ingéré |

Les guides de référence : R6X GUI.0503, compte rendu M023 GUI.0505, C68 GUI.0504 — résumés
dans les [fiches de flux](flux/index.md). Le
compte rendu d'une demande M023 indique, par PDL, s'il a été publié ou rejeté, et
pourquoi. Le résultat est livré sur
le portail **et** sur le SFTP. Commander au format **JSON** : c'est celui que
l'ingestion lit, et R64 / R67 sont alors landés au prochain run. Un ZIP téléchargé du
portail est en clair et peut être landé à la main.

Pour préparer une demande groupée : `GET /perimetre/pdls.csv` donne la liste des PDL
au périmètre à une date.

!!! info "C2-C4 (> 36 kVA)"
    Par le portail, l'historique mensuel et la courbe de charge sont limités à
    **12 mois**, portés à 24 par Enedis (NMO-CF_015E). L'API Mesures Enedis annonce davantage, mais ElectriCore n'a pas de
    client pour cette API.

## Obtenir un historique manquant

**Étape 0 — l'a-t-on déjà reçu ?** Si le PDL a été au périmètre et que l'instance a été
déployée tard, les archives encore publiées sur le SFTP se rejouent : bot `/ingestion`,
mode `resync`. La durée de conservation du SFTP Enedis n'est pas documentée. Un ancien
prestataire de réception des flux peut aussi détenir des archives (attention aux clés de
déchiffrement de l'époque).

**Étape 1 — où en est le PDL ?**

| Situation | Ce qui reste possible |
|---|---|
| Au périmètre | M023 (étape 2), coupé à la dernière mise en service |
| Sorti, ou jamais au périmètre | rien côté Enedis : seules les archives de l'étape 0, ou ce que fournit le client (anciennes factures, exports de son espace client Enedis) |

**Étape 2 — quelle grandeur ?** (C5, PDL au périmètre)

| Besoin | Option 1 | Option 2 | Atterrit dans |
|---|---|---|---|
| Consommation mensuelle par cadran | M023 **R67** | M023 **R64** (consentement), différence d'index au 1er du mois | R67 → `flux_r67`, en périodes de relevé : proratiser ; R64 → `releves` |
| Index à des dates précises | M023 R64 | rejeu R151 (étape 0) | `releves` |
| Courbe de charge | M023 R63 | — | nulle part : traitement hors ElectriCore |
| Pmax | M023 R66 | — | nulle part |
| Plus de 36 mois, ou avant la mise en service | impossible côté Enedis | anciennes factures du client | nulle part |

!!! warning "Avant l'entrée au périmètre"
    Des index R64 antérieurs à l'entrée du PDL (client resté sur place, arrivé par CFNE)
    atterrissent bien dans `releves`, mais les méta-périodes partent de l'historique C15 :
    ElectriCore ne produira pas de consommation mensuelle pour ces mois-là sans calcul
    ad hoc.

## Ce qui reste à éclaircir

- Profondeur R67 : 36 mois constatés en M023, l'API Mesures annonce 60 mois pour les
  mesures facturantes.
- Les abonnements récurrents R64B / R63B / R66B ont une nomenclature que l'ingestion ne
  capte probablement pas.
- Délais, coûts (catalogue des prestations Enedis) et limites par demande M023 (guide
  Webservices M023 GUI.0502, non disponible).

## Pour aller plus loin

Comment les flux sont ingérés, landés et rejoués : [ingestion des flux](../ingestion.md).

[Retour à Trouver la donnée](index.md).
