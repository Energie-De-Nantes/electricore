# Trois registres de savoir : runtime, structurel, réglementaire

## Contexte

Le mot « configuration » recouvre dans le dépôt trois savoirs de natures différentes, que
[docs/configuration.md](../configuration.md) inventorie aujourd'hui sans les distinguer :

- des **valeurs d'environnement** (`SFTP__URL`, clés AES, `DUCKDB_PATH`, tokens) — section 1
  de l'inventaire ;
- du **savoir structurel** (les 7 cadrans, les colonnes de chaque flux, les schémas
  dbt/Pandera) — porté par le code et les modèles ;
- des **taux régulés** (`turpe_rules.csv`, `accise_rules.csv`, `cta_rules.csv`) — section 2.

Deux chantiers ouverts butent sur cette confusion, chacun à sa façon : [#141](https://github.com/Energie-De-Nantes/electricore/issues/141)
(config runtime éclatée en trois loaders, aucun fail-fast) et [#147](https://github.com/Energie-De-Nantes/electricore/issues/147)
(savoir de typage déclaré trois fois : dbt ↔ loaders ↔ Pandera). La revue d'architecture du
12/06/2026 a montré qu'ils partagent une pathologie — *un savoir déclaré N fois qui dérive
en silence* — et un remède de même **forme**, mais que forcer un **mécanisme** commun
coupelerait des savoirs qui changent pour des raisons et à des rythmes différents.

S'ajoute une question laissée ouverte par le projet de module Odoo « saisie des taux par
les facturistes » : quand les taux régulés vivront aussi dans un ERP, qui fait autorité en
cas de divergence ? La question est structurante parce qu'ElectriCore n'est pas l'outil
d'une seule structure : **EDN est le premier prototype d'une fédération de communs de
l'électricité**, et la lib a vocation à être autogérée par cette fédération. Le savoir
partagé doit vivre dans le commun — pas dans l'instance.

## Décision

### Les trois registres

| registre | exemples | autorité | rythme de changement | où il vit |
|---|---|---|---|---|
| **runtime** | `SFTP__URL`, `AES__*`, `DUCKDB_PATH`, tokens | l'instance (ops) | au déploiement | l'environnement (`.env` / env système) ; module canonique `electricore/config/runtime.py` (#141) |
| **structurel** | cadrans, colonnes des flux, schémas dbt/Pandera | Enedis, transcrit dans le code | aux releases (versions XSD, conventions) | le code versionné ; canonique : `core/models/cadrans.py`, cible de la discovery #147 |
| **réglementaire** | taux régulés (TURPE, Accise, CTA) | le régulateur (contenu) ; le commun (copie locale) | calendrier CRE / loi de finances | CSV versionnés `electricore/config/*_rules.csv` ; seam = paramètre `regles=` des pipelines |

**Test décisif d'affectation : qui a autorité sur la valeur, et à quel rythme change-t-elle ?**
Corollaire pratique : « la valeur change-t-elle entre deux déploiements de la même version
du code ? » — oui → runtime ; non → structurel ou réglementaire.

### Le principe partagé (la forme commune)

**Un savoir = un module canonique.** Les consommateurs reçoivent une interface typée
(objet frozen, LazyFrame au schéma connu) — jamais des noms bruts à ré-encoder localement.
La validation vit au seam et nomme ce qui manque : fail-fast par point d'entrée pour le
runtime (#141), contrats Pandera pour le structurel (« Contrat de pipeline », CONTEXT.md),
`valider_regles_presentes()` pour le réglementaire.

### Gouvernance du registre réglementaire

**La lib — le commun — est l'autorité sur les taux régulés.** En conséquence :

- la mise à jour d'un taux est une **contribution** : PR sur les CSV, accompagnée du
  document fondateur, **merge humain** ; la distribution est une release ;
- la **surveillance** (détecter qu'une réglementation a changé) est déléguée — membre
  non technique de la fédération ou alerte automatisée, peu importe le canal : le point
  d'entrée est toujours la contribution, jamais un flux de données ;
- un ERP peut offrir un chemin de **saisie** qui aboutit à une contribution — jamais une
  source de taux lue à runtime par les pipelines ;
- chaque ligne de taux porte sa **référence réglementaire** (délibération CRE, article de
  loi de finances) — colonne `reference` à introduire dans les trois CSV ; le **millésime**
  d'un fichier (dernier changement intégré) s'en dérive et s'expose (API, bot). Les deux
  termes sont définis dans [core/CONTEXT.md](../../electricore/core/CONTEXT.md).

Si un jour une instance doit choisir sa source de taux, ce *sélecteur* serait du registre
runtime — le *contenu* des taux, jamais. Même domaine métier, deux registres : c'est le
test d'affectation qui répartit, pas le sujet.

## Raison

1. **Autorités et rythmes disjoints.** Le runtime appartient à l'instance et change au
   déploiement ; le structurel appartient à Enedis et change par release ; le réglementaire
   appartient au régulateur et change à son calendrier. Une machinerie commune (un
   « méga-module config ») recoulerait dans un même moule des savoirs que tout sépare —
   exactement le couplage que #141 et #147 cherchent à défaire.

2. **Reproductibilité des factures.** Une facture doit être rejouable depuis une version
   de la lib, pas depuis l'état d'un ERP à un instant donné. Des taux lus à runtime dans
   Odoo suspendraient chaque montant calculé à un état de base externe non versionné.

3. **La fédération mutualise la veille.** Un membre repère la délibération CRE, contribue
   une fois, toutes les instances en bénéficient à la release suivante. Des taux par
   instance fragmenteraient précisément le savoir que le commun existe pour mettre en
   commun — et l'instance no-ERP ([ADR-0022](0022-surface-bot-domaines-hybrides.md), #159)
   resterait sans taux.

4. **Auditabilité.** La référence réglementaire ligne à ligne rend chaque taux justifiable
   en revue de PR comme en contrôle a posteriori ; le millésime rend la fraîcheur d'une
   instance observable de l'extérieur (endpoint, bot) sans inspection du code.

### Alternatives écartées

- **Machinerie commune aux trois registres** (« tout en pydantic-settings », ou un module
  config unique absorbant les CSV de taux) — confond la forme partagée (module canonique,
  interface typée, validation au seam) avec un mécanisme partagé ; rejetée pour les raisons
  du point 1.
- **L'ERP comme source de taux à runtime** — autonomie de saisie maximale, mais perd la
  reproductibilité (point 2), introduit la dérive inter-instances (point 3) et laisse
  l'instance no-ERP sans taux. La saisie ERP reste possible *en amont* d'une contribution.
- **Surcharge hybride** (CSV packagé en socle, Odoo en surcharge si présent, divergence
  observable par millésime) — réintroduit la dérive en la rendant simplement visible ;
  écartée tant qu'aucun besoin de taux par instance n'existe : les taux régulés sont
  nationaux.

## Conséquences

- **#141** implémente le registre runtime : module canonique `electricore/config/runtime.py`,
  fail-fast par point d'entrée (l'API ne valide jamais le domaine bot ; le bot ne valide
  Odoo que si l'instance a un ERP ; les notebooks ne valident que ce qu'ils demandent).
  L'entrée « Config partagée » de core/CONTEXT.md (« stdlib-only ») devra être révisée
  consciemment si pydantic-settings est retenu.
- **#147** instruit le registre structurel ; sa discovery peut citer cette ADR comme cadre
  (même forme, mécanisme propre : qui génère qui entre `cadrans.py`, dbt et Pandera).
- **Registre réglementaire — issues de suite** (aucune ne bloque #141) :
  1. colonne `reference` dans les trois CSV + millésime dérivé, exposé via API/bot ;
  2. check de péremption sur rythmes attendus (TURPE ~1ᵉʳ août, accise ~loi de finances)
     branché sur la surveillance bot — un warning, jamais d'auto-correction ;
  3. chemin de contribution guidé pour non-techniques (formulaire ou template d'issue →
     PR proposée, merge humain).
- [docs/configuration.md](../configuration.md) sera restructuré par registres dans #141
  (sa section 1 devient la doc du module runtime ; les sections suivantes restent
  l'inventaire des deux autres registres).

## Limites à connaître

- Les rythmes attendus d'un check de péremption sont des **heuristiques**, pas de la
  veille juridique : un changement hors calendrier (TURPE exceptionnel en cours d'année)
  passe sous le radar tant qu'un humain ne le signale pas.
- La fédération est en construction — EDN en est le prototype ; « merge humain » désigne
  aujourd'hui les mainteneurs EDN, demain les mainteneurs que la fédération se donnera.
- Cette ADR nomme les registres, leurs autorités et la forme commune ; elle ne fige pas
  les mécanismes internes de #141 (choix pydantic-settings vs dataclasses) ni de #147
  (sens de la génération) — chacun reste instruit dans son chantier.
