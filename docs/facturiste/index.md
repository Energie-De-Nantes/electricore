---
fraicheur: 2026-07-05
---

# Facturer

Cette section s'adresse à toi si tu es **facturiste** : tu prépares et tu émets la
facturation mensuelle des contrats énergie.

**Deux états à ne pas confondre** — ce cadrage revient dans toute cette section :

- **L'actuel** : ElectriCore calcule la facturation et l'expose **en lecture**. C'est
  **toi** qui écris dans Odoo, à la main, via des notebooks que tu valides à vue.
  L'Odoo de production est ancien et ne sait pas (encore) venir chercher ses données
  tout seul. C'est le sujet de cette page.
- **Le futur** (section plus bas) : ElectriCore devient la source de vérité et l'ERP
  vient **tirer** ses données directement, sans notebook ni validation humaine à
  chaque mois. C'est **déjà construit** côté ElectriCore, mais ce n'est pas encore le
  chemin de production.

## Le cycle actuel, en un coup d'œil

Chaque mois : ElectriCore ingère les flux Enedis, calcule une proposition de
facturation vérifiable (énergie, abonnement, TURPE, taxes), puis **toi** — via un
notebook que tu valides à vue — l'écris dans Odoo. Aucune écriture n'a lieu sans que
tu aies regardé et cliqué.

![Comment la facturation arrive dans Odoo aujourd'hui](facturiste-vue-ensemble.png)

## Le cycle de facturation, pas à pas

Concrètement, voici le déroulé — un poste à préparer **une fois**, puis un cycle
**chaque mois** qui reste largement manuel (c'est temporaire, le temps qu'Odoo sache
tirer ses données). À gauche, les étapes dans l'ordre ; à droite, la commande ou
l'artefact concret.

![Le cycle de facturation actuel, pas à pas](facturiste-cycle-actuel.png)

**Préparer le poste (une fois) :** récupère tes accès dans un fichier `.env`
(connexion Odoo `ODOO__*` + `ELECTRICORE_API_URL` / `ELECTRICORE_API_KEY`), installe
l'outil opérateur (`uv tool install "electricore[notebooks]"`), puis lance
`electricore-notebooks` — il sert les notebooks marimo en **lecture seule** sur
`http://127.0.0.1:2718/apps/accueil`. Le détail est dans le
[guide d'onboarding notebook](../operateur-notebook.md).

**Le filet de sécurité :** chaque notebook qui écrit dans Odoo (`injection_rsc`,
`facturation`) garde une **double garde** — mode *simulation* coché par défaut, et
écriture réelle bloquée derrière le bouton « Injecter dans Odoo ». Rien ne part dans
Odoo tant que tu n'as pas regardé puis cliqué.

## Tes outils aujourd'hui

- **Notebooks marimo** (`facturation`, `injection_rsc`), servis en local par
  `electricore-notebooks` — tu y valides puis injectes dans Odoo. Voir
  [l'onboarding notebook](../operateur-notebook.md).
- **Bot Telegram**, ton cockpit **en lecture seule** : fraîcheur des données, contrôles,
  téléchargement de rapports, estimation de provision. Il n'écrit jamais dans Odoo.
  Voir le [guide du bot](../guide-bot-facturiste.md).
- **Exports Excel / Arrow** (rapport + détail) pour la facturation, l'accise et la CTA
  — pour auditer ou traiter hors de l'outillage ElectriCore.
- **Check pré-facturation** : vérifications côté Odoo avant d'émettre (RSC manquante,
  factures en brouillon, cohérence des dates) — depuis le bot ou en export XLSX.

## Ce qui se prépare (le futur — déjà construit, pas encore en prod)

Le jour où l'Odoo cible saura tirer ses données, les notebooks d'écriture
disparaîtront. Ce qui existe déjà côté ElectriCore pour ce futur :

- **Méta-périodes** et **chronologie** : les périodes mensuelles et la frise complète
  d'un contrat, exposées par l'API pour qu'un ERP les lise directement (la
  chronologie est exposée par l'API, mais son outillage facturiste reste à venir).
- **Turpe-variable** : un calculateur sans état auquel l'ERP soumet une assiette et
  reçoit un montant, au lieu de recalculer le tarif lui-même.
- **Résolution RSC** : retrouver la situation contractuelle à partir d'un identifiant
  d'affaire Enedis, sans passer par le notebook `injection_rsc`.
- **`electricore-client`** : un paquet léger pour qu'un ERP (ou le futur module
  `souscriptions_odoo`) consomme ElectriCore sans embarquer tout le moteur.

## Pour aller plus loin

- **[Ce qui a changé](changelog-facturiste.md)** — le changelog facturiste, release
  par release.
- **[Guide du bot Telegram](../guide-bot-facturiste.md)**
- **[Onboarding notebook opérateur](../operateur-notebook.md)**

[Retour à l'accueil](../index.md).
