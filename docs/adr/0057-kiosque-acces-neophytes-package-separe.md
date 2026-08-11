# 0057 — Kiosque : accès néophytes en apps marimo hébergées, package séparé

- Status: accepted
- Date: 2026-08-11

## Contexte

Les deux accès existants à ElectriCore supposent tous deux un humain technique : le
**module Odoo** (l'ERP tire les données, ADR-0027) suppose un déploiement Odoo, et les
**Notebooks** (`electricore-notebooks`, marimo `edit`/`run` local, ADR-0009) supposent un
poste de travail, une installation `uv`, un `.env`. Un troisième public existe — des
néophytes qui veulent consulter leurs propres données (exports, relevés, facturation) sans
rien installer, juste un navigateur. Ni Odoo ni les notebooks locaux ne répondent à ce
besoin.

Contraintes du public visé :
- **Navigateur seul** — pas de checkout, pas de `.env`, pas de ligne de commande.
- **Pas de comptes** — un service self-hosted par petite structure (« entité »), pas
  d'inscription/mot de passe à gérer côté ElectriCore.
- **Lecture seule** — le Kiosque ne doit jamais écrire (dans Enedis, dans un ERP, nulle
  part) ; c'est un tableau de bord, pas un outil d'exploitation.
- **Zéro secret côté service** — le service qui sert les pages web ne doit détenir aucune
  clé applicative pour le compte de l'utilisateur·ice.

## Décision

### 1. Un paquet workspace de plus, même patron qu'`electricore-client`

`packages/electricore-kiosque/` (src-layout), membre du workspace uv racine
(`[tool.uv.workspace] members = ["packages/*"]`, glob déjà en place — ADR-0043), pyproject
et version propres. Dépendances : `marimo`, `fastapi`, `uvicorn`, `electricore-client`
(workspace) — **jamais** le moteur `electricore` (pas de `polars`/`duckdb`, cf. ADR-0043).
Le Kiosque est un *consommateur* de l'API au même titre qu'un intégrateur externe, pas un
sous-module du moteur.

### 2. Apps marimo `run` hébergées, une image / N configs

Chaque app métier (exports, facturation lisible…) est un notebook marimo servi en mode
**`run`** (lecture seule, pas d'édition possible depuis le navigateur — même garde que les
notebooks opérateur). Une **seule image Docker** sert **toutes** les entités clientes ; ce
qui varie par déploiement est la **configuration** (catalogue actif, titre), jamais le code.
Assemblage ASGI par chaînage `marimo.create_asgi_app().with_app(path=..., root=...)` :
l'accueil est lui-même un notebook monté à la racine (`path=""`), chaque app active du
catalogue est montée sous `/{nom}`.

### 3. La clé API est la seule identité — zéro secret côté serveur

Pas de comptes, pas de base utilisateurs. L'utilisateur·ice colle sa clé API dans le
notebook (saisie navigateur, tranche #705) ; le service Kiosque ne la stocke jamais côté
serveur, ne la voit qu'en transit vers l'API `electricore` via `electricore-client`. La clé
**est** l'identité et le périmètre de données — même modèle que le bot Telegram et les
notebooks opérateur, poussé jusqu'au bout : aucun état d'authentification propre au Kiosque.

### 4. Config par entité, sous-domaine dédié

Deux variables d'environnement pilotent une instance : `KIOSQUE__APPS` (liste de noms
séparés par virgules, sélection dans le catalogue) et `KIOSQUE__TITRE` (nom affiché par
l'accueil). **Fail-fast** au démarrage si `KIOSQUE__APPS` contient un nom hors catalogue —
une faute de frappe se détecte au déploiement, pas en production face à l'utilisateur·ice.
Déploiement visé : sous-domaine `kiosque.<slug>.electricore.fr` par entité (même patron de
nommage que le reste de l'infra multi-provider, ADR-0044).

### Alternatives écartées

- **Export WASM/Pyodide (notebooks marimo exportés statiques)** : marimo peut exporter en
  HTML/WASM exécuté côté navigateur. Écarté : empreinte mémoire ~2 Go par onglet (Pyodide +
  runtime Python complet chargé client-side) intenable sur le matériel modeste visé par des
  néophytes, et empaquetage des dépendances (polars/httpx) dans le bundle WASM non maîtrisé.
- **Repo séparé** : fragmenterait le code alors que l'infra workspace uv + release découplée
  existe déjà pour exactement ce cas (`electricore-client`, ADR-0043) — un repo de plus
  n'apporte rien qu'un membre `packages/*` de plus n'apporte pas, avec la CI et le versioning
  à reconstruire à côté.
- **Clé API "baked" côté serveur** (une image par config avec la clé en dur ou en secret
  d'infra) : réinvente des comptes par la bande — il faudrait alors gérer qui a accès à
  quelle image/config, exactement le problème que « la clé est l'identité » évite.
- **Une image Docker par client** : chaque nouvelle entité imposerait un build + une image à
  maintenir en plus de la config — enfer de maintenance à l'échelle de N entités, alors
  qu'une image partagée + config par variables d'environnement scale sans y toucher.

## Conséquences

- Troisième accès externe acté dans [CONTEXT-MAP.md](../../CONTEXT-MAP.md) aux côtés du
  module Odoo et des Notebooks, tous via `api` + `electricore-client`.
- Le catalogue d'apps réelles (exports…) et la saisie de clé côté navigateur arrivent en
  tranche suivante (#705) ; ce document couvre le squelette d'assemblage (#704).
- `electricore-notebooks` cesse d'être qualifié de « pont transitoire » : son cycle de vie
  devient explicite — notebook exploratoire (dev, marimo `edit`) → app opérateur locale
  (`electricore-notebooks`, marimo `run` sur poste) → app Kiosque hébergée (marimo `run`
  multi-tenant) — trois étapes assumées du même objet, pas un pont à démolir.
- Un intégrateur Kiosque n'installe jamais `polars`/`duckdb`/`fastapi` du moteur — mêmes
  garanties de légèreté qu'`electricore-client` (dont il dépend).

## Statut

Implémenté par #703 (cet ADR + carte des trois accès) et #704 (squelette
`packages/electricore-kiosque` : assemblage ASGI, accueil, config par entité, fail-fast).
Tranches suivantes : #705 (catalogue d'apps réelles + saisie de clé API navigateur),
#706-#708 (PRD #702).

S'appuie sur ADR-0043 (`electricore-client`, paquet workspace séparé), ADR-0009
(architecture API-centrique, notebooks comme état transitoire du dev local), ADR-0027
(« Odoo tire »), ADR-0044 (secrets-as-code, nommage par slug d'entité).
