# Onboarding d'un opérateur notebook (pont transitoire #414)

> **Pont transitoire.** Les notebooks opérateur (`facturation`, `injection_rsc`)
> sont la voie **actuelle** par laquelle un humain valide puis **écrit dans Odoo**
> (human-in-the-loop, [ADR-0012](adr/0012-api-read-only-odoo.md)).
> Ils disparaîtront à l'arrivée de `souscriptions_odoo` (« Odoo tire »). Ce guide
> décrit le strict nécessaire pour qu'un **opérateur non-dev** s'en serve aujourd'hui.

## Ce que fait l'opérateur

Sur **son propre poste**, l'opérateur lance une commande qui sert les notebooks comme
des apps web **en lecture seule** (`localhost`). Il visualise un mois de facturation /
les RSC, **valide**, puis clique « Injecter dans Odoo ». Aucun git, aucun code, aucune
édition de pipeline.

Les notebooks démarrent en **mode simulation** (rien n'est écrit). L'écriture réelle
exige de décocher la simulation **et** de cliquer le bouton d'injection — deux gestes
délibérés.

## Pré-requis

- [uv](https://docs.astral.sh/uv/getting-started/installation/) installé (une ligne
  `curl`, pas de droits admin requis).
- Un **compte Odoo** (l'opérateur écrit sous **sa propre identité Odoo** → l'historique
  Odoo trace qui a injecté quoi).

## 1. Installer (sans git, sans checkout)

```bash
uv tool install 'electricore[notebooks]'
```

Cela fournit la commande `electricore-notebooks` (le moteur est publié sur PyPI). Pour
mettre à jour plus tard : `uv tool upgrade electricore`.

## 2. Recevoir son `.env`

Les identifiants ne se devinent pas et ne se versionnent pas : **l'admin remet un fichier
`.env` prêt**, hors-bande (gestionnaire de mots de passe partagé, ou en personne). Il
contient :

```dotenv
# Accès en LECTURE à l'API ElectriCore (clé partagée, lecture seule)
ELECTRICORE_API_URL=https://edn.electricore.fr
ELECTRICORE_API_KEY=...

# Écriture Odoo — TES identifiants Odoo (ton login, pas un compte partagé)
ODOO__URL=https://odoo.exemple.fr
ODOO__DB=...
ODOO__USERNAME=toi@exemple.fr
ODOO__PASSWORD=...
```

> ⚠️ Ce fichier porte **tes accès Odoo en écriture** : traite-le comme un mot de passe
> (pas de partage, pas de dépôt git). En cas de départ / fuite : l'admin **désactive ton
> user Odoo** (révoque l'écriture) ; la clé API partagée, elle, ne donne que la **lecture**
> de l'API.

## 3. Lancer

Pose le `.env` dans un dossier dédié, place-toi dedans, lance :

```bash
mkdir -p ~/electricore-operateur && cd ~/electricore-operateur
# (y déposer le fichier .env reçu)
electricore-notebooks
```

Le launcher lit le `.env` **du dossier courant**, valide la configuration (il **nomme**
toute variable manquante), sert les notebooks sur `http://127.0.0.1:2718/apps/...` et
ouvre le navigateur sur la page d'accueil.

Si une variable manque, le message la nomme avec son rôle — complète le `.env` et relance.

## Pourquoi pas de clé age / SSH ici

L'opérateur **ne rejoint pas** le schéma secrets-as-code ([ADR-0044](adr/0044-secrets-as-code-sops-age.md)) :
pas de clé age, pas de deploy key, pas de `sops`. Cet appareil sert la **box** (service
sans surveillance, longue vie, secrets versionnés et tirés en GitOps) — pas un poste
humain qui lance une app temporaire. Pour quelques identifiants livrés une fois à un
non-dev, un `.env` remis à la main est la voie simple et suffisante. Si la révocation
fine de l'accès API devenait un besoin, le chemin de montée est une **clé API
par-opérateur** au trousseau `API__TROUSSEAU__*` ([ADR-0046](adr/0046-convention-noms-env-par-domaine-identite-secrets.md)) —
pas une refonte.
