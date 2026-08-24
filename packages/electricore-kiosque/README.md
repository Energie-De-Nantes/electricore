# electricore-kiosque

Service qui assemble des notebooks [marimo](https://marimo.io) `run` (lecture seule) en une
seule app web, pour des néophytes qui consultent leurs données via un navigateur seul — voir
[ADR-0057](../../docs/adr/0057-kiosque-acces-neophytes-package-separe.md).

Aucun secret côté serveur : la clé API `electricore` est saisie par l'utilisateur·ice dans le
notebook et sert de seule identité (tranche #705).

## Configuration (par entité)

- `KIOSQUE__APPS` — noms séparés par des virgules, sélection dans le catalogue à monter.
- `KIOSQUE__TITRE` — nom de l'entité, affiché par l'accueil.
- `KIOSQUE__API_URL` — URL de l'API `electricore` interrogée par les notebooks (ex.
  `exports`). **Requise**, sans défaut codé en dur : c'est de la config de déploiement,
  fournie par le `config.env` du provider (`electricore-secrets`, ADR-0044). Absente →
  message explicite au moment où un notebook en a besoin (pas au démarrage du service).

Un nom absent du catalogue dans `KIOSQUE__APPS` fait échouer le démarrage (message explicite).

## Catalogue

- **`exports`** — clé API (saisie navigateur, jamais stockée côté serveur) → tableau
  filtrable de la facturation mensuelle via `electricore-client` → téléchargement CSV de
  la vue filtrée (bouton natif de `mo.ui.table`). Clé invalide ou révoquée : message clair,
  pas de stacktrace.

## Lancement local

```bash
env KIOSQUE__APPS=exports KIOSQUE__TITRE="Ma structure" KIOSQUE__API_URL=https://mon-api.exemple.fr \
    uv run --package electricore-kiosque electricore-kiosque
```
