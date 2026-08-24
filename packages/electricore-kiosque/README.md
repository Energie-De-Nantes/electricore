# electricore-kiosque

Service qui assemble des notebooks [marimo](https://marimo.io) `run` (lecture seule) en une
seule app web, pour des néophytes qui consultent leurs données via un navigateur seul — voir
[ADR-0057](../../docs/adr/0057-kiosque-acces-neophytes-package-separe.md).

Aucun secret côté serveur : la clé API `electricore` est saisie par l'utilisateur·ice dans le
notebook et sert de seule identité (tranche #705).

## Configuration (par entité)

- `KIOSQUE__APPS` — noms séparés par des virgules, sélection dans le catalogue à monter.
- `KIOSQUE__TITRE` — nom de l'entité, affiché par l'accueil.

Un nom absent du catalogue dans `KIOSQUE__APPS` fait échouer le démarrage (message explicite).

## Lancement local

```bash
env KIOSQUE__APPS= KIOSQUE__TITRE="Ma structure" uv run --package electricore-kiosque electricore-kiosque
```

Le catalogue réel (exports…) arrive en tranche suivante (#705) ; ce squelette part d'un
catalogue vide.
