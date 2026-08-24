# electricore-kiosque

Service qui assemble des notebooks [marimo](https://marimo.io) `run` (lecture seule) en une
seule app web, pour des néophytes qui consultent leurs données via un navigateur seul — voir
[ADR-0057](../../docs/adr/0057-kiosque-acces-neophytes-package-separe.md).

Aucun secret côté serveur : la clé API `electricore` est saisie par l'utilisateur·ice dans le
notebook et sert de seule identité.

## Configuration (par entité)

- `KIOSQUE__APPS` — noms séparés par des virgules, sélection dans le catalogue à monter.
- `KIOSQUE__TITRE` — nom de l'entité, affiché par l'accueil.
- `KIOSQUE__API_URL` — URL de l'API `electricore` interrogée par les notebooks (ex.
  `exports`). **Requise**, sans défaut codé en dur : c'est de la config de déploiement,
  fournie par le `config.env` du provider (`electricore-secrets`, ADR-0044). Absente →
  message explicite au moment où un notebook en a besoin (pas au démarrage du service).

Un nom absent du catalogue dans `KIOSQUE__APPS` fait échouer le démarrage (message explicite).

## Catalogue

- **`exports`** — clé API (saisie navigateur, jamais stockée côté serveur), trois onglets
  à chargement paresseux (`mo.ui.tabs(..., lazy=True)`), chacun tableau filtrable +
  téléchargement CSV (bouton natif de `mo.ui.table`) :
  - **Facturation** — méta-périodes mensuelles.
  - **Relevés** — mart canonique harmonisé (ADR-0029) ; fenêtre par défaut le dernier
    mois calendaire, filtres pdl + plage de dates, plafond dur côté kiosque avec bandeau
    « vue tronquée » quand il est atteint.
  - **Flux bruts** — tables Enedis fidèles à la source (dropdown en dur : c15, r151, r15,
    f15_detail, f12_detail, r64), filtre pdl optionnel, avertissement conventions Enedis
    (colonnes propres à chaque flux, ni dédoublonnage ni harmonisation inter-flux) ; même
    plafond dur avec bandeau « vue tronquée » que Relevés ; table absente de la box →
    message propre.

  Filtres (pdl/dates pour Relevés, dropdown table/pdl pour Flux bruts) déclarés dans leurs
  propres cellules réactives, hors des fonctions passées à `mo.ui.tabs(..., lazy=True)` —
  des widgets créés à l'intérieur d'une fonction paresseuse seraient des candidats GC,
  invisibles aux changements ultérieurs (#721).

  Les onglets Relevés/Flux bruts passent par `electricore-client[arrow]`
  (`ElectricoreArrowClient`, amendement 2026-08-24 à l'ADR-0057) — polars entre dans le
  kiosque via cet extra public du client, jamais via le moteur `electricore`. Clé invalide
  ou révoquée : message clair, pas de stacktrace.

## Lancement local

```bash
env KIOSQUE__APPS=exports KIOSQUE__TITRE="Ma structure" KIOSQUE__API_URL=https://mon-api.exemple.fr \
    uv run --package electricore-kiosque electricore-kiosque
```
