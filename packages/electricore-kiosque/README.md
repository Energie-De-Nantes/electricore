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
  ou révoquée : message clair, pas de stacktrace. Même traitement pour les erreurs
  opérationnelles côté serveur (ingestion en cours, API injoignable, versions
  client/serveur désynchronisées, #722).

## Lancement local

```bash
env KIOSQUE__APPS=exports KIOSQUE__TITRE="Ma structure" KIOSQUE__API_URL=https://mon-api.exemple.fr \
    uv run --package electricore-kiosque electricore-kiosque
```

## Image Docker

Une seule image publique, publiée sur `ghcr.io/energie-de-nantes/electricore-kiosque` —
tout le catalogue embarqué, la personnalisation par entité reste de la configuration au
lancement (voir [ADR-0057](../../docs/adr/0057-kiosque-acces-neophytes-package-separe.md)).
Aucun secret embarqué, aucun build par client ([`Dockerfile`](Dockerfile)).

```bash
docker run --rm -p 8765:8765 \
    -e KIOSQUE__APPS=exports \
    -e KIOSQUE__TITRE="Ma structure" \
    -e KIOSQUE__API_URL=https://mon-api.exemple.fr \
    ghcr.io/energie-de-nantes/electricore-kiosque:latest
```

Puis ouvrir `http://localhost:8765`. Changer `KIOSQUE__APPS`/`KIOSQUE__TITRE` change les
apps servies sans rebuild — c'est le même artefact pour toutes les entités.

### Publier une nouvelle version de l'image

Même logique de découplage que `electricore-client` (tag dédié, pas de release moteur) :

1. Bumper `version` dans [`pyproject.toml`](pyproject.toml) via une PR normale, merger.
2. Tagger `kiosque-vX.Y.Z` (ou une pré-release `kiosque-vX.Y.ZrcN`/`aN`/`bN`/`.devN`) sur
   `main` et pousser le tag.
3. Le workflow [`release-kiosque.yml`](../../.github/workflows/release-kiosque.yml) build,
   scanne (TruffleHog), fume l'image ([`smoke.sh`](smoke.sh)), puis pousse sur ghcr.io.
   Tag stable → `:X.Y.Z` + `:X.Y` + `:latest` ; pré-release → seulement `:X.Y.ZrcN` (ne
   touche jamais `:latest`, même convention que l'image moteur).

Chaque PR construit + scanne + fume l'image (sans la pousser) via le même workflow
réutilisable ([`docker-image-kiosque.yml`](../../.github/workflows/docker-image-kiosque.yml),
appelé par `ci.yml`) — les régressions Docker sont visibles avant le tag.
