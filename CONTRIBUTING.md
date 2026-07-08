# Contribuer à ElectriCore

Merci pour votre intérêt ! ElectriCore est un outil libre (AGPL-3.0) de traitement des flux Enedis pour les fournisseurs alternatifs d'électricité. Les contributions sont les bienvenues — bug reports, suggestions, PRs.

> [!IMPORTANT]
> **Les PR non sollicitées générées par des agents ou des outils automatisés ne
> sont pas acceptées** pour l'instant : le projet n'est pas outillé pour les
> accueillir et ne les recherche pas — elles seront fermées sans être fusionnées,
> quelle que soit leur qualité.
>
> Le label `ready-for-agent` sur une issue n'est **pas** une invitation à
> contribuer : il est réservé aux agents du mainteneur, qui se l'assignent au
> triage (cf. [docs/agents/triage-labels.md](docs/agents/triage-labels.md)). Le
> chemin de déploiement (`deploy/`, exécuté en root, manipulation de secrets) et
> la chaîne crypto/SOPS sont maintenus en interne.

## Installation de l'environnement de développement

Le projet utilise [uv](https://docs.astral.sh/uv/) pour la gestion des dépendances.

```bash
# Cloner
git clone https://github.com/Energie-De-Nantes/electricore.git
cd electricore

# Installer l'ensemble (core + ingestion dlt/dbt + tests + typecheck)
uv sync --extra ingestion --extra dbt --group test --group typecheck

# Installer les hooks pre-commit (ruff lint + format à chaque commit local)
uvx pre-commit install

# Installer le hook pre-push (testsuite avant chaque git push)
uvx pre-commit install --hook-type pre-push
```

Python 3.12 ou 3.13 requis.

## Lancer les tests

```bash
# Suite complète (~30s)
uv run --group test pytest

# Exécution parallèle
uv run --group test pytest -n auto

# Avec couverture (rapport texte)
uv run --group test pytest --cov=electricore
```

La suite complète passe en une trentaine de secondes. Quelques tests sont volontairement skippés (intégration Odoo nécessitant des secrets, génération de snapshots).

Le hook **pre-push** exécute automatiquement la suite avant chaque `git push` (cf. installation ci-dessus). Pour le lancer manuellement sans pousser :

```bash
uvx pre-commit run --hook-stage pre-push --all-files
```

**Pas besoin de secrets Enedis/Odoo** pour développer : les tests qui en dépendent se skippent automatiquement quand `secrets.toml` ou les variables d'environnement sont absents.

## Lint et type-checking

Les hooks pre-commit gèrent ruff automatiquement à chaque commit. Pour lancer manuellement :

```bash
# Lint + auto-fix
uvx ruff check --fix

# Format
uvx ruff format

# Type-checking (scope étroit : core/loaders, core/writers, core/builds)
uv run --group typecheck mypy
```

La CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) exécute les trois sur chaque PR (Python 3.12 et 3.13). Une PR avec lint/typecheck/tests rouges ne passera pas.

## Soumettre une PR

1. **Brancher depuis `main`** — par convention, le nom de branche reflète le type de changement : `feat/turpe-tempo`, `fix/r151-parsing`, `chore/refactor-loaders`.
2. **Commiter en Conventional Commits** en français — voir le `git log` pour le style. Préfixes utilisés dans le repo :
   - `feat(scope): ...` — nouvelle fonctionnalité métier
   - `fix(scope): ...` — correction de bug
   - `chore(scope): ...` — maintenance, dépendances, outillage
   - `ci: ...` — workflows GitHub Actions
   - `docs(scope): ...` — documentation
   - `refactor(scope): ...` — refactor sans changement de comportement
3. **Garder les PRs focalisées** — un sujet par PR, plus facile à reviewer.
4. **Pousser la branche et ouvrir une PR** vers `main`. La CI s'exécute automatiquement.
5. **Documentation à jour** — une PR qui change un comportement visible d'un rôle
   (usager·ère, opérateur·ice, déployeur·euse, contributeur·ice, mainteneur·e) met à
   jour la page correspondante du site dans la même PR (voir la
   [charte de la documentation](docs/contribuer/charte-documentation.md) pour qui
   sont ces rôles et où vit leur page). Règle opposable en revue : le test de parité
   (`tests/unit/test_parite_docs.py`) garantit qu'aucune page n'est orpheline de la
   nav, pas que son contenu a suivi le changement — ça reste à la reviewer·euse de
   vérifier.

## Conventions techniques

- **Polars uniquement** (pas de pandas) — pipelines en `LazyFrame` chaînés. Voir [ADR-0002](docs/adr/0002-polars-uniquement.md).
- **Validation Pandera** sur les schémas de DataFrame en entrée/sortie des fonctions publiques (décorateur `@pa.check_types(lazy=True)`).
- **Timezone `Europe/Paris`** par défaut sur toutes les dates.
- **Noms de colonnes** au format `grandeur_cadran_unité` (ex : `energie_hp_kwh`, `puissance_souscrite_kva`).
- **Tout en français** : variables, fonctions, classes, colonnes, commentaires. Seules les APIs de bibliothèques tierces restent en anglais (`pl.col`, `.filter`…). Voir [ADR-0004](docs/adr/0004-langue-francaise.md).
- **Pas d'accents dans les identifiants Python** (variables, fonctions, classes, colonnes). Préférer `energie_consommee` à `énergie_consommée`, `cout_total` à `coût_total`. Les accents restent autorisés dans les docstrings, messages et commentaires.
- **Fonctions : `verbe_complement_complement`** — verbe à l'infinitif + compléments en snake_case (`calculer_consommation`, `valider_releve`, `recuperer_donnees`).
- **Vocabulaire métier** : se référer à [CONTEXT-MAP.md](CONTEXT-MAP.md) qui pointe vers les `CONTEXT.md` par module (le glossaire métier vit dans [`electricore/core/CONTEXT.md`](electricore/core/CONTEXT.md)).
- **`notebooks/` et `scripts/`** sont exclus de ruff et mypy : conventions différentes (marimo, scripts ad-hoc).

## Process de release (mainteneurs)

```bash
# 1. Bumper la version dans pyproject.toml
# 2. Ajouter la section dans CHANGELOG.md
git commit -am "chore(release): vX.Y.Z"
git push

# 3. Tag + push du tag
git tag vX.Y.Z && git push origin vX.Y.Z
```

Le workflow [`.github/workflows/release.yml`](.github/workflows/release.yml) prend le relais : build, publication sur PyPI (trusted publishing OIDC, sans token), création de la release GitHub avec artefacts et notes auto-générées.

## Reporter un bug ou suggérer une feature

Ouvrir une [issue GitHub](https://github.com/Energie-De-Nantes/electricore/issues) avec :
- Pour un bug : version d'electricore, version de Python, étapes pour reproduire, comportement attendu vs observé.
- Pour une feature : le cas d'usage métier (quel flux Enedis, quel calcul, quel intégrateur) plutôt que la solution technique directement.
