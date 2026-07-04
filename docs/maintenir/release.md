---
fraicheur: 2026-07-04
---

# Processus de release

Deux cycles de publication distincts et indépendants : le **moteur** (paquet
`electricore`, tags `vX.Y.Z…`) et le **client léger** (paquet `electricore-client`, tags
`client-vX.Y.Z…`, [ADR-0043](../adr/0043-electricore-client-paquet-separe.md)). Ce qui suit
décrit le moteur ; le client suit le même principe (tag = action publiante) avec un
paquet PyPI en moins de sortie (pas d'image Docker).

## 1. Avant de taguer : bump en PR, comme tout le reste

Une release commence par une **PR ordinaire**, relue et mergée humainement comme
n'importe quelle autre :

- version bumpée dans `pyproject.toml` (`[project].version`) ;
- `uv.lock` régénéré en cohérence ;
- une entrée `CHANGELOG.md` (format [Keep a Changelog](https://keepachangelog.com/fr/1.0.0/),
  [Semantic Versioning](https://semver.org/lang/fr/)) décrivant la release.

Le tag n'est posé **qu'après** le merge de cette PR sur `main`.

## 2. Le tag est l'action publiante

Le workflow [`.github/workflows/release.yml`](https://github.com/Energie-De-Nantes/electricore/blob/main/.github/workflows/release.yml)
se déclenche sur un `push` de tag matchant :

- **stable** : `vX.Y.Z` (ex. `v3.5.0`) ;
- **pré-release** (PEP 440) : `vX.Y.ZaN`, `vX.Y.ZbN`, `vX.Y.ZrcN`, `vX.Y.Z.devN` (ex. `v3.5.0rc3`).

À partir de là, tout est automatique :

1. **Build** — `uv build` (sdist + wheel), puis un garde-fou vérifie que
   `dist/electricore-<version>.tar.gz`/`.whl` existe bien pour la version du tag
   (le nom du fichier doit correspondre — sinon la publication s'arrête là).
2. **PyPI** — publication en Trusted Publishing (OIDC), `skip-existing: true`.
3. **GitHub release** — créée avec notes auto-générées ; `--prerelease` posé
   automatiquement si le tag n'est pas de la forme stricte `vX.Y.Z`.
4. **Image Docker → ghcr.io** — le gate build → scan de secrets → smoke test → push
   (workflow réutilisable `docker-image.yml`, partagé avec la CI où il tourne sans
   push sur chaque PR) publie :
   - **stable** : `ghcr.io/energie-de-nantes/electricore:X.Y.Z` **+** `:X.Y` **+** `:latest` ;
   - **pré-release** : uniquement `:X.Y.Z...` exact — jamais `:latest` ni `:X.Y`, pour ne
     pas rediriger une instance qui suit la stable vers une rc.

Aucun bloc `concurrency` sur ce workflow : une release ne s'annule jamais en cours de
publication.

## 3. Promotion rc → stable : une PR, pas un rebuild

Passer d'une release candidate validée à la stable correspondante est une **promotion
pure** : le même arbre de fichiers, où **seuls les fichiers de version** changent
(`pyproject.toml`, `uv.lock`, `CHANGELOG.md` qui documente la stable et son historique
rc-par-rc). Pas de nouveau commit fonctionnel entre la dernière rc et la stable — la
discipline attendue est un diff strictement limité à ces trois fichiers, vérifiable par
comparaison d'arbre. Cette discipline est un **usage de mainteneur**, pas une règle
qu'un job CI vérifie aujourd'hui.

## 4. Frontière : la release s'arrête à l'image publiée

Le cycle décrit ici se termine à la publication du paquet PyPI et de l'image
`ghcr.io/energie-de-nantes/electricore`. **Déployer une instance** (tirer la nouvelle
image sur un VPS, `docker compose up -d`) est un acte **séparé**, à la charge du·de la
déployeur·euse d'une instance donnée ([ADR-0011](../adr/0011-deploiement-vps-docker.md),
[ADR-0015](../adr/0015-deploiement-multi-instance.md)) — jamais automatique ni piloté
depuis ce dépôt.

## Pour aller plus loin

[Section Développer](../contribuer/developper.md) (installation dev,
tests, CI) · [guide de déploiement](https://github.com/Energie-De-Nantes/electricore/blob/main/docs/deploiement.md) ·
[ADR-0043 — `electricore-client` paquet séparé](../adr/0043-electricore-client-paquet-separe.md).

[Retour à Maintenir](index.md).
