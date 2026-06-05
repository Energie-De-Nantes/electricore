# Tests du script d'installation

Deux niveaux : unit pour les helpers purs (`deploy/lib/`), e2e pour le script complet dans une VM jetable.

## Tests unitaires (rapides, sans dépendance)

```bash
./deploy/tests/unit.sh
```

Lance ~30 assertions sur les validateurs (`validate_slug`, `validate_aes_key`, etc.) et la détection OS (avec fixtures `/etc/os-release` mockées). Sortie type :

```
→ validate.sh
  ✓ slug 'edn'
  ✓ slug 'enargia-test'
  ✗ slug 'EDN' (majuscules)
  …
N passed, 0 failed
```

Bash uniquement, pas de framework. Pour ajouter un test : éditer `unit.sh` (voir les exemples existants).

## Tests e2e (sandbox VM)

```bash
./deploy/tests/e2e/multipass.sh up        # 30-60s : launch Ubuntu 24.04 + mount repo
./deploy/tests/e2e/multipass.sh run       # exec install.sh dans la VM
./deploy/tests/e2e/multipass.sh shell     # debug interactif
./deploy/tests/e2e/multipass.sh snap clean
./deploy/tests/e2e/multipass.sh restore clean   # rollback rapide
./deploy/tests/e2e/multipass.sh down      # cleanup
```

**Prérequis :** [Multipass](https://multipass.run/install) installé (`sudo snap install multipass` sur Ubuntu).

Le harnais monte le repo sur `/repo` dans la VM, injecte une clé SSH root bidon, et lance `install.sh` en `sudo`. Snapshot/restore permettent d'itérer sans relancer un `up` complet.

## Cycle de dev recommandé

1. Modifier `deploy/lib/<fichier>.sh` ou `deploy/install.sh`.
2. Si on a touché à une fonction pure : `./deploy/tests/unit.sh` (en boucle, ~instantané).
3. Si on a touché à l'orchestration : `./deploy/tests/e2e/multipass.sh run` (puis `restore` pour réessayer).
4. Avant push : `unit.sh` doit être vert ; e2e idéalement aussi.

## Ajouter une fonction + son test

1. Ajouter la fonction dans `deploy/lib/<fichier>.sh`.
2. Ajouter 2-3 assertions dans `deploy/tests/unit.sh` (cas nominal + cas d'échec).
3. `./deploy/tests/unit.sh` doit afficher `N passed, 0 failed`.
