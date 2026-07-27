# Tests du script d'installation

Deux niveaux : unit pour les helpers purs (`deploy/lib/`), e2e pour le script complet dans une VM jetable.

## Tests unitaires (rapides, sans dépendance)

```bash
./deploy/tests/unit.sh
```

Lance les assertions sur les validateurs d'args CLI (`validate_slug`, `validate_domain`), la politique de split (`validate_config_env`) et la détection OS (avec fixtures `/etc/os-release` mockées). Sortie type :

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

**Prérequis : [Multipass](https://multipass.run/install) installé.**

| OS | Commande |
|---|---|
| **Ubuntu / Debian** | `sudo snap install multipass` |
| **macOS** | `brew install --cask multipass` |
| **Arch / CachyOS** | ⚠️ `paru -S canonical-multipass` (AUR) actuellement cassé avec gcc 15 (link error sur `Scrt1.o`). Alternative : tester sur un vrai VPS Hetzner (CX21 ~5€/mois) — plus représentatif de toute façon. |
| **Windows** | Installeur sur multipass.run/install |

**Fallback si pas de Multipass :** test direct sur un VPS frais (Hetzner / Scaleway / OVH). Workflow identique, juste plus lent à itérer.

Le harnais monte le repo sur `/repo` dans la VM, injecte une clé SSH root bidon, et lance `install.sh` en `sudo`. Snapshot/restore permettent d'itérer sans relancer un `up` complet.

### Scénario e2e : box non-vierge (#656, cas Enargia)

Le préflight sshd (#656) doit refuser le durcissement tant qu'un compte existant
serait coupé du SSH par mot de passe (compte de dépôt Enedis, shape SFTP-only) — puis
passer une fois remédié :

```bash
./deploy/tests/e2e/multipass.sh up
./deploy/tests/e2e/multipass.sh seed-password-account enedis_deposit
./deploy/tests/e2e/multipass.sh harden                        # doit ÉCHOUER (exit non-zero)
./deploy/tests/e2e/multipass.sh verify-preflight refuse enedis_deposit
./deploy/tests/e2e/multipass.sh remediate-key enedis_deposit
./deploy/tests/e2e/multipass.sh harden                        # doit RÉUSSIR
./deploy/tests/e2e/multipass.sh verify-preflight pass enedis_deposit
# finding 3 (diff avant/après) : un nouveau compte au mot de passe, jamais migré,
# sur une box DÉJÀ durcie — le reconfigure ne doit pas re-bloquer (silencieux).
./deploy/tests/e2e/multipass.sh seed-password-account legacy_svc
./deploy/tests/e2e/multipass.sh harden                        # doit RÉUSSIR À NOUVEAU
./deploy/tests/e2e/multipass.sh verify-preflight already-hardened legacy_svc
./deploy/tests/e2e/multipass.sh down
```

`harden` invoque le wrapper autonome `deploy/harden.sh` — ce qui exerce aussi le
critère « le wrapper bénéficie du même préflight que l'installeur ».

### Scénario e2e : composant relais (#657)

`install.sh --relais` pose le socle commun + le composant relais seul (mini-compose
tag-pinné `RELAIS_VERSION`, timer systemd) — sans domaine, sans Caddy, sans push réel
vers un partenaire. Il doit refuser tant que la clé SSH partenaire n'est pas au chemin
de convention, en 600 :

```bash
./deploy/tests/e2e/multipass.sh up
./deploy/tests/e2e/multipass.sh run --slug relais --relais --deploy-repo <url>
                                              # doit ÉCHOUER (clé SSH partenaire absente)
./deploy/tests/e2e/multipass.sh verify-relais refuse relais
./deploy/tests/e2e/multipass.sh seed-relais-key relais
./deploy/tests/e2e/multipass.sh run --slug relais --relais --deploy-repo <url>
                                              # doit RÉUSSIR
./deploy/tests/e2e/multipass.sh verify-relais posed relais
./deploy/tests/e2e/multipass.sh down
```

`<url>` : un vrai dépôt de déploiement privé (secrets-as-code, ADR-0044) — l'identité
de la box + le trousseau AES mutualisé sont requis même côté relais seul.

## Aller-retour crypto onboarding (vrais binaires, anti-régression #453)

```bash
./deploy/tests/secrets_roundtrip.sh
```

Exerce le chemin **HOST-CRYPTO bout-en-bout** avec de vrais `sops` + `age-keygen` (pas les fakes) :
`generate_box_identities` → `sops encrypt` → `box_can_decrypt` / `_ingestion_read_scheduler_key` + assert négatif (mauvaise clé → échec).

**En CI** (job `deploy-tests`) : les outils sont installés (mêmes versions pinnées que le job `test`) → le test tourne pour de VRAI et une régression crypto casse CI.

**En local sans sops/age** : skip propre (`skipped: sops/age absent`, exit 0) — pas de régression locale.

### Ce que la vérification couvre

| Vérification | Fichier | Vecteur de régression gardé |
|---|---|---|
| Aller-retour crypto réel (generate → encrypt → decrypt) | `secrets_roundtrip.sh` | Outil hôte absent, `box_can_decrypt` cassé, clé scheduler non extraite |
| Assert négatif (autre clé → échec) | `secrets_roundtrip.sh` | `box_can_decrypt` serait un rubber-stamp |
| Fake docker modèle le fail-fast de l'entrypoint | `fixtures/fake_bin/docker` + `unit.sh` | Crypto reroutée via l'image sans bypass |
| Bypass `ELECTRICORE_DECRYPT=off` toujours fonctionnel | `unit.sh` (section fake docker #453) | Smoke d'importabilité cassé |

### Ce que la vérification ne couvre PAS délibérément

- **Vrai Docker / runtime de l'image** : couvert par `tests/integration/test_entrypoint_dechiffrement.py` (job `test` de CI). Pas de vrai `docker` ni `docker compose` dans le job `deploy-tests`.
- **Pull du dépôt de déploiement réel** : couvert par les tests e2e (VM Multipass ou VPS). `pull_deploy_repo` utilise le fake `git` en tests unitaires.
- **Validation du contenu de secrets.env** (format des clés) : SSOT pydantic (`electricore/config/runtime.py`), vérifié par le conteneur au runtime (ADR-0049).

## Cycle de dev recommandé

1. Modifier `deploy/lib/<fichier>.sh` ou `deploy/install.sh`.
2. Si on a touché à une fonction pure : `./deploy/tests/unit.sh` (en boucle, ~instantané).
3. Si on a touché au crypto onboarding : `./deploy/tests/secrets_roundtrip.sh` (nécessite sops + age).
4. Si on a touché à l'orchestration : `./deploy/tests/e2e/multipass.sh run` (puis `restore` pour réessayer).
5. Avant push : `unit.sh` + `secrets_roundtrip.sh` doivent être verts ; e2e idéalement aussi.

## Ajouter une fonction + son test

1. Ajouter la fonction dans `deploy/lib/<fichier>.sh`.
2. Ajouter 2-3 assertions dans `deploy/tests/unit.sh` (cas nominal + cas d'échec).
3. `./deploy/tests/unit.sh` doit afficher `N passed, 0 failed`.
