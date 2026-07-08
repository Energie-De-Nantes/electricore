# `deploy/lib/`

Helpers bash sourcés par [`../install.sh`](../install.sh) (et, pour le sous-ensemble
durcissement, par le wrapper autonome [`../harden.sh`](../harden.sh)). Ordre de chargement
canonique : `LIB_FILES` dans `install.sh`.

- [`log.sh`](log.sh) — helpers de sortie (couleurs ANSI désactivables via `NO_COLOR`, étapes numérotées).
- [`cli.sh`](cli.sh) — parsing des arguments de `install.sh`. Pure, pas de side-effect.
- [`validate.sh`](validate.sh) — `validate_slug` + `validate_domain` uniquement. Renvoient 0/1, pas de sortie ; le format des secrets (AES/URL/email) n'est plus validé ici, voir `env_validate.sh` ci-dessous.
- [`os.sh`](os.sh) — détection OS via `/etc/os-release`, mockable avec `OS_RELEASE_PATH=...` (utilisé par les tests).
- [`system.sh`](system.sh) — provisioning OS idempotent (paquets apt, Docker, UFW).
- [`user.sh`](user.sh) — création du user système `<slug>`, home `/srv/<slug>/`, groupe docker, clé SSH (ADR-0017).
- [`harden.sh`](harden.sh) — durcissement VPS (ADR-0031) : `harden_vps()` orchestre admin `ops` + sudo, verrouillage sshd (root-off, clé only), fail2ban, unattended-upgrades. Réversion `unharden_vps()` (retire le drop-in sshd, la jail, la conf upgrades ; `ops` conservé sauf `--purge-ops`). Fonctions de rendu pures (`render_sshd_hardening`, `render_fail2ban_jail`, …) testables, side-effects idempotents. Wrappers autonomes : [`../harden.sh`](../harden.sh) / [`../unharden.sh`](../unharden.sh).
- [`config.sh`](config.sh) — téléchargement et patch des fichiers de configuration de l'instance depuis `raw.githubusercontent.com`.
- [`env_validate.sh`](env_validate.sh) — valide la *politique* du split config/secrets (secrets-as-code, ADR-0044) : la moitié claire `config.env` (versionnée) ne porte aucun secret en clair. Le *contenu* de `secrets.env` (format des clés AES/API, URL SFTP) n'est pas validé ici mais par la SSOT pydantic (`electricore/config/runtime.py`), vérifiée par le vrai conteneur aux étapes 11-12 d'`install.sh` (ADR-0049).
- [`secrets.sh`](secrets.sh) — cœur du modèle secrets-as-code (ADR-0044) : installation des outils crypto sur l'hôte, génération des identités age + SSH de la box (clés privées nées sur la box, jamais exportées), pull du dépôt de déploiement privé, validation du split config/secret. Commandes externes (`sops`, `age`, `age-keygen`, `ssh-keygen`, `git`) appelées par leur nom → stubbées en fake-binaries par les tests.
- [`dns.sh`](dns.sh) — vérification que `<domain>` résout vers l'IP publique du VPS.
- [`stack.sh`](stack.sh) — démarrage de la stack docker compose (en tant que `<slug>`) et attente du healthcheck.
- [`ingestion.sh`](ingestion.sh) — lancement d'un test ingestion (échantillon 2 fichiers/flux) pour vérifier la chaîne SFTP → déchiffrement AES → DuckDB.

Tests : [`../tests/unit.sh`](../tests/unit.sh).
