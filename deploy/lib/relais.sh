# shellcheck shell=bash
# Composant relais de l'installeur (#637, #657, PRD #655) : mini-compose dédié,
# clé SSH partenaire (vérifiée, jamais générée), unités systemd (compose run --rm).
#
# Runtime PARTAGÉ avec la stack (même image ghcr + même entrypoint SOPS, ADR-0044) mais
# DÉCOUPLÉ : tag pinné par RELAIS_VERSION (pas ELECTRICORE_VERSION), timer systemd dédié
# (`docker compose … run --rm`, pas `up -d`) — jamais de conteneur persistant, jamais de
# domaine/Caddy (cf. install.sh). Toutes les fonctions sont idempotentes.

# ─── Clé SSH partenaire ──────────────────────────────────────────────────────
# Jamais générée ni copiée par l'installeur (#657 AC3) : seule la clé PUBLIQUE installée
# chez le partenaire (Haulogy) est valide — l'opérateur dépose la PRIVÉE au chemin de
# convention, l'installeur vérifie présence + droits et refuse sinon.

# relais_ssh_key_path <slug>
# Chemin de convention de la clé SSH PRIVÉE du partenaire, à la racine du home
# d'instance (comme age.key / ssh_deploy_key, cf. secrets.sh) — PAS dans providers/ :
# ce n'est pas un secret versionné SOPS, elle ne quitte jamais cette box.
relais_ssh_key_path() {
    printf '%s/relais_ssh_key\n' "${SRV_BASE:-/srv}/$1"
}

# relais_ssh_key_mode_ok <path>
# Vrai si <path> existe et n'expose aucun droit group/other (600, 400…). Pur — un
# `stat` suffit, testable sans être root (même précédent que authorized_keys_present,
# harden.sh).
relais_ssh_key_mode_ok() {
    local path="$1"
    [[ -f "$path" ]] || return 1
    local mode
    mode=$(stat -c '%a' "$path" 2>/dev/null) || return 1
    [[ "$mode" =~ ^[1-7]00$ ]]
}

# check_relais_ssh_key <slug>
# Refuse (die) AVANT tout si la clé SSH partenaire est absente ou trop permissive —
# l'installeur ne copie ni ne génère JAMAIS de clé privée (#657 AC3). Si présente et
# verrouillée : aligne l'ownership sur CONTAINER_UID/GID (user.sh, défaut 1000:1000)
# pour que le conteneur puisse la LIRE une fois montée ro — le code du relais ne passe
# aucun key_filename ; fsspec/paramiko ne cherche que les noms standards dans le HOME
# du user conteneur (electricore, home /app — cf. deploy/docker/Dockerfile
# `useradd --home-dir /app`).
check_relais_ssh_key() {
    local slug="$1"
    local path
    path=$(relais_ssh_key_path "$slug")
    if [[ ! -f "$path" ]]; then
        die "clé SSH partenaire absente (${path})." \
            "Copier la clé PRIVÉE dédiée sur cette box (seule sa PUBLIQUE, installée chez le partenaire, est valide — jamais l'inverse) : scp <clé> root@<box>:${path} && chmod 600 ${path} — puis relancer. L'installeur ne génère ni ne copie jamais de clé privée."
    fi
    relais_ssh_key_mode_ok "$path" || \
        die "clé SSH partenaire trop permissive (${path})." \
            "chmod 600 ${path} puis relancer."
    chown "${CONTAINER_UID:-1000}:${CONTAINER_GID:-1000}" "$path" 2>/dev/null || true
    log_ok "clé SSH partenaire présente, verrouillée (${path})"
}

# ─── Fichiers posés (mini-compose + unités systemd) ─────────────────────────
# Fonctions pures de rendu (mêmes conventions que render_sshd_hardening / render_
# fail2ban_jail dans harden.sh) : sortie sur stdout, aucun side-effect, testables en
# unitaire sans root ni VPS.

# render_relais_compose
# Émet le mini-compose du relais sur stdout. Statique (pas de paramètre) : les
# substitutions dépendantes de l'instance (${INSTANCE_SLUG}, ${RELAIS_VERSION}) sont
# résolues par `docker compose --env-file` depuis config.env, EXACTEMENT comme
# deploy/docker/docker-compose.yml (même mécanisme, ADR-0044) — pas de rendu bash pour
# elles. Volumes relatifs à deploy/relais/ (2 niveaux sous le home d'instance, comme
# deploy/docker/ pour la stack).
render_relais_compose() {
    cat <<'EOF'
# ElectriCore — composant relais (#637, #657) : recopie déchiffrée des flux Enedis vers
# le SFTP d'un partenaire. Mini-compose DÉDIÉ, séparé de deploy/docker/docker-compose.yml —
# même image ghcr + même entrypoint SOPS que la stack, mais tag épinglé par RELAIS_VERSION
# (PAS ELECTRICORE_VERSION : la stack bouge souvent, le relais quasi jamais).
#
# Généré par install.sh --relais (deploy/lib/relais.sh::render_relais_compose) — ne pas
# éditer à la main, régénéré à chaque reconfigure.
#
# Layout attendu (miroir de deploy/docker/docker-compose.yml, ADR-0017/ADR-0044) :
#   /srv/<slug>/relais_ssh_key                     ← clé SSH PRIVÉE partenaire (600,
#                                                     JAMAIS générée par l'installeur)
#   /srv/<slug>/deploy/relais/compose-relais.yml   ← ce fichier
#
# Invocation (systemd timer, run-and-remove — PAS de `up -d`) :
#   docker compose --env-file ../../config.env -f compose-relais.yml run --rm relais
#
# Le journal DuckDB du relais vit dans le volume nommé `relais_data`, PAS un bind-mount
# host : `docker compose run --rm` détruit le conteneur à chaque passage — un volume
# nommé est le seul état qui survit entre deux runs ET à un bump de RELAIS_VERSION (le
# volume n'est jamais recréé par un changement de tag d'image).

services:

  relais:
    image: ghcr.io/energie-de-nantes/electricore:${RELAIS_VERSION:-latest}
    container_name: electricore-relais
    command:
      - python
      - -m
      - electricore.ingestion.relais
    env_file:
      - ../../config.env
    environment:
      RELAIS__DESTINATION_DB: /data-relais/relais.duckdb
      # Secrets SOPS+age (ADR-0044) : l'entrypoint déchiffre secrets.env — trousseau AES
      # mutualisé avec la stack (une rotation Enedis atteint les deux composants).
      SOPS_AGE_KEY_FILE: /run/secrets/age.key
      SECRETS_ENV_FILE: /run/secrets/secrets.env
    volumes:
      - relais_data:/data-relais
      - ../../age.key:/run/secrets/age.key:ro
      - ../../providers/${INSTANCE_SLUG:-electricore}/secrets.env:/run/secrets/secrets.env:ro
      # Clé SSH partenaire — montée au nom par défaut attendu par paramiko (le code du
      # relais ne passe aucun key_filename ; fsspec/paramiko ne cherche que les noms
      # standards) dans le HOME du user conteneur (electricore, uid 1000, home /app —
      # cf. deploy/docker/Dockerfile `useradd --home-dir /app`). Présence + droits (600)
      # vérifiés par l'installeur avant le premier run (jamais générée ici, #657).
      - ../../relais_ssh_key:/app/.ssh/id_ed25519:ro
      # Mode "fichiers collocés" (RELAIS__SOURCE_URL=file://...) : décommenter et adapter
      # si la source est un répertoire de dépôt LOCAL plutôt qu'un SFTP distant — le
      # conteneur (uid 1000) doit pouvoir LIRE ce répertoire (droits hôte à vérifier, #657).
      # - /var/enedis:/var/enedis:ro

volumes:
  relais_data:
EOF
}

# render_relais_service <slug>
# Émet l'unité systemd du service relais sur stdout — ADAPTÉE de l'ancienne unité
# bare-metal (deploy/relais/electricore-relais.service, #637) : ExecStart appelle
# désormais `docker compose … run --rm` au lieu de `python -m …` directement. User=
# et WorkingDirectory= dépendent du slug (paramètre), donc RENDUE plutôt que statique
# — contrairement au mini-compose (render_relais_compose), qu'un `${VAR}` compose
# suffit à paramétrer.
render_relais_service() {
    local slug="$1"
    local home="${SRV_BASE:-/srv}/${slug}"
    cat <<EOF
[Unit]
Description=Relais de flux Enedis déchiffrés vers SFTP partenaire — conteneurisé (#637, #657)
After=docker.service network-online.target
Wants=network-online.target
Requires=docker.service

[Service]
Type=oneshot
User=${slug}
WorkingDirectory=${home}/deploy/relais
ExecStart=/usr/bin/docker compose --env-file ../../config.env -f compose-relais.yml run --rm relais
# Un échec par-zip isolé est avalé en interne par le relais (cf.
# electricore/ingestion/relais/pipeline.py) ; seul un run "aveugle" (0 push réussi,
# au moins 1 échec) sort en erreur non-zéro — docker compose run propage ce code de
# sortie jusqu'à cette unité, qui passe alors en failed (#657).
EOF
}

# render_relais_timer
# Émet l'unité systemd du timer sur stdout — inchangée par rapport à l'ancienne unité
# bare-metal (deploy/relais/electricore-relais.timer) : le balayage réconciliant ne
# dépend ni du slug ni du mode d'exécution (bare-metal ou conteneurisé).
render_relais_timer() {
    cat <<'EOF'
[Unit]
Description=Déclenche electricore-relais.service périodiquement (balayage réconciliant, #637)

[Timer]
# Balayage réconciliant : la continuité tient au RE-LISTING périodique, pas à un
# événement — un intervalle raisonnable suffit (pas de curseur à perdre : chaque
# passage relit toute la source, cf. incremental=False côté pipeline).
OnBootSec=5min
OnUnitActiveSec=15min
Persistent=true
Unit=electricore-relais.service

[Install]
WantedBy=timers.target
EOF
}

# install_relais_units <slug>
# Pose le mini-compose + les unités systemd, puis active le timer. Idempotent :
# régénère les fichiers à chaque appel (reconfigure) ; `enable --now` ne redémarre pas
# un timer déjà actif. N'exécute JAMAIS de `docker compose run` elle-même — le premier
# run réel se produit au premier déclenchement du timer (OnBootSec=5min), jamais
# pendant l'installation (cf. install.sh, pas de test ingestion côté relais).
install_relais_units() {
    local slug="$1"
    local home="${SRV_BASE:-/srv}/${slug}"
    local relais_dir="${home}/deploy/relais"
    install -d "$relais_dir"
    render_relais_compose > "${relais_dir}/compose-relais.yml"
    chown -R "${slug}:${slug}" "$relais_dir" 2>/dev/null || true
    render_relais_service "$slug" > /etc/systemd/system/electricore-relais.service
    render_relais_timer > /etc/systemd/system/electricore-relais.timer
    systemctl daemon-reload
    systemctl enable --now electricore-relais.timer
    log_ok "compose relais + timer systemd posés (${relais_dir}/compose-relais.yml, electricore-relais.timer actif)"
}
