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
            "Copier la clé PRIVÉE dédiée sur cette box (seule sa PUBLIQUE, installée chez le partenaire, est valide — jamais l'inverse) :
     box durcie (défaut) : scp <clé> ops@<box>:/tmp/relais_ssh_key
                           puis : ssh ops@<box> 'sudo install -m 600 /tmp/relais_ssh_key ${path} && sudo rm /tmp/relais_ssh_key'
     non durcie          : scp <clé> root@<box>:${path} && chmod 600 ${path}
   puis relancer. L'installeur ne génère ni ne copie jamais de clé privée."
    fi
    relais_ssh_key_mode_ok "$path" || \
        die "clé SSH partenaire trop permissive (${path})." \
            "chmod 600 ${path} puis relancer."
    chown "${CONTAINER_UID:-1000}:${CONTAINER_GID:-1000}" "$path" 2>/dev/null || true
    log_ok "clé SSH partenaire présente, verrouillée (${path})"
}

# ensure_relais_flux_deposit <dir>
# Dépôt local des flux (cas RELAIS__SOURCE_URL=file://…, nominal Enargia) : créé s'il
# manque (lisible par le conteneur, CONTAINER_UID) ; un dépôt EXISTANT n'est JAMAIS
# modifié — sur la box Enargia c'est /flux/enedis, le répertoire de production où
# Enedis dépose : ses droits appartiennent au SFTP en place, pas à l'installeur (on
# avertit seulement si le conteneur ne pourra pas le lire).
ensure_relais_flux_deposit() {
    local dir="$1"
    if [[ -d "$dir" ]]; then
        if [[ ${EUID:-0} -eq 0 ]] && ! sudo -u "#${CONTAINER_UID:-1000}" test -r "$dir" -a -x "$dir" 2>/dev/null; then
            log_warn "dépôt ${dir} illisible par le conteneur (uid ${CONTAINER_UID:-1000}) — le relais échouera bruyamment au listing ; ouvrir la lecture (groupe/ACL) sans toucher au SFTP en place."
        else
            log_ok "dépôt local des flux présent (${dir}) — droits laissés tels quels"
        fi
        return 0
    fi
    mkdir -p "$dir"
    chmod 750 "$dir"
    chown "${CONTAINER_UID:-1000}:${CONTAINER_GID:-1000}" "$dir" 2>/dev/null || true
    log_ok "dépôt local des flux créé (${dir}, lecture conteneur)"
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
# volume n'est jamais recréé par un changement de tag d'image). Monté sur /data (chemin
# que le Dockerfile chown pour electricore uid 1000) : un volume nommé frais hérite de
# cette propriété — tout autre point de montage naîtrait root:root, journal inécrivable.
# Pas de collision avec duckdb_data de la stack : projet compose distinct (name: explicite
# ci-dessous — sans lui le namespace dériverait du nom de dossier).

name: electricore-relais

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
      RELAIS__DESTINATION_DB: /data/relais.duckdb
      # Secrets SOPS+age (ADR-0044) : l'entrypoint déchiffre secrets.env — trousseau AES
      # mutualisé avec la stack (une rotation Enedis atteint les deux composants).
      SOPS_AGE_KEY_FILE: /run/secrets/age.key
      SECRETS_ENV_FILE: /run/secrets/secrets.env
    volumes:
      - relais_data:/data
      - ../../age.key:/run/secrets/age.key:ro
      - ../../providers/${INSTANCE_SLUG:-electricore}/secrets.env:/run/secrets/secrets.env:ro
      # Clé SSH partenaire — montée au nom par défaut attendu par paramiko (le code du
      # relais ne passe aucun key_filename ; fsspec/paramiko ne cherche que les noms
      # standards) dans le HOME du user conteneur (electricore, uid 1000, home /app —
      # cf. deploy/docker/Dockerfile `useradd --home-dir /app`). Présence + droits (600)
      # vérifiés par l'installeur avant le premier run (jamais générée ici, #657).
      - ../../relais_ssh_key:/app/.ssh/id_ed25519:ro
      # Dépôt local des flux (RELAIS__SOURCE_URL=file://…, cas nominal Enargia) : monté
      # ro au MÊME chemin dedans/dehors — l'URL de config.env vaut telle quelle des deux
      # côtés. FLUX_DEPOSIT_DIR (config.env, garde de cohérence env_validate) désigne le
      # dossier réel — Enargia : /flux/enedis, arborescence par flux OK (listing récursif
      # **/*.zip, filtre sur le NOM des zips). Créé par l'installeur s'il manque ; un
      # dépôt EXISTANT garde ses droits (avertissement si illisible uid 1000). Inerte si
      # la source est un SFTP distant (dossier vide).
      - ${FLUX_DEPOSIT_DIR:-/srv/electricore/flux-deposit}:${FLUX_DEPOSIT_DIR:-/srv/electricore/flux-deposit}:ro

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
# Alerte mail si le run échoue (#659, câblée sur ce rendu conteneurisé par #668) —
# voir render_relais_alerte_service / electricore-relais-alerte.service.
OnFailure=electricore-relais-alerte.service

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

# ─── Alerte mail OnFailure= (#659, câblée sur ce layout conteneurisé par #668) ──────
# L'alerte a été conçue pour le chemin bare-metal (deploy/relais/electricore-relais-
# alerte.service lisait /etc/electricore-relais/relais.env, qui n'existe pas ici) — le
# composant conteneurisé (#657) ne l'activait pas. render_relais_alerte_service reprend
# EXACTEMENT le même hook shell+msmtp (render_relais_alerte_script, sans Python : il
# doit survivre à un docker/SOPS cassé) mais pointe son EnvironmentFile= sur le
# config.env réel du layout (#657), où RELAIS_ALERTE_MAILS est déjà prévu.

# render_relais_alerte_script
# Émet le hook d'alerte OnFailure= sur stdout — statique (identique quel que soit le
# slug), mais RENDU (pas un fichier fetché à part) : install.sh ne télécharge que
# deploy/lib/*.sh (fetch_lib_files) ; ce script doit donc voyager comme heredoc, au
# même titre que render_relais_compose.
render_relais_alerte_script() {
    cat <<'EOF'
#!/usr/bin/env bash
# Hook d'alerte OnFailure= du relais (#659, câblé sur le layout conteneurisé par #668) :
# mail vers les destinataires de RELAIS_ALERTE_MAILS (config.env du provider, #657)
# quand electricore-relais.service échoue (run aveugle, #643 — voir
# electricore/ingestion/relais/pipeline.py). Volontairement SANS Python : le scénario
# où l'alerte est la plus nécessaire est précisément celui où le conteneur/docker/SOPS
# est en panne — ce script tourne host-level, hors du conteneur du relais.
set -euo pipefail

UNIT="electricore-relais.service"

# systemd ne coupe PAS les commentaires de fin de ligne dans EnvironmentFile= (à la
# différence de dotenv/compose) : un « a@x.fr  # ops » écrit à la main passerait « # »
# et « ops » à msmtp comme destinataires → échec, AUCUN mail — précisément quand on en
# a besoin (revue #669). On ampute nous-mêmes, et une valeur réduite à un commentaire
# retombe sur le chemin « vide ».
RELAIS_ALERTE_MAILS="${RELAIS_ALERTE_MAILS:-}"
RELAIS_ALERTE_MAILS="${RELAIS_ALERTE_MAILS%%#*}"

if [[ -z "${RELAIS_ALERTE_MAILS//[[:space:],]/}" ]]; then
    echo "electricore-relais-alerte: RELAIS_ALERTE_MAILS absent/vide — aucun mail envoyé" >&2
    exit 0
fi

# ${HOSTNAME} (posé par bash lui-même) et pas $(hostname) : sous set -e, un binaire
# hostname absent ou en échec tuerait le script AVANT le moindre envoi.
sujet="[electricore] échec de ${UNIT} sur ${HOSTNAME:-inconnu}"
corps="$(journalctl -u "$UNIT" --no-pager -n 50 2>/dev/null)" || corps="(journalctl indisponible pour ${UNIT})"

# CSV → tableau bash : msmtp attend les destinataires en arguments séparés. L'espace
# dans IFS absorbe le « a@x.fr, b@y.fr » écrit à la main (sinon msmtp reçoit " b@y.fr").
IFS=', ' read -ra destinataires <<< "$RELAIS_ALERTE_MAILS"

{
    printf 'To: %s\n' "$RELAIS_ALERTE_MAILS"
    printf 'Subject: %s\n' "$sujet"
    printf '\n%s\n' "$corps"
} | msmtp --file=/etc/electricore-relais/msmtprc -- "${destinataires[@]}"
EOF
}

# render_relais_alerte_msmtprc <slug> <host> <port> <from> <user>
# Émet le msmtprc du hook d'alerte sur stdout — pure comme les autres render_* de ce
# fichier (aucun accès disque/réseau), mais PARAMÉTRÉE par les valeurs non-secrètes de
# config.env (ALERTE__SMTP__{HOST,PORT,FROM,USER}, ADR-0046 §7 : routage = config.env,
# clair) : c'est install_relais_alerte_units qui les lit (read_env_var) et les passe ici
# — même découpage caller-fait-l'I/O / rendu-reste-pur que render_relais_service.
#
# Le token SMTP (ALERTE__SMTP__PASSWORD, secrets.env chiffré) n'apparaît JAMAIS dans ce
# fichier (#674 — c'était le dernier secret hors secrets-as-code de tout le composant
# relais, objection Virgile posée en revue Enargia le 28/07) : `passwordeval` l'extrait
# à l'ENVOI par un `sops decrypt` HÔTE (même motif que _ingestion_read_scheduler_key,
# deploy/lib/ingestion.sh) — SOPS_AGE_KEY_FILE pointe la clé age de LA BOX
# (/srv/<slug>/age.key, déjà là depuis l'onboarding, ADR-0044), sur le secrets.env déjà
# pullé (providers/<slug>/secrets.env). `bash -o pipefail -c '...'` : si le sops hôte
# échoue (clé absente/invalide, champ manquant), le PIPE entier remonte son code de
# sortie non-zéro (sans pipefail, seul le code de `head -1`, toujours 0, survivrait) —
# msmtp voit passwordeval échouer, logue bruyamment sur stderr (capté par
# `journalctl -u electricore-relais-alerte.service`), pas de crash silencieux : mode
# dégradé assumé (cf. deploy/relais/README.md « Alerte mail »).
render_relais_alerte_msmtprc() {
    local slug="$1" host="$2" port="$3" from="$4" user="$5"
    local home="${SRV_BASE:-/srv}/${slug}"
    local secrets="${home}/providers/${slug}/secrets.env"
    local agekey="${home}/age.key"
    cat <<EOF
# ElectriCore — msmtprc du hook d'alerte relais (#659/#668/#674). RENDU par install.sh
# --relais (deploy/lib/relais.sh::render_relais_alerte_msmtprc) — ne pas éditer à la
# main, régénéré à chaque reconfigure. Le token SMTP n'est JAMAIS écrit ici en clair :
# passwordeval l'extrait de secrets.env (chiffré SOPS+age) à l'envoi (#674).
defaults
tls on
tls_starttls on

account electricore-relais
host ${host}
port ${port}
auth on
from ${from}
user ${user}
passwordeval bash -o pipefail -c "SOPS_AGE_KEY_FILE=${agekey} sops decrypt --input-type dotenv --output-type dotenv ${secrets} | sed -n s/^ALERTE__SMTP__PASSWORD=//p | head -1"

account default : electricore-relais
EOF
}

# render_relais_alerte_service <slug>
# Émet l'unité systemd du service d'alerte sur stdout — ADAPTÉE de l'ancienne unité
# bare-metal (#659) : EnvironmentFile= pointe désormais sur le config.env du layout
# conteneurisé (#657, ${SRV_BASE:-/srv}/<slug>/config.env — où vit déjà
# RELAIS_ALERTE_MAILS) au lieu de /etc/electricore-relais/relais.env, qui n'existe pas
# dans ce layout. Paramétrée par slug comme render_relais_service ; msmtprc (chemin de
# convention host-level /etc/electricore-relais/, un seul token SMTP par box, pas par
# instance) est désormais RENDUE elle aussi (#674, render_relais_alerte_msmtprc) — plus
# aucune édition manuelle.
render_relais_alerte_service() {
    local slug="$1"
    local home="${SRV_BASE:-/srv}/${slug}"
    cat <<EOF
[Unit]
Description=Alerte mail sur échec du relais (OnFailure=, #659, câblée conteneur #668)
# Déclenchée exclusivement par OnFailure=electricore-relais-alerte.service posé
# sur electricore-relais.service — jamais lancée directement par le timer.

[Service]
Type=oneshot
# Pas de User= : root par défaut, nécessaire pour lire le journal de l'unité du
# relais (journalctl -u electricore-relais.service) et msmtprc en 600.
# RELAIS_ALERTE_MAILS vit dans le MÊME config.env que le relais (#657/#668) :
# modifier les destinataires se fait en éditant ce seul fichier, sans toucher aux
# unités ni au script.
EnvironmentFile=${home}/config.env
ExecStart=/usr/local/bin/electricore-relais-alerte.sh
EOF
}

# install_relais_alerte_units <slug>
# Pose le hook d'alerte (script + unité) — pendant #668 de install_relais_units.
# N'active PAS l'unité elle-même (pas d'enable --now : electricore-relais-alerte.service
# ne se déclenche que via OnFailure=, jamais par un timer). Idempotent : régénère les
# deux fichiers à chaque appel. Le paquet msmtp est une dépendance DU COMPOSANT (#668) :
# ensure_packages l'installe ici (no-op s'il est déjà là) ; seul
# /etc/electricore-relais/msmtprc (600, token SMTP) reste posé à la main — jamais dans
# git ni dans l'image, voir deploy/relais/README.md.
install_relais_alerte_units() {
    local slug="$1"
    ensure_packages msmtp
    install -d -m 700 /etc/electricore-relais
    # Pose atomique (install = unlink + nouvel inode) : le hook peut être EN COURS
    # d'exécution pendant un reconfigure — un « > » direct le tronquerait sous bash,
    # qui lit les scripts au fil de l'eau (précédent : grant_nopasswd_sudo, harden.sh).
    local tmp
    tmp="$(mktemp)"
    render_relais_alerte_script > "$tmp"
    install -m 0755 "$tmp" /usr/local/bin/electricore-relais-alerte.sh
    rm -f "$tmp"
    render_relais_alerte_service "$slug" > /etc/systemd/system/electricore-relais-alerte.service
    systemctl daemon-reload
    log_ok "hook d'alerte mail posé (/usr/local/bin/electricore-relais-alerte.sh, electricore-relais-alerte.service) — /etc/electricore-relais/msmtprc (600, token SMTP) reste à poser à la main (deploy/relais/README.md)"
}
