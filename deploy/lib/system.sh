# shellcheck shell=bash
# Provisioning OS : apt packages, Docker, UFW. Tout est idempotent.
# Requiert root (les fonctions n'utilisent pas `sudo` — l'orchestrateur ré-exécute en root).

ensure_packages() {
    local pkgs=("$@")
    local missing=()
    for pkg in "${pkgs[@]}"; do
        dpkg -s "$pkg" >/dev/null 2>&1 || missing+=("$pkg")
    done
    if [[ ${#missing[@]} -eq 0 ]]; then
        log_skip "paquets déjà installés : ${pkgs[*]}"
        return 0
    fi
    log_info "installation : ${missing[*]}"
    DEBIAN_FRONTEND=noninteractive apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends "${missing[@]}"
    log_ok "paquets installés"
}

install_docker_if_missing() {
    if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
        log_skip "Docker + plugin compose déjà installés ($(docker --version | head -c 40)…)"
        return 0
    fi
    log_info "téléchargement et exécution de get-docker.com…"
    curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
    sh /tmp/get-docker.sh
    rm -f /tmp/get-docker.sh
    # Vérifie que compose plugin est dispo (get-docker.com l'inclut depuis 2022).
    docker compose version >/dev/null 2>&1 || die \
        "Docker installé mais plugin compose introuvable." \
        "Voir https://docs.docker.com/compose/install/"
    log_ok "Docker installé ($(docker --version | head -c 40)…)"
}

setup_ufw() {
    if ! command -v ufw >/dev/null 2>&1; then
        ensure_packages ufw
    fi
    ufw allow OpenSSH >/dev/null
    ufw allow 80/tcp  >/dev/null
    ufw allow 443/tcp >/dev/null
    ufw allow 443/udp >/dev/null  # HTTP/3
    # --force pour ne pas demander confirmation interactive
    ufw --force enable >/dev/null
    log_ok "UFW actif : OpenSSH + 80/tcp + 443/tcp + 443/udp"
}

# Retourne l'IP publique IPv4 du VPS. Plusieurs sources pour la robustesse.
get_public_ip() {
    local ip=""
    for url in https://api.ipify.org https://ifconfig.me https://icanhazip.com; do
        ip=$(curl -fsSL --max-time 5 "$url" 2>/dev/null) && [[ -n "$ip" ]] && break
    done
    [[ -n "$ip" ]] && printf '%s\n' "$ip" || return 1
}
