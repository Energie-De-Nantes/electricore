# shellcheck shell=bash
# Démarrage de la stack docker compose et attente du healthcheck.
# La stack est exécutée en tant que <slug> (membre du groupe docker).

# compose_up <slug>
# Lance `docker compose --env-file ../../config.env up -d` depuis /srv/<slug>/deploy/docker/
# en tant que <slug>. Le `--env-file` est requis pour que les substitutions
# ${INSTANCE_SLUG}/${BACKUPS_PATH}/${ELECTRICORE_VERSION} soient résolues
# (cf. commentaire en tête de docker-compose.yml). Depuis ADR-0044, la source des
# substitutions est `config.env` (clair) — plus de `.env` ; les credentials vivent
# dans `secrets.env` (chiffré, déchiffré par l'entrypoint de l'image).
compose_up() {
    local slug="$1"
    local home="/srv/${slug}"
    # On utilise `sudo -u` pour matcher l'uid host du user (cohérence permissions
    # bind-mount). Note : le user <slug> est dans le groupe docker (cf. user.sh).
    sudo -u "$slug" -- bash -c \
        "cd '${home}/deploy/docker' && docker compose --env-file ../../config.env up -d"
    # Le Caddyfile est un bind-mount PAR INODE : `up -d` ne recrée pas le conteneur quand
    # seul le contenu change, et `sed -i` (substitute_caddyfile) écrit un NOUVEL inode —
    # le conteneur reste attaché à l'ancien, c.-à-d. le template brut écrit par curl.
    # Un `caddy reload` chargerait donc le template non substitué (vécu sur enargia,
    # #707 : domaine principal tombé). Seule une recréation re-résout le chemin.
    # ~1 s de coupure ; les certificats survivent dans le volume caddy_data.
    sudo -u "$slug" -- bash -c \
        "cd '${home}/deploy/docker' && docker compose --env-file ../../config.env up -d --force-recreate caddy"
}

# wait_for_health <slug> [<max_retries>] [<delay_seconds>]
# Poll le healthcheck de l'API jusqu'à `database.accessible: true` ou autre OK.
# Défauts : 30 retries × 2s = 60s max.
wait_for_health() {
    local slug="$1"
    local max="${2:-30}"
    local delay="${3:-2}"
    local i=1
    while (( i <= max )); do
        # On interroge l'API directement via le conteneur (pas par le domaine
        # public — on évite les soucis de certificat pendant l'init Caddy).
        local resp
        resp=$(sudo -u "$slug" -- bash -c \
            "cd /srv/${slug}/deploy/docker && docker compose exec -T api \
             curl -fsS http://localhost:8001/health" 2>/dev/null) || true
        if [[ -n "$resp" ]] && echo "$resp" | grep -q '"status"[[:space:]]*:[[:space:]]*"ok"'; then
            log_ok "API healthy après ${i} tentatives"
            return 0
        fi
        (( i < max )) && sleep "$delay"
        i=$((i + 1))
    done
    return 1
}
