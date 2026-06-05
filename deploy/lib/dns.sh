# shellcheck shell=bash
# Vérification que <domain> résout vers l'IP publique du VPS.
# Cf. system.sh::get_public_ip.

# resolve_a <domain> — retourne la 1ère IP A (silencieux).
resolve_a() {
    local domain="$1"
    # `dig +short` peut renvoyer plusieurs lignes (round-robin), on prend la 1ère IPv4
    dig +short A "$domain" 2>/dev/null | grep -E '^[0-9]+\.' | head -1
}

# wait_for_dns <domain> <expected_ip> [<max_retries>] [<delay_seconds>]
# Boucle jusqu'à ce que A(<domain>) == <expected_ip>. Renvoie 1 après timeout.
# Défauts : 10 retries × 30s = 5 min max.
wait_for_dns() {
    local domain="$1"
    local expected="$2"
    local max="${3:-10}"
    local delay="${4:-30}"
    local i=1
    while (( i <= max )); do
        local got
        got=$(resolve_a "$domain")
        if [[ "$got" == "$expected" ]]; then
            log_ok "DNS OK : ${domain} → ${got}"
            return 0
        fi
        log_info "tentative ${i}/${max} : ${domain} → '${got:-(rien)}', attendu '${expected}'"
        (( i < max )) && sleep "$delay"
        i=$((i + 1))
    done
    return 1
}
