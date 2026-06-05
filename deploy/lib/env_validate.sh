# shellcheck shell=bash
# Validation post-édition d'un .env d'instance. Lit le fichier, vérifie chaque
# variable obligatoire selon les règles décrites dans .env.example.
# Renvoie 0 si OK, 1 si erreurs (et liste les erreurs sur stdout).
#
# Dépend de validate.sh (validate_slug, validate_aes_key, validate_api_key, validate_url).

# read_env_var <env_file> <key>
# Extrait la valeur de <key> dans le .env (gère KEY=value avec/sans guillemets,
# ignore les # comments en fin de ligne).
read_env_var() {
    local file="$1"
    local key="$2"
    awk -v k="$key" -F= '
        $1 == k {
            value=""
            for (i=2; i<=NF; i++) { value = value (i>2 ? "=" : "") $i }
            sub(/[[:space:]]+#.*$/, "", value)
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            gsub(/^["]|["]$/, "", value)
            print value
            exit
        }
    ' "$file"
}

# validate_env_file <env_file> <expected_slug>
# Vérifie INSTANCE_SLUG matche expected, API_KEY, AES__CURRENT__{KEY,IV},
# SFTP__URL sont présents et valides. Imprime les erreurs sur stdout.
# Retour : 0 si valide, 1 sinon.
validate_env_file() {
    local file="$1"
    local expected_slug="$2"
    local errors=()

    [[ -r "$file" ]] || { echo "fichier introuvable : $file"; return 1; }

    local slug api_key sftp aes_key aes_iv
    slug=$(read_env_var "$file" INSTANCE_SLUG)
    api_key=$(read_env_var "$file" API_KEY)
    sftp=$(read_env_var "$file" SFTP__URL)
    aes_key=$(read_env_var "$file" AES__CURRENT__KEY)
    aes_iv=$(read_env_var "$file" AES__CURRENT__IV)

    [[ "$slug" == "$expected_slug" ]] || \
        errors+=("INSTANCE_SLUG='${slug}' ne matche pas le slug attendu '${expected_slug}'")

    validate_api_key "$api_key" || \
        errors+=("API_KEY manquant ou trop court (≥ 32 caractères requis)")

    validate_url "$sftp" || \
        errors+=("SFTP__URL invalide (attendu sftp://… ou file://…) : '${sftp}'")

    # Format v1 (AES__KEY/IV) accepté en fallback pour compatibilité ascendante
    if [[ -z "$aes_key" ]]; then
        aes_key=$(read_env_var "$file" AES__KEY)
        aes_iv=$(read_env_var "$file" AES__IV)
    fi
    validate_aes_key "$aes_key" || \
        errors+=("AES__CURRENT__KEY (ou AES__KEY) absent ou pas en hex 32/64 chars")
    validate_aes_iv "$aes_iv" || \
        errors+=("AES__CURRENT__IV (ou AES__IV) absent ou pas en hex 32/64 chars")

    if [[ ${#errors[@]} -gt 0 ]]; then
        printf '%s\n' "${errors[@]}"
        return 1
    fi
    return 0
}

# prepend_errors_to_env <env_file> <errors_text>
# Réécrit le .env avec un bloc d'erreurs en tête (commenté), pour guider la
# ré-édition de l'utilisateur.
prepend_errors_to_env() {
    local env_file="$1"
    local errors="$2"
    local tmp
    tmp=$(mktemp)
    # Markers uniques (improbables dans un .env utilisateur) pour pouvoir
    # supprimer un bloc précédent sans toucher aux séparateurs `# ===` de
    # sections, présents naturellement dans .env.example.
    local start='# >>>VALIDATION-ERROR-BLOCK-BEGIN<<<'
    local end='# >>>VALIDATION-ERROR-BLOCK-END<<<'
    {
        echo "$start"
        echo "# VALIDATION ÉCHOUÉE — corrige ces points puis ferme l'éditeur"
        while IFS= read -r line; do
            [[ -n "$line" ]] && echo "#  x $line"
        done <<< "$errors"
        echo "$end"
        echo
        # Supprime un éventuel bloc d'erreurs précédent (idempotence des relances).
        sed "/${start}/,/${end}/d" "$env_file"
    } > "$tmp"
    mv "$tmp" "$env_file"
    # Conserve l'ownership précédent (sed -i / mktemp peut le casser).
    [[ -n "${ENV_OWNER:-}" ]] && chown "$ENV_OWNER" "$env_file" 2>/dev/null || true
    chmod 600 "$env_file"
}
