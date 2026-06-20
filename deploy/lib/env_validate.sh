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
# Vérifie INSTANCE_SLUG matche expected, API_KEY, SFTP__URL et le trousseau AES
# (AES__TROUSSEAU__<label>__{KEY,IV}) sont présents et valides. Imprime les erreurs
# sur stdout. Retour : 0 si valide, 1 sinon.
validate_env_file() {
    local file="$1"
    local expected_slug="$2"
    local errors=()

    [[ -r "$file" ]] || { echo "fichier introuvable : $file"; return 1; }

    local slug api_key sftp
    slug=$(read_env_var "$file" INSTANCE_SLUG)
    api_key=$(read_env_var "$file" API_KEY)
    sftp=$(read_env_var "$file" SFTP__URL)

    [[ "$slug" == "$expected_slug" ]] || \
        errors+=("INSTANCE_SLUG='${slug}' ne matche pas le slug attendu '${expected_slug}'")

    validate_api_key "$api_key" || \
        errors+=("API_KEY manquant ou trop court (≥ 32 caractères requis)")

    validate_url "$sftp" || \
        errors+=("SFTP__URL invalide (attendu sftp://… ou file://…) : '${sftp}'")

    # Trousseau AES (ADR-0037, ADR-0040) : ≥ 1 clé AES__TROUSSEAU__<label>__KEY en hex 32
    # (AES-128) ou 64 (AES-256). L'IV (__IV) est OPTIONNEL : présent ⇒ schéma IV-fixe ;
    # absent ⇒ schéma IV-préfixé (AES-256, l'IV est en tête de chaque fichier). Les <label>
    # sont arbitraires → on les découvre dans le fichier (lignes `#` ignorées par l'ancrage).
    local labels label key iv
    mapfile -t labels < <(
        grep -oE '^[[:space:]]*AES__TROUSSEAU__.+__KEY[[:space:]]*=' "$file" 2>/dev/null \
            | sed -E 's/^[[:space:]]*AES__TROUSSEAU__(.+)__KEY[[:space:]]*=.*/\1/' | sort -u
    )
    if [[ ${#labels[@]} -eq 0 ]]; then
        errors+=("trousseau AES vide : ajoutez au moins une clé AES__TROUSSEAU__<label>__KEY (hex 32 ou 64)")
    else
        for label in "${labels[@]}"; do
            key=$(read_env_var "$file" "AES__TROUSSEAU__${label}__KEY")
            iv=$(read_env_var "$file" "AES__TROUSSEAU__${label}__IV")
            if ! validate_aes_key "$key"; then
                errors+=("AES__TROUSSEAU__${label}__KEY absent ou pas en hex 32/64 chars")
            elif [[ -n "$iv" ]] && ! validate_aes_iv "$iv"; then
                # IV optionnel (ADR-0040) : absent ⇒ schéma IV-préfixé. Présent ⇒ doit être valide.
                errors+=("AES__TROUSSEAU__${label}__IV présent mais pas en hex 32/64 (retire-le pour le schéma IV-préfixé AES-256)")
            fi
        done
    fi

    if [[ ${#errors[@]} -gt 0 ]]; then
        printf '%s\n' "${errors[@]}"
        return 1
    fi
    return 0
}

# strip_validation_error_block <env_file>
# Supprime le bloc d'erreurs (marqueurs VALIDATION-ERROR-BLOCK-BEGIN/END et
# son contenu) du .env, quand la validation vient de réussir. Idempotent :
# no-op si aucun bloc n'est présent. Les clés et séparateurs # === sont préservés.
strip_validation_error_block() {
    local env_file="$1"
    local start='# >>>VALIDATION-ERROR-BLOCK-BEGIN<<<'
    local end='# >>>VALIDATION-ERROR-BLOCK-END<<<'
    # Vérifie si le bloc est présent avant de réécrire le fichier
    grep -qF "$start" "$env_file" 2>/dev/null || return 0
    local owner; owner=$(stat -c '%u:%g' "$env_file" 2>/dev/null || echo "")
    local tmp; tmp=$(mktemp)
    # Supprime le bloc (début inclus, fin incluse) et la ligne vide qui le suit
    sed "/${start}/,/${end}/d" "$env_file" | sed '/./,$!d' > "$tmp"
    mv "$tmp" "$env_file"
    [[ -n "$owner" ]] && chown "$owner" "$env_file" 2>/dev/null || true
    chmod 600 "$env_file"
}

# prepend_errors_to_env <env_file> <errors_text>
# Réécrit le .env avec un bloc d'erreurs en tête (commenté), pour guider la
# ré-édition de l'utilisateur.
prepend_errors_to_env() {
    local env_file="$1"
    local errors="$2"
    local owner; owner=$(stat -c '%u:%g' "$env_file" 2>/dev/null || echo "")
    local tmp; tmp=$(mktemp)
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
    [[ -n "$owner" ]] && chown "$owner" "$env_file" 2>/dev/null || true
    chmod 600 "$env_file"
}
