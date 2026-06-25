# shellcheck shell=bash
# Validation du split config/secret d'une instance (secrets-as-code, ADR-0044) : la moitié
# CLAIRE (config.env, versionnée) et la moitié SECRÈTE (secrets.env, chiffrée SOPS+age).
# Chaque validateur lit le fichier, vérifie ses variables obligatoires, renvoie 0 si OK,
# 1 si erreurs (et liste les erreurs sur stdout).
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

# ─────────────────────────────────────────────────────────────────────────────
# Split config/secret (ADR-0044) : la config d'instance est scindée en deux.
#   config.env  (clair, versionné)  — config NON secrète + substitutions compose
#   secrets.env (chiffré SOPS+age)  — UNIQUEMENT des credentials
# Les deux fonctions ci-dessous valident chaque moitié.
# ─────────────────────────────────────────────────────────────────────────────

# validate_config_env <config_file> <expected_slug>
# Valide la moitié CLAIRE : INSTANCE_SLUG (matche), ELECTRICORE_VERSION et
# BACKUPS_PATH présents. AUCUN secret ici (sinon erreur — un secret en clair dans
# config.env est une fuite). Imprime les erreurs sur stdout ; 0 si OK, 1 sinon.
validate_config_env() {
    local file="$1"
    local expected_slug="$2"
    local errors=()

    [[ -r "$file" ]] || { echo "config.env introuvable : $file"; return 1; }

    local slug version backups
    slug=$(read_env_var "$file" INSTANCE_SLUG)
    version=$(read_env_var "$file" ELECTRICORE_VERSION)
    backups=$(read_env_var "$file" BACKUPS_PATH)

    [[ "$slug" == "$expected_slug" ]] || \
        errors+=("INSTANCE_SLUG='${slug}' ne matche pas le slug attendu '${expected_slug}'")
    [[ -n "$version" ]] || errors+=("ELECTRICORE_VERSION manquant (substitution compose)")
    [[ -n "$backups" ]] || errors+=("BACKUPS_PATH manquant (substitution compose)")

    # Garde-fou anti-fuite : un credential n'a RIEN à faire dans config.env (clair).
    local leaked
    leaked=$(grep -oE '^[[:space:]]*(API__TROUSSEAU__|API_KEY|API_KEYS|SFTP__URL|BOT__(TOKEN|ALLOWED_USERS)|AES__TROUSSEAU__|ODOO__PASSWORD)' "$file" 2>/dev/null | head -1)
    [[ -z "$leaked" ]] || \
        errors+=("secret en clair détecté dans config.env (« ${leaked} ») — il doit vivre dans secrets.env chiffré")

    if [[ ${#errors[@]} -gt 0 ]]; then
        printf '%s\n' "${errors[@]}"
        return 1
    fi
    return 0
}

# validate_secrets_plaintext <plaintext_file>
# Valide la moitié SECRÈTE déjà DÉCHIFFRÉE (dotenv en clair, jamais sur disque en prod) :
# trousseau API (ADR-0046 §4), SFTP__URL, trousseau AES (ADR-0037/0040, __IV optionnel)
# présents et valides. Imprime les erreurs sur stdout ; 0 si OK, 1 sinon.
validate_secrets_plaintext() {
    local file="$1"
    local errors=()

    [[ -r "$file" ]] || { echo "secrets (clair) introuvable : $file"; return 1; }

    local sftp
    sftp=$(read_env_var "$file" SFTP__URL)

    # Trousseau API (ADR-0046 §4) : ≥ 1 clé API__TROUSSEAU__<consommateur>__KEY (≥ 32 chars).
    local api_labels api_label api_cle
    mapfile -t api_labels < <(
        grep -oE '^[[:space:]]*API__TROUSSEAU__.+__KEY[[:space:]]*=' "$file" 2>/dev/null \
            | sed -E 's/^[[:space:]]*API__TROUSSEAU__(.+)__KEY[[:space:]]*=.*/\1/' | sort -u
    )
    if [[ ${#api_labels[@]} -eq 0 ]]; then
        errors+=("trousseau API vide : ajoutez au moins une clé API__TROUSSEAU__<consommateur>__KEY (≥ 32 caractères)")
    else
        for api_label in "${api_labels[@]}"; do
            api_cle=$(read_env_var "$file" "API__TROUSSEAU__${api_label}__KEY")
            validate_api_key "$api_cle" || \
                errors+=("API__TROUSSEAU__${api_label}__KEY absent ou trop court (≥ 32 caractères)")
        done
    fi

    validate_url "$sftp" || \
        errors+=("SFTP__URL invalide (attendu sftp://… ou file://…) : '${sftp}'")

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

# validate_secrets_env <secrets_file> <age_keyfile>
# Déchiffre <secrets_file> (SOPS+age) avec <age_keyfile> et valide le clair via
# `validate_secrets_plaintext`. 0 si OK, 1 sinon (erreurs stdout). NB : chemin de
# validation côté MACHINE ADMIN à la (re)config — PAS le runtime (l'entrypoint
# `sops exec-env` n'écrit, lui, jamais de clair). Ici le clair transite par un tmp 0600
# shreddé immédiatement (cf. plus bas pourquoi un fichier et pas un pipe).
validate_secrets_env() {
    local file="$1"
    local keyfile="$2"
    [[ -r "$file" ]]    || { echo "secrets.env introuvable : $file"; return 1; }
    [[ -r "$keyfile" ]] || { echo "clé age introuvable : $keyfile"; return 1; }
    local plaintext
    if ! plaintext=$(SOPS_AGE_KEY_FILE="$keyfile" sops decrypt --input-type dotenv --output-type dotenv "$file" 2>/dev/null); then
        echo "déchiffrement SOPS échoué (clé age non destinataire ? fichier corrompu ?)"
        return 1
    fi
    # validate_secrets_plaintext relit son argument plusieurs fois (un read_env_var/grep
    # par variable) → une substitution de process (pipe à lecture unique) ne convient pas.
    # Le clair touche donc un tmp 0600 (mktemp), shreddé immédiatement après validation.
    # (Pas de trap de nettoyage ici : install.sh possède déjà un `trap … INT TERM` au
    #  niveau process qu'on ne doit pas écraser depuis une fonction de lib.)
    local tmp; tmp=$(mktemp)
    printf '%s\n' "$plaintext" > "$tmp"
    local rc=0 out
    out=$(validate_secrets_plaintext "$tmp") || rc=1
    shred -u "$tmp" 2>/dev/null || rm -f "$tmp"
    [[ -n "$out" ]] && printf '%s\n' "$out"
    return "$rc"
}
