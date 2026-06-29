# shellcheck shell=bash
# Validation de la POLITIQUE de split d'une instance (secrets-as-code, ADR-0044) : la moitié
# CLAIRE (config.env, versionnée) porte les substitutions compose et AUCUN secret en clair.
# Renvoie 0 si OK, 1 si erreurs (listées sur stdout).
#
# Le CONTENU de secrets.env (format des clés AES/API, URL SFTP) n'est PAS validé ici : la SSOT
# du schéma est le registre pydantic (electricore/config/runtime.py), vérifié par le vrai
# conteneur via `sops exec-env` aux étapes 11-12 d'install.sh (ADR-0049).

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
