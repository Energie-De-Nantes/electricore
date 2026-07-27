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

# validate_config_env <config_file> <expected_slug> [<component>]
# Valide la moitié CLAIRE : INSTANCE_SLUG (matche) + les substitutions requises par
# <component> ("stack", défaut, ou "relais", #657). AUCUN secret ici (sinon erreur —
# un secret en clair dans config.env est une fuite). Imprime les erreurs sur stdout ;
# 0 si OK, 1 sinon.
#
# Le config.env d'un provider est PARTAGÉ par les deux composants installables sur une
# box (ADR-0044, PRD #655) : une box relais-seul n'a pas besoin de BACKUPS_PATH (pas de
# stack), une box stack-seule n'a pas besoin de RELAIS_VERSION — chaque composant ne
# réclame que ses propres clés, jamais celles de l'autre (bump d'ELECTRICORE_VERSION ne
# doit rien exiger côté relais, et réciproquement).
validate_config_env() {
    local file="$1"
    local expected_slug="$2"
    local component="${3:-stack}"
    local errors=()

    [[ -r "$file" ]] || { echo "config.env introuvable : $file"; return 1; }

    local slug
    slug=$(read_env_var "$file" INSTANCE_SLUG)
    [[ "$slug" == "$expected_slug" ]] || \
        errors+=("INSTANCE_SLUG='${slug}' ne matche pas le slug attendu '${expected_slug}'")

    if [[ "$component" == "relais" ]]; then
        local relais_version
        relais_version=$(read_env_var "$file" RELAIS_VERSION)
        [[ -n "$relais_version" ]] || \
            errors+=("RELAIS_VERSION manquant (tag GHCR du composant relais, #657)")
        # Cohérence dépôt local (#657) : en mode file://, le conteneur ne voit que ce
        # qu'on lui monte — FLUX_DEPOSIT_DIR doit désigner EXACTEMENT le dossier de
        # l'URL (montage chemin-identique dans compose-relais.yml), sinon le relais
        # est aveugle à sa source.
        local source_url deposit_dir
        source_url=$(read_env_var "$file" RELAIS__SOURCE_URL)
        if [[ "$source_url" == file://* ]]; then
            deposit_dir=$(read_env_var "$file" FLUX_DEPOSIT_DIR)
            [[ -n "$deposit_dir" && "file://${deposit_dir}" == "$source_url" ]] || \
                errors+=("RELAIS__SOURCE_URL en file:// exige FLUX_DEPOSIT_DIR=${source_url#file://} (montage du dépôt dans le conteneur, #657)")
        fi
    else
        local version backups
        version=$(read_env_var "$file" ELECTRICORE_VERSION)
        backups=$(read_env_var "$file" BACKUPS_PATH)
        [[ -n "$version" ]] || errors+=("ELECTRICORE_VERSION manquant (substitution compose)")
        [[ -n "$backups" ]] || errors+=("BACKUPS_PATH manquant (substitution compose)")
    fi

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
