# shellcheck shell=bash
# Secrets-as-code (ADR-0044) : installation des outils crypto sur l'hôte, génération des
# deux identités de la box (age + SSH), pull du dépôt de déploiement privé, validation du
# split config/secret.
#
# Toutes les commandes externes (sops, age, age-keygen, ssh-keygen, git) sont appelées PAR
# LEUR NOM (via le PATH) → les tests `deploy/tests/` les stubbent en fake-binaries.
#
# Décisions clés (ADR-0044) :
#   §4 — chaque box génère SA paire age + SA paire SSH ; les clés PRIVÉES naissent sur la
#        box et n'en sortent jamais (/srv/<slug>/, 600). L'opérateur enregistre les deux
#        PUBLIQUES (age → destinataire .sops.yaml ; SSH → deploy key RO). La crypto tourne
#        avec les outils de l'HÔTE (sops + age installés par `ensure_secrets_tools` ;
#        ssh-keygen déjà présent sur tout VPS). L'image ne sert QU'au runtime : son
#        entrypoint SOPS fail-fast n'est pas fait pour un `docker run age-keygen`.
#   §5 — le ciphertext arrive par auto-pull : la box clone/pull le dépôt privé via sa
#        deploy key RO et lit providers/<slug>/{config.env,secrets.env}.

# Commande git, surchargeable pour les tests.
GIT_BIN="${GIT_BIN:-git}"
# Racine des homes d'instance (/srv en prod, ADR-0017). Surchargeable pour les tests.
SRV_BASE="${SRV_BASE:-/srv}"

# Outils crypto installés sur l'hôte (mêmes versions/checksums que le Dockerfile de l'image,
# binaires statiques amd64). La box manipule ses clés et déchiffre le pré-flight avec eux.
SOPS_VERSION="${SOPS_VERSION:-3.9.4}"
SOPS_SHA256="${SOPS_SHA256:-5488e32bc471de7982ad895dd054bbab3ab91c417a118426134551e9626e4e85}"
AGE_VERSION="${AGE_VERSION:-1.2.1}"
AGE_SHA256="${AGE_SHA256:-7df45a6cc87d4da11cc03a539a7470c15b1041ab2b396af088fe9990f7c79d50}"

# ensure_secrets_tools
# Installe sops + age + age-keygen dans /usr/local/bin si absents (curl + checksum, même
# motif que le Dockerfile). Idempotent. ssh-keygen est déjà présent sur tout VPS (openssh).
ensure_secrets_tools() {
    local bin=/usr/local/bin
    if ! command -v sops >/dev/null 2>&1; then
        curl -fsSL -o "${bin}/sops" \
            "https://github.com/getsops/sops/releases/download/v${SOPS_VERSION}/sops-v${SOPS_VERSION}.linux.amd64" \
            && echo "${SOPS_SHA256}  ${bin}/sops" | sha256sum -c - >/dev/null 2>&1 \
            && chmod +x "${bin}/sops" \
            || { log_err "installation de sops échouée (téléchargement ou checksum)" >&2; return 1; }
        log_ok "sops installé (${bin}/sops, v${SOPS_VERSION})" >&2
    else
        log_skip "sops déjà présent ($(command -v sops))" >&2
    fi
    if ! command -v age-keygen >/dev/null 2>&1; then
        local tmp; tmp=$(mktemp -d)
        curl -fsSL -o "${tmp}/age.tgz" \
            "https://github.com/FiloSottile/age/releases/download/v${AGE_VERSION}/age-v${AGE_VERSION}-linux-amd64.tar.gz" \
            && echo "${AGE_SHA256}  ${tmp}/age.tgz" | sha256sum -c - >/dev/null 2>&1 \
            && tar -xzf "${tmp}/age.tgz" -C "$tmp" \
            && mv "${tmp}/age/age" "${tmp}/age/age-keygen" "${bin}/" \
            && chmod +x "${bin}/age" "${bin}/age-keygen" \
            || { log_err "installation de age échouée (téléchargement ou checksum)" >&2; rm -rf "$tmp"; return 1; }
        rm -rf "$tmp"
        log_ok "age + age-keygen installés (${bin}, v${AGE_VERSION})" >&2
    else
        log_skip "age déjà présent ($(command -v age-keygen))" >&2
    fi
}

# generate_age_identity <home>
# Génère la paire age de la box avec `age-keygen` (outil hôte), écrit la clé privée dans
# <home>/age.key (600), et imprime la clé PUBLIQUE sur stdout. Idempotent : si
# <home>/age.key existe déjà, on ne régénère pas (on re-dérive juste la pub).
generate_age_identity() {
    local home="$1"
    local keyfile="${home}/age.key"
    if [[ -f "$keyfile" ]]; then
        log_skip "clé age déjà présente (${keyfile}) — conservée" >&2
    else
        # age-keygen écrit la clé privée sur stdout (+ la pub en commentaire) et la pub
        # sur stderr. On capture la privée dans le keyfile, sans jamais l'exposer.
        age-keygen 2>/dev/null > "$keyfile" \
            || { log_err "échec age-keygen (outil hôte absent ? cf. ensure_secrets_tools)" >&2; return 1; }
        chmod 600 "$keyfile"
        log_ok "paire age générée (${keyfile}, 600)" >&2
    fi
    age_public_key "$keyfile"
}

# age_public_key <keyfile>
# Dérive la clé publique age depuis la clé privée avec `age-keygen -y` (outil hôte).
age_public_key() {
    local keyfile="$1"
    age-keygen -y "$keyfile" 2>/dev/null
}

# generate_ssh_deploy_identity <home>
# Génère la paire SSH de la box avec `ssh-keygen` ed25519 (outil hôte, openssh), clé privée
# dans <home>/ssh_deploy_key (600), et imprime la clé PUBLIQUE sur stdout. Idempotent.
generate_ssh_deploy_identity() {
    local home="$1"
    local keyfile="${home}/ssh_deploy_key"
    if [[ -f "$keyfile" ]]; then
        log_skip "clé SSH de déploiement déjà présente (${keyfile}) — conservée" >&2
    else
        # ssh-keygen écrit keyfile + keyfile.pub directement (openssh est sur tout VPS).
        ssh-keygen -t ed25519 -N '' -C "deploy-key" -f "$keyfile" >/dev/null 2>&1 \
            || { log_err "échec ssh-keygen (openssh absent ?)" >&2; return 1; }
        chmod 600 "$keyfile"
        [[ -f "${keyfile}.pub" ]] && chmod 644 "${keyfile}.pub"
        log_ok "paire SSH de déploiement générée (${keyfile}, 600)" >&2
    fi
    cat "${keyfile}.pub"
}

# generate_box_identities <slug>
# Génère les DEUX identités de la box et imprime les deux clés publiques sous une
# forme parlante pour l'opérateur (à enregistrer : age → destinataire ; SSH → deploy key).
# Renvoie 0 si OK. La sortie « publique » va sur stdout (capturable), les logs sur stderr.
generate_box_identities() {
    local slug="$1"
    local home="${SRV_BASE}/${slug}"
    local age_pub ssh_pub
    age_pub=$(generate_age_identity "$home") || return 1
    ssh_pub=$(generate_ssh_deploy_identity "$home") || return 1
    chown "$slug:$slug" "${home}/age.key" "${home}/ssh_deploy_key" "${home}/ssh_deploy_key.pub" 2>/dev/null || true
    printf 'AGE_PUBLIC_KEY=%s\n' "$age_pub"
    printf 'SSH_DEPLOY_PUBKEY=%s\n' "$ssh_pub"
}

# pull_deploy_repo <repo_url> <slug>
# Clone (ou pull si déjà cloné) le dépôt de déploiement PRIVÉ via la deploy key RO de
# la box, dans /srv/<slug>/deploy-repo, puis expose providers/<slug>/ à la racine du
# home (symlink ou copie) là où le compose l'attend (../../providers/<slug>/).
# La deploy key est de faible valeur (ADR-0044 §5) : elle ne donne accès qu'à du
# ciphertext, inutile sans la clé age.
pull_deploy_repo() {
    local repo_url="$1"
    local slug="$2"
    local home="${SRV_BASE}/${slug}"
    local repo_dir="${home}/deploy-repo"
    local deploy_key="${home}/ssh_deploy_key"
    [[ -f "$deploy_key" ]] || { log_err "deploy key absente (${deploy_key}) — génère d'abord l'identité." >&2; return 1; }
    # GIT_SSH_COMMAND épingle la clé RO et coupe l'interactivité (StrictHostKeyChecking).
    local git_ssh="ssh -i ${deploy_key} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
    if [[ -d "${repo_dir}/.git" ]]; then
        GIT_SSH_COMMAND="$git_ssh" "$GIT_BIN" -C "$repo_dir" pull --ff-only \
            || { log_err "git pull du dépôt de déploiement a échoué" >&2; return 1; }
        log_ok "dépôt de déploiement mis à jour (${repo_dir})" >&2
    else
        GIT_SSH_COMMAND="$git_ssh" "$GIT_BIN" clone "$repo_url" "$repo_dir" \
            || { log_err "git clone du dépôt de déploiement a échoué" >&2; return 1; }
        log_ok "dépôt de déploiement cloné (${repo_dir})" >&2
    fi
    # Le compose attend ../../providers/<slug>/{config.env,secrets.env}. On relie le
    # dossier providers/ du dépôt à la racine du home → ${home}/providers/<slug>/ résout.
    local providers_src="${repo_dir}/providers"
    [[ -d "${providers_src}/${slug}" ]] || { log_err "providers/${slug}/ absent du dépôt de déploiement." >&2; return 1; }
    ln -sfn "$providers_src" "${home}/providers"
    # Le config.env CLAIR (substitutions compose) est tiré à la racine du home pour
    # `docker compose --env-file ../../config.env` (cf. stack.sh::compose_up).
    if [[ -f "${providers_src}/${slug}/config.env" ]]; then
        cp "${providers_src}/${slug}/config.env" "${home}/config.env"
    fi
    log_ok "providers/${slug}/ relié depuis le dépôt (${home}/providers → ${providers_src})" >&2
}

# print_onboarding_pending <slug> <domain> <age_pub> <ssh_pub>
# Affiche le message d'onboarding EN DEUX TEMPS (ADR-0044 §4) : les deux clés
# publiques à enregistrer + la marche à suivre, puis comment finir avec reconfigure.
print_onboarding_pending() {
    local slug="$1" domain="$2" age_pub="$3" ssh_pub="$4"
    cat <<EOF

  ${_C_BOLD}Onboarding en deux temps — la box ${slug} attend l'enregistrement de ses clés.${_C_RESET}

  La box a généré ses DEUX identités (clés privées en /srv/${slug}/, 600, elles
  ne quittent jamais la box). Enregistre maintenant les DEUX clés PUBLIQUES :

  1) Clé age PUBLIQUE → destinataire .sops.yaml du dépôt de déploiement
     (puis: sops updatekeys providers/${slug}/secrets.env ; commit ; push)

       ${age_pub}

  2) Clé SSH PUBLIQUE → deploy key EN LECTURE SEULE du dépôt de déploiement

       ${ssh_pub}

  Une fois les deux enregistrées et poussées, termine l'onboarding :

       sudo bash install.sh --slug ${slug} --domain ${domain} --deploy-repo <url>

  (reconfigure : la box pull le ciphertext, déchiffre, et démarre la stack.)

EOF
}

# box_can_decrypt <slug>
# Vrai (0) si la box est capable de déchiffrer : clé age présente ET secrets.env présent
# ET la clé age publique de la box est destinataire (le déchiffrement réussit). On teste
# le déchiffrement RÉEL d'au moins une valeur (sops decrypt --extract impossible sans
# clé → on tente un decrypt complet, silencieux). Sert au garde-fou onboarding 2 temps.
box_can_decrypt() {
    local slug="$1"
    local home="${SRV_BASE}/${slug}"
    local keyfile="${home}/age.key"
    local secrets="${home}/providers/${slug}/secrets.env"
    [[ -f "$keyfile" && -f "$secrets" ]] || return 1
    SOPS_AGE_KEY_FILE="$keyfile" sops decrypt --input-type dotenv --output-type dotenv "$secrets" >/dev/null 2>&1
}
