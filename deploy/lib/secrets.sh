# shellcheck shell=bash
# Secrets-as-code (ADR-0044) : génération des deux identités de la box (age + SSH),
# pull du dépôt de déploiement privé, validation du split config/secret.
#
# Toutes les commandes externes (docker, git, age) sont appelées PAR LEUR NOM (via
# le PATH) → les tests `deploy/tests/` les stubbent en fake-binaries (motif maison).
#
# Décisions clés (ADR-0044) :
#   §4 — chaque box génère SA paire age + SA paire SSH ; les clés PRIVÉES naissent sur
#        la box et n'en sortent jamais (/srv/<slug>/, 600). L'opérateur enregistre les
#        deux PUBLIQUES (age → destinataire .sops.yaml ; SSH → deploy key RO).
#        Génération VIA L'IMAGE (age-keygen / ssh-keygen embarqués) → zéro dép hôte.
#   §5 — le ciphertext arrive par auto-pull : la box clone/pull le dépôt privé via sa
#        deploy key RO et lit providers/<slug>/{config.env,secrets.env}.

# Image runtime par défaut (porte age + ssh-keygen + sops).
SECRETS_IMAGE="${SECRETS_IMAGE:-ghcr.io/energie-de-nantes/electricore:latest}"
# Commandes externes, surchargeable pour les tests.
DOCKER_BIN="${DOCKER_BIN:-docker}"
GIT_BIN="${GIT_BIN:-git}"
# Racine des homes d'instance (/srv en prod, ADR-0017). Surchargeable pour les tests.
SRV_BASE="${SRV_BASE:-/srv}"

# generate_age_identity <home>
# Génère la paire age de la box VIA L'IMAGE (age-keygen), écrit la clé privée dans
# <home>/age.key (600), et imprime la clé PUBLIQUE sur stdout. Idempotent : si
# <home>/age.key existe déjà, on ne régénère pas (on re-dérive juste la pub).
generate_age_identity() {
    local home="$1"
    local keyfile="${home}/age.key"
    if [[ -f "$keyfile" ]]; then
        log_skip "clé age déjà présente (${keyfile}) — conservée" >&2
    else
        # age-keygen écrit la clé privée sur stdout (+ la pub en commentaire) et la
        # pub sur stderr. On capture la privée via l'image, sans jamais l'exposer.
        "$DOCKER_BIN" run --rm "$SECRETS_IMAGE" age-keygen 2>/dev/null > "$keyfile" \
            || { log_err "échec age-keygen via l'image" >&2; return 1; }
        chmod 600 "$keyfile"
        log_ok "paire age générée (${keyfile}, 600)" >&2
    fi
    age_public_key "$keyfile"
}

# age_public_key <keyfile>
# Dérive la clé publique age depuis la clé privée VIA L'IMAGE (age-keygen -y).
age_public_key() {
    local keyfile="$1"
    "$DOCKER_BIN" run --rm -v "${keyfile}:/age.key:ro" "$SECRETS_IMAGE" \
        age-keygen -y /age.key 2>/dev/null
}

# generate_ssh_deploy_identity <home>
# Génère la paire SSH de la box VIA L'IMAGE (ssh-keygen ed25519), clé privée dans
# <home>/ssh_deploy_key (600), et imprime la clé PUBLIQUE sur stdout. Idempotent.
generate_ssh_deploy_identity() {
    local home="$1"
    local keyfile="${home}/ssh_deploy_key"
    if [[ -f "$keyfile" ]]; then
        log_skip "clé SSH de déploiement déjà présente (${keyfile}) — conservée" >&2
    else
        # ssh-keygen écrit keyfile + keyfile.pub. On le lance dans l'image, le home
        # monté en RW pour récupérer les deux fichiers.
        "$DOCKER_BIN" run --rm -v "${home}:/out" "$SECRETS_IMAGE" \
            ssh-keygen -t ed25519 -N '' -C "deploy-key" -f /out/ssh_deploy_key \
            >/dev/null 2>&1 \
            || { log_err "échec ssh-keygen via l'image" >&2; return 1; }
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
        # chown_instance_home (install.sh étape 7) chowne tout le home vers le user du slug
        # — y compris deploy-repo/.git — pour que le compose (sudo -u <slug>) lise les
        # providers/ via le symlink. Mais ce pull tourne en root → "dubious ownership".
        # safe.directory réconcilie : contenu lisible par le user, root opère le pull sans broncher.
        GIT_SSH_COMMAND="$git_ssh" "$GIT_BIN" -c safe.directory="$repo_dir" -C "$repo_dir" pull --ff-only \
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
