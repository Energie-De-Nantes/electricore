# shellcheck shell=bash
# Parsing des arguments de install.sh. Pure : pas de side-effect (sauf set -- ...).

usage() {
    cat <<EOF
Usage: sudo bash install.sh --slug <slug> --domain <fqdn> [options]

Arguments obligatoires :
  --slug <slug>          Identifiant court de l'instance ([a-z0-9-]+, 2-32 chars)
  --domain <fqdn>        Domaine public (ex: edn.electricore.fr)

Options :
  --ssh-pubkey <key>     Clé SSH publique pour le user de service <slug>
                         (défaut: copie depuis ~root/.ssh/authorized_keys)
  --admin-pubkey <key>   Clé SSH publique pour l'admin ops (durcissement, ADR-0031)
                         (défaut: copie depuis ~root/.ssh/authorized_keys)
  --email <addr>         Email pour les notifications Let's Encrypt
                         (défaut: laisse le placeholder du Caddyfile, à éditer)
  --version <tag>        Tag GHCR à déployer (défaut: latest)
  --deploy-repo <url>    Dépôt de déploiement PRIVÉ (secrets-as-code, ADR-0044) d'où
                         pull providers/<slug>/{config.env,secrets.env}. La box y accède
                         via sa deploy key SSH RO (générée à l'install).
  --env-from <file>      Charge un .env pré-rempli au lieu d'ouvrir l'éditeur
                         (legacy/migration ; utile pour les tests e2e et scripts)
  --skip-dns             N'attend pas la propagation DNS (test local)
  --no-harden            Saute tout le durcissement VPS (ADR-0031)
  --no-sshd              Durcit mais ne verrouille pas sshd (garde root SSH actif)
  --no-fail2ban          Durcit mais saute fail2ban
  --no-unattended-upgrades  Durcit mais saute les mises à jour automatiques
  --no-color             Désactive les couleurs ANSI dans la sortie
  -h, --help             Affiche cette aide

Cf. docs/deploiement.md, ADR-0017 (layout /srv/<slug>/), ADR-0015 (multi-instance).
EOF
}

# parse_args "$@" — remplit les vars globales OPT_SLUG, OPT_DOMAIN, OPT_SSH_PUBKEY,
# OPT_ADMIN_PUBKEY, OPT_EMAIL, OPT_VERSION, OPT_ENV_FROM, OPT_SKIP_DNS, OPT_NO_HARDEN,
# OPT_NO_SSHD, OPT_NO_FAIL2BAN, OPT_NO_UNATTENDED. Renvoie 0 si OK, 1 si erreur.
parse_args() {
    OPT_SLUG=""
    OPT_DOMAIN=""
    OPT_SSH_PUBKEY=""
    OPT_ADMIN_PUBKEY=""
    OPT_EMAIL=""
    OPT_VERSION="latest"
    OPT_DEPLOY_REPO=""
    OPT_ENV_FROM=""
    OPT_SKIP_DNS=0
    OPT_NO_HARDEN=0
    OPT_NO_SSHD=0
    OPT_NO_FAIL2BAN=0
    OPT_NO_UNATTENDED=0
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --slug)         OPT_SLUG="${2:-}"; shift 2 ;;
            --domain)       OPT_DOMAIN="${2:-}"; shift 2 ;;
            --ssh-pubkey)   OPT_SSH_PUBKEY="${2:-}"; shift 2 ;;
            --admin-pubkey) OPT_ADMIN_PUBKEY="${2:-}"; shift 2 ;;
            --email)        OPT_EMAIL="${2:-}"; shift 2 ;;
            --version)      OPT_VERSION="${2:-}"; shift 2 ;;
            --deploy-repo)  OPT_DEPLOY_REPO="${2:-}"; shift 2 ;;
            --env-from)     OPT_ENV_FROM="${2:-}"; shift 2 ;;
            --skip-dns)     OPT_SKIP_DNS=1; shift ;;
            --no-harden)    OPT_NO_HARDEN=1; shift ;;
            --no-sshd)      OPT_NO_SSHD=1; shift ;;
            --no-fail2ban)  OPT_NO_FAIL2BAN=1; shift ;;
            --no-unattended-upgrades) OPT_NO_UNATTENDED=1; shift ;;
            --no-color)     export NO_COLOR=1; shift ;;
            -h|--help)      usage; exit 0 ;;
            *)
                echo "Argument inconnu : $1" >&2
                return 1 ;;
        esac
    done
    [[ -n "$OPT_SLUG" ]]   || { echo "--slug manquant" >&2; return 1; }
    [[ -n "$OPT_DOMAIN" ]] || { echo "--domain manquant" >&2; return 1; }
    return 0
}
