#!/usr/bin/env bash
# add-provider.sh — ajoute un destinataire age à un provider et re-chiffre l'enveloppe.
#
# Outil ADMIN (machine admin, dépôt de déploiement privé), PAS sur le VPS. Il :
#   1. ajoute une clé age publique (box, ou nouvelle clé admin) aux destinataires du
#      .sops.yaml du provider (idempotent) ;
#   2. lance `sops updatekeys` → re-chiffre l'ENVELOPPE de secrets.env vers le nouveau
#      jeu de destinataires (sans changer les valeurs).
#
# Isolation cryptographique (ADR-0044 §6) : une box n'est destinataire que de SON
# provider → elle ne déchiffre que les siens. La CI n'est JAMAIS destinataire.
#
# Clé admin d'escrow (ADR-0046 §8) : chaque .sops.yaml porte DEUX destinataires admin
# distincts — opérationnel + escrow hors-ligne — destinataires PERMANENTS de chaque règle.
# Ne JAMAIS les retirer : c'est `updatekeys` re-keyé vers eux + la box qui garde les
# secrets récupérables. L'escrow est le filet si la clé admin opérationnelle est perdue
# (sans destinataire admin vivant, plus aucun re-keying possible). Cet outil n'ajoute
# qu'une box ; il ne touche pas aux destinataires admin déjà en place.
#
# ⚠️ Distinction cruciale (ADR-0044) : `updatekeys` ne rote que l'ENVELOPPE. Si un
#    secret a pu fuiter, le changer AUSSI à la source — updatekeys ne le protège pas.
#    (L'escrow non plus n'est pas un remède à une fuite — il préserve le re-keying.)
#
# Usage :
#   deploy/add-provider.sh --provider-dir providers/<slug> --age-pubkey age1xxxx…
#   deploy/add-provider.sh --provider-dir providers/<slug> --age-pubkey age1xxxx… --no-updatekeys
#
# Les tests deploy-shell sourcent ce fichier (guard main_add_provider) et stubbent
# `sops` en fake-binary (motif maison).

SOPS_BIN="${SOPS_BIN:-sops}"

usage_add_provider() {
    cat <<'EOF'
Usage: add-provider.sh --provider-dir <dir> --age-pubkey <age1…> [--no-updatekeys]

  --provider-dir <dir>   Dossier du provider (contient .sops.yaml + secrets.env)
  --age-pubkey <key>     Clé age PUBLIQUE à ajouter aux destinataires (box ou admin)
  --no-updatekeys        N'exécute pas `sops updatekeys` (ajoute juste le destinataire)
  -h, --help             Cette aide

Cf. ADR-0044 (secrets-as-code), docs/deploiement.md.
EOF
}

# parse_add_provider_args "$@" — remplit OPT_PROVIDER_DIR, OPT_AGE_PUBKEY, OPT_NO_UPDATEKEYS.
parse_add_provider_args() {
    OPT_PROVIDER_DIR=""
    OPT_AGE_PUBKEY=""
    OPT_NO_UPDATEKEYS=0
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --provider-dir) OPT_PROVIDER_DIR="${2:-}"; shift 2 ;;
            --age-pubkey)   OPT_AGE_PUBKEY="${2:-}"; shift 2 ;;
            --no-updatekeys) OPT_NO_UPDATEKEYS=1; shift ;;
            -h|--help)      usage_add_provider; exit 0 ;;
            *) echo "Argument inconnu : $1" >&2; return 2 ;;
        esac
    done
    [[ -n "$OPT_PROVIDER_DIR" ]] || { echo "--provider-dir manquant" >&2; return 1; }
    [[ -n "$OPT_AGE_PUBKEY" ]]   || { echo "--age-pubkey manquant" >&2; return 1; }
    [[ "$OPT_AGE_PUBKEY" =~ ^age1[0-9a-z]+$ ]] || { echo "--age-pubkey ne ressemble pas à une clé age (age1…) : $OPT_AGE_PUBKEY" >&2; return 1; }
    return 0
}

# add_recipient_to_sops <sops_yaml> <age_pubkey>
# Ajoute <age_pubkey> à la liste des destinataires age du .sops.yaml (idempotent :
# no-op si déjà présent). Insère sous le 1er bloc `- age:` rencontré, en respectant
# l'indentation de la 1re entrée existante. Renvoie 0 si OK.
add_recipient_to_sops() {
    local sops_yaml="$1"
    local pubkey="$2"
    [[ -r "$sops_yaml" ]] || { echo "fichier .sops.yaml introuvable : $sops_yaml" >&2; return 1; }
    if grep -qF "$pubkey" "$sops_yaml"; then
        echo "destinataire déjà présent (idempotent) : $pubkey" >&2
        return 0
    fi
    # Indentation des entrées age existantes (1re ligne `- age1…` après la clé `age:`,
    # qui peut être en tête de bloc `      - age:` ou `age:`). On repère l'entrée
    # `- age1…` existante et on reprend son indentation pour aligner la nouvelle.
    local indent
    indent=$(grep -oE '^[[:space:]]*- age1[0-9a-z]+' "$sops_yaml" | head -1 | sed -E 's/- age1.*//')
    [[ -n "$indent" ]] || indent="          "  # repli : 10 espaces (sous key_groups/age:)
    local tmp; tmp=$(mktemp)
    # Insère la nouvelle entrée juste après la ligne qui ouvre le bloc age (`age:`),
    # qu'elle soit `age:` seule ou `- age:` (item de key_groups).
    awk -v key="$pubkey" -v ind="$indent" '
        { print }
        !done && /^[[:space:]]*(-[[:space:]]+)?age:[[:space:]]*$/ { print ind "- " key; done=1 }
    ' "$sops_yaml" > "$tmp"
    mv "$tmp" "$sops_yaml"
    echo "destinataire ajouté : $pubkey" >&2
}

# updatekeys_provider <provider_dir>
# Lance `sops updatekeys` sur secrets.env du provider → re-chiffre l'enveloppe vers le
# nouveau jeu de destinataires. Suppose le .sops.yaml à jour et la clé admin disponible.
updatekeys_provider() {
    local provider_dir="$1"
    local secrets="${provider_dir}/secrets.env"
    [[ -f "$secrets" ]] || { echo "secrets.env introuvable : $secrets" >&2; return 1; }
    # --yes : non-interactif. sops lit .sops.yaml du dossier courant du fichier.
    ( cd "$provider_dir" && "$SOPS_BIN" updatekeys --yes secrets.env )
}

# add_provider <provider_dir> <age_pubkey> <run_updatekeys>
# Orchestration : ajoute le destinataire puis (si demandé) re-chiffre l'enveloppe.
add_provider() {
    local provider_dir="$1"
    local pubkey="$2"
    local run_updatekeys="${3:-1}"
    add_recipient_to_sops "${provider_dir}/.sops.yaml" "$pubkey" || return 1
    if [[ "$run_updatekeys" == "1" ]]; then
        updatekeys_provider "$provider_dir" || return 1
    else
        echo "updatekeys sauté (--no-updatekeys)" >&2
    fi
}

main_add_provider() {
    set -euo pipefail
    parse_add_provider_args "$@" || { usage_add_provider; exit 2; }
    local run=1; [[ "$OPT_NO_UPDATEKEYS" -eq 1 ]] && run=0
    add_provider "$OPT_PROVIDER_DIR" "$OPT_AGE_PUBKEY" "$run"
    echo "✓ provider mis à jour : ${OPT_PROVIDER_DIR}"
    echo "  Rappel (ADR-0044) : updatekeys ne rote que l'enveloppe — un secret qui a"
    echo "  pu fuiter doit AUSSI être changé à la source. Pense à commit + push."
}

# Guard : exécute main_add_provider seulement si lancé, pas sourcé (tests).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main_add_provider "$@"
fi
