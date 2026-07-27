#!/usr/bin/env bash
# Sandbox VM Multipass pour tester deploy/install.sh sans VPS réel.
# Cf. issue #48 et docs/deploiement.md.
set -euo pipefail

VM_NAME="${VM_NAME:-electricore-sandbox}"
UBUNTU_VERSION="${UBUNTU_VERSION:-24.04}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

usage() {
    cat <<EOF
Usage: $0 <command> [args]

Commands:
  up                Lance la VM ${VM_NAME} (Ubuntu ${UBUNTU_VERSION}, 4G/2CPU/20G),
                    monte le repo sur /repo, injecte une clé SSH root bidon.
  down              Supprime la VM (delete + purge).
  run [args...]     Exécute install.sh dans la VM.
                    Défaut: --slug test --domain test.local --skip-dns
  harden [args...]  Exécute le wrapper autonome deploy/harden.sh dans la VM
                    (rétro-durcissement, ADR-0031 #262).
  unharden [args]   Exécute deploy/unharden.sh dans la VM (réversion).
  verify            Lance les assertions de durcissement (ADR-0031) dans la VM,
                    via `multipass exec` (pas SSH — survit au root-off).
  verify-reverse    Assertions de réversion (drop-in retiré, root SSH rétabli).
  shell             Ouvre un shell interactif dans la VM.
  snap <name>       Crée un snapshot.
  restore <name>    Restaure un snapshot (rapide pour itérer).
  status            État de la VM.

  Scénario « box non-vierge » (#656 — cas Enargia, compte SFTP au mot de passe) :
  seed-password-account <user>  Crée <user> (shell nologin, mot de passe usable,
                                 AUCUNE clé) — simule le compte de dépôt Enedis.
  remediate-key <user>          Pose une clé SSH pour <user> (migration en clé).
  verify-preflight refuse <user>            Assertions : durcissement REFUSÉ (drop-in absent).
  verify-preflight pass <user>               Assertions : durcissement PASSE (drop-in présent,
                                              <user> toujours protégé par sa clé).
  verify-preflight already-hardened <user>   Assertions : reconfigure PASSE sans remédiation
                                              sur une box déjà durcie (finding 3, diff avant/après).

  Scénario « composant relais » (#657) :
  seed-relais-key <slug> [perms]   Pose une clé SSH partenaire de test (600 par défaut,
                                    ou <perms> ex. 644 pour tester le refus).
  remove-relais-key <slug>         Retire la clé SSH partenaire de test.
  verify-relais refuse <slug>      Assertions : install.sh --relais REFUSÉ (clé SSH absente).
  verify-relais posed  <slug>      Assertions : état posé (timer actif, compose présent, clé 600).

  Séquence complète :
    ./multipass.sh up
    ./multipass.sh seed-password-account enedis_deposit
    ./multipass.sh harden                        # doit ÉCHOUER (exit non-zero)
    ./multipass.sh verify-preflight refuse enedis_deposit
    ./multipass.sh remediate-key enedis_deposit
    ./multipass.sh harden                        # doit RÉUSSIR
    ./multipass.sh verify-preflight pass enedis_deposit
    # finding 3 (diff avant/après) : un NOUVEAU compte au mot de passe, jamais
    # migré, sur une box DÉJÀ durcie — le reconfigure ne doit PAS re-bloquer
    # (avant=no déjà : le compte n'a jamais pu se connecter par mot de passe).
    ./multipass.sh seed-password-account legacy_svc
    ./multipass.sh harden                        # doit RÉUSSIR À NOUVEAU (silencieux)
    ./multipass.sh verify-preflight already-hardened legacy_svc
    ./multipass.sh down

  Scénario « composant relais » (#657) — install.sh --relais sur une machine vierge,
  socle commun + mini-compose + timer, sans push réel vers un partenaire :
    ./multipass.sh up
    ./multipass.sh run --slug relais --relais --deploy-repo <url-dépôt-déploiement>
                                                  # doit ÉCHOUER (clé SSH partenaire absente)
    ./multipass.sh verify-relais refuse relais
    ./multipass.sh seed-relais-key relais
    ./multipass.sh run --slug relais --relais --deploy-repo <url-dépôt-déploiement>
                                                  # doit RÉUSSIR
    ./multipass.sh verify-relais posed relais
    ./multipass.sh down

Variables :
  VM_NAME           (défaut: electricore-sandbox)
  UBUNTU_VERSION    (défaut: 24.04)

Prérequis : Multipass installé (https://multipass.run/install).
EOF
}

require_multipass() {
    command -v multipass >/dev/null 2>&1 || {
        echo "multipass non installé. Voir https://multipass.run/install" >&2
        exit 1
    }
}

# multipass exec re-joint ses arguments en une chaîne que le shell de la VM
# RE-PARSE : quotes et pipes d'un `bash -c "…"` s'y évaporent (constaté à la
# première exécution réelle : `bash -c echo` nu, pipe exécuté hors du -c,
# prompts ssh-keygen au up). Tout script multi-commandes passe donc par STDIN —
# transmis verbatim, jamais re-parsé. Les exec à arguments simples (chemins,
# flags, sans métacaractère shell) restent en ligne : le re-parse y est sans effet.
vm_root() {
    multipass exec "$VM_NAME" -- sudo bash -s
}

cmd_up() {
    if multipass info "$VM_NAME" >/dev/null 2>&1; then
        echo "VM ${VM_NAME} existe déjà. Utiliser '$0 down' avant de recréer." >&2
        return 1
    fi
    multipass launch "$UBUNTU_VERSION" --name "$VM_NAME" --memory 4G --cpus 2 --disk 20G
    multipass mount "$REPO_ROOT" "${VM_NAME}:/repo"
    multipass exec "$VM_NAME" -- sudo mkdir -p /root/.ssh
    vm_root <<'EOF'
set -euo pipefail
ssh-keygen -t ed25519 -N '' -f /root/.ssh/id_ed25519 -q
cat /root/.ssh/id_ed25519.pub > /root/.ssh/authorized_keys
EOF
    echo "✓ VM ${VM_NAME} prête. Repo monté sur /repo."
}

cmd_down() {
    multipass delete "$VM_NAME" --purge 2>/dev/null || true
    echo "✓ VM ${VM_NAME} supprimée."
}

cmd_run() {
    local args=("$@")
    [[ ${#args[@]} -eq 0 ]] && args=(--slug test --domain test.local --skip-dns)
    multipass exec "$VM_NAME" -- sudo bash /repo/deploy/install.sh "${args[@]}"
}

cmd_harden()         { multipass exec "$VM_NAME" -- sudo bash /repo/deploy/harden.sh "$@"; }
cmd_unharden()       { multipass exec "$VM_NAME" -- sudo bash /repo/deploy/unharden.sh "$@"; }
cmd_verify()         { multipass exec "$VM_NAME" -- sudo bash /repo/deploy/tests/e2e/assert_harden.sh "$@"; }
cmd_verify_reverse() { multipass exec "$VM_NAME" -- sudo bash /repo/deploy/tests/e2e/assert_unharden.sh "$@"; }
cmd_shell()   { multipass shell "$VM_NAME"; }
cmd_snap()    { multipass snapshot "$VM_NAME" --name "${1:?nom de snapshot requis}"; }
cmd_restore() { multipass restore "${VM_NAME}.${1:?nom de snapshot requis}"; }
cmd_status()  { multipass info "$VM_NAME" 2>/dev/null || echo "VM ${VM_NAME} n'existe pas."; }

# ── Scénario « box non-vierge » (#656, cas Enargia) ─────────────────────────

# cmd_seed_password_account <user>
# Crée <user> avec un mot de passe UTILISABLE et AUCUNE clé authorized_keys —
# shell nologin délibéré (le compte de dépôt Enedis réel est SFTP-only via
# ForceCommand internal-sftp ; le préflight ne doit PAS se fier au shell, #656 AC4).
cmd_seed_password_account() {
    local user="${1:?user requis}"
    vm_root <<EOF
set -euo pipefail
id -u '${user}' >/dev/null 2>&1 || useradd --create-home --shell /usr/sbin/nologin '${user}'
echo '${user}:electricore-test-656' | chpasswd
EOF
    echo "✓ Compte ${user} semencé : mot de passe usable, shell nologin, AUCUNE clé (cas Enargia #656)."
}

# cmd_remediate_key <user>
# Remédiation #1 (migration en clé) : génère une paire de clés jetable et l'installe
# comme authorized_keys de <user>. Alternative à une exception Match User.
cmd_remediate_key() {
    local user="${1:?user requis}"
    vm_root <<EOF
set -euo pipefail
install -d -m 700 -o '${user}' -g '${user}' /home/${user}/.ssh
ssh-keygen -t ed25519 -N '' -f /tmp/${user}_key -q
cat /tmp/${user}_key.pub > /home/${user}/.ssh/authorized_keys
chown '${user}:${user}' /home/${user}/.ssh/authorized_keys
chmod 600 /home/${user}/.ssh/authorized_keys
rm -f /tmp/${user}_key /tmp/${user}_key.pub
EOF
    echo "✓ Clé SSH posée pour ${user} (remédiation #656 : migration en clé)."
}

cmd_verify_preflight() { multipass exec "$VM_NAME" -- sudo bash /repo/deploy/tests/e2e/assert_preflight.sh "$@"; }

# ── Scénario « composant relais » (#657) ────────────────────────────────────

# cmd_seed_relais_key <slug> [perms]
# Pose une clé SSH partenaire de TEST (jetable, jamais la vraie clé Haulogy) au chemin
# de convention /srv/<slug>/relais_ssh_key. <perms> défaut 600 (cas nominal) ; passer
# une valeur plus ouverte (644) pour exercer le refus « droits trop permissifs ».
cmd_seed_relais_key() {
    local slug="${1:?slug requis}"
    local perms="${2:-600}"
    multipass exec "$VM_NAME" -- sudo bash -c "
        install -d /srv/${slug}
        ssh-keygen -t ed25519 -N '' -f /tmp/relais_test_key -q
        cp /tmp/relais_test_key /srv/${slug}/relais_ssh_key
        chmod ${perms} /srv/${slug}/relais_ssh_key
        rm -f /tmp/relais_test_key /tmp/relais_test_key.pub
    "
    echo "✓ Clé SSH partenaire de test posée pour ${slug} (droits ${perms})."
}

# cmd_remove_relais_key <slug>
# Retire la clé de test — pour rejouer le scénario « refuse » sur une box déjà passée.
cmd_remove_relais_key() {
    local slug="${1:?slug requis}"
    multipass exec "$VM_NAME" -- sudo rm -f "/srv/${slug}/relais_ssh_key"
    echo "✓ Clé SSH partenaire de test retirée pour ${slug}."
}

cmd_verify_relais() { multipass exec "$VM_NAME" -- sudo bash /repo/deploy/tests/e2e/assert_relais.sh "$@"; }

require_multipass
CMD="${1:-}"; shift || true
case "$CMD" in
    up|down|run|harden|unharden|verify|shell|snap|restore|status) "cmd_$CMD" "$@" ;;
    verify-reverse)          cmd_verify_reverse "$@" ;;
    seed-password-account)   cmd_seed_password_account "$@" ;;
    remediate-key)           cmd_remediate_key "$@" ;;
    verify-preflight)        cmd_verify_preflight "$@" ;;
    seed-relais-key)         cmd_seed_relais_key "$@" ;;
    remove-relais-key)       cmd_remove_relais_key "$@" ;;
    verify-relais)           cmd_verify_relais "$@" ;;
    *) usage; exit 1 ;;
esac
