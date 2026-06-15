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
  verify            Lance les assertions de durcissement (ADR-0031) dans la VM,
                    via `multipass exec` (pas SSH — survit au root-off).
  shell             Ouvre un shell interactif dans la VM.
  snap <name>       Crée un snapshot.
  restore <name>    Restaure un snapshot (rapide pour itérer).
  status            État de la VM.

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

cmd_up() {
    if multipass info "$VM_NAME" >/dev/null 2>&1; then
        echo "VM ${VM_NAME} existe déjà. Utiliser '$0 down' avant de recréer." >&2
        return 1
    fi
    multipass launch "$UBUNTU_VERSION" --name "$VM_NAME" --memory 4G --cpus 2 --disk 20G
    multipass mount "$REPO_ROOT" "${VM_NAME}:/repo"
    multipass exec "$VM_NAME" -- sudo mkdir -p /root/.ssh
    multipass exec "$VM_NAME" -- sudo bash -c \
        "ssh-keygen -t ed25519 -N '' -f /root/.ssh/id_ed25519 && \
         cat /root/.ssh/id_ed25519.pub > /root/.ssh/authorized_keys"
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

cmd_harden()  { multipass exec "$VM_NAME" -- sudo bash /repo/deploy/harden.sh "$@"; }
cmd_verify()  { multipass exec "$VM_NAME" -- sudo bash /repo/deploy/tests/e2e/assert_harden.sh "$@"; }
cmd_shell()   { multipass shell "$VM_NAME"; }
cmd_snap()    { multipass snapshot "$VM_NAME" --name "${1:?nom de snapshot requis}"; }
cmd_restore() { multipass restore "${VM_NAME}.${1:?nom de snapshot requis}"; }
cmd_status()  { multipass info "$VM_NAME" 2>/dev/null || echo "VM ${VM_NAME} n'existe pas."; }

require_multipass
CMD="${1:-}"; shift || true
case "$CMD" in
    up|down|run|harden|verify|shell|snap|restore|status) "cmd_$CMD" "$@" ;;
    *) usage; exit 1 ;;
esac
