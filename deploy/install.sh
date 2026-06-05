#!/usr/bin/env bash
# ElectriCore installer — provisionne une instance mono-tenant sur VPS frais.
# Cf. ADR-0017 (layout /srv/<slug>/) et issues #48 (chemin nominal) / #49 (rerun + ETL test).
#
# Stub : les étapes seront implémentées par #48 et #49. Les helpers purs
# (validation .env, détection OS) vivent déjà dans deploy/lib/ et sont
# testés par deploy/tests/unit.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=lib/validate.sh
source "${SCRIPT_DIR}/lib/validate.sh"
# shellcheck source=lib/os.sh
source "${SCRIPT_DIR}/lib/os.sh"

echo "ElectriCore installer — stub (cf. #48/#49)."
echo "Helpers chargés depuis ${SCRIPT_DIR}/lib/."
echo "OS détecté : $(detect_os)"
