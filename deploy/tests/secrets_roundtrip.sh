#!/usr/bin/env bash
# Test d'aller-retour onboarding crypto (ADR-0044) avec de VRAIS binaires sops + age.
#
# Exercice le chemin HOST-CRYPTO bout-en-bout :
#   generate_box_identities → sops encrypt → box_can_decrypt / _ingestion_read_scheduler_key
# Avec un provider + un home d'instance JETABLES.
#
# Skip propre (exit 0) si sops ou age-keygen sont absents — pas de régression locale
# sans outils. En CI (deploy-tests), les outils sont installés ⇒ le test tourne pour de VRAI.
#
# Réutilise le motif ok/ko/PASS/FAIL de unit.sh (cohérence de sortie).

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="${SCRIPT_DIR}/../lib"

# shellcheck source=../lib/log.sh
source "${LIB_DIR}/log.sh"
# shellcheck source=../lib/secrets.sh
source "${LIB_DIR}/secrets.sh"
# shellcheck source=../lib/ingestion.sh
source "${LIB_DIR}/ingestion.sh"

PASS=0; FAIL=0
ok() { printf '  \033[32m✓\033[0m %s\n' "$1"; PASS=$((PASS+1)); }
ko() { printf '  \033[31m✗\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }

# ─── Skip propre si les vrais outils sont absents ────────────────────────────
if ! command -v sops >/dev/null 2>&1 || ! command -v age-keygen >/dev/null 2>&1; then
    echo "  skipped: sops/age absent — test non lancé (installe sops + age pour l'exécuter)"
    exit 0
fi

echo "→ secrets_roundtrip.sh (vrais sops + age, provider jetable)"

# ─── Setup : home + provider jetables ────────────────────────────────────────
TEST_SLUG="roundtrip-$$"
srv_root=$(mktemp -d)
export SRV_BASE="$srv_root"
install -d "${srv_root}/${TEST_SLUG}"
install -d "${srv_root}/${TEST_SLUG}/providers/${TEST_SLUG}"

# ─── 1. generate_box_identities avec les vrais age-keygen + ssh-keygen ───────
out=$(generate_box_identities "$TEST_SLUG" 2>/dev/null)
grep -q "^AGE_PUBLIC_KEY=age1" <<<"$out" \
    && ok "generate_box_identities: AGE_PUBLIC_KEY réelle (préfixe age1)" \
    || ko "generate_box_identities: pas d'AGE_PUBLIC_KEY valide"
grep -q "^SSH_DEPLOY_PUBKEY=ssh-ed25519" <<<"$out" \
    && ok "generate_box_identities: SSH_DEPLOY_PUBKEY réelle (ed25519)" \
    || ko "generate_box_identities: pas de SSH_DEPLOY_PUBKEY valide"
[[ -f "${srv_root}/${TEST_SLUG}/age.key" ]] \
    && ok "generate_box_identities: age.key créée" \
    || ko "generate_box_identities: age.key absente"
perm=$(stat -c '%a' "${srv_root}/${TEST_SLUG}/age.key" 2>/dev/null)
[[ "$perm" == "600" ]] \
    && ok "generate_box_identities: age.key en 600" \
    || ko "generate_box_identities: age.key permissions incorrectes (got ${perm})"

# Récupère la vraie clé publique age pour le chiffrement
BOX_AGE_PUBKEY=$(grep "^AGE_PUBLIC_KEY=" <<<"$out" | cut -d= -f2-)

# ─── 2. Chiffrer un secrets.env valide pour la box ───────────────────────────
plaintext="${srv_root}/secrets_plain.env"
cat > "$plaintext" <<EOF
API__TROUSSEAU__librewatt__KEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
API__TROUSSEAU__scheduler__KEY=ssssssssssssssssssssssssssssssss
SFTP__URL=sftp://user:pass@sftp.example.fr:22/exports
AES__TROUSSEAU__aes256_2026__KEY=00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff
EOF

secrets_path="${srv_root}/${TEST_SLUG}/providers/${TEST_SLUG}/secrets.env"
SOPS_AGE_RECIPIENTS="$BOX_AGE_PUBKEY" \
    sops encrypt --input-type dotenv --output-type dotenv "$plaintext" \
    > "$secrets_path" 2>/dev/null \
    && ok "sops encrypt: secrets.env chiffré pour la box" \
    || ko "sops encrypt: échec du chiffrement"

# ─── 3. Asserts POSITIFS : déchiffrement réel ────────────────────────────────
box_can_decrypt "$TEST_SLUG" \
    && ok "box_can_decrypt: réussit avec la vraie clé (la box est destinataire)" \
    || ko "box_can_decrypt: devait réussir avec la vraie clé"

sched_key=$(SRV_BASE="$srv_root" _ingestion_read_scheduler_key "$TEST_SLUG" 2>/dev/null)
[[ "$sched_key" == "ssssssssssssssssssssssssssssssss" ]] \
    && ok "_ingestion_read_scheduler_key: extrait la clé scheduler réelle" \
    || ko "_ingestion_read_scheduler_key: valeur inattendue (got '${sched_key}')"

# ─── 4. Assert NÉGATIF : mauvaise clé → déchiffrement impossible ─────────────
other_root=$(mktemp -d)
install -d "${other_root}/${TEST_SLUG}/providers/${TEST_SLUG}"
# Génère une AUTRE paire age (non destinataire du secrets.env ci-dessus)
SAVED_SRV="$SRV_BASE"
export SRV_BASE="$other_root"
generate_age_identity "${other_root}/${TEST_SLUG}" >/dev/null 2>&1
# Copie le même ciphertext (chiffré pour la BONNE box, pas pour celle-ci)
cp "$secrets_path" "${other_root}/${TEST_SLUG}/providers/${TEST_SLUG}/secrets.env"
box_can_decrypt "$TEST_SLUG" \
    && ko "box_can_decrypt: devait ÉCHOUER avec une clé non-destinataire (déchiffrement factice ?)" \
    || ok "box_can_decrypt: échoue avec une clé non-destinataire (déchiffrement réel prouvé)"
export SRV_BASE="$SAVED_SRV"

# ─── Cleanup ─────────────────────────────────────────────────────────────────
rm -rf "$srv_root" "$other_root" 2>/dev/null || true
unset SRV_BASE

echo
if [[ "$FAIL" -eq 0 ]]; then
    printf "\033[32m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 0
else
    printf "\033[31m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
    exit 1
fi
