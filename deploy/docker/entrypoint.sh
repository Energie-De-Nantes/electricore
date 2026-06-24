#!/bin/sh
# electricore-entrypoint — déchiffre les secrets SOPS+age puis exec la commande (ADR-0044 §2-§3).
#
# Place dans l'image juste après tini :   tini -- electricore-entrypoint <commande…>
# Le déchiffrement se fait DANS l'environnement du process (`sops exec-env`), jamais
# en fichier clair, puis `exec` la commande (uvicorn / supercronic). La voie exec-env
# gère les noms de variables DYNAMIQUES du trousseau AES (AES__TROUSSEAU__<label>__KEY,
# ADR-0037) sans énumération.
#
# Variables d'environnement :
#   SECRETS_ENV_FILE     chemin du `secrets.env` chiffré (défaut /run/secrets/secrets.env)
#   SOPS_AGE_KEY_FILE    chemin de la clé age privée (lu par sops ; défaut /run/secrets/age.key)
#   ELECTRICORE_DECRYPT  =off → bypass total (dev/test conteneur, ADR-0044 §3)
#
# Repli = ÉCHEC DUR (ADR-0044 §3) : pas de retombée silencieuse sur un run sans secrets
# (qui serait une API sans API_KEY, une ingestion sans SFTP/AES — une mauvaise config masquée).

set -eu

SECRETS_ENV_FILE="${SECRETS_ENV_FILE:-/run/secrets/secrets.env}"
SOPS_AGE_KEY_FILE="${SOPS_AGE_KEY_FILE:-/run/secrets/age.key}"
export SOPS_AGE_KEY_FILE

# Échappatoire explicite et documentée (ADR-0044 §3) : dev/test conteneur sans secrets.
if [ "${ELECTRICORE_DECRYPT:-}" = "off" ]; then
    echo "electricore-entrypoint: ELECTRICORE_DECRYPT=off → déchiffrement contourné." >&2
    exec "$@"
fi

# Fail-fast (ADR-0044 §3) : sans clé age NI fichier chiffré, on échoue bruyamment.
if [ ! -f "$SECRETS_ENV_FILE" ]; then
    echo "electricore-entrypoint: secrets SOPS requis introuvables." >&2
    echo "  fichier chiffré attendu : ${SECRETS_ENV_FILE} (absent)" >&2
    echo "  monter le secrets.env chiffré, ou poser ELECTRICORE_DECRYPT=off (dev/test)." >&2
    exit 1
fi
if [ ! -f "$SOPS_AGE_KEY_FILE" ]; then
    echo "electricore-entrypoint: secrets SOPS requis, aucune clé age." >&2
    echo "  clé age attendue : ${SOPS_AGE_KEY_FILE} (absente)" >&2
    echo "  monter la clé age (RO), ou poser ELECTRICORE_DECRYPT=off (dev/test)." >&2
    exit 1
fi

# `sops exec-env FILE CMD` exécute CMD via `sh -c` (un seul argument-commande, pas
# de positionnels). On re-sérialise donc l'argv d'origine en une chaîne shell sûre
# (quoting POSIX) pour la `exec` telle quelle, sans jamais écrire de fichier clair.
_quote_argv() {
    for a in "$@"; do
        printf "'%s' " "$(printf '%s' "$a" | sed "s/'/'\\\\''/g")"
    done
}

exec sops exec-env "$SECRETS_ENV_FILE" "exec $(_quote_argv "$@")"
