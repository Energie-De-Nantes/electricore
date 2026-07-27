#!/usr/bin/env bash
# Hook d'alerte OnFailure= du relais (#659, PRD #658) : mail vers les destinataires
# de RELAIS_ALERTE_MAILS quand electricore-relais.service échoue (run aveugle, #643 —
# voir electricore/ingestion/relais/pipeline.py). Volontairement SANS Python : le
# scénario où l'alerte est la plus nécessaire est précisément celui où le venv du
# relais est cassé.
set -euo pipefail

UNIT="electricore-relais.service"

if [[ -z "${RELAIS_ALERTE_MAILS:-}" ]]; then
    echo "electricore-relais-alerte: RELAIS_ALERTE_MAILS absent/vide — aucun mail envoyé" >&2
    exit 0
fi

# ${HOSTNAME} (posé par bash lui-même) et pas $(hostname) : sous set -e, un binaire
# hostname absent ou en échec tuerait le script AVANT le moindre envoi.
sujet="[electricore] échec de ${UNIT} sur ${HOSTNAME:-inconnu}"
corps="$(journalctl -u "$UNIT" --no-pager -n 50 2>/dev/null)" || corps="(journalctl indisponible pour ${UNIT})"

# CSV → tableau bash : msmtp attend les destinataires en arguments séparés. L'espace
# dans IFS absorbe le « a@x.fr, b@y.fr » écrit à la main (sinon msmtp reçoit " b@y.fr").
IFS=', ' read -ra destinataires <<< "$RELAIS_ALERTE_MAILS"

{
    printf 'To: %s\n' "$RELAIS_ALERTE_MAILS"
    printf 'Subject: %s\n' "$sujet"
    printf '\n%s\n' "$corps"
} | msmtp --file=/etc/electricore-relais/msmtprc -- "${destinataires[@]}"
