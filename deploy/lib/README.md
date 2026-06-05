# `deploy/lib/`

Helpers bash sourcés par [`../install.sh`](../install.sh).

- [`validate.sh`](validate.sh) — validateurs purs (slug, AES, URL, email, domaine). Renvoient 0/1, pas de sortie.
- [`os.sh`](os.sh) — détection OS via `/etc/os-release`, mockable avec `OS_RELEASE_PATH=...` (utilisé par les tests).

Tests : [`../tests/unit.sh`](../tests/unit.sh).
