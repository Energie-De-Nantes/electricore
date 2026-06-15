# `deploy/lib/`

Helpers bash sourcés par [`../install.sh`](../install.sh) (et, pour le sous-ensemble
durcissement, par le wrapper autonome [`../harden.sh`](../harden.sh)).

- [`validate.sh`](validate.sh) — validateurs purs (slug, AES, URL, email, domaine). Renvoient 0/1, pas de sortie.
- [`os.sh`](os.sh) — détection OS via `/etc/os-release`, mockable avec `OS_RELEASE_PATH=...` (utilisé par les tests).
- [`harden.sh`](harden.sh) — durcissement VPS (ADR-0031) : `harden_vps()` orchestre admin `ops` + sudo, verrouillage sshd (root-off, clé only), fail2ban, unattended-upgrades. Réversion `unharden_vps()` (retire le drop-in sshd, la jail, la conf upgrades ; `ops` conservé sauf `--purge-ops`). Fonctions de rendu pures (`render_sshd_hardening`, `render_fail2ban_jail`, …) testables, side-effects idempotents. Wrappers autonomes : [`../harden.sh`](../harden.sh) / [`../unharden.sh`](../unharden.sh).

Tests : [`../tests/unit.sh`](../tests/unit.sh).
