"""Façade de compatibilité sur le registre runtime (#141, ADR-0025).

`APISettings` ne lit plus l'environnement : elle compose les domaines runtime
(`api`, `bot`, `odoo`). Conservée comme adaptateur mince pour la stabilité des
appelants et des tests ; la source de vérité est `electricore/config/runtime.py`.

Lectures « souples » pour le bot et Odoo : l'API ne requiert ni l'un ni l'autre
(no-ERP servi, ADR-0022 ; l'API ne valide jamais le domaine bot, ADR-0025), donc
l'absence de config rend une valeur vide plutôt que de lever.
"""

from dataclasses import dataclass

from electricore.config import runtime


@dataclass(frozen=True)
class APISettings:
    """Adaptateur sur les domaines runtime, interface héritée d'APISettings.

    `@dataclass` sans champ : objet-vue sans état propre (toute la config vit dans
    le registre runtime) — la forme sanctionnée par ADR-0018 pour un adaptateur.
    """

    # --- domaine api (champs à défaut, n'échoue jamais) ---
    @property
    def api_title(self) -> str:
        return runtime.api().titre

    @property
    def api_version(self) -> str:
        return runtime.api().version

    @property
    def api_description(self) -> str:
        return runtime.api().description

    @property
    def instance_slug(self) -> str:
        return runtime.api().instance_slug

    def get_valid_api_keys(self) -> list[str]:
        return runtime.api().cles_valides()

    # --- domaine bot (souple : l'API ne requiert pas le bot) ---
    @property
    def telegram_bot_token(self) -> str:
        return runtime.bot().token if runtime.bot_est_configure() else ""

    @property
    def telegram_notify_chat_id(self) -> str:
        return runtime.bot().notify_chat_id if runtime.bot_est_configure() else ""

    def get_telegram_allowed_users(self) -> set[int]:
        return runtime.bot().utilisateurs_autorises() if runtime.bot_est_configure() else set()

    # --- odoo (souple : no-ERP servi) ---
    def get_odoo_config(self) -> dict[str, str]:
        return runtime.odoo().model_dump()

    @property
    def is_odoo_configured(self) -> bool:
        """Dérivé de `get_odoo_config()` : patcher cette méthode (tests) pilote
        donc aussi `is_odoo_configured`, comportement hérité d'APISettings."""
        try:
            self.get_odoo_config()
            return True
        except Exception:
            return False


# Instance globale (façade) — la config réelle vit dans le registre runtime.
settings = APISettings()
