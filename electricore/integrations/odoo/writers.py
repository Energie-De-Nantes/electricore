"""
Connecteur Odoo avec capacités d'écriture.

Ce module fournit OdooWriter pour créer et modifier des données dans Odoo ERP.
"""

import copy
import logging
from collections.abc import Hashable
from dataclasses import dataclass, field
from typing import Any

from .reader import OdooReader

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class EchecEcriture:
    """Échec d'écriture sur un record : id Odoo visé + raison XML-RPC."""

    id: int
    raison: str


@dataclass(frozen=True, slots=True)
class RapportEcriture:
    """Rapport de `OdooWriter.update()` (#571).

    Un échec XML-RPC sur un record n'interrompt plus la boucle : chaque record
    restant est quand même tenté, et chaque catégorie est rapportée plutôt que
    de stopper net ou de finir en simple log. Relancer `update()` est sûr
    (écriture idempotente, mêmes valeurs sur les mêmes ids) : le rapport sert
    à décider quoi corriger avant de relancer, pas à empêcher de relancer.
    """

    ids_reussis: list[int] = field(default_factory=list)
    echecs: list[EchecEcriture] = field(default_factory=list)
    sans_id: list[dict[Hashable, Any]] = field(default_factory=list)


class OdooWriter(OdooReader):
    """
    Connecteur Odoo avec capacités d'écriture.

    Hérite de OdooReader et étend les méthodes autorisées pour inclure les écritures.
    """

    # Étendre les méthodes autorisées avec les opérations d'écriture
    _ALLOWED_METHODS = OdooReader._ALLOWED_METHODS | {
        "create",
        "write",
        "unlink",
        "copy",
        "action_confirm",
        "action_cancel",
        "action_done",
        "button_confirm",
        "button_cancel",
        "toggle_active",
    }

    def __init__(self, config: dict[str, str], sim: bool = False, **kwargs: Any) -> None:
        """
        Initialise le connecteur avec mode simulation.

        Args:
            config: Configuration de connexion (obligatoire)
            sim: Mode simulation (n'écrit pas réellement)
            **kwargs: Arguments passés à OdooReader
        """
        super().__init__(config, **kwargs)
        self._sim = sim

    def create(self, model: str, records: list[dict[Hashable, Any]]) -> list[int]:
        """
        Crée des enregistrements dans Odoo.

        Args:
            model: Modèle Odoo
            records: Liste des enregistrements à créer

        Returns:
            List[int]: Liste des IDs créés
        """
        if self._sim:
            logger.info(f"# {len(records)} {model} creation called. [simulated]")
            return []

        # Nettoyer les données pour XML-RPC
        clean_records = []
        for record in records:
            clean_record = {}
            for k, v in record.items():
                if v is not None and not (hasattr(v, "__len__") and len(v) == 0):
                    clean_record[k] = v
            clean_records.append(clean_record)

        result = self.execute(model, "create", [clean_records])
        created_ids = result if isinstance(result, list) else [result]

        logger.info(f"{model} #{created_ids} created in Odoo db.")
        return created_ids

    def update(self, model: str, records: list[dict[Hashable, Any]]) -> RapportEcriture:
        """
        Met à jour des enregistrements dans Odoo.

        Args:
            model: Modèle Odoo
            records: Liste des enregistrements à mettre à jour (doivent contenir 'id')

        Returns:
            RapportEcriture : ids réussis, échecs (id + raison), records sans 'id'.
            Un échec XML-RPC sur un record n'interrompt pas la boucle (#571) — les
            appelants qui ignorent le retour (ex. `injection_rsc.py`) continuent de
            fonctionner sans modification.

        Note:
            Les valeurs None sont filtrées : un champ à None ne sera pas modifié dans Odoo.
            Pour effacer explicitement un champ, passer False.
            Attention : OdooReader._normalize_for() convertit les False Odoo en None,
            donc un round-trip read→update ne modifiera jamais les champs vides.
        """
        rapport = RapportEcriture()
        records_copy = copy.deepcopy(records)

        for record in records_copy:
            if "id" not in record:
                logger.warning(f"Record missing 'id' field, skipping: {record}")
                rapport.sans_id.append(record)
                continue

            record_id = int(record["id"])
            del record["id"]

            # Nettoyer les données
            clean_data = {
                k: v for k, v in record.items() if v is not None and not (hasattr(v, "__len__") and len(v) == 0)
            }

            try:
                if not self._sim:
                    self.execute(model, "write", [[record_id], clean_data])
                rapport.ids_reussis.append(record_id)
            except Exception as e:
                logger.warning(f"{model} #{record_id} write failed: {e}")
                rapport.echecs.append(EchecEcriture(id=record_id, raison=str(e)))

        mode_text = " [simulated]" if self._sim else ""
        logger.info(f"{len(rapport.ids_reussis)} {model} #{rapport.ids_reussis} written in Odoo db.{mode_text}")
        if rapport.echecs:
            logger.warning(f"{len(rapport.echecs)} {model} write(s) failed: {rapport.echecs}")

        return rapport
