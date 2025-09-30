"""
Connecteur Odoo avec capacités d'écriture.

Ce module fournit OdooWriter pour créer et modifier des données dans Odoo ERP.
"""

import copy
import logging
from typing import Dict, List, Any, Hashable

from electricore.core.loaders.odoo import OdooReader

logger = logging.getLogger(__name__)


class OdooWriter(OdooReader):
    """
    Connecteur Odoo avec capacités d'écriture.

    Hérite de OdooReader et étend les méthodes autorisées pour inclure les écritures.
    """

    # Étendre les méthodes autorisées avec les opérations d'écriture
    _ALLOWED_METHODS = OdooReader._ALLOWED_METHODS | {
        'create', 'write', 'unlink', 'copy', 'action_confirm', 'action_cancel',
        'action_done', 'button_confirm', 'button_cancel', 'toggle_active'
    }

    def __init__(self, config: Dict[str, str], sim: bool = False, **kwargs):
        """
        Initialise le connecteur avec mode simulation.

        Args:
            config: Configuration de connexion (obligatoire)
            sim: Mode simulation (n'écrit pas réellement)
            **kwargs: Arguments passés à OdooReader
        """
        super().__init__(config, **kwargs)
        self._sim = sim

    @OdooReader._ensure_connection
    def create(self, model: str, records: List[Dict[Hashable, Any]]) -> List[int]:
        """
        Crée des enregistrements dans Odoo.

        Args:
            model: Modèle Odoo
            records: Liste des enregistrements à créer

        Returns:
            List[int]: Liste des IDs créés
        """
        if self._sim:
            logger.info(f'# {len(records)} {model} creation called. [simulated]')
            return []

        # Nettoyer les données pour XML-RPC
        clean_records = []
        for record in records:
            clean_record = {}
            for k, v in record.items():
                if v is not None and not (hasattr(v, '__iter__') and len(str(v)) == 0):
                    clean_record[k] = v
            clean_records.append(clean_record)

        result = self.execute(model, 'create', [clean_records])
        created_ids = result if isinstance(result, list) else [result]

        logger.info(f'{model} #{created_ids} created in Odoo db.')
        return created_ids

    @OdooReader._ensure_connection
    def update(self, model: str, records: List[Dict[Hashable, Any]]) -> None:
        """
        Met à jour des enregistrements dans Odoo.

        Args:
            model: Modèle Odoo
            records: Liste des enregistrements à mettre à jour (doivent contenir 'id')
        """
        updated_ids = []
        records_copy = copy.deepcopy(records)

        for record in records_copy:
            if 'id' not in record:
                logger.warning(f"Record missing 'id' field, skipping: {record}")
                continue

            record_id = int(record['id'])
            del record['id']

            # Nettoyer les données
            clean_data = {k: v for k, v in record.items()
                         if v is not None and not (hasattr(v, '__iter__') and len(str(v)) == 0)}

            if not self._sim:
                self.execute(model, 'write', [[record_id], clean_data])
            updated_ids.append(record_id)

        mode_text = " [simulated]" if self._sim else ""
        logger.info(f'{len(records_copy)} {model} #{updated_ids} written in Odoo db.{mode_text}')