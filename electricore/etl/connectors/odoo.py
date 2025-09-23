"""
Connecteur Odoo pour ElectriCore ETL avec support Polars.

Adapté du connecteur original pour retourner des DataFrames Polars
au lieu de pandas DataFrame.
"""

import xmlrpc.client
import polars as pl
import dlt
from typing import Any, Hashable, Optional, Dict, List
import copy
import time
import logging

logger = logging.getLogger(__name__)


class OdooReader:
    """
    Connecteur Odoo en lecture seule avec support Polars.

    Utilise XML-RPC pour se connecter à Odoo et retourne des DataFrames Polars.
    Restreint aux méthodes de lecture uniquement pour des raisons de sécurité.
    """

    # Méthodes autorisées en lecture seule
    _ALLOWED_METHODS = {
        'search', 'search_read', 'read', 'search_count', 'name_search',
        'name_get', 'fields_get', 'default_get', 'get_metadata',
        'check_access_rights', 'exists'
    }

    def __init__(self, config: Dict[str, str],
                 url: Optional[str] = None,
                 db: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None):
        """
        Initialise le connecteur Odoo avec configuration explicite.

        Args:
            config: Dictionnaire de configuration obligatoire
            url: URL du serveur Odoo (surcharge config si fourni)
            db: Nom de la base de données (surcharge config si fourni)
            username: Nom d'utilisateur (surcharge config si fourni)
            password: Mot de passe (surcharge config si fourni)
        """
        # Configuration explicite obligatoire
        self._url = url or config.get('url') or config.get('ODOO_URL')
        self._db = db or config.get('db') or config.get('ODOO_DB')
        self._username = username or config.get('username') or config.get('ODOO_USERNAME')
        self._password = password or config.get('password') or config.get('ODOO_PASSWORD')

        # Vérification des paramètres requis
        if not all([self._url, self._db, self._username, self._password]):
            missing = []
            if not self._url: missing.append('url')
            if not self._db: missing.append('db')
            if not self._username: missing.append('username')
            if not self._password: missing.append('password')
            raise ValueError(f"Paramètres manquants dans la configuration: {', '.join(missing)}")

        self._uid: Optional[int] = None
        self._proxy: Optional[Any] = None

    @property
    def is_connected(self) -> bool:
        """Vérifie si la connexion est active."""
        return self._uid is not None and self._proxy is not None

    def __enter__(self):
        """Support du gestionnaire de contexte."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Déconnexion propre."""
        self.disconnect()
        logger.info(f'Disconnected from {self._db} Odoo db.')

    def _ensure_connection(func):
        """Décorateur pour s'assurer que la connexion est active."""
        def wrapper(self, *args, **kwargs):
            if not self.is_connected:
                self.connect()
            return func(self, *args, **kwargs)
        return wrapper

    def connect(self) -> None:
        """Établit la connexion à Odoo."""
        self._uid = self._get_uid()
        self._proxy = xmlrpc.client.ServerProxy(f'{self._url}/xmlrpc/2/object')
        logger.info(f'Connected to {self._db} Odoo database.')

    def disconnect(self) -> None:
        """Ferme la connexion à Odoo."""
        if self.is_connected:
            if hasattr(self._proxy, '_ServerProxy__transport'):
                self._proxy._ServerProxy__transport.close()
            self._uid = None
            self._proxy = None

    def _get_uid(self) -> int:
        """
        Authentifie l'utilisateur et retourne l'ID utilisateur.

        Returns:
            int: ID utilisateur Odoo

        Raises:
            Exception: Si l'authentification échoue
        """
        common_proxy = xmlrpc.client.ServerProxy(f"{self._url}/xmlrpc/2/common")
        uid = common_proxy.authenticate(self._db, self._username, self._password, {})
        if not uid:
            raise Exception(f"Authentication failed for user {self._username} on {self._db}")
        return uid

    @_ensure_connection
    def execute(self, model: str, method: str, args: Optional[List] = None,
                kwargs: Optional[Dict] = None) -> Any:
        """
        Exécute une méthode sur le serveur Odoo.

        SÉCURITÉ: Seules les méthodes autorisées par la classe sont permises.

        Args:
            model: Modèle Odoo (ex: 'res.partner')
            method: Méthode à exécuter (ex: 'search_read')
            args: Arguments positionnels
            kwargs: Arguments nommés

        Returns:
            Résultat de l'exécution

        Raises:
            ValueError: Si la méthode n'est pas dans _ALLOWED_METHODS
        """
        # Vérification de sécurité : méthodes autorisées par la classe
        if method not in self._ALLOWED_METHODS:
            raise ValueError(
                f"Méthode '{method}' non autorisée dans {self.__class__.__name__}. "
                f"Méthodes autorisées: {', '.join(sorted(self._ALLOWED_METHODS))}. "
                f"Utilisez OdooWriter pour les opérations d'écriture."
            )

        args = args if args is not None else []
        kwargs = kwargs if kwargs is not None else {}

        logger.debug(f'Executing {method} on {model} with args {args} and kwargs {kwargs}')

        result = self._proxy.execute_kw(
            self._db, self._uid, self._password,
            model, method, args, kwargs
        )

        return result if isinstance(result, list) else [result]

    def search_read(self, model: str, domain: List = None,
                   fields: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Recherche et lit des enregistrements, retourne un DataFrame Polars.

        Args:
            model: Modèle Odoo
            domain: Filtre de recherche (ex: [['active', '=', True]])
            fields: Champs à récupérer

        Returns:
            pl.DataFrame: DataFrame Polars avec les résultats
        """
        domain = domain if domain is not None else []
        filters = [domain] if domain else [[]]
        kwargs = {'fields': fields} if fields else {}

        response = self.execute(model, 'search_read', args=filters, kwargs=kwargs)

        if not response:
            # Retourner un DataFrame vide avec la structure appropriée
            if fields:
                schema = {field: pl.Utf8 for field in fields}
                schema['id'] = pl.Int64
            else:
                schema = {'id': pl.Int64}
            return pl.DataFrame(schema=schema)

        df = pl.DataFrame(response)
        # Renommer la colonne id pour éviter les conflits
        if 'id' in df.columns:
            df = df.rename({'id': f'{model.replace(".", "_")}_id'})

        return df

    def read(self, model: str, ids: List[int],
             fields: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Lit des enregistrements par ID, retourne un DataFrame Polars.

        Args:
            model: Modèle Odoo
            ids: Liste des IDs à lire
            fields: Champs à récupérer

        Returns:
            pl.DataFrame: DataFrame Polars avec les résultats
        """
        if not ids:
            if fields:
                schema = {field: pl.Utf8 for field in fields}
                schema['id'] = pl.Int64
            else:
                schema = {'id': pl.Int64}
            return pl.DataFrame(schema=schema)

        kwargs = {'fields': fields} if fields else {}
        response = self.execute(model, 'read', [ids], kwargs)

        df = pl.DataFrame(response)
        # Renommer la colonne id pour éviter les conflits
        if 'id' in df.columns:
            df = df.rename({'id': f'{model.replace(".", "_")}_id'})

        return df

    @classmethod
    def as_dlt_resource(cls, model: str, resource_name: Optional[str] = None,
                       domain: List = None, fields: Optional[List[str]] = None):
        """
        Crée directement une resource dlt depuis les secrets dlt.

        Cette méthode ne nécessite pas d'instancier le connecteur au préalable.
        Elle utilise dlt.secrets pour récupérer la configuration Odoo.

        Args:
            model: Modèle Odoo à extraire
            resource_name: Nom de la resource (par défaut: nom du modèle)
            domain: Filtre de recherche
            fields: Champs à récupérer

        Returns:
            dlt.resource: Resource dlt prête à être utilisée
        """
        resource_name = resource_name or model.replace('.', '_')

        @dlt.resource(name=resource_name, write_disposition="replace")
        def odoo_resource():
            # Utilise dlt.secrets directement dans le contexte dlt
            config = dlt.secrets.get('odoo', {})
            with cls(config=config) as odoo:
                df = odoo.search_read(model, domain, fields)
                # Convertir en dictionnaires pour dlt
                for row in df.iter_rows(named=True):
                    yield row

        return odoo_resource


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