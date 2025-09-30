"""
Connecteur Odoo en lecture seule avec support Polars.

Ce module fournit OdooReader, un connecteur XML-RPC en lecture seule
qui retourne des DataFrames Polars.
"""

import xmlrpc.client
import polars as pl
import logging
from typing import Any, Optional, Dict, List

from .config import OdooConfig, FieldsCache

logger = logging.getLogger(__name__)


class OdooReader:
    """
    Connecteur Odoo en lecture seule avec support Polars.

    Utilise XML-RPC pour se connecter à Odoo et retourne des DataFrames Polars.
    Restreint aux méthodes de lecture uniquement pour des raisons de sécurité.

    Example:
        >>> with OdooReader(config) as odoo:
        ...     df = odoo.search_read('res.partner', [('customer', '=', True)])
        ...     print(df.head())
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

        Example:
            >>> config = {'url': 'https://demo.odoo.com', 'db': 'demo',
            ...           'username': 'admin', 'password': 'admin'}
            >>> odoo = OdooReader(config)
        """
        # Créer configuration immutable
        config_dict = {
            'url': url or config.get('url') or config.get('ODOO_URL'),
            'db': db or config.get('db') or config.get('ODOO_DB'),
            'username': username or config.get('username') or config.get('ODOO_USERNAME'),
            'password': password or config.get('password') or config.get('ODOO_PASSWORD'),
        }
        self._config = OdooConfig.from_dict(config_dict)

        # Cache pour métadonnées des champs
        self._fields_cache = FieldsCache()

        # État de connexion
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
        logger.info(f'Disconnected from {self._config.db} Odoo db.')

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
        self._proxy = xmlrpc.client.ServerProxy(f'{self._config.url}/xmlrpc/2/object')
        logger.info(f'Connected to {self._config.db} Odoo database.')

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
        common_proxy = xmlrpc.client.ServerProxy(f"{self._config.url}/xmlrpc/2/common")
        uid = common_proxy.authenticate(
            self._config.db, self._config.username, self._config.password, {}
        )
        if not uid:
            raise Exception(
                f"Authentication failed for user {self._config.username} on {self._config.db}"
            )
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
            self._config.db, self._uid, self._config.password,
            model, method, args, kwargs
        )

        return result if isinstance(result, list) else [result]

    def _normalize_for(self, response: List[Dict]) -> pl.DataFrame:
        """
        Normalise les données Odoo pour Polars.

        Convertit les False en None car Odoo utilise False pour les champs vides
        alors que Polars attend None/null.

        Args:
            response: Liste de dictionnaires depuis Odoo

        Returns:
            pl.DataFrame: DataFrame Polars normalisé
        """
        if not response:
            return pl.DataFrame()

        # Normaliser les False en None pour Polars
        for record in response:
            for key, value in record.items():
                if value is False:
                    record[key] = None

        # Augmenter infer_schema_length pour gérer correctement les champs many2one
        # qui sont des listes [id, name] - Polars doit scanner plus de lignes
        return pl.DataFrame(response, strict=False, infer_schema_length=None)

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

        Example:
            >>> df = odoo.search_read('res.partner', [('customer', '=', True)],
            ...                       fields=['name', 'email'])
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

        df = self._normalize_for(response)
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

        Example:
            >>> df = odoo.read('res.partner', [1, 2, 3], fields=['name', 'email'])
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

        df = self._normalize_for(response)
        # Renommer la colonne id pour éviter les conflits
        if 'id' in df.columns:
            df = df.rename({'id': f'{model.replace(".", "_")}_id'})

        return df

    def get_field_info(self, model: str, field_name: str) -> Optional[Dict]:
        """
        Récupère les métadonnées d'un champ avec cache.

        Args:
            model: Modèle Odoo
            field_name: Nom du champ

        Returns:
            Dict: Métadonnées du champ (type, relation, etc.) ou None si non trouvé

        Example:
            >>> info = odoo.get_field_info('sale.order', 'partner_id')
            >>> print(info['type'])  # 'many2one'
            >>> print(info['relation'])  # 'res.partner'
        """
        # Vérifier le cache
        cached = self._fields_cache.get(model, field_name)
        if cached is not None:
            return cached

        # Récupérer depuis Odoo
        try:
            result = self.execute(model, 'fields_get', args=[[field_name]])
            fields_info = result[0] if isinstance(result, list) else result
            field_info = fields_info.get(field_name)
            self._fields_cache.set(model, field_name, field_info)
            return field_info
        except Exception as e:
            logger.warning(f"Cannot get field info for {model}.{field_name}: {e}")
            self._fields_cache.set(model, field_name, None)
            return None

    def get_relation_model(self, model: str, field_name: str) -> Optional[str]:
        """
        Détermine automatiquement le modèle cible d'une relation.

        Args:
            model: Modèle source
            field_name: Champ de relation

        Returns:
            str: Nom du modèle cible ou None si pas une relation

        Example:
            >>> target = odoo.get_relation_model('sale.order', 'partner_id')
            >>> print(target)  # 'res.partner'
        """
        field_info = self.get_field_info(model, field_name)
        if field_info and field_info.get('type') in ['many2one', 'one2many', 'many2many']:
            return field_info.get('relation')
        return None