"""
Connecteur Odoo pour ElectriCore ETL avec support Polars.

Adapté du connecteur original pour retourner des DataFrames Polars
au lieu de pandas DataFrame.
"""

from __future__ import annotations

import xmlrpc.client
import polars as pl
import dlt
from typing import Any, Hashable, Optional, Dict, List
from dataclasses import dataclass, field
import copy
import time
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OdooQuery:
    """
    Query builder immutable pour chaîner les opérations Odoo + Polars.

    Permet de composer facilement des requêtes complexes avec suivis de relations
    et enrichissements de données depuis Odoo.
    """

    connector: OdooReader
    lazy_frame: pl.LazyFrame
    _field_mappings: Dict[str, str] = field(default_factory=dict)
    _current_model: Optional[str] = None

    # === Méthodes atomiques privées ===

    def _detect_relation_info(self, field_name: str) -> tuple[Optional[str], Optional[str]]:
        """
        Détecte le type de relation et le modèle cible.

        Returns:
            (type_relation, modele_cible) ou (None, None) si non trouvé
        """
        if self._current_model is None:
            return None, None

        field_info = self.connector.get_field_info(self._current_model, field_name)
        if field_info is None:
            return None, None

        relation_type = field_info.get('type')
        target_model = field_info.get('relation') if relation_type in ['many2one', 'one2many', 'many2many'] else None

        return relation_type, target_model

    def _prepare_dataframe(self, df: pl.DataFrame, field_name: str, relation_type: str) -> pl.DataFrame:
        """
        Prépare le DataFrame (explode si nécessaire selon le type de relation).
        """
        if relation_type in ['one2many', 'many2many']:
            return df.explode(field_name)
        return df

    def _extract_ids(self, df: pl.DataFrame, field_name: str, relation_type: str) -> List[int]:
        """
        Extrait les IDs uniques depuis un champ selon son type.
        """
        if relation_type == 'many2one':
            # Gérer les champs many2one [id, name]
            id_col = df[field_name]
            if id_col.dtype == pl.List:
                # Extraire l'ID depuis [id, name]
                unique_ids = [
                    int(id) for id in df.select(
                        pl.col(field_name).list.get(0)
                    ).to_series().unique().to_list()
                    if id is not None
                ]
            else:
                # Simple ID field
                unique_ids = [
                    int(id) for id in df[field_name].unique().to_list()
                    if id is not None
                ]
        else:
            # one2many, many2many : IDs directs (après explode)
            unique_ids = [
                int(id) for id in df[field_name].unique().to_list()
                if id is not None
            ]

        return unique_ids

    def _fetch_related_data(self, target_model: str, ids: List[int], fields: Optional[List[str]]) -> pl.DataFrame:
        """
        Récupère les données liées depuis Odoo.
        """
        if not ids:
            # Retourner DataFrame vide avec schéma approprié
            if fields:
                schema = {field: pl.Utf8 for field in fields}
                schema[f'{target_model.replace(".", "_")}_id'] = pl.Int64
            else:
                schema = {f'{target_model.replace(".", "_")}_id': pl.Int64}
            return pl.DataFrame(schema=schema)

        # Récupérer depuis Odoo
        related_df = self.connector.search_read(target_model, [('id', 'in', ids)], fields)

        if related_df.is_empty():
            return related_df

        # Renommer 'id' vers nom avec alias pour éviter conflits
        target_alias = target_model.replace('.', '_')
        id_column = f'{target_alias}_id'

        if 'id' in related_df.columns:
            related_df = related_df.rename({'id': id_column})

        return related_df

    def _join_dataframes(self, left_df: pl.DataFrame, right_df: pl.DataFrame,
                        field_name: str, relation_type: str, target_model: str) -> pl.DataFrame:
        """
        Joint les DataFrames en gérant les conflits de noms et types.
        """
        if right_df.is_empty():
            return left_df

        # Générer alias et nom de colonne ID
        target_alias = target_model.replace('.', '_')
        id_column = f'{target_alias}_id'

        # Renommer colonnes en conflit
        rename_mapping = {}
        for col in right_df.columns:
            if col != id_column and col in left_df.columns:
                new_name = f'{col}_{target_alias}'
                rename_mapping[col] = new_name

        if rename_mapping:
            right_df = right_df.rename(rename_mapping)

        # Préparer la clé de jointure
        if relation_type == 'many2one':
            if left_df[field_name].dtype == pl.List:
                # Extraire l'ID depuis [id, name] et convertir en entier
                left_df = left_df.with_columns([
                    pl.col(field_name).list.get(0).cast(pl.Int64).alias(f'{field_name}_id_join')
                ])
                join_key = f'{field_name}_id_join'
            else:
                # S'assurer que la clé est en entier
                left_df = left_df.with_columns([
                    pl.col(field_name).cast(pl.Int64)
                ])
                join_key = field_name
        else:
            # one2many, many2many : jointure directe
            join_key = field_name

        # Effectuer la jointure
        result = left_df.join(
            right_df,
            left_on=join_key,
            right_on=id_column,
            how='left'
        )

        # Nettoyer les colonnes temporaires de jointure (*_id_join)
        temp_join_columns = [col for col in result.columns if col.endswith('_id_join')]
        if temp_join_columns:
            result = result.drop(temp_join_columns)

        return result

    # === Méthode centrale ===

    def _enrich_data(self, field_name: str, target_model: Optional[str] = None,
                     fields: Optional[List[str]] = None) -> tuple[pl.LazyFrame, str]:
        """
        Méthode centrale pour enrichir avec des données liées.

        Args:
            field_name: Champ de relation
            target_model: Modèle cible (auto-détecté si None)
            fields: Champs à récupérer

        Returns:
            (LazyFrame enrichi, modèle cible utilisé)

        Raises:
            ValueError: Si impossible de déterminer le modèle cible
        """
        # Détection automatique du type et modèle
        relation_type, detected_model = self._detect_relation_info(field_name)

        if target_model is None:
            target_model = detected_model

        if target_model is None:
            raise ValueError(f"Cannot determine target model for field '{field_name}' in model '{self._current_model}'. Please specify target_model explicitly.")

        if relation_type is None:
            raise ValueError(f"Field '{field_name}' is not a relation field in model '{self._current_model}'.")

        # Préparer le DataFrame courant
        current_df = self.lazy_frame.collect()

        # Explode si nécessaire selon le type de relation
        prepared_df = self._prepare_dataframe(current_df, field_name, relation_type)

        # Extraire les IDs uniques
        unique_ids = self._extract_ids(prepared_df, field_name, relation_type)

        if not unique_ids:
            # Pas d'IDs, retourner le DataFrame préparé sans modification
            return prepared_df.lazy(), target_model

        # Récupérer les données liées
        related_df = self._fetch_related_data(target_model, unique_ids, fields)

        # Joindre les DataFrames
        result_df = self._join_dataframes(prepared_df, related_df, field_name, relation_type, target_model)

        return result_df.lazy(), target_model

    # === API publique ===

    def follow(self, relation_field: str, target_model: Optional[str] = None, fields: Optional[List[str]] = None) -> OdooQuery:
        """
        Navigue vers une relation (change le modèle courant).

        Détecte automatiquement le type de relation et applique explode si nécessaire.
        Convient pour one2many, many2many et many2one.

        Args:
            relation_field: Champ de relation (ex: 'invoice_ids', 'partner_id')
            target_model: Modèle cible - détecté automatiquement si non fourni
            fields: Champs à récupérer du modèle cible

        Returns:
            Nouvelle OdooQuery avec le modèle courant changé vers target_model

        Example:
            >>> query.follow('invoice_ids', fields=['name', 'invoice_date'])
            >>> query.follow('partner_id', fields=['name', 'email'])
        """
        lazy_frame, target_model = self._enrich_data(relation_field, target_model, fields)

        return OdooQuery(
            connector=self.connector,
            lazy_frame=lazy_frame,
            _field_mappings=self._field_mappings,
            _current_model=target_model  # Navigation : change le modèle courant
        )

    def enrich(self, relation_field: str, target_model: Optional[str] = None, fields: Optional[List[str]] = None) -> OdooQuery:
        """
        Enrichit avec des données liées (garde le modèle courant).

        Détecte automatiquement le type de relation et applique explode si nécessaire.
        Convient pour one2many, many2many et many2one.

        Args:
            relation_field: Champ de relation (ex: 'invoice_ids', 'partner_id')
            target_model: Modèle cible - détecté automatiquement si non fourni
            fields: Champs à récupérer du modèle cible

        Returns:
            Nouvelle OdooQuery avec le modèle courant inchangé

        Example:
            >>> query.enrich('partner_id', fields=['name', 'email'])  # Ajoute détails partenaire
            >>> query.enrich('invoice_ids', fields=['name', 'amount'])  # Ajoute détails factures (explode)
        """
        lazy_frame, _ = self._enrich_data(relation_field, target_model, fields)

        return OdooQuery(
            connector=self.connector,
            lazy_frame=lazy_frame,
            _field_mappings=self._field_mappings,
            _current_model=self._current_model  # Enrichissement : garde le modèle courant
        )

    def with_details(self, id_field: str, target_model: Optional[str] = None, fields: Optional[List[str]] = None) -> OdooQuery:
        """
        Enrichit avec des détails d'un modèle lié (many-to-one).

        Args:
            id_field: Champ contenant l'ID ou [id, name] (ex: 'partner_id')
            target_model: Modèle cible (ex: 'res.partner') - détecté automatiquement si non fourni
            fields: Champs à récupérer

        Returns:
            Nouvelle OdooQuery avec les détails ajoutés

        Example:
            >>> query.with_details('partner_id', fields=['name', 'email'])
            >>> # ou explicite: query.with_details('partner_id', 'res.partner', ['name'])
        """
        # Détection automatique du modèle cible si non fourni
        if target_model is None and self._current_model is not None:
            target_model = self.connector.get_relation_model(self._current_model, id_field)
            if target_model is None:
                raise ValueError(f"Cannot determine target model for field '{id_field}' in model '{self._current_model}'. Please specify target_model explicitly.")

        if target_model is None:
            raise ValueError("target_model must be specified when _current_model is not set")

        current_df = self.lazy_frame.collect()

        # Extraire les IDs (gérer les many2one [id, name])
        id_col = current_df[id_field]
        if id_col.dtype == pl.List:
            # Many2one field [id, name] - extraire les IDs
            unique_ids = [
                int(id) for id in current_df.select(
                    pl.col(id_field).list.get(0)
                ).to_series().unique().to_list()
                if id is not None
            ]
        else:
            # Simple ID field
            unique_ids = [
                int(id) for id in current_df[id_field].unique().to_list()
                if id is not None
            ]

        if not unique_ids:
            return self

        # Récupérer les données depuis Odoo
        related_df = self.connector.read(target_model, unique_ids, fields)

        # Vérifier si on a récupéré des données
        if related_df.is_empty():
            # Pas de données trouvées, retourner le DataFrame actuel sans modification
            return self

        # Générer un alias unique et renommer la colonne 'id'
        target_alias = target_model.replace('.', '_')
        id_column = f'{target_alias}_id'

        # Renommer 'id' vers le nom avec alias pour éviter les conflits
        if 'id' in related_df.columns:
            related_df = related_df.rename({'id': id_column})

        # Renommer pour éviter les conflits
        rename_mapping = {}
        for col in related_df.columns:
            if col != id_column and col in current_df.columns:
                new_name = f'{col}_{target_alias}'
                rename_mapping[col] = new_name

        if rename_mapping:
            related_df = related_df.rename(rename_mapping)

        # Préparer la clé de jointure
        if current_df[id_field].dtype == pl.List:
            # Extraire l'ID depuis [id, name] et convertir en entier
            current_df = current_df.with_columns([
                pl.col(id_field).list.get(0).cast(pl.Int64).alias(f'{id_field}_id_join')
            ])
            join_key = f'{id_field}_id_join'
        else:
            # S'assurer que la clé est en entier
            current_df = current_df.with_columns([
                pl.col(id_field).cast(pl.Int64)
            ])
            join_key = id_field

        # Join
        result = current_df.join(
            related_df,
            left_on=join_key,
            right_on=id_column,
            how='left'
        )

        return OdooQuery(
            connector=self.connector,
            lazy_frame=result.lazy(),
            _field_mappings={**self._field_mappings, **rename_mapping},
            _current_model=self._current_model  # Garder le modèle actuel car with_details n'en change pas
        )

    def filter(self, *conditions) -> OdooQuery:
        """Applique des filtres Polars."""
        return OdooQuery(
            connector=self.connector,
            lazy_frame=self.lazy_frame.filter(*conditions),
            _field_mappings=self._field_mappings,
            _current_model=self._current_model
        )

    def select(self, *columns) -> OdooQuery:
        """Sélectionne des colonnes spécifiques."""
        return OdooQuery(
            connector=self.connector,
            lazy_frame=self.lazy_frame.select(*columns),
            _field_mappings=self._field_mappings,
            _current_model=self._current_model
        )

    def rename(self, mapping: Dict[str, str]) -> OdooQuery:
        """Renomme des colonnes."""
        return OdooQuery(
            connector=self.connector,
            lazy_frame=self.lazy_frame.rename(mapping),
            _field_mappings={**self._field_mappings, **mapping},
            _current_model=self._current_model
        )

    def lazy(self) -> pl.LazyFrame:
        """Retourne le LazyFrame pour opérations Polars avancées."""
        return self.lazy_frame

    def collect(self) -> pl.DataFrame:
        """Exécute la query et retourne le DataFrame."""
        return self.lazy_frame.collect()


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
        # Cache pour les métadonnées des champs
        self._fields_cache: Dict[str, Dict[str, Dict]] = {}
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

    def _normalize_for_polars(self, response: List[Dict]) -> pl.DataFrame:
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

        return pl.DataFrame(response, strict=False)

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

        df = self._normalize_for_polars(response)
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

        df = self._normalize_for_polars(response)
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
        """
        # Vérifier le cache
        if model not in self._fields_cache:
            self._fields_cache[model] = {}

        if field_name not in self._fields_cache[model]:
            # Récupérer les métadonnées depuis Odoo
            try:
                result = self.execute(model, 'fields_get', args=[[field_name]])
                fields_info = result[0] if isinstance(result, list) else result
                self._fields_cache[model][field_name] = fields_info.get(field_name)
            except Exception as e:
                logger.warning(f"Cannot get field info for {model}.{field_name}: {e}")
                self._fields_cache[model][field_name] = None

        return self._fields_cache[model][field_name]

    def get_relation_model(self, model: str, field_name: str) -> Optional[str]:
        """
        Détermine automatiquement le modèle cible d'une relation.

        Args:
            model: Modèle source
            field_name: Champ de relation

        Returns:
            str: Nom du modèle cible ou None si pas une relation
        """
        field_info = self.get_field_info(model, field_name)
        if field_info and field_info.get('type') in ['many2one', 'one2many', 'many2many']:
            return field_info.get('relation')
        return None

    def query(self, model: str, domain: List = None, fields: Optional[List[str]] = None) -> OdooQuery:
        """
        Point d'entrée du query builder.

        Args:
            model: Modèle Odoo à requêter
            domain: Filtre Odoo (ex: [('state', '=', 'sale')])
            fields: Champs à récupérer initialement

        Returns:
            OdooQuery pour chaîner les opérations

        Example:
            >>> with OdooReader(config) as odoo:
            ...     df = (odoo.query('sale.order', domain=[('x_pdl', '!=', False)])
            ...         .follow('invoice_ids', 'account.move')
            ...         .follow('invoice_line_ids', 'account.move.line')
            ...         .filter(pl.col('quantity') > 0)
            ...         .collect())
        """
        df = self.search_read(model, domain, fields)
        return OdooQuery(connector=self, lazy_frame=df.lazy(), _current_model=model)

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