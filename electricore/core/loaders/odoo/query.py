"""
Query builder immutable pour Odoo avec support Polars.

Ce module fournit OdooQuery, un builder de requêtes fonctionnel
qui permet de naviguer et enrichir les données Odoo de manière fluide.
"""

from __future__ import annotations

import polars as pl
from typing import Dict, List, Optional
from dataclasses import dataclass, field

import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OdooQuery:
    """
    Query builder immutable pour chaîner les opérations Odoo + Polars.

    Permet de composer facilement des requêtes complexes avec suivis de relations
    et enrichissements de données depuis Odoo.

    Example:
        >>> with OdooReader(config) as odoo:
        ...     df = (odoo.query('sale.order', domain=[('x_pdl', '!=', False)])
        ...         .follow('invoice_ids', fields=['name', 'amount_total'])
        ...         .filter(pl.col('amount_total') > 100)
        ...         .collect())
    """

    connector: 'OdooReader'  # Type hint forward reference
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