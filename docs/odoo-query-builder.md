# Odoo Query Builder - Philosophie et Architecture

## Vue d'ensemble

Le Query Builder Odoo est un système de requêtage fluide qui simplifie l'exploration et l'enrichissement de données depuis Odoo vers des DataFrames Polars. Il transforme des opérations complexes d'extraction, transformation et jointure en une API intuitive et chainable.

## Philosophie de conception

### 1. **Séparation claire des responsabilités**

L'architecture distingue deux intentions fondamentales :

- **🧭 Navigation** (`follow()`) : Explorer les relations en changeant de contexte
- **🔗 Enrichissement** (`enrich()`) : Compléter les données sans changer de contexte

```python
# Navigation : je veux explorer les factures d'une commande
order.follow('invoice_ids')  # Contexte change : sale.order → account.move

# Enrichissement : je veux ajouter les détails du partenaire à ma commande
order.enrich('partner_id')   # Contexte reste : sale.order (+ détails partenaire)
```

### 2. **Auto-détection intelligente**

Plus besoin de spécifier manuellement les modèles cibles. Le système utilise l'API `fields_get` d'Odoo pour détecter automatiquement :

- Le type de relation (`many2one`, `one2many`, `many2many`)
- Le modèle cible (`res.partner`, `account.move`, etc.)
- La nécessité d'explode pour les relations multiples

```python
# AVANT (verbeux et sujet aux erreurs)
.follow('invoice_ids', 'account.move', fields=['name'])
.follow('invoice_line_ids', 'account.move.line', fields=['product_id'])
.enrich('product_id', 'product.product', fields=['name'])

# APRÈS (simplifié et robuste)
.follow('invoice_ids', fields=['name'])
.follow('invoice_line_ids', fields=['product_id'])
.enrich('product_id', fields=['name'])
```

### 3. **Architecture modulaire DRY**

Le code évite la duplication en décomposant les opérations en méthodes atomiques réutilisables :

```
_detect_relation_info()  →  Détection type/modèle
_prepare_dataframe()     →  Explode si nécessaire
_extract_ids()          →  Extraction des IDs
_fetch_related_data()   →  Récupération depuis Odoo
_join_dataframes()      →  Jointures avec gestion conflits
        ↓
_enrich_data()          →  Orchestration centrale
        ↓
follow() / enrich()     →  API publique sémantique
```

### 4. **Gestion transparente de la complexité**

Le système gère automatiquement les aspects techniques complexes :

- **Types de relations** : Comportement adapté selon `many2one`/`one2many`/`many2many`
- **Conflits de noms** : Renommage automatique avec suffixes (`name_res_partner`)
- **Conversions de types** : String → Int pour les jointures
- **Colonnes temporaires** : Nettoyage automatique des `*_id_join`
- **Gestion d'erreurs** : Retour gracieux si pas de données

## Architecture technique

### Structure des classes

```python
@dataclass(frozen=True)
class OdooQuery:
    connector: OdooReader           # Accès aux données Odoo
    lazy_frame: pl.LazyFrame        # Données courantes (lazy)
    _field_mappings: Dict[str, str] # Mapping colonnes renommées
    _current_model: Optional[str]   # Modèle Odoo courant pour auto-détection
```

### Flux de données

```
1. query('sale.order')
   ↓ [search_read] → extraction initiale depuis Odoo
   ↓ [pl.DataFrame.lazy()] → conversion en LazyFrame
   ↓ [_current_model = 'sale.order']

2. .follow('invoice_ids')
   ↓ [_detect_relation_info] → type='one2many', model='account.move'
   ↓ [_prepare_dataframe] → explode()
   ↓ [_extract_ids] → [123, 456, 789]
   ↓ [_fetch_related_data] → search_read('account.move', ...)
   ↓ [_join_dataframes] → join + cleanup
   ↓ [_current_model = 'account.move']

3. .enrich('partner_id')
   ↓ [_detect_relation_info] → type='many2one', model='res.partner'
   ↓ [_prepare_dataframe] → pas d'explode
   ↓ [_extract_ids] → extraction depuis [id, name]
   ↓ [_fetch_related_data] → search_read('res.partner', ...)
   ↓ [_join_dataframes] → join + cleanup
   ↓ [_current_model = 'account.move'] (inchangé)
```

### Cache et performance

- **Cache des métadonnées** : `_fields_cache` évite les appels répétés à `fields_get`
- **LazyFrame** : Exécution différée jusqu'à `.collect()`
- **Optimisations Polars** : Pushdown predicates, projections, etc.

## Patterns d'utilisation

### Pattern 1 : Exploration pure (Navigation)

```python
# Explorer la hiérarchie commande → facture → lignes → produits
result = (odoo.query('sale.order')
    .follow('invoice_ids')      # → account.move
    .follow('invoice_line_ids') # → account.move.line
    .follow('product_id')       # → product.product
    .collect())
```

### Pattern 2 : Enrichissement contextuel

```python
# Enrichir une commande avec tous ses détails
result = (odoo.query('sale.order')
    .enrich('partner_id', fields=['name', 'email'])     # + partenaire
    .enrich('user_id', fields=['name'])                 # + commercial
    .enrich('invoice_ids', fields=['name', 'amount'])   # + factures (explode)
    .collect())
```

### Pattern 3 : Mixte (Recommandé)

```python
# Naviguer puis enrichir à chaque niveau
result = (odoo.query('sale.order')
    .enrich('partner_id', fields=['name'])              # Enrichir commande
    .follow('invoice_ids')                              # Navigator → factures
    .enrich('partner_id', fields=['email'])             # Enrichir factures
    .follow('invoice_line_ids')                         # Navigator → lignes
    .enrich('product_id', fields=['name', 'categ_id'])  # Enrichir lignes
    .collect())
```

## Avantages de l'approche

### 1. **Simplicité d'usage**

- API fluide et intuitive
- Pas de connaissance technique requise des modèles Odoo
- Auto-complétion IDE avec types statiques

### 2. **Robustesse**

- Gestion automatique des cas d'erreur
- Validation des relations via métadonnées Odoo
- Nettoyage automatique des artéfacts techniques

### 3. **Performance**

- Cache des métadonnées
- Utilisation optimale de Polars LazyFrame
- Jointures efficaces avec gestion des types

### 4. **Maintenabilité**

- Code DRY avec méthodes atomiques
- Tests faciles grâce à la modularité
- Évolution simple (nouveaux wrappers)

### 5. **Expressivité**

- Intention claire : navigation vs enrichissement
- Composition flexible selon les besoins
- Code auto-documenté

## Cas d'usage métier

### Analyse des ventes par PDL

```python
# Récupérer toutes les données de facturation détaillées
facturation = (odoo.query('sale.order', domain=[('x_pdl', '!=', False)])
    .enrich('partner_id', fields=['name'])              # Client
    .follow('invoice_ids')                              # → Factures
    .follow('invoice_line_ids')                         # → Lignes
    .enrich('product_id', fields=['name', 'categ_id'])  # → Produits
    .collect())

# Analyse avec Polars
stats = facturation.with_columns([
    pl.col('price_total').sum().over('x_pdl').alias('total_pdl'),
    pl.col('price_total').sum().over('categ_id').alias('total_categorie')
])
```