# Query builders fluents et immuables

L'accès aux données passe par `DuckDBQuery` et `OdooQuery`, des builders chaînables immuables (`.filter().limit().collect()`) plutôt que par du SQL brut, un ORM, ou des appels Polars directs sur les sources. Trois motivations cumulées :

1. **Composabilité** — chaîner filtres et jointures entre couches sans manipuler de strings SQL ni perdre le pushdown au moteur sous-jacent.
2. **Immutabilité** — chaque appel retourne une nouvelle instance, ce qui permet de forker une requête de base entre notebooks/pipelines sans bug de mutation partagée.
3. **Uniformité** — la même API fluente couvre DuckDB et Odoo (XML-RPC), les pipelines ignorent la source réelle. C'est une vraie couche d'abstraction à maintenir, acceptée pour réduire la charge cognitive quand un même ingénieur passe de l'un à l'autre.
