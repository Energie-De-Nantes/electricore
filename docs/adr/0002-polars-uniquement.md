# Polars exclusivement, pas de pandas

Les pipelines de calcul utilisent Polars uniquement (LazyFrame chaînés, expressions `pl.Expr` composables). Le déclencheur de la migration depuis pandas n'était pas la performance brute mais la **composabilité fonctionnelle** : `pl.Expr` permet d'écrire des transformations pures `Fn(Series) -> Series` qu'on chaîne sans muter d'état — un pattern impraticable avec l'API pandas. Pandas est exclu y compris dans les notebooks d'exploration ; toute dépendance qui en réintroduirait est à proscrire.
