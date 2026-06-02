# Monorepo unique au lieu de packages séparés

Le projet a été initialement scindé en `electriflux` (ETL : SFTP, déchiffrement, parsing XML) et `electricore` (logique métier pure) pour séparer infrastructure et calcul. Maintenir des schémas Pandera dupliqués entre les deux dépôts est devenu plus coûteux que la frontière n'était utile, et la consolidation en un monorepo unique a éliminé cette duplication. Pour un projet solo sur un domaine cohésif (données Enedis), le coût de coordination entre packages dépasse le bénéfice d'une séparation logique.
