# Triage Labels

Les skills parlent en termes de cinq rôles canoniques. Ce fichier mappe ces rôles aux labels effectivement utilisés dans le tracker (GitHub Issues du dépôt).

| Rôle skill          | Label GitHub        | Signification                                       |
| ------------------- | ------------------- | --------------------------------------------------- |
| `needs-triage`      | `needs-triage`      | Le mainteneur doit évaluer l'issue                  |
| `needs-info`        | `needs-info`        | En attente d'informations supplémentaires du reporter |
| `ready-for-agent`   | `ready-for-agent`   | Spécifié de bout en bout, prêt pour un agent AFK    |
| `ready-for-human`   | `ready-for-human`   | Nécessite une implémentation humaine                |
| `wontfix`           | `wontfix`           | Ne sera pas traité                                  |

Quand une skill mentionne un rôle (par ex. « apply the AFK-ready triage label »), utiliser la chaîne de la colonne *Label GitHub*.

Modifier la colonne de droite pour refléter tout renommage futur côté GitHub.

## Convention `ready-for-agent`

Le label `ready-for-agent` est **réservé aux agents du mainteneur**. Au moment où il est posé, l'issue est **immédiatement auto-assignée au mainteneur** : ce n'est pas une invitation ouverte à contribuer.

Les PR externes non sollicitées — en particulier celles générées par des agents ou des outils automatisés — qui répondent à ces issues sont fermées sans être fusionnées, indépendamment de leur qualité (cf. [CONTRIBUTING.md](../../CONTRIBUTING.md)).
