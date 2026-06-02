# Issue tracker: GitHub

Issues et PRDs pour ce dépôt vivent comme issues GitHub sur `Energie-De-Nantes/electricore`. Utiliser la CLI `gh` pour toutes les opérations.

## Conventions

- **Créer une issue** : `gh issue create --title "..." --body "..."`. Utiliser un heredoc pour les corps multi-lignes.
- **Lire une issue** : `gh issue view <number> --comments`, en filtrant les commentaires avec `jq` si besoin, et en récupérant aussi les labels.
- **Lister les issues** : `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` avec les filtres `--label` / `--state` appropriés.
- **Commenter** : `gh issue comment <number> --body "..."`
- **Appliquer / retirer un label** : `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Fermer** : `gh issue close <number> --comment "..."`

Le dépôt cible est inféré du `git remote -v` — `gh` le détecte automatiquement quand exécuté depuis le clone.

## Quand une skill dit "publier sur le tracker"

Créer une issue GitHub.

## Quand une skill dit "récupérer le ticket pertinent"

Exécuter `gh issue view <number> --comments`.

## Conventions de titre

Les commits suivent le Conventional Commits en français (voir CONTRIBUTING.md). Les titres d'issues n'ont pas de préfixe imposé mais doivent être clairs et orientés *résultat* plutôt que *implémentation*.
