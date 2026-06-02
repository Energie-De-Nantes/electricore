# Domain Docs

Comment les engineering skills doivent consommer la documentation métier de ce dépôt.

## Avant d'explorer, lire

- [**`CONTEXT-MAP.md`**](../../CONTEXT-MAP.md) à la racine — pointe vers un `CONTEXT.md` par module. Lire celui (ou ceux) pertinents au sujet de la tâche.
- [**`docs/adr/`**](../adr/) — lire les ADRs qui touchent la zone sur laquelle on s'apprête à travailler. Les ADRs sont au niveau racine (pas de `docs/adr/` par module pour l'instant).

Si un de ces fichiers n'existe pas, **avancer en silence**. Ne pas signaler son absence ; ne pas proposer de le créer en amont. La skill productrice (`/grill-with-docs`) les crée paresseusement quand les termes ou décisions sont effectivement résolus.

## Structure de fichiers

Ce dépôt est multi-contextes :

```
/
├── CONTEXT-MAP.md
├── docs/
│   └── adr/                          ← décisions au niveau système
└── electricore/
    ├── core/CONTEXT.md               ← vocabulaire métier (canonique)
    ├── etl/CONTEXT.md                ← vocabulaire ingestion
    ├── api/CONTEXT.md                ← vocabulaire service REST
    └── bot/CONTEXT.md                ← vocabulaire bot Telegram
```

Le glossaire métier vit dans `core` ; les autres modules ne redéfinissent pas les termes qu'ils consomment, ils s'y réfèrent.

## Utiliser le vocabulaire du glossaire

Quand votre sortie nomme un concept du domaine (titre d'issue, proposition de refactor, hypothèse de bug, nom de test), utiliser le terme tel que défini dans le `CONTEXT.md` pertinent. Ne pas dériver vers des synonymes explicitement marqués `_Éviter_`.

Si le concept dont vous avez besoin n'est pas dans le glossaire, c'est un signal — soit vous inventez du vocabulaire que le projet n'utilise pas (à reconsidérer), soit il y a une vraie lacune (noter pour `/grill-with-docs`).

## Signaler les conflits avec un ADR

Si votre sortie contredit un ADR existant, le surfacer explicitement plutôt que de l'écraser en silence :

> _Contradicts ADR-0007 (event-sourced orders) — but worth reopening because…_
