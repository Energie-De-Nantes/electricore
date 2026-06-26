# Exemple de scaffolding `providers/<slug>/` (secrets-as-code, ADR-0044)

> **Ceci est un EXEMPLE clairement marqué.** electricore (public, AGPL) ne porte que
> le **mécanisme + le scaffolding** ([ADR-0044](../../../docs/adr/0044-secrets-as-code-sops-age.md) §6).
> Les **vrais** secrets, le **vrai** `.sops.yaml` et les **vraies** clés publiques
> vivent dans un **dépôt de déploiement privé séparé** — jamais ici. Les destinataires
> ci-dessous sont **FACTICES**.

Forme multi-cible adoptée dès une seule instance (ADR-0044 §7) : chaque fournisseur a
son dossier `providers/<slug>/` dans le dépôt privé, avec sa propre liste de
destinataires → **isolation cryptographique** (une box ne déchiffre que les siens).

## Fichiers

| Fichier | Rôle | Versionné | Chiffré |
|---|---|---|---|
| `.sops.yaml` | destinataires age (admin **toujours** + box) | oui | non |
| `config.env` | config CLAIRE (substitutions compose + non-secret) | oui | non |
| `secrets.env` | credentials | oui | **oui** (SOPS + age) |

Les `*.example` ici sont des **gabarits**. Dans le dépôt privé, on les copie sans le
suffixe `.example`, on remplace les valeurs FACTICES, et on chiffre `secrets.env`.

## Ajouter un provider / une box

Utilise [`deploy/add-provider.sh`](../../add-provider.sh) : il ajoute un destinataire
(clé age publique d'une box) à `.sops.yaml` et lance `sops updatekeys` pour **re-chiffrer
l'enveloppe** de `secrets.env` vers le nouveau jeu de destinataires.

> **Distinction cruciale** (ADR-0044) : `sops updatekeys` ne rote que l'**enveloppe**
> (le re-wrap de la clé de données). Si un secret a pu **fuiter**, le changer **AUSSI à
> la source** (nouvelle clé Enedis, nouveau token, etc.) — `updatekeys` ne le protège pas.

## La CI n'est jamais destinataire

La CI build des images publiques **sans secret** (garanties par le scan d'image) : elle
ne déchiffre jamais. N'ajoute **jamais** de clé « CI » à un `.sops.yaml` (ADR-0044 §6).
