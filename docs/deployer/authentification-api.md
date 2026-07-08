---
fraicheur: 2026-07-08
---

# Authentification de l'API

L'API REST ElectriCore s'authentifie par **clé API** (`X-API-Key`) — pas d'OAuth, pas de
session. Ce document synthétise ce qu'un·e déployeur·euse doit savoir pour provisionner
et faire tourner des clés ; le détail code (`security.py`, modèles Pydantic) reste dans le
[README de l'API](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/api/README.md).

## Transport : header uniquement

La clé passe **exclusivement** par le header `X-API-Key` (`security.py`, `APIKeyHeader`) —
pas de paramètre de requête.

```bash
curl -H "X-API-Key: $API_KEY" "https://<slug>.electricore.fr/flux/r151"
```

Endpoints publics (aucune clé requise) : `GET /`, `GET /health`, `GET /docs`, `GET /redoc`,
`GET /openapi.json`.

## Le trousseau `API__TROUSSEAU__<consommateur>__KEY` (ADR-0046 §4)

Une **clé par consommateur** (`librewatt`, `bot`, `scheduler`…), label dynamique choisi par
l'opérateur — pas de clé unique partagée. Chaque clé fait ≥ 32 caractères :

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

En production, le trousseau vit dans le `secrets.env` **chiffré** du dépôt de déploiement
(secrets-as-code, [ADR-0044](../adr/0044-secrets-as-code-sops-age.md)) — jamais dans un
fichier clair sur la box. Voir
[Variables d'instance](../deploiement.md#variables-dinstance-configenv--secretsenv) pour le
format complet et [Rotation des clés AES](../deploiement.md#rotation-des-clés-aes-trousseau-n-clés-adr-0037)
pour la procédure d'édition chiffrée (même geste `sops <fichier>`, transposé au trousseau
`API__TROUSSEAU__*` plutôt qu'`AES__TROUSSEAU__*`).

Labeller par **consommateur** (pas par environnement ni par date) permet une **révocation
ciblée** : retirer la ligne `API__TROUSSEAU__bot__KEY` du `secrets.env`, commit, push,
`reconfigure` — seul le bot perd l'accès, les autres clés restent valides. Les logs
d'authentification portent le label, donc l'attribution d'un appel à son consommateur est
immédiate.

## Bonnes pratiques

- **Une clé par consommateur**, jamais de clé partagée — révocation ciblée, attribution
  dans les logs.
- **≥ 32 caractères**, générée aléatoirement (`secrets.token_urlsafe(32)`), jamais
  hard-codée dans le code.
- **HTTPS en production** (Caddy + Let's Encrypt, cf. [guide de déploiement](../deploiement.md)) —
  une clé API en clair sur un lien non chiffré est compromise.
- **`.env` local jamais commité** (`.gitignore`) ; en production le trousseau vit chiffré
  dans `secrets.env` (ADR-0044), jamais en clair sur la box.
- **Rotation** : ajouter la nouvelle clé sous un nouveau label, propager au consommateur,
  puis retirer l'ancienne — pas de fenêtre où le consommateur est bloqué.

## Vérifier une clé

Appeler n'importe quel endpoint protégé : `200` = clé valide, `401` = clé absente ou
retirée du trousseau.

```bash
curl -H "X-API-Key: $API_KEY" "https://<slug>.electricore.fr/flux/r151?limit=1"
```

## Dépannage — 401 Unauthorized

```json
{"detail": "Clé API requise dans le header 'X-API-Key'"}
```

- Vérifier que la clé est bien fournie dans le header `X-API-Key`.
- Vérifier qu'elle correspond à une entrée du trousseau `secrets.env` déchiffré côté box
  (`docker compose exec api env | grep API__TROUSSEAU` pour un diagnostic in-container).
- Une clé retirée du trousseau (rotation) cesse immédiatement de fonctionner — pas de
  grâce period.

[Retour à Déployer](index.md).
