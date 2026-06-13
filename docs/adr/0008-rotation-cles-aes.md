# Format `secrets.toml` à deux clés AES (stopgap rotation)

Enedis effectue des rotations de clés AES périodiques, et pendant la fenêtre de transition il faut pouvoir lire **à la fois** les anciens fichiers (déjà chiffrés avec l'ancienne clé) et les nouveaux. Le format `secrets.toml` accepte donc `[aes.current]` (obligatoire) + `[aes.previous]` (optionnel) : le déchiffreur essaie `current` puis tombe en cascade sur `previous` ([electricore/ingestion/transformers/crypto.py](../../electricore/ingestion/transformers/crypto.py)).

## Statut

Solution pragmatique mise en place sous pression de temps — « on avait d'autres chats à fouetter ». Tient le besoin opérationnel actuel mais ne passera pas à l'échelle :

- N'absorbe **qu'une rotation à la fois** (clés N et N-1). Une seconde rotation rapide avant suppression de `previous` casserait le déchiffrement des fichiers les plus anciens.
- Pas de gestion de N clés ni de métadonnée par fichier indiquant quelle clé a chiffré quoi.

Une refonte vers un véritable trousseau de clés sera nécessaire à terme.

**Suite (grill 13/06/2026).** Le véhicule a changé : [#141](https://github.com/Energie-De-Nantes/electricore/issues/141) (ADR-0025) retire `secrets.toml` au profit des variables d'environnement (`AES__CURRENT__*` / `AES__PREVIOUS__*`) — la décision de fond (cascade à deux clés) survit, son support `secrets.toml` non. Et la bascule Enedis **AES-128 → AES-256 (nuit du 8-9 juin 2026)** rapproche le « terme » : l'archive historique straddle deux ciphers (absorbés par la longueur de clé). Design de la refonte tranché et tracé en [issue #221](https://github.com/Energie-De-Nantes/electricore/issues/221), qui portera l'ADR supersédant celle-ci : trousseau **N-clés** en registre **runtime** ([ADR-0024](0024-trois-registres-de-savoir.md)), **sélection par essai** (l'oracle PKCS7 + magic ZIP tranche, aucune date requise), fenêtres de validité **dérivées du corpus & exposées** mais jamais pilotes de sélection.
