# Format `secrets.toml` à deux clés AES (stopgap rotation)

Enedis effectue des rotations de clés AES périodiques, et pendant la fenêtre de transition il faut pouvoir lire **à la fois** les anciens fichiers (déjà chiffrés avec l'ancienne clé) et les nouveaux. Le format `secrets.toml` accepte donc `[aes.current]` (obligatoire) + `[aes.previous]` (optionnel) : le déchiffreur essaie `current` puis tombe en cascade sur `previous` ([electricore/ingestion/transformers/crypto.py](../../electricore/ingestion/transformers/crypto.py)).

## Statut

Solution pragmatique mise en place sous pression de temps — « on avait d'autres chats à fouetter ». Tient le besoin opérationnel actuel mais ne passera pas à l'échelle :

- N'absorbe **qu'une rotation à la fois** (clés N et N-1). Une seconde rotation rapide avant suppression de `previous` casserait le déchiffrement des fichiers les plus anciens.
- Pas de gestion de N clés ni de métadonnée par fichier indiquant quelle clé a chiffré quoi.

Une refonte vers un véritable trousseau de clés sera nécessaire à terme.
