# Schéma de déchiffrement AES à IV préfixé : réalisation du seam ADR-0037

## Statut

Accepté (grill 20/06/2026). Réalise le *seam* parqué par
[ADR-0037](0037-trousseau-cles-aes-n-cles-selection-par-essai.md) (« une fonction `decrypt_*`
par schéma, le trial balaierait les schémas ») et **corrige une prémisse** de cet ADR (cf.
*Raison*). Ne change ni le trousseau N-clés, ni la sélection par essai, ni l'escalade per-flux.

## Contexte

La bascule AES-128 → AES-256 (nuit du 8-9 juin 2026, [#221](https://github.com/Energie-De-Nantes/electricore/issues/221))
n'était jusqu'ici connue que par sa longueur de clé. Le premier vrai fichier AES-256 a été
**sondé** ([`scripts/sonde_format_aes.py`](../../scripts/sonde_format_aes.py) — sweep d'hypothèses
`(clé, IV, mode)` jugé par l'oracle auto-validant `decrypt_file_aes`, padding PKCS7 + magic ZIP).
Résultat sans ambiguïté (faux positif ≈ 2⁻⁴⁰) sur un `ENEDIS_R64_…` :

> **AES-256-CBC, IV préfixé** : les **16 premiers octets de chaque fichier sont l'IV** (en clair,
> frais à chaque fichier) ; le reste est le ciphertext.

Enedis ne livre donc **que la clé** (64 hex = 32 octets), sans IV — l'IV voyage dans le fichier.
C'est un **schéma** distinct de l'historique AES-128, où l'IV était un second secret livré par
Enedis, configuré à côté de la clé et réutilisé pour tous les fichiers.

Le code ne sait pas l'exprimer : [`PaireCles.iv`](../../electricore/config/runtime.py#L70-L82) est
**requis** (une entrée AES-256 sans IV ne se charge pas), et
[`decrypt_with_key_chain`](../../electricore/ingestion/transformers/crypto.py#L137-L161) injecte un
**IV constant** dans CBC, sans notion de « détacher l'IV de la tête du fichier ».

## Décision

### 1. Deux schémas de déchiffrement coexistent, routés par présence d'IV

On nomme **schéma** la façon dont l'IV d'un fichier est obtenu :

- **IV-fixe** (AES-128 legacy) — IV en config, réutilisé ; déchiffre **tout** le fichier ;
- **IV-préfixé** (AES-256) — IV = `fichier[:16]`, ciphertext = `fichier[16:]`.

Une entrée du trousseau **avec** IV configuré ⟹ schéma IV-fixe ; **sans** IV ⟹ schéma IV-préfixé.
`PaireCles.iv` devient **optionnel** (`str | None`) ; `chaine()` propage le `None` ;
`decrypt_with_key_chain` branche sur `iv is None` et fait le split `[:16]`/`[16:]` avant d'appeler
**`decrypt_file_aes` inchangé** (il reste le primitif `(données, clé, IV)` + oracle).

Le routage clé→schéma est **1:1 par construction** : l'opérateur configure un IV **ssi** Enedis en
fournit un. L'oracle par essai sépare les schémas **sans faux positif croisé** (une clé legacy
essayée sur un fichier préfixé, ou l'inverse, échoue padding/magic). L'escalade per-flux d'ADR-0037
fonctionne sans modification (un flux basculé sans sa clé 256 = flux aveugle → `failed` → alerte).

### 2. Pas de discriminateur de schéma explicite (YAGNI)

On **surcharge** « pas d'IV en config » pour signifier « schéma IV-préfixé ». Tant qu'il n'existe que
ces deux schémas, c'est sans ambiguïté. Un **tag** de schéma par entrée
(`AES__TROUSSEAU__<label>__SCHEME=…`) est écarté : il réintroduit le « protocole modélisé » qu'ADR-0037
a refusé, pour un monde à deux schémas. Le jour où un *troisième* schéma sans IV-config apparaît
(IV-zéro, IV-dérivé, ou vrai changement de mode CBC→GCM = nouvel oracle), on ajoutera le discriminateur
**à ce moment-là**.

## Alternatives écartées

- **Tag de schéma explicite par entrée** — rejeté (YAGNI, *supra*) : surface de config en plus pour
  zéro gain dans un monde à deux schémas, à l'encontre de « la config reste bête, le trial balaie ».
- **Essayer les deux schémas pour chaque clé, en aveugle** — rejeté : le routage par présence d'IV est
  1:1 et déterministe ; balayer les schémas en aveugle double les essais sans rien apporter (et une clé
  AES-128 n'a, elle, pas d'IV à inventer pour le schéma préfixé).

## Raison — prémisse ADR-0037 corrigée

ADR-0037 §*Sélection par essai* affirmait : *« AES-128 et AES-256 sont le même schéma (AES-CBC-PKCS7,
longueur de clé auto-sélectionnée), pas deux protocoles. »* **Le premier fichier de prod a falsifié
cette prémisse** : l'IV-préfixé est bien un second schéma (la longueur de clé n'était que la partie
visible). La conséquence est mineure et entièrement dans l'esprit d'ADR-0037 — son *seam* explicitement
parqué (« une fonction `decrypt_*` par schéma, le trial balaierait les schémas ») est simplement
**réalisé plus tôt que prévu, et par un autre axe** (cadrage de l'IV, pas changement de mode). Le reste
d'ADR-0037 (trousseau N-clés, sélection par essai, escalade per-flux, rupture de format) tient inchangé.

## Conséquences

- [`runtime.py`](../../electricore/config/runtime.py) : `PaireCles.iv` optionnel ; `octets()`/`chaine()`
  propagent `None` ; le message de `ConfigurationManquante` n'exige plus l'IV.
- [`crypto.py`](../../electricore/ingestion/transformers/crypto.py) : `decrypt_with_key_chain` route sur
  `iv is None` (split IV-préfixé) ; `decrypt_file_aes` inchangé.
- **Migration `.env` EDN** : ajouter `AES__TROUSSEAU__aes256_2026__KEY=<64hex>` **sans** `__IV` ; garder
  les labels AES-128 (avec IV) pour relire les archives. *Quels flux ont basculé* (R64 seul vs tous) se
  confirme à la sonde — sans incidence sur le design (le trial couvre un parc mixte).

  > **Révisé par [ADR-0044](0044-secrets-as-code-sops-age.md)** : depuis secrets-as-code, ce label
  > AES-256 (sans `__IV`) s'ajoute non plus dans un `.env` clair mais dans le `secrets.env` **chiffré**
  > du provider : `sops providers/<slug>/secrets.env` → commit → push → `reconfigure`.
- Glossaire : [`electricore/ingestion/CONTEXT.md`](../../electricore/ingestion/CONTEXT.md) — terme
  **« Schéma de déchiffrement »** ajouté, l'entrée *Trousseau* corrigée (la bascule **change de schéma**,
  pas seulement de longueur de clé).
- [ADR-0037](0037-trousseau-cles-aes-n-cles-selection-par-essai.md) reçoit un renvoi vers le présent ADR.
