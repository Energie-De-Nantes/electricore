# Trousseau de clés AES N-clés : sélection par essai, escalade d'échec per-flux

## Contexte

[ADR-0008](0008-rotation-cles-aes.md) a posé une cascade AES à **deux slots** (`current` → `previous`)
comme stopgap explicite, et nommait déjà sa limite : *« N'absorbe qu'une rotation à la fois. »* Ce
« terme » est atteint :

- L'instance EDN porte déjà **deux clés AES-128** (`current` + `previous`, une rotation passée). La
  bascule Enedis **AES-128 → AES-256 dans la nuit du 8-9 juin 2026** ([memory `project_aes256_bascule`],
  [issue #221](https://github.com/Energie-De-Nantes/electricore/issues/221)) exige une **3ᵉ** clé. Le
  cascade 2-slots est plein.
- L'incident a été **silencieux 10 jours** : [`crypto.py`](../../electricore/ingestion/transformers/crypto.py#L205-L207)
  avale l'échec de déchiffrement (`except Exception: return`), le job d'ingestion se termine en
  *succès* à 0 ligne, et la surveillance bot ([`surveillance.py`](../../electricore/bot/surveillance.py#L44))
  n'alerte que sur un job `failed`. Aucune alerte n'est partie.

[#221](https://github.com/Energie-De-Nantes/electricore/issues/221) avait pré-tranché le design au grill
du 13/06 (trousseau N-clés, registre runtime, sélection par essai). Le grill du 18/06 l'a resserré :
abandon de l'utilitaire de bornes et de la compat de format (YAGNI), et ajout de l'escalade d'échec —
le vrai manque révélé par l'incident.

## Décision

### 1. Trousseau N-clés en registre runtime, format unique

Le domaine `aes` du registre runtime ([ADR-0025](0025-registre-runtime-pydantic-settings.md)) expose un
**trousseau de taille arbitraire** : `trousseau: dict[str, PaireCles]`, alimenté par
`AES__TROUSSEAU__<label>__KEY` / `__IV`. Le `<label>` est un nom **parlant** choisi par l'opérateur
(`aes256_2026`, `aes128_2024`…) qui remonte tel quel dans `chaine()`, donc dans les logs et les
diagnostics. Le modèle `Aes` ne porte **que** ce champ : `current` / `previous` / plat-v1 sont retirés.

La forme « liste indexée » (`AES__TROUSSEAU__0__…`) est **écartée** : pydantic-settings ne supporte pas
les tableaux depuis l'environnement (*« Arrays are not supported »*, doc + test) et imposerait des
indices contigus. Le `dict[str, …]` via délimiteur imbriqué est, lui, documenté et testé.

### 2. Sélection par essai, sans index ni protocole

On tente les clés du trousseau dans un **ordre indifférent** ; le déchiffrement est son propre oracle
(padding PKCS7 + magic bytes ZIP `PK\x03\x04` — faux positif ≈ 2⁻⁴⁰). La cascade N-clés est **déjà**
la forme de [`decrypt_with_key_chain`](../../electricore/ingestion/transformers/crypto.py#L115-L139) ;
le seul changement est `chaine()` qui renvoie les N entrées du trousseau. Aucune **date** ne pilote la
sélection. Aucun **protocole** n'est modélisé : AES-128 et AES-256 sont le *même* schéma
(AES-CBC-PKCS7, longueur de clé auto-sélectionnée), pas deux protocoles.

> **Prémisse corrigée (20/06/2026, [ADR-0040](0040-schema-dechiffrement-aes-iv-prefixe.md)).** Le premier
> vrai fichier AES-256 a montré qu'il s'agit bien d'un **second schéma** (IV *préfixé* dans le fichier,
> non livré par Enedis), pas du même. Le *seam* parqué ici — « une fonction `decrypt_*` par schéma, le
> trial balaie les schémas » — est réalisé par ADR-0040. Le reste de cet ADR tient inchangé.

### 3. Rupture de format assumée, compat de **données** préservée

Pas de compatibilité ascendante du **format** de config : l'unique instance de prod (EDN) réécrit son
`.env` une fois, les futures instances ([ADR-0015](0015-deploiement-multi-instance.md)) démarrent au
propre. En revanche la compat de **données** est non négociable : les anciennes clés AES-128 deviennent
des **entrées labellisées du trousseau** → l'archive historique reste déchiffrable (« lire le passé »).

### 4. Escalade d'échec **per-flux** (fin du fail silencieux)

L'échec de déchiffrement cesse d'être avalé. Politique : **par flux**, un flux qui a des fichiers mais
**0 déchiffrement réussi** (≥ 1 échec) fait passer le job à `failed` → la surveillance bot alerte. Un
échec **isolé** noyé dans des succès (fichier corrompu) est **toléré**, compté, et tracé en warn-log.
L'agrégation succès/échec par resource vit au niveau runner (il orchestre déjà une resource par flux).

## Raison

- **Sélection par essai, pas index temporel.** Enedis ne fournit pas de fenêtres de validité fiables ;
  l'oracle est auto-suffisant et le coût de N essais est négligeable (AES tourne au Go/s, noyé dans les
  I/O SFTP). Un index temporel ajouterait une surface de dérive sans gain.
- **Per-flux, pas seuil global.** Enedis fait évoluer le chiffrement **par flux indépendamment** (typiquement
  à l'ajout d'un nouveau flux). Un seuil global en % noierait un flux qui bascule seul ; le per-flux le
  capte exactement.
- **Utilitaire de bornes écarté (YAGNI).** Son seul rôle porteur — détecter une **lacune de couverture**
  (un segment qu'aucune clé ne déchiffre = clé manquante) — est déjà tenu par l'escalade d'échec. Exposer
  des fenêtres de validité dérivées était du pur *nice-to-have*. Évaluer les vieilles transitions se réduit
  à « rassembler les clés qu'on a, les mettre dans le trousseau, laisser l'alerte signaler ce qui résiste ».
- **Protocole non modélisé (YAGNI).** Un vrai changement de mode (CBC→GCM = nouvel oracle) n'a pas eu lieu
  et on ne connaît pas sa forme future. Le *seam* est noté : une fonction `decrypt_*` par schéma, le trial
  balaierait les schémas. On ne pré-construit pas.
- **Rupture de format.** Préserver les noms d'env legacy protégerait un parc qui n'existe pas (≈ 1 instance
  en prod aujourd'hui, responsabilité humaine assumée, ADR-0015). Le coût : un edit manuel unique.

## Conséquences

- `Aes` simplifié (un champ `trousseau`) ; `chaine()` → N entrées ; cascade et oracle inchangés.
- [`crypto.py`](../../electricore/ingestion/transformers/crypto.py) : l'`except … return` silencieux est
  remplacé par un comptage propagé ; le runner agrège par flux et décide `failed`.
- **Migration EDN** : réécrire l'unique `.env` (2 clés AES-128 + la nouvelle AES-256, toutes labellisées),
  puis resync — l'escalade signalera tout segment encore sans clé.
- [ADR-0008](0008-rotation-cles-aes.md) **superseded**.

> **Procédure de rotation révisée par [ADR-0044](0044-secrets-as-code-sops-age.md)** : le trousseau ne
> vit plus dans un `.env` édité sur la box mais dans le `secrets.env` **chiffré** du provider. Rotation :
> `sops providers/<slug>/secrets.env` (édition chiffrée in-place, ajout d'un label) → commit → push →
> `reconfigure` (la box pull + déchiffre + restart). **Distinction cruciale** : `sops updatekeys` ne rote
> que l'**enveloppe** (destinataires) ; une clé AES qui a pu fuiter doit être changée **à la source**.

## Extension (#445) : escalade généralisée du **déchiffrement** à la **chaîne**

L'escalade d'échec (§4) ne visait que l'étage `decrypt`. Un spike (`dlt 1.27.2`) a montré que
les deux étages aval se comportaient de façon **opposée**, et tous deux mal :

| Étage | Sur un fichier fautif | Observable ? | Rayon de souffle |
|-------|-----------------------|--------------|------------------|
| `decrypt` (`crypto.py`) | `except` → compte `echec`, continue | ✅ escalade si flux aveugle | isolé (par fichier/flux) |
| `unzip` (`archive.py`) | `except: log + return`, **aucun compteur** | ❌ **silencieux** — job reste `completed` | drop les documents du zip |
| `parse` (`sftp_enedis_brut.py`) | **lève** `PipelineStepFailed` → run crashé | ✅ bruyant | ❌ **catastrophique + non isolé** : aborte **tout** l'`extract` (aucun flux ne land, dbt ne tourne pas) |

Deux problèmes distincts : `unzip` était le vrai trou **silencieux** ; `parse` était **trop
bruyant et non isolé** — un seul document R64 malformé crashait le landing de **tous** les flux
du cycle, violant le placement délibéré de l'escalade *après* dbt (« pour que les flux sains
continuent de couler malgré le flux aveugle »).

**Décision.** Donner aux trois étages la discipline uniforme de `decrypt` : **attraper → compter
→ continuer**. Un fichier fautif n'est jamais une raison d'interrompre le run. Le comptage vit
dans **un** objet par flux — `StatsDechiffrement` renommé **`StatsChaine`** (compteurs par étage :
`fichiers`/`dechiffres`/`extraits`/`documents` + `echecs_dechiffrement`/`echecs_extraction`/
`echecs_linearisation`) — partagé par les trois factories de transformer. Le prédicat unique
`flux_aveugle()` est généralisé de « 0 déchiffré » à **« 0 document produit ET ≥ 1 échec »** :
peu importe l'étage qui a rendu le flux muet, c'est l'`echecs()` **explicite** qui distingue un
flux *drop-par-erreur* (escalade) d'un flux *vide par nature* (zip sans fichier interne — ère CSV
R64, smoke `max_files` — `echecs() == 0` ⇒ jamais d'escalade). Le runner `flux_sans_dechiffrement`
devient **`flux_aveugles`** ; chaque `StatsChaine` est imprimé au bilan (capté dans `job.output`).

**Choix délibéré : ne PAS aborter le landing sur un échec isolé `parse`/`unzip`.** Comme pour
`decrypt`, un fichier fautif noyé dans des documents sains est toléré (compté, visible), pas
escaladé. Seul un flux **entièrement** muet malgré des échecs bascule le job en `failed`. Le
prédicat généralisé **subsume** l'ancienne cécité de déchiffrement.

Porté par [#445](https://github.com/Energie-De-Nantes/electricore/issues/445).

## Statut

Accepté (grill 18/06/2026 ; extension #445 du 27/06/2026). Supersède [ADR-0008](0008-rotation-cles-aes.md).
Porté par [#221](https://github.com/Energie-De-Nantes/electricore/issues/221) (réécrite) et
[#445](https://github.com/Energie-De-Nantes/electricore/issues/445) (extension chaîne). Glossaire :
[`electricore/ingestion/CONTEXT.md`](../../electricore/ingestion/CONTEXT.md) (« Trousseau de clés AES »,
« escalade d'échec de chaîne »).
