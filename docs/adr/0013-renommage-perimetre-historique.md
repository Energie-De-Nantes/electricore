# Renommage Périmètre → Historique

## Contexte

Le module `electricore/core/pipelines/perimetre.py` et la classe Pandera `HistoriquePerimetre` portaient deux concepts mélangés : (a) la séquence temporelle des événements contractuels d'un PDL, et (b) l'état du portefeuille à une date donnée. En préparation des endpoints API orientés métier (v1.6+), cette ambiguïté empêchait de nommer proprement ce qu'on expose côté client.

## Décision

Deux concepts séparés dans le glossaire `electricore/core/CONTEXT.md` :

- **Historique** : séquence temporelle ordonnée des événements contractuels, **enrichie pour la facturation** (impacts abonnement/énergie, événements FACTURATION mensuels artificiels). C'est ce que produit l'ex-`pipeline_perimetre`. Sortie validée par un nouveau modèle Pandera `Historique`.
- **Périmètre** : ensemble des PDLs actifs à une date donnée (snapshot). Concept inscrit au glossaire mais sans implémentation tant qu'aucun cas d'usage ne le réclame.

Côté code :

- `perimetre.py` → `historique.py`
- `pipeline_perimetre()` → `pipeline_historique()`
- Suppression de `HistoriquePerimetre` (qui validait la sortie brute de `c15()`) — le brut n'a pas de nom métier autonome et n'est plus validé Pandera au load. Validation déplacée à la sortie de `pipeline_historique`.

## Raison

Le terme "périmètre" évoque naturellement un état à un instant (« qui est dans notre portefeuille aujourd'hui ? ») plutôt qu'une série temporelle. Garder ce mot pour désigner la série créait une dissonance pour quiconque arrivait sur le code. Réserver "Historique" pour la série permet d'aligner code et intuition métier, et libère "Périmètre" pour le concept de snapshot le jour où il sera implémenté.

Le choix d'**absorber l'enrichissement dans le concept "Historique"** (plutôt que de distinguer "Historique brut" vs "Historique enrichi") évite une surface lexicale redondante : aucun consommateur métier ne manipule le brut directement, il est toujours médiatisé par le pipeline d'enrichissement.

Le coût assumé : perte de la validation Pandera early à la sortie de `c15()`. Acceptable car la validation à la sortie de `pipeline_historique` couvre les mêmes propriétés, et l'absence de modèle pour le brut évite de nommer un concept ("Évènements contractuels", "Historique brut"…) qui n'aurait servi qu'à la validation interne.
