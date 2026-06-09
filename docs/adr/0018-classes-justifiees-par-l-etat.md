# Une classe doit être justifiée par son état interne

## Contexte

Le code Python autorise deux idiomes pour porter de la donnée et du comportement : la **classe** (avec son `__init__`, ses méthodes, son `self`) et la **fonction de module** (avec des `@dataclass(frozen=True)` ou des `NamedTuple` pour les valeurs structurées). Sans règle explicite, les deux dérivent l'un vers l'autre — soit en classes-namespace qui regroupent des fonctions liées (anti-pattern Java en Python), soit en fonctions qui simulent maladroitement l'encapsulation par closure.

Plusieurs incidents légers dans `electricore/` ont rendu cette tension visible :

- [`HistoriqueTaux`](../../electricore/core/models/historique_taux.py) — une *classe* qui n'expose qu'un `@staticmethod`, sans `__init__`, sans état. Pure namespace déguisée.
- [`OdooQuery`](../../electricore/integrations/odoo/query.py) et [`DuckDBQuery`](../../electricore/core/loaders/duckdb/query.py) — deux query builders qui ont convergé indépendamment vers la même forme (`@dataclass(frozen=True)` + méthodes retournant une nouvelle instance), sans qu'aucune ADR n'inscrive la convention.
- [`RapportFacturation`](../../electricore/integrations/odoo/facturation.py) — `NamedTuple` alors que tous les autres value objects de la codebase sont `@dataclass(frozen=True)`. Aucun caller n'exploite la sémantique tuple (positionnel, déballage) — la dérive est silencieuse.
- [`DuckDBConfig`](../../electricore/core/loaders/duckdb/config.py) et [`APIKeyInfo`](../../electricore/api/security.py) — `__init__` manuel pour ce qui pourrait être un `@dataclass(frozen=True)`.

À chaque revue d'architecture, la question « pourquoi cette classe ? » se pose à nouveau. Sans convention écrite, la réponse drift au fil des PRs.

## Décision

**Une classe n'existe que si son état interne est justifié.** Trois justifications sont reconnues — toutes les autres situations s'expriment en fonctions de module.

### Les trois justifications

1. **Cycle de vie d'une connexion** — l'objet encapsule une ressource externe qui requiert un `__enter__` / `__exit__` (session XML-RPC, pool HTTP, fichier ouvert, transaction). Exemples : [`OdooReader`](../../electricore/integrations/odoo/reader.py), [`OdooWriter`](../../electricore/integrations/odoo/writers.py).

2. **Builder compositionnel** — l'objet accumule du sens à travers une chaîne d'appels (`.filter().follow().enrich()`) que les appels individuels ne portent pas. Forme imposée : `@dataclass(frozen=True, slots=True)` + méthodes retournant une **nouvelle instance** (pas de mutation). Exemples : [`OdooQuery`](../../electricore/integrations/odoo/query.py), [`DuckDBQuery`](../../electricore/core/loaders/duckdb/query.py).

3. **Cache ou job mutable** — l'état EST le point : un cache (`FieldsCache`) ou un job dont le statut évolue dans le temps (`ETLJob`). Forme : classe avec `__init__` pour les caches, `@dataclass(slots=True)` (non-frozen) pour les jobs.

### Tous les autres cas

- **Value object pur** (DataFrame bundle, configuration, snapshot de paramètres) → `@dataclass(frozen=True, slots=True)`. Pas de méthodes, pas de logique. Si un comportement est nécessaire, c'est une fonction du module qui prend la valeur en argument.

- **Namespace de fonctions liées** → **module Python**, jamais une classe. Pas de `class Foo: @staticmethod def bar(...)`. Si `bar()` n'a besoin d'aucun état, c'est une fonction de premier niveau dans le module.

### Pourquoi `@dataclass(frozen=True, slots=True)` plutôt que `NamedTuple`

Choix retenu pour tous les value objects de la codebase, y compris ceux qui retournent plusieurs valeurs depuis une fonction (`RapportFacturation`) :

- **`replace()`** comme API publique de mise à jour fonctionnelle (`dataclasses.replace(r, lignes=new_lignes)`). La version `NamedTuple` (`_replace()`) est underscore-prefixée et signale « usage interne », ce qui brouille l'intention.
- **Pas de fuite de sémantique tuple.** `NamedTuple` autorise `r[0]`, `for x in r: ...` — accès positionnel qui contourne les noms de champ. `@dataclass` ne fuit pas.
- **Introspection de première classe** : `fields()`, `asdict()`, `replace()` sans underscore-prefix.
- **État de l'art moderne** : `attrs`, `pydantic v2`, `msgspec` utilisent tous des records nommés, jamais des tuples. `NamedTuple` était l'idiome 3.5–3.7 ; depuis 3.7 (`dataclasses`) et 3.10 (`slots=True`), `@dataclass(frozen=True, slots=True)` est le choix canonique.
- **`slots=True`** est gratuit dès 3.10 : économie mémoire (pas de `__dict__`), détection de typo (`instance.lignes_typo = ...` lève `AttributeError`), accès attribut plus rapide. Python 3.12+ (requis par ce projet) supporte sans réserve.

### Tableau récapitulatif

| Cas d'usage | Convention | Exemples canoniques |
|---|---|---|
| Cycle de vie d'une connexion | classe + `__enter__`/`__exit__` | `OdooReader`, `OdooWriter` |
| Builder compositionnel | `@dataclass(frozen=True, slots=True)` + méthodes retournant une nouvelle instance | `OdooQuery`, `DuckDBQuery` |
| Cache mutable | classe avec `__init__` | `FieldsCache` |
| Job avec état évoluant | `@dataclass(slots=True)` | `ETLJob` |
| Value object pur | `@dataclass(frozen=True, slots=True)` | `OdooConfig`, `ContexteMensuel`, `QueryConfig`, `Column`, `FluxSchema` |
| Namespace de fonctions | **module Python**, pas de classe | (contre-exemple : `HistoriqueTaux`) |

Les modèles Pandera (`pa.DataFrameModel`) et Pydantic (`BaseModel`) sortent de cette convention — ils ont leur propre machinerie de déclaration de schéma et ne sont pas des dataclasses.

## Raison

Trois facteurs cumulés :

1. **Coût cognitif d'une classe vs fonction.** Lire un module de fonctions est linéaire — chaque fonction porte sa signature et son corps. Lire une classe oblige à reconstituer mentalement le cycle de vie : quand est-ce instancié, qui appelle quoi dans quel ordre, quels invariants sont maintenus entre les méthodes. Quand il n'y a pas d'état à porter, ce coût est gratuit. Les justifications (1)-(2)-(3) sont exactement les cas où l'état porte un sens que la fonction de module ne peut pas porter aussi clairement.

2. **L'AI-navigability et la testabilité y gagnent en miroir.** Une fonction pure est testable sans `setUp`, sans fixture de classe, sans réflexion sur l'ordre d'appel. Un agent (humain ou LLM) qui lit `enrichir_liens(df, base_url, model) -> pl.DataFrame` voit l'entrée et la sortie d'un coup d'œil. La version `LinkBuilder(...).enrich(df, model)` exige de lire le constructeur et l'état partagé avant de comprendre l'appel — coût pur, gain nul.

3. **Le standard converge vers ça.** Toutes les libs « value-object » modernes (`attrs`, `pydantic v2`, `msgspec`) ont fait ce choix. Les codebases Python fonctionnelles récentes (Polars lui-même, FastAPI dans ses dépendances) suivent la même règle. Inscrire la convention maintenant aligne sur l'écosystème et évite la dérive lente vers le Java-en-Python.

### Alternatives écartées

- **Classes everywhere, méthodes par défaut** — l'idiome OO classique. Rejeté : amène un cycle de vie implicite à chaque type, alourdit les tests, brouille la frontière fonction/objet. Le code Python n'est pas Java.

- **Fonctions everywhere, dicts pour les value objects** — extrême inverse. Rejeté : perd les noms de champ typés, casse l'autocomplétion IDE, rend les schémas opaques aux type-checkers et à Pandera.

- **`NamedTuple` plutôt que `@dataclass(frozen=True)` pour les value objects** — l'idiome Python pré-3.7. Rejeté pour les raisons listées plus haut (`replace()` underscore, fuite tuple, divergence avec le reste de la codebase).

## Conséquences

### Drift identifiée à corriger

Les cas suivants ne sont pas conformes à la convention et seront corrigés (PR de cleanup unique, ~8 éditions mécaniques) :

- [`HistoriqueTaux`](../../electricore/core/models/historique_taux.py) → fonction de module `historique_taux_schema(taux_col)`. Aucun caller n'utilise la classe ; le `@staticmethod` est purement décoratif.
- [`DuckDBConfig`](../../electricore/core/loaders/duckdb/config.py) → `@dataclass(frozen=True, slots=True)` ; `table_mappings` devient une constante `_TABLE_MAPPINGS` du module.
- [`APIKeyInfo`](../../electricore/api/security.py) → `@dataclass(frozen=True, slots=True)`.
- [`RapportFacturation`](../../electricore/integrations/odoo/facturation.py) → `@dataclass(frozen=True, slots=True)` (migration depuis `NamedTuple`).
- Tous les `@dataclass(frozen=True)` existants → ajout de `slots=True` : `OdooConfig`, `ContexteMensuel`, `QueryConfig`, `Column`, `FluxSchema`, `OdooQuery`, `DuckDBQuery`.
- `ETLJob` (`@dataclass` non-frozen) → ajout de `slots=True`.

### Garde-fou exécutable

Un test d'architecture (`tests/architecture/test_no_namespace_classes.py`, à créer) parse tous les fichiers `electricore/**/*.py` et échoue sur les classes qui n'ont :
- ni `__init__` custom,
- ni décorateur `@dataclass`,
- ni héritage de `pa.DataFrameModel` / `BaseModel` / `StrEnum` / `NamedTuple` (allow-list explicite).

Une telle classe est par définition un namespace, donc une violation. Le test attrape la régression au lieu d'attendre la prochaine revue.

### Modules futurs

Toute nouvelle fonctionnalité (par exemple le module `integrations/odoo/verification.py` extrayant les checks pré-facturation) suit la convention dès la première ligne : pas de `Verificateur` classe, juste `verifier(odoo) -> ResultatVerification` avec `ResultatVerification` en `@dataclass(frozen=True, slots=True)`.

## Limites à connaître avant un changement d'échelle

- **L'allow-list du test d'architecture est explicite, pas inférée.** Une nouvelle lib qui expose son propre type de schéma (par ex. `msgspec.Struct`) devra être ajoutée à la liste autorisée. C'est un coût acceptable — c'est exactement le moment où on veut s'arrêter et confirmer que le nouveau type correspond bien à un value object pur.

- **`slots=True` casse `__dict__`-based dynamic attribute access.** Du code qui ferait `setattr(instance, "champ_dynamique", x)` sur un attribut non-déclaré lève `AttributeError`. C'est *voulu* — c'est la détection de typo. Mais une lib externe qui patche dynamiquement des attributs (rare) serait incompatible. Aucun cas connu dans la codebase aujourd'hui.

- **Pas de seam pour « value object polymorphe ».** Si un jour un cas légitime apparaît où plusieurs records doivent partager une interface commune (héritage de dataclass frozen), la convention demandera un ajustement. Les héritages d'`OdooReader` (`OdooWriter`) marchent parce qu'`OdooReader` n'est pas un dataclass ; un héritage entre deux `@dataclass(frozen=True, slots=True)` est techniquement faisable mais pas testé. À traiter le jour où le cas se présente.
