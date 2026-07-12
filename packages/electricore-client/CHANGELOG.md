# Changelog — electricore-client

Toutes les évolutions notables de ce paquet sont consignées ici.
Le format suit [Keep a Changelog](https://keepachangelog.com/fr/1.0.0/) ;
le versionnage suit [SemVer](https://semver.org/lang/fr/).

## [0.5.0] — 2026-07-12

### Ajouté
- Méthode `provision_estimation(pdl)` : GET RPC typé `/provision/estimation`,
  estimation de provision d'un lissé dérivée de R67 (cold-start, ADR-0048,
  #630). `RapportProvision` en kWh, zéro € ; `flux_r67` absent → 503
  `precondition` → `PreconditionNonRemplie`.
- Méthode `sorties(rsc=...)` : POST `/perimetre/sorties`, fin de souscription
  gouvernée par le fait C15 (souscriptions_odoo#21, ADR 0031 côté addon,
  #632). Renvoie une `LigneSortie` (code `RES`/`CFNS`, `date_sortie` jour
  civil demi-ouvert ADR-0042/0052) pour chaque RSC **sortie** du lot demandé
  — une RSC encore présente ou inconnue n'apparaît simplement pas (pas
  d'erreur, cas nominal). Modèles `SortiesRequest` / `LigneSortie`
  single-sourcés.

## [0.3.0] — 2026-07-08

### Ajouté
- Méthode `prestations(rsc=...)` : flux JSONL typé de `PrestationF15`, les
  prestations et indemnités ponctuelles du flux F15 (`unite='UNITE'`) à
  refacturer côté Odoo (contrat v1, souscriptions_odoo#37). Pull-tout sans
  fenêtre temporelle ni pagination — dédup côté consommateur par
  `reference`, une **référence de contenu electricore** (sha256 tronqué du
  contenu canonique de la ligne, construit en Python pur ; ce n'est pas une
  référence Enedis, le F15 n'a aucun identifiant de ligne). Contrat :
  `docs/contrat-prestations.md`.

## [0.2.0] — 2026-07-02

### Ajouté
- **Discrimination des erreurs par l'en-tête `X-Error-Kind`** (#424) : le
  transport ne mappe plus sur le seul code HTTP. `503 + ingestion_lock` →
  `IngestionEnCours` ; `422 + precondition` → **`PreconditionNonRemplie`**
  (nouvelle exception, détail serveur conservé) ; les autres erreurs HTTP se
  propagent inchangées.
- Méthode `resoudre_rsc(ids)` : POST `/facturation/rsc`, résolution
  `id_Affaire` → RSC (#282). Modèles `ResolutionRscRequest` /
  `ResultatResolutionRsc` single-sourcés.
- `PreconditionNonRemplie` ré-exportée au **top-level** du paquet, comme les
  trois autres exceptions (`from electricore_client import
  PreconditionNonRemplie`).

## [0.1.0] — 2026-06-23

### Ajouté
- Squelette du paquet `electricore-client` (top-level `electricore_client`),
  distribué séparément du moteur. Dépendances de base **httpx + pydantic**.
- Substrat de transport partagé `_BaseClient` : URL de base, en-tête
  `X-API-Key`, timeout, conversion d'erreur HTTP **503 → `IngestionEnCours`**,
  garde de version de contrat asymétrique (warn si serveur en avance, raise si
  en retard).
- Modèle d'en-têtes de métadonnées `EnTetesMeta` (`contract_version`, `mois`,
  `grain`).
- Méthode `meta_periodes(...)` : flux JSONL typé de `PeriodeMeta` (contrat v3,
  relevés imbriqués ADR-0038), context-manager, métadonnées en en-têtes,
  sans pagination, `.collect()`.
- Méthode `chronologie(...)` : flux JSONL d'une union discriminée
  `LigneChronologie` (`LigneEvenement | LigneReleve | LignePeriodeEnergie`,
  contrat v1), validation `pdl` XOR `rsc` côté client.
- Méthode `turpe_variable(...)` : POST RPC, résultats typés indexés par l'`id`
  opaque ré-émis. Modèles `LigneTurpeVariable` / résultat single-sourcés.
- Extra `[arrow]` : client Arrow historique (`flux/releves/facturation/accise/
  cta` → `pl.DataFrame`) dans `electricore_client.arrow`, polars importé
  paresseusement (la base reste polars-free).
- CI/CD : `release-client.yml` (tags `client-v*`, publication PyPI Trusted
  Publishing / OIDC) + job `test-client` (install isolé, garantie polars-free).
  Conception : ADR-0043.
