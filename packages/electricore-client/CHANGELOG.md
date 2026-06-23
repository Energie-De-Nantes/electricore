# Changelog — electricore-client

Toutes les évolutions notables de ce paquet sont consignées ici.
Le format suit [Keep a Changelog](https://keepachangelog.com/fr/1.0.0/) ;
le versionnage suit [SemVer](https://semver.org/lang/fr/).

## [0.1.0] — non publié

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
