# ElectriCore API - Authentication par Clés API

API REST sécurisée pour accéder aux données flux Enedis avec authentification par clés API.

## 🔐 Authentification & configuration

Authentification par `X-API-Key` (header ou query param), trousseau
`API__TROUSSEAU__<consommateur>__KEY` (une clé par consommateur, ADR-0046 §4). Le volet
déploiement (génération des clés, secrets-as-code en prod, révocation ciblée, dépannage
401) est synthétisé côté site : voir
[docs/deployer/authentification-api.md](https://github.com/Energie-De-Nantes/electricore/blob/main/docs/deployer/authentification-api.md).
L'inventaire complet des variables d'environnement du domaine `api` (registre runtime,
`electricore/config/runtime.py`) vit dans
[docs/configuration.md](https://github.com/Energie-De-Nantes/electricore/blob/main/docs/configuration.md).

Aperçu minimal, développement local :

```bash
curl -H "X-API-Key: $API_KEY" "http://localhost:8000/flux/r151"
python -c "import secrets; print(secrets.token_urlsafe(32))"   # générer une clé
```

```env
# .env local — une clé par consommateur (label dynamique)
API__TROUSSEAU__librewatt__KEY=votre-cle-consommateur-generee-ici
API__TROUSSEAU__bot__KEY=une-autre-cle-pour-le-bot
```

## 📡 Endpoints

### Endpoints Publics (sans authentification)

- `GET /` - Informations générales et exemples
- `GET /health` - Statut de l'API et de la base de données
- `GET /docs` - Documentation interactive Swagger (**source de vérité de la surface
  d'endpoints** — la liste ci-dessous n'en donne qu'un aperçu par domaine, elle se périme
  vite)
- `GET /redoc` - Documentation alternative

### Endpoints Sécurisés (authentification requise, aperçu par domaine)

- `GET /flux/{table_name}` · `/info` - Flux Enedis bruts (métadonnées : lignes, fraîcheur `derniere_date`)
- `GET /releves` · `/info` - Mart canonique des relevés (ADR-0029)
- `POST /ingestion/run` · `GET /ingestion/jobs` · `/jobs/{id}` - Jobs d'ingestion (modes `test`/`all`/`rebuild`/`resync`)
- `GET /taxes/...` - Calculs accise et CTA (exports Arrow/XLSX) — *requiert Odoo*
- `GET /facturation/meta-periodes` · `/chronologie` - Flux JSONL typés (méta-périodes, frise facturiste)
- `POST /facturation/turpe-variable` - Calculateur TURPE variable (RPC typé)
- `GET /facturation/rapport.xlsx` · `/documents.xlsx` · `/check/odoo` - Rapports & contrôles pré-facturation — *requiert Odoo*
- `GET /perimetre/affaires` - Cockpit des affaires SGE ouvertes (X12/X13)
- `POST /perimetre/sorties` - Sorties du périmètre par lot de RSC (RPC typé, ADR-0052)
- `GET /admin/api-keys` - Configuration des clés API

Pour la liste exhaustive et à jour (schémas, paramètres, exemples) : `/docs`.

## 🚀 Exemples d'utilisation

### Lister les tables disponibles
```bash
curl "http://localhost:8000/"
```

### Accéder aux données avec authentification
```bash
# Via header (recommandé)
curl -H "X-API-Key: $API_KEY" "http://localhost:8000/flux/r151?limit=10"

# Via query parameter
curl "http://localhost:8000/flux/r151?api_key=$API_KEY&limit=10"

# Filtrer par PRM
curl -H "X-API-Key: $API_KEY" "http://localhost:8000/flux/c15?prm=12345678901234"

# Obtenir les métadonnées d'une table
curl -H "X-API-Key: $API_KEY" "http://localhost:8000/flux/r151/info"
```

### Pagination
```bash
curl -H "X-API-Key: $API_KEY" "http://localhost:8000/flux/r151?limit=50&offset=100"
```

## 🔍 Monitoring

```bash
curl "http://localhost:8000/health"
curl -H "X-API-Key: $API_KEY" "http://localhost:8000/admin/api-keys"   # infos sur la clé
```

## 🚨 Dépannage

Bonnes pratiques clés, rotation, secrets-as-code et dépannage du 401 : voir
[docs/deployer/authentification-api.md](https://github.com/Energie-De-Nantes/electricore/blob/main/docs/deployer/authentification-api.md).

### Erreur 500 - Internal Server Error

```json
{
  "detail": "Base de données inaccessible: ..."
}
```

**Solutions** :
- Vérifiez que le fichier DuckDB existe
- Vérifiez les permissions de lecture
- Consultez les logs de l'application

## 🏗️ Développement

### Structure des modules

```
electricore/api/
├── __init__.py
├── main.py          # Application FastAPI principale (montage des routers)
├── config.py        # Configuration avec Pydantic Settings
├── security.py      # Système d'authentification
├── models.py        # Modèles Pydantic
├── decorators.py    # Décorateurs transverses (gestion d'erreurs)
├── routers/         # Un router par domaine (14, issue #82) :
│                    #   admin, affaires, chronologie, facturation, flux, ingestion,
│                    #   meta_periodes, prestations, provision, releves, rsc, sorties,
│                    #   taxes, turpe_variable — liste vivante : electricore/api/routers/
├── serializers/     # Sérialisation des réponses (Arrow, XLSX)
├── services/        # Services métier : duckdb, ingestion, taxes, facturation, check_facturation
└── README.md        # Cette documentation
```

### Tests

```bash
# Tester l'API localement
uv run uvicorn electricore.api.main:app --reload

# Accéder à la documentation
open http://localhost:8000/docs
```

### Extension

Pour ajouter de nouveaux endpoints sécurisés :

```python
from electricore.api.security import get_current_api_key

@app.get("/mon-endpoint")
async def mon_endpoint(api_key: str = Depends(get_current_api_key)):
    return {"message": "Accès autorisé"}
```