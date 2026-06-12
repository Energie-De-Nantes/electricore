# ElectriCore API - Authentication par Clés API

API REST sécurisée pour accéder aux données flux Enedis avec authentification par clés API.

## 🔐 Authentification

L'API utilise un système de clés API pour sécuriser l'accès aux données. Deux méthodes sont supportées :

### Header HTTP (Recommandé)
```bash
curl -H "X-API-Key: votre_cle_api" "http://localhost:8000/flux/r151"
```

### Query Parameter
```bash
curl "http://localhost:8000/flux/r151?api_key=votre_cle_api"
```

## ⚙️ Configuration

### 1. Créer le fichier .env

```bash
cp .env.example .env
```

### 2. Générer une clé API sécurisée

```bash
# Méthode 1 : Python
python -c "import secrets; print(secrets.token_urlsafe(32))"

# Méthode 2 : OpenSSL
openssl rand -base64 32
```

### 3. Configurer les clés API

```env
# Une seule clé
API_KEY=votre-cle-generee-ici

# Ou plusieurs clés (séparées par des virgules)
API_KEYS=cle1,cle2,cle3
```

### 4. Configuration optionnelle

```env
# Désactiver certaines méthodes d'authentification
ENABLE_API_KEY_HEADER=true
ENABLE_API_KEY_QUERY=false

# Personnaliser les métadonnées
API_TITLE="Mon API ElectriCore"
API_VERSION="1.0.0"
```

## 📡 Endpoints

### Endpoints Publics (sans authentification)

- `GET /` - Informations générales et exemples
- `GET /health` - Statut de l'API et de la base de données
- `GET /docs` - Documentation interactive Swagger
- `GET /redoc` - Documentation alternative

### Endpoints Sécurisés (authentification requise)

- `GET /flux/{table_name}` - Données d'une table flux
- `GET /flux/{table_name}/info` - Métadonnées d'une table (lignes, fraîcheur `derniere_date`)
- `POST /ingestion/run` - Déclencher un job d'ingestion (modes `test`/`all`/`rebuild`/`resync`, sélection de flux)
- `GET /ingestion/jobs`, `GET /ingestion/jobs/{id}` - Suivi des jobs d'ingestion
- `GET /taxes/...` - Calculs accise et CTA (exports Arrow/XLSX)
- `GET /facturation/...` - Documents mensuels ; `GET /facturation/check/odoo.xlsx` - check pré-facturation
- `GET /admin/api-keys` - Configuration des clés API

Les routes `/etl/*` restent des alias hors schéma OpenAPI de `/ingestion/*` pour une release (cf. issue #193).

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

## 🛡️ Sécurité

### Bonnes pratiques

1. **Clés longues et aléatoires** : Utilisez au minimum 32 caractères
2. **Variables d'environnement** : Ne jamais hard-coder les clés dans le code
3. **HTTPS en production** : Toujours utiliser une connexion chiffrée
4. **Rotation régulière** : Changez les clés API périodiquement
5. **Principe du moindre privilège** : Une clé par service/utilisateur

### Fichier .env

**⚠️ Important** : Le fichier `.env` ne doit jamais être commité dans le contrôle de version.

```bash
# Ajouter .env au .gitignore
echo ".env" >> .gitignore
```

## 🔍 Monitoring

### Vérifier le statut de l'API
```bash
curl "http://localhost:8000/health"
```

### Informations sur votre clé API
```bash
curl -H "X-API-Key: $API_KEY" "http://localhost:8000/admin/api-keys"
```

## 🚨 Dépannage

### Erreur 401 - Unauthorized

```json
{
  "detail": "Clé API requise. Utilisez le header 'X-API-Key' ou le paramètre '?api_key='"
}
```

**Solutions** :
- Vérifiez que la clé API est bien fournie
- Vérifiez le format du header : `X-API-Key: votre_cle`
- Vérifiez que la clé correspond à celle dans `.env`

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
├── routers/         # Un router par domaine : admin, flux, ingestion, taxes, facturation
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