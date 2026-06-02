# Déploiement VPS via Docker Compose

ElectriCore tourne en production sur un VPS unique avec une stack `docker-compose` ([deploy/docker/](../../deploy/docker/)) : conteneur API (FastAPI), conteneur bot (Telegram), conteneur scheduler (cron → appels HTTP vers `/etl/run`), reverse-proxy Caddy avec TLS automatique. Pas d'alternative sérieusement évaluée — c'était l'évidence pour un opérateur solo. PaaS managé écarté (volumes persistants pour DuckDB, mode SFTP collocé), Kubernetes écarté (sur-dimensionné pour 4 conteneurs sur une machine).

## Limites à connaître avant un changement d'échelle

- **VPS unique** : pas de haute dispo, pas de réplication. Une panne du VPS = indisponibilité totale jusqu'au redémarrage.
- **Volume DuckDB local** : la base vit sur le disque du VPS. Sauvegardes nocturnes via `EXPORT DATABASE` (voir [docs/deploiement.md](../deploiement.md)) ; restauration manuelle.
- **Mono-tenant** : la stack est conçue pour un fournisseur d'énergie à la fois. Multi-tenant nécessiterait des changements significatifs (segmentation des clés API, isolation des bases).
