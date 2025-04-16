# ⚡ ElectriCore - Moteur de calculs métier pour les données énergétiques

**ElectriCore** est un module dédié au traitement et à l'analyse des données issues du réseau électrique. Il constitue la **brique métier principale** pour les outils de supervision et de gestion énergétique, tels que **LibreWatt**, **un module Odoo**, et d'autres interfaces exploitant les données d'Enedis.

## 📌 Fonctionnalités principales

✅ **Transformation des données brutes** en formats exploitables\
✅ **Calcul des indicateurs métier** (rendement, consommation, anomalies…)\
✅ **Gestion multi-sources** pour agréger les données de différentes origines\
✅ **Export des résultats** vers divers outils (Odoo, LibreWatt, bases de données…)\
✅ **Haute testabilité** pour garantir la fiabilité des calculs

---

## 🚀 Sources de données supportées

ElectriCore est conçu pour fonctionner avec différentes sources de données, notamment :

- 🌡️ **ElectriFlux** : Données extraites des fichiers XML Enedis
- 🔗 **API SOAP Enedis** ( à venir )

---

## 🤦‍♂️ Architecture

ElectriCore est structuré en plusieurs modules indépendants :

📺 **electricore/**\
├── `core/` → Fonctions métier (calculs, agrégation de données…)\
├── `inputs/` → Connecteurs pour récupérer les données (`from_electriflux.py`, `from_soap.py`…)\
├── `outputs/` → Interfaces pour stocker/exporter (`to_odoo.py`, `to_postgres.py`…)\
├── `tests/` → Suite de tests unitaires et validation des algorithmes

```mermaid
graph TD

    subgraph inputs ["inputs/from_electriflux"]
        style inputs stroke-dasharray: 5 5
        R15["R15"]
        R151["R151"]
        C15["C15"]
    end

    subgraph core ["core"]
        style core stroke-dasharray: 5 5
        Périmètre["Périmètre"]
        Relevés["Relevés"]
        Energies["Energies"]
        Taxes["Taxes"]
    end

    R15 -->|Relevés| Relevés
    R151 -->|Relevés| Relevés
    C15 -->|HistoriquePérimètre| Périmètre

    Périmètre -->|SituationPérimètre| Energies
    Périmètre -->|VariationsMCT| Taxes

    Relevés -->| RelevéIndex | Energies

    Energies -->|Alimente| Taxes
    Energies -->|Alimente| outputs

    Taxes -->|Alimente| outputs


```
---

## 📊 Utilisation

### Exemple d’appel à **ElectriCore** pour facturer depuis les flux :

Nécéssite electriflux, et le chargement de certain secrets dans des variables d'environnement (cf Doc ElectriFlux)

```python
from electriflux.simple_reader import process_flux

from electricore.inputs.flux import lire_flux_c15
historique = lire_flux_c15(process_flux('C15', flux_path / 'C15'))

from electricore.inputs.flux import lire_flux_r151
relevés = lire_flux_r151(process_flux('R151', flux_path / 'R151'))


from zoneinfo import ZoneInfo
PARIS_TZ = ZoneInfo("Europe/Paris")
deb = pd.to_datetime('2025-01-01').tz_localize(PARIS_TZ)
fin = pd.to_datetime('2025-02-01').tz_localize(PARIS_TZ)

from electricore.core.services import facturation
factu = facturation(deb, fin, historique, relevés)

```

---

## 🔍 Tests et validation

ElectriCore est conçu pour être **hautement testable**. Avant toute modification, lancez les tests unitaires :

```bash
pytest tests/
```

TODO : Mettre en place un pipeline CI/CD est en place pour garantir la stabilité du projet et éviter les régressions.

---
## 🏗️ Roadmap

✔️ Implémentation du moteur de calculs métier\
✔️ Intégration avec ElectriFlux\
✔️ Utiliser pandera https://pandera.readthedocs.io/en/stable/ pour valider les dataframes\
⏳ Implémentation des tests (délégué)\
⏳ CI/CD\
⏳ Ajout d’un connecteur vers l’API SOAP Enedis\
⏳ Stockage des résultats en base de données\
⏳ Documentation API détaillée

### Ajout de fonctionnalités : 

✔️ Traitement des flux Facturants Fxx\
⏳ Calcul automatique des cas compliqués (MCT et co)\
⏳ Gestion des prestations\
⏳ Traitement des Affaires, lecture\
⏳ Traitement des Affaires, écriture\
⏳ Suivi et maintien des souscriptions aux services de données\

