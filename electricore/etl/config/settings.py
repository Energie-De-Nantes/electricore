"""
Configuration centralisée pour les pipelines ETL.
Charge la configuration des flux et expose les constantes principales.
"""

import logging
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)

# Constantes de chemin
ETL_DIR = Path(__file__).parent.parent
CONFIG_DIR = ETL_DIR / "config"
CONFIG_FILE = CONFIG_DIR / "flux.yaml"

def load_flux_config() -> dict:
    """
    Charge la configuration YAML des flux Enedis.
    
    Returns:
        dict: Configuration des flux par type
        
    Raises:
        FileNotFoundError: Si le fichier de configuration n'existe pas
        yaml.YAMLError: Si erreur de parsing YAML
    """
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.debug("Configuration chargée pour %d types de flux: %s", len(config), list(config.keys()))
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Fichier de configuration introuvable: {CONFIG_FILE}")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Erreur de parsing YAML dans {CONFIG_FILE}: {e}")

# Configuration globale des flux (chargée au premier import)
FLUX_CONFIG = load_flux_config()

# Constantes utiles
FLUX_TYPES = list(FLUX_CONFIG.keys())