"""Test bout-en-bout du déchiffrement SOPS+age à l'entrypoint (ADR-0044, issue #418).

On exécute le VRAI `deploy/docker/entrypoint.sh` contre un VRAI `sops`/`age` (pas
de mock) : la fixture `secrets.env` (valeurs FACTICES) est chiffrée à la clé de test
committée `tests/fixtures/sops/test_age.key`. On vérifie :

- chemin nominal : les credentials atterrissent dans l'env du process exec'd,
  Y COMPRIS un nom de variable DYNAMIQUE `AES__TROUSSEAU__demo__KEY` (ADR-0037),
  ce qui prouve l'absence d'énumération (la voie `sops exec-env`, ADR-0044 §2) ;
- fail-fast : sans clé age, l'entrypoint échoue bruyamment (ADR-0044 §3) ;
- fail-fast : sans fichier chiffré, idem ;
- échappatoire : `ELECTRICORE_DECRYPT=off` contourne le déchiffrement (ADR-0044 §3).

Si `sops`/`age` ne sont pas sur le PATH, le test se skip (l'image et la CI les
embarquent ; cf. Dockerfile + .github/workflows/ci.yml).
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
ENTRYPOINT = _REPO_ROOT / "deploy" / "docker" / "entrypoint.sh"
FIXTURES = _REPO_ROOT / "tests" / "fixtures" / "sops"
AGE_KEY = FIXTURES / "test_age.key"
SECRETS_ENV = FIXTURES / "secrets.env"

# Outils requis pour le test bout-en-bout (vrai déchiffrement, pas de mock).
sops_present = shutil.which("sops") is not None
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not sops_present, reason="sops/age absents du PATH (embarqués par l'image + CI)"),
]


def _run_entrypoint(args, *, env_extra=None):
    """Lance l'entrypoint avec `args` (la commande à exec) et renvoie le CompletedProcess."""
    env = dict(os.environ)
    env.setdefault("SECRETS_ENV_FILE", str(SECRETS_ENV))
    env.setdefault("SOPS_AGE_KEY_FILE", str(AGE_KEY))
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        ["sh", str(ENTRYPOINT), *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_dechiffre_et_injecte_les_credentials():
    """Chemin nominal : les secrets factices sont dans l'env de la commande exec'd."""
    cp = _run_entrypoint(["sh", "-c", "echo API_KEY=$API_KEY; echo SFTP=$SFTP__URL; echo ODOO=$ODOO__URL"])
    assert cp.returncode == 0, cp.stderr
    assert "API_KEY=test_api_key_factice_0123456789abcdef" in cp.stdout
    assert "SFTP=sftp://user:pass@sftp.example.test:22/exports" in cp.stdout
    assert "ODOO=https://odoo.example.test" in cp.stdout


def test_injecte_un_nom_de_variable_dynamique_du_trousseau_aes():
    """Le label AES `demo` est DYNAMIQUE : injecté sans énumération (ADR-0037/§2)."""
    cp = _run_entrypoint(["sh", "-c", "echo DEMO=$AES__TROUSSEAU__demo__KEY"])
    assert cp.returncode == 0, cp.stderr
    assert "DEMO=00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff" in cp.stdout


def test_fail_fast_sans_cle_age():
    """Sans clé age : échec dur, message clair, la commande ne tourne pas (ADR-0044 §3)."""
    cp = _run_entrypoint(
        ["sh", "-c", "echo NE_DOIT_PAS_TOURNER"],
        env_extra={"SOPS_AGE_KEY_FILE": "/nonexistent-age.key"},
    )
    assert cp.returncode != 0
    assert "NE_DOIT_PAS_TOURNER" not in cp.stdout
    assert "aucune clé age" in cp.stderr.lower()


def test_fail_fast_sans_fichier_chiffre():
    """Sans secrets.env chiffré : échec dur (ADR-0044 §3)."""
    cp = _run_entrypoint(
        ["sh", "-c", "echo NE_DOIT_PAS_TOURNER"],
        env_extra={"SECRETS_ENV_FILE": "/nonexistent-secrets.env"},
    )
    assert cp.returncode != 0
    assert "NE_DOIT_PAS_TOURNER" not in cp.stdout
    assert "introuvable" in cp.stderr.lower()


def test_bypass_electricore_decrypt_off():
    """`ELECTRICORE_DECRYPT=off` contourne le déchiffrement (échappatoire dev/test)."""
    cp = _run_entrypoint(
        ["sh", "-c", "echo contourne_ok"],
        env_extra={
            "ELECTRICORE_DECRYPT": "off",
            # Même sans secrets valides, le bypass doit aboutir.
            "SOPS_AGE_KEY_FILE": "/nonexistent-age.key",
            "SECRETS_ENV_FILE": "/nonexistent-secrets.env",
        },
    )
    assert cp.returncode == 0, cp.stderr
    assert "contourne_ok" in cp.stdout
