"""Isolation cryptographique multi-destinataires SOPS+age (ADR-0044 §6, issue #420).

Un `secrets.env` chiffré pour DEUX destinataires (admin + box) :
- se déchiffre avec la clé de l'UN **ou** de l'AUTRE (chaque box destinataire lit) ;
- ÉCHOUE avec une clé NON destinataire (outsider) — la box d'un autre provider ne
  peut pas le déchiffrer. L'isolation entre instances est cryptographique, pas une
  convention de dossiers.

Test contre un VRAI `sops`/`age` (clés + ciphertext committés à valeurs FACTICES).
Se skip si `sops` n'est pas sur le PATH (image + CI l'embarquent).
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
MULTI = _REPO_ROOT / "tests" / "fixtures" / "sops" / "multi"
SECRETS_ENV = MULTI / "secrets.env"

sops_present = shutil.which("sops") is not None
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not sops_present, reason="sops/age absents du PATH (embarqués par l'image + CI)"),
]


def _decrypt_with(keyfile: Path):
    """Tente `sops decrypt` avec <keyfile> comme clé age. Renvoie le CompletedProcess."""
    return subprocess.run(
        [
            "sops",
            "decrypt",
            "--input-type",
            "dotenv",
            "--output-type",
            "dotenv",
            str(SECRETS_ENV),
        ],
        env={"SOPS_AGE_KEY_FILE": str(keyfile), "PATH": os.environ["PATH"]},
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.parametrize("recipient", ["admin", "box"])
def test_chaque_destinataire_dechiffre(recipient):
    """Le ciphertext se déchiffre avec la clé de CHAQUE destinataire (admin OU box)."""
    cp = _decrypt_with(MULTI / f"{recipient}.key")
    assert cp.returncode == 0, cp.stderr
    assert "API_KEY=multi_recipient_factice_secret_value_xyz" in cp.stdout


def test_non_destinataire_echoue():
    """Une clé NON destinataire (autre provider) NE déchiffre PAS — isolation crypto."""
    cp = _decrypt_with(MULTI / "outsider.key")
    assert cp.returncode != 0
    assert "multi_recipient_factice_secret_value_xyz" not in cp.stdout
