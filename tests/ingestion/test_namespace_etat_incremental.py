"""Le namespace d'état incrémental est stable quel que soit le nombre de flux (#346).

Avant le fix, un run mono-flux (`ingestion r64`) et un run multi-flux (`ingestion all`)
stockaient leurs curseurs sous DEUX namespaces dlt différents (`flux_enedis_brut` vs
`flux_brut_<stem>`). Conséquence : un `ingestion <flux>` seul repartait d'un curseur
vide et RE-TÉLÉCHARGEAIT tout le flux. Le fix épingle le `source_name` de la resource
filesystem interne sur `ESPACE_ETAT_INCREMENTAL` → un seul namespace.

Ce test verrouille l'invariant : mono et multi posent le curseur sous le MÊME namespace.
"""

import json
import os
import time

import dlt
import pytest

from electricore.config import runtime
from electricore.ingestion.sources.sftp_enedis import ESPACE_ETAT_INCREMENTAL
from electricore.ingestion.sources.sftp_enedis_brut import flux_enedis_brut

_CONFIG = {
    "C15": {"file_pattern": "**/*_C15_*.zip", "format": "xml", "file_regex": "*_C15_*.xml"},
    "R64": {"file_pattern": "**/R63_R64_R65_R66_R67_C68/*_R64_*.zip", "format": "json", "file_regex": "*.JSON"},
}


@pytest.fixture
def bucket_local(tmp_path):
    """Un bucket file:// avec un leurre C15 et un leurre R64 (datés, contenu bidon)."""
    b = tmp_path / "bucket"
    (b / "R63_R64_R65_R66_R67_C68").mkdir(parents=True)
    for nom in ("a_C15_x.zip", "R63_R64_R65_R66_R67_C68/a_R64_x.zip"):
        f = b / nom
        f.write_bytes(b"pas un vrai zip")
        ts = time.mktime((2026, 1, 1, 12, 0, 0, 0, 0, -1))
        os.utime(f, (ts, ts))
    return b


def _namespace_du_curseur(pipelines_dir, resource: str) -> str | None:
    state = json.loads((pipelines_dir / "flux_brut_t" / "state.json").read_text())
    for source_name, source_state in state.get("sources", {}).items():
        if resource in source_state.get("resources", {}):
            return source_name
    return None


@pytest.mark.parametrize("flux", [["R64"], ["C15", "R64"]], ids=["mono", "multi"])
def test_namespace_etat_stable_quel_que_soit_le_nombre_de_flux(flux, tmp_path, bucket_local, monkeypatch):
    monkeypatch.setenv("SFTP__URL", f"file://{bucket_local}/")
    monkeypatch.setenv("AES__KEY", "00112233445566778899aabbccddeeff")
    monkeypatch.setenv("AES__IV", "ffeeddccbbaa99887766554433221100")
    runtime.vider_cache()

    pipelines_dir = tmp_path / "pipelines"
    pipeline = dlt.pipeline(
        pipeline_name="flux_brut_t",
        destination=dlt.destinations.duckdb(str(tmp_path / "t.duckdb")),
        dataset_name="flux_raw",
        pipelines_dir=str(pipelines_dir),
    )
    pipeline.run(flux_enedis_brut({k: _CONFIG[k] for k in flux}, max_files=None))

    # Le curseur R64 (présent en mono ET en multi) doit toujours vivre au même endroit.
    assert _namespace_du_curseur(pipelines_dir, "filesystem_raw_r64") == ESPACE_ETAT_INCREMENTAL
