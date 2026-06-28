"""Un flux peut éteindre l'incrémental (`incremental: false`) → il re-liste TOUT.

Le bug de classe : l'incrémental dlt vit sur le **listing** (modification_date), pas sur
le **landing**. Un flux qui liste un fichier mais échoue en aval (decrypt/parse → 0
document) **avance quand même son curseur** → le fichier est sauté à JAMAIS au run
suivant. C'est ce qui a vidé `raw_r67` en prod (M023 du jour skippés).

Fix scopé : R67 (flux ponctuel, peu volumineux) éteint l'incrémental dans flux.yaml ; il
liste tout à chaque run, le merge `file_name` en aval dédoublonne. Les flux quotidiens
(R64/R151/C15…) gardent leur incrémental.

Ces tests verrouillent : (1) flux.yaml éteint bien l'incrémental pour R67 ; (2) à travers
`flux_enedis_brut`, un flux `incremental: false` ne pose AUCUN curseur de listing, là où un
flux incrémental en pose un (même avec 0 document atterri — le cœur du bug).
"""

import json
import os
import time
from pathlib import Path

import dlt
import pytest
import yaml

from electricore.config import runtime
from electricore.ingestion.sources.sftp_enedis_brut import flux_enedis_brut

_CONFIG = {
    # R64 : incrémental par défaut (le curseur DOIT avancer au listing — bug de classe).
    "R64": {"file_pattern": "**/R63_R64_R65_R66_R67_C68/*_R64_*.zip", "format": "json", "file_regex": "*.JSON"},
    # R67 : incrémental éteint → re-liste tout, aucun curseur posé (le fix).
    "R67": {
        "file_pattern": "**/R63_R64_R65_R66_R67_C68/*_R67_*.zip",
        "format": "json",
        "file_regex": "*.json",
        "incremental": False,
    },
}


def test_flux_yaml_eteint_l_incremental_pour_r67():
    """La config de prod livrée porte bien la décision : R67 re-liste tout."""
    chemin = Path(__file__).parents[2] / "electricore" / "ingestion" / "config" / "flux.yaml"
    flux = yaml.safe_load(chemin.read_text())
    assert flux["R67"].get("incremental") is False


@pytest.fixture
def bucket_local(tmp_path):
    """Un bucket file:// avec un leurre R64 et un leurre R67 (datés, contenu bidon)."""
    b = tmp_path / "bucket"
    (b / "R63_R64_R65_R66_R67_C68").mkdir(parents=True)
    for nom in ("R63_R64_R65_R66_R67_C68/a_R64_x.zip", "R63_R64_R65_R66_R67_C68/a_R67_x.zip"):
        f = b / nom
        f.write_bytes(b"pas un vrai zip")
        ts = time.mktime((2026, 1, 1, 12, 0, 0, 0, 0, -1))
        os.utime(f, (ts, ts))
    return b


def _etat_resource(pipelines_dir, resource: str) -> dict | None:
    """État dlt d'une resource de listing (sftp), tous namespaces confondus."""
    state = json.loads((pipelines_dir / "flux_brut_t" / "state.json").read_text())
    for source_state in state.get("sources", {}).values():
        resources = source_state.get("resources", {})
        if resource in resources:
            return resources[resource]
    return None


def test_incremental_off_ne_pose_aucun_curseur_de_listing(tmp_path, bucket_local, monkeypatch):
    """Cœur du fix : à travers `flux_enedis_brut`, le flux incrémental (R64) pose un curseur
    `modification_date` — même avec 0 document atterri (contenu bidon → decrypt échoue) :
    c'est exactement le piège qui sautait les fichiers non atterris. Le flux `incremental:
    false` (R67), lui, n'en pose AUCUN → il re-listera le fichier au run suivant."""
    monkeypatch.setenv("SFTP__URL", f"file://{bucket_local}/")
    monkeypatch.setenv("AES__TROUSSEAU__test__KEY", "00112233445566778899aabbccddeeff")
    monkeypatch.setenv("AES__TROUSSEAU__test__IV", "ffeeddccbbaa99887766554433221100")
    runtime.vider_cache()

    pipelines_dir = tmp_path / "pipelines"
    pipeline = dlt.pipeline(
        pipeline_name="flux_brut_t",
        destination=dlt.destinations.duckdb(str(tmp_path / "t.duckdb")),
        dataset_name="flux_raw",
        pipelines_dir=str(pipelines_dir),
    )
    pipeline.run(flux_enedis_brut(_CONFIG, max_files=None))

    etat_r64 = _etat_resource(pipelines_dir, "filesystem_raw_r64")
    etat_r67 = _etat_resource(pipelines_dir, "filesystem_raw_r67")

    # R64 incrémental : un curseur de listing existe (le bug de classe, ici inoffensif).
    assert etat_r64 is not None and "incremental" in etat_r64
    # R67 incrémental éteint : aucun curseur → le fichier non atterri se rejouera.
    assert etat_r67 is None or "incremental" not in etat_r67
