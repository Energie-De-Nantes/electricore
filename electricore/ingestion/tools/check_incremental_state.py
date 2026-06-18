#!/usr/bin/env python3
"""Inspecte l'état incrémental dlt du pipeline d'ingestion brut (ADR-0020).

Le curseur incrémental de chaque flux (`modification_date`) vit dans l'état dlt,
PAS dans la base : c'est lui qui évite de re-télécharger les fichiers Enedis déjà
landés. Cet outil le donne à lire, par namespace de source.

Nom du pipeline : résolu comme le runner (`flux_brut_<stem de la base>`,
cf. runner.lander_brut), depuis le registre runtime — donc pas de nom codé en dur.

⚠️ Le namespace d'état dlt n'est pas stable : un run mono-flux (`ingestion r64`)
et un run multi-flux (`ingestion all`) ne stockent pas leurs curseurs sous la même
clé de source (`flux_enedis_brut` vs `flux_brut_<stem>`). Cet outil affiche TOUS
les namespaces et signale la fragmentation le cas échéant.

Usage : uv run python -m electricore.ingestion.tools.check_incremental_state
"""

import json
from datetime import UTC, datetime

import dlt

from electricore.config import runtime


def _nom_pipeline() -> str:
    """Même résolution que le runner : `flux_brut_<stem de la base de prod>`."""
    return f"flux_brut_{runtime.duckdb().chemin.stem}"


def _age_jours(valeur: str) -> str:
    """Âge en jours d'une date ISO, pour repérer un curseur qui n'avance plus."""
    try:
        date_obj = datetime.fromisoformat(str(valeur).replace("Z", "+00:00"))
        if date_obj.tzinfo is None:
            date_obj = date_obj.replace(tzinfo=UTC)
        return f" (il y a {(datetime.now(date_obj.tzinfo) - date_obj).days} j)"
    except (ValueError, TypeError):
        return ""


def check_incremental_state() -> None:
    """Affiche le curseur incrémental de chaque resource, par namespace de source."""
    nom = _nom_pipeline()
    print("=" * 80)
    print("🔍 ÉTAT INCRÉMENTAL DU PIPELINE")
    print("=" * 80)
    print(f"Pipeline : {nom}")

    pipeline = dlt.pipeline(pipeline_name=nom)
    state = pipeline.state
    sources = state.get("sources", {})
    if not sources:
        print("\n❌ Aucune source dans l'état (pipeline jamais lancé localement ?)")
        return

    # Namespaces portant un curseur incrémental (resources filesystem_*).
    namespaces_avec_curseur = []
    for source_name, source_state in sources.items():
        resources = source_state.get("resources", {})
        curseurs = {r: rs for r, rs in resources.items() if "incremental" in rs}
        if not curseurs:
            continue
        namespaces_avec_curseur.append(source_name)
        print(f"\n📦 Namespace de source : {source_name}")
        for res_name, res_state in sorted(curseurs.items()):
            for inc_name, inc_state in res_state["incremental"].items():
                last = inc_state.get("last_value")
                if last is None:
                    print(f"   📄 {res_name} [{inc_name}] : non initialisé")
                else:
                    print(f"   📄 {res_name} [{inc_name}] : {last}{_age_jours(last)}")

    # Le quirk : plusieurs namespaces = curseurs fragmentés (cf. issue à venir).
    if len(namespaces_avec_curseur) > 1:
        print("\n" + "=" * 80)
        print("⚠️  CURSEURS FRAGMENTÉS SUR PLUSIEURS NAMESPACES")
        print("=" * 80)
        print(
            "Plusieurs namespaces portent des curseurs : "
            f"{', '.join(namespaces_avec_curseur)}.\n"
            "Cause connue : le namespace d'état dlt dépend du nombre de flux du run\n"
            "(mono-flux vs multi-flux). Un run `ingestion all` ne lit que le namespace\n"
            "multi-flux ; les curseurs posés par un backfill mono-flux y sont invisibles\n"
            "(re-fetch dédupliqué par le merge sur file_name — sans corruption)."
        )

    # État brut JSON des incréments, pour debug fin.
    print("\n" + "=" * 80)
    print("🔧 ÉTAT BRUT (debug)")
    print("=" * 80)
    for source_name, source_state in sources.items():
        for res_name, res_state in source_state.get("resources", {}).items():
            if "incremental" in res_state:
                print(f"\n{source_name}.{res_name} :")
                print(json.dumps(res_state["incremental"], indent=2, default=str))


if __name__ == "__main__":
    check_incremental_state()
