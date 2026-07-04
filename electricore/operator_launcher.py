"""Lanceur opérateur `electricore-notebooks` — pont transitoire (#414).

PONT TRANSITOIRE — retirer à l'arrivée de `souscriptions_odoo`.

Une seule commande pour qu'un opérateur non-dev fasse tourner les deux notebooks
Odoo opérationnels (`facturation`, `injection_rsc`) comme apps marimo en mode
**run** (lecture seule) : visualiser → valider → cliquer « Injecter dans Odoo ».
Aucun git, aucun code, aucune édition de config.

Les notebooks ne sont PAS modifiés : leur garde de sécurité (mode simulation
`value=True` par défaut + écriture `OdooWriter` gardée par `mo.stop(run_button)`)
reste intacte. Ce module ne fait que valider l'environnement, résoudre le dossier
embarqué et servir l'app ASGI marimo.
"""

from __future__ import annotations

import os
import sys
import threading
import webbrowser
from importlib import resources
from pathlib import Path

from electricore.config import runtime

# Variables d'env opérateur (hors creds Odoo, validés via le registre runtime),
# avec le rôle métier de chacune — pour un message d'erreur actionnable.
_VARS_OPERATEUR: dict[str, str] = {
    "ELECTRICORE_API_URL": "URL de l'API ElectriCore consommée par les notebooks",
    "ELECTRICORE_API_KEY": "clé d'API ElectriCore pour authentifier les appels",
}

# Hôte/port d'écoute — localhost only (outil opérateur sur poste local).
_HOTE = "127.0.0.1"
_PORT = 2718  # port marimo par défaut
# Préfixe de montage du dossier dynamique. marimo 0.23.9 REFUSE path="/" (exige un
# préfixe non vide) → chaque notebook est servi sous /apps/<nom>.
_PREFIXE_APPS = "/apps"
# Notebook d'accueil servi à la racine fonctionnelle : `DynamicDirectoryMiddleware`
# n'expose AUCUN index sous /apps, donc on ouvre le navigateur sur cette page de
# liens vers les autres notebooks (sinon l'opérateur ne peut en atteindre qu'un seul).
_NOTEBOOK_ACCUEIL = "accueil"


def url_navigateur(base: str, noms: list[str]) -> str | None:
    """URL d'ouverture du navigateur : l'accueil si présent, sinon le 1er notebook.

    Le middleware de dossier dynamique ne fournit pas d'index ; ouvrir l'accueil
    (`/apps/accueil`) donne à l'opérateur les liens vers tous les notebooks. En son
    absence (config dégradée), on retombe sur le 1er notebook trié, ou rien si vide.
    """
    if _NOTEBOOK_ACCUEIL in noms:
        return f"{base}/{_NOTEBOOK_ACCUEIL}"
    if noms:
        return f"{base}/{noms[0]}"
    return None


def _manquantes_operateur() -> dict[str, str]:
    """Variables opérateur absentes ou vides, mappées sur leur rôle."""
    return {var: role for var, role in _VARS_OPERATEUR.items() if not os.environ.get(var, "").strip()}


def _manquantes_odoo() -> list[str]:
    """Noms des variables Odoo manquantes (bloc unique ODOO__*, #439).

    S'appuie sur le registre runtime (source de vérité, ADR-0025) : la connexion
    Odoo est validée ; les variables absentes remontent via `ConfigurationManquante`.
    """
    try:
        runtime.odoo()
    except runtime.ConfigurationManquante as exc:
        return [nom for noms in exc.manquantes.values() for nom in noms]
    return []


def charger_env() -> None:
    """Charge les `.env` (paquet + répertoire courant) dans `os.environ`.

    Les creds Odoo passent par pydantic-settings (`_env_file`), mais
    `ELECTRICORE_API_URL`/`ELECTRICORE_API_KEY` sont lues en direct via `os.getenv`
    (ici et dans les notebooks). Sans ce chargement, un opérateur qui remplit `.env`
    resterait « manquant ». Comme les apps marimo tournent dans le MÊME process
    uvicorn lancé par `main()`, ce seul chargement les rend visibles à la fois pour
    la validation et pour les notebooks.

    Deux emplacements, du plus prioritaire au moins prioritaire :
    1. `FICHIER_ENV` — `.env` ancré sur le paquet. En **checkout dev** c'est le `.env`
       racine du dépôt. Après `uv tool install 'electricore[notebooks]'`, ce chemin
       tombe dans `site-packages/` (absent) → no-op.
    2. `./.env` du **répertoire courant** — c'est là que l'opérateur tool-installé
       dépose le `.env` qu'on lui a livré (`cd <dossier> && electricore-notebooks`).
       Sans ce 2ᵉ emplacement, la promesse « aucun git, dépose un `.env`, lance »
       serait intenable hors checkout (cf. docs/operateur-notebook.md).

    `override=False` : une vraie variable d'env-système l'emporte toujours, et le
    `.env` paquet l'emporte sur celui du CWD (précédence documentée, ADR-0024).
    `FICHIER_ENV is None` (seam de test) → on ne charge rien.
    """
    from dotenv import load_dotenv

    if runtime.FICHIER_ENV is not None:
        load_dotenv(runtime.FICHIER_ENV, override=False)
        load_dotenv(Path.cwd() / ".env", override=False)


def valider_environnement() -> None:
    """Valide l'environnement requis ; sinon imprime un message clair et sort (non-nul).

    Requis : les creds Odoo lus par `charger_config_odoo()` (bloc ODOO__*) +
    `ELECTRICORE_API_URL` + `ELECTRICORE_API_KEY`. L'opérateur ne doit jamais
    deviner : chaque variable manquante est nommée avec son rôle.
    """
    odoo_manquantes = _manquantes_odoo()
    operateur_manquantes = _manquantes_operateur()

    if not odoo_manquantes and not operateur_manquantes:
        return

    lignes = ["❌ Configuration incomplète : impossible de lancer les notebooks opérateur.", ""]
    if odoo_manquantes:
        lignes.append("Connexion Odoo (creds chargés par charger_config_odoo) — variables manquantes :")
        lignes += [f"  - {nom}" for nom in odoo_manquantes]
        lignes.append("")
    if operateur_manquantes:
        lignes.append("Accès API ElectriCore — variables manquantes :")
        lignes += [f"  - {var} : {role}" for var, role in operateur_manquantes.items()]
        lignes.append("")
    lignes.append("Renseignez-les dans le fichier .env (voir .env.example) puis relancez electricore-notebooks.")

    print("\n".join(lignes), file=sys.stderr)
    sys.exit(1)


def dossier_notebooks() -> Path:
    """Chemin du dossier contenant les notebooks opérateur embarqués.

    En distribution (wheel), les sources sont force-incluses dans
    `electricore/_operator_notebooks/` et résolues via `importlib.resources`.
    En dev (arbre source), ce dossier n'existe pas → on retombe sur `notebooks/`
    à la racine du dépôt (les sources n'y sont jamais déplacées).
    """
    try:
        ressource = resources.files("electricore").joinpath("_operator_notebooks")
        with resources.as_file(ressource) as chemin:
            if chemin.is_dir():
                return chemin
    except (ModuleNotFoundError, FileNotFoundError):
        pass

    # Repli dev : dossier notebooks/ à la racine du dépôt.
    racine = Path(__file__).resolve().parents[1]
    return racine / "notebooks"


def construire_app(directory: str | Path, *, prefixe: str = _PREFIXE_APPS):
    """Construit l'app ASGI marimo servant chaque notebook du dossier en mode run.

    `with_dynamic_directory` route nativement chaque `.py` du dossier sous `prefixe`
    → pas de menu custom. Le mode run (lecture seule) est le défaut de
    `create_asgi_app`. marimo 0.23.9 exige un préfixe non vide (`/apps` par défaut).
    """
    import marimo

    return marimo.create_asgi_app().with_dynamic_directory(path=prefixe, directory=str(directory)).build()


def main() -> None:
    """Point d'entrée `electricore-notebooks` : charge le .env, valide, sert, ouvre le navigateur.

    `--edit` : sert les notebooks en mode édition au lieu du mode run. L'API ASGI
    marimo est run-only → on remplace le process par `python -m marimo edit`
    (même venv), l'env étant déjà chargé et validé.
    """
    charger_env()
    valider_environnement()

    dossier = dossier_notebooks()

    if "--edit" in sys.argv[1:]:
        os.execv(sys.executable, [sys.executable, "-m", "marimo", "edit", str(dossier)])

    app = construire_app(dossier)

    base = f"http://{_HOTE}:{_PORT}{_PREFIXE_APPS}"
    noms = sorted(p.stem for p in dossier.glob("*.py") if not p.name.startswith("_"))
    print("📓 Notebooks opérateur servis (mode run / lecture seule) :", file=sys.stderr)
    for nom in noms:
        print(f"   - {base}/{nom}", file=sys.stderr)
    print("   (chaque notebook garde son mode simulation par défaut)", file=sys.stderr)

    # Ouvrir le navigateur sur l'accueil dès que le serveur est prêt (best-effort,
    # non bloquant) : c'est la seule page qui liste tous les notebooks servis.
    cible = url_navigateur(base, noms)
    if cible:
        threading.Timer(1.0, lambda: webbrowser.open(cible)).start()

    import uvicorn

    uvicorn.run(app, host=_HOTE, port=_PORT)


if __name__ == "__main__":
    main()
