"""Onglet ouvert préservé — preuve dans un VRAI kernel marimo (#721).

`app.run()` (mode script, cf. `test_exports.py`) ne peut pas attraper ce bug :
sans kernel il n'y a ni `UIElementRegistry`, ni GC d'élément non lié, ni
`set_ui_element_value`. Or le bug vivait exactement là — `mo.ui.tabs(...)`
passé directement à `mo.output.replace()` n'était lié à aucun nom, donc
collecté à la fin de la cellule (le registre ne tient qu'un weakref) ; le
kernel jetait alors le clic d'onglet (« A UI element may go out of scope if it
was not assigned to a global variable », `marimo/_runtime/runtime.py`),
`on_change` ne partait jamais, `mo.state` restait sur « Facturation » et le
re-render suivant ramenait le visiteur au premier onglet.

Le test monte donc un kernel marimo réel (`marimo._runtime.kernel_lifecycle`)
et rejoue la séquence du navigateur : saisie de la clé, clic sur « Flux
bruts », puis changement d'un filtre. API interne de marimo : le module est
skippé si elle bouge, plutôt que de rougir sur un refactor amont.
"""

from __future__ import annotations

import asyncio
import contextlib
import queue
import sys

import pytest
from electricore_kiosque import exports, helpers

try:
    from marimo._ast.cell import CellConfig
    from marimo._config.config import DEFAULT_CONFIG
    from marimo._messaging.types import KernelStreams, NoopStream
    from marimo._runtime.commands import (
        AppMetadata,
        CreateNotebookCommand,
        ExecuteCellCommand,
        UpdateUIElementCommand,
    )
    from marimo._runtime.kernel_lifecycle import KernelArgs, kernel_session
    from marimo._session.model import SessionMode
except ImportError as exc:  # pragma: no cover - dépend de la version de marimo
    pytest.skip(f"API interne marimo indisponible : {exc}", allow_module_level=True)


@contextlib.contextmanager
def _kernel_du_notebook():
    """Instancie les cellules de `exports.py` dans un kernel marimo vivant."""
    cellules = exports.app._cell_manager
    ids = list(cellules.cell_ids())
    codes = [cellules.get_cell_code(i) for i in ids]

    args = KernelArgs(
        streams=KernelStreams(stream=NoopStream(), stdout=None, stderr=None, stdin=None),
        debugger=None,
        configs={i: CellConfig() for i in ids},
        app_metadata=AppMetadata(query_params={}, cli_args={}, app_config=exports.app._config),
        user_config=DEFAULT_CONFIG,
        mode=SessionMode.RUN,
        control_queue=queue.Queue(),
        set_ui_element_queue=queue.Queue(),
        virtual_file_storage=None,
    )
    # `create_kernel` remplace `sys.modules["__main__"]`, `sys.argv` et `sys.path`.
    principal, argv, chemins = sys.modules["__main__"], sys.argv, list(sys.path)
    try:
        with kernel_session(args) as (kernel, ctx):
            yield kernel, ctx, ids, codes
    finally:
        sys.modules["__main__"], sys.argv, sys.path[:] = principal, argv, chemins


def _element(registre, genre: str, **args):
    """Premier élément vivant du registre de classe `genre` (et d'args donnés)."""
    for oid, ref in registre._objects.items():
        objet = ref()
        if objet is None or type(objet).__name__ != genre:
            continue
        if all(objet._component_args.get(k) == v for k, v in args.items()):
            return oid, objet
    return None, None


def test_l_onglet_ouvert_survit_au_changement_de_filtre(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.test.invalide")
    monkeypatch.setattr(helpers, "recuperer_meta_periodes", lambda cle, **kw: [])
    monkeypatch.setattr(helpers, "recuperer_releves", lambda cle, **kw: ([], False))
    monkeypatch.setattr(helpers, "recuperer_flux", lambda cle, table, **kw: ([], False))

    async def scenario() -> None:
        with _kernel_du_notebook() as (kernel, ctx, ids, codes):
            await kernel.instantiate(
                CreateNotebookCommand(
                    execution_requests=tuple(
                        ExecuteCellCommand(cell_id=i, code=c) for i, c in zip(ids, codes, strict=True)
                    ),
                    cell_ids=tuple(ids),
                    set_ui_element_value_request=UpdateUIElementCommand(object_ids=[], values=[]),
                    auto_run=True,
                )
            )
            registre = ctx.ui_element_registry

            async def poser(oid, valeur) -> None:
                await kernel.set_ui_element_value(
                    UpdateUIElementCommand(object_ids=[oid], values=[valeur]),
                    notify_frontend=False,
                )

            # 1. Le visiteur colle sa clé → la cellule des onglets tourne.
            cle_id, _ = _element(registre, "text", kind="password")
            await poser(cle_id, "une-cle")

            onglets_id, onglets = _element(registre, "tabs")
            assert onglets is not None, (
                "mo.ui.tabs absent du registre : élément non lié à un nom, collecté "
                "en fin de cellule — tout clic d'onglet sera jeté par le kernel"
            )

            # 2. Clic sur « Flux bruts » : le frontend envoie l'INDEX de l'onglet
            #    (chaîne), que `tabs._convert_value` retraduit en nom d'onglet.
            await poser(onglets_id, str(onglets._tab_keys.index("Flux bruts")))
            assert kernel.globals["onglet_ouvert"]() == "Flux bruts"

            # 3. Changement d'un filtre : la cellule des onglets est reconstruite.
            table_id, _ = _element(registre, "dropdown")
            await poser(table_id, ["r151"])

            assert kernel.globals["onglet_ouvert"]() == "Flux bruts"
            _, rebati = _element(registre, "tabs")
            assert rebati is not None
            assert rebati._initial_value == "Flux bruts", "le mo.ui.tabs reconstruit repart sur son premier onglet"

    asyncio.run(scenario())
