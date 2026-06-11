"""Tâches de fond du bot (notifications de fin de job ETL).

Module séparé pour casser le cycle d'import entre `app.py` (assemblage)
et les handlers qui lancent des tâches.
"""

import asyncio

_background_tasks: set[asyncio.Task] = set()


def create_task(coro) -> asyncio.Task:
    """Crée une tâche en la gardant en référence pour pouvoir l'annuler au shutdown."""
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task
