"""Doubles Telegram partagés par les tests des handlers du bot.

Stubs minimaux des objets python-telegram-bot consommés par les handlers :
on teste le comportement (textes, claviers, éditions, documents), pas PTB.
"""

from types import SimpleNamespace


class FakeMessage:
    def __init__(self, chat_id=1, message_id=100):
        self.chat_id = chat_id
        self.message_id = message_id
        self.replies: list[tuple[str, dict]] = []

    async def reply_html(self, text: str, **kwargs):
        self.replies.append((text, kwargs))
        return SimpleNamespace(chat_id=self.chat_id, message_id=self.message_id + 1)

    async def reply_text(self, text: str, **kwargs):
        self.replies.append((text, kwargs))


class FakeBot:
    def __init__(self):
        self.edits: list[tuple[int, int, str, dict]] = []
        self.documents: list[tuple[int, dict]] = []

    async def edit_message_text(self, text: str, chat_id: int, message_id: int, **kwargs):
        self.edits.append((chat_id, message_id, text, kwargs))

    async def send_document(self, chat_id: int, **kwargs):
        self.documents.append((chat_id, kwargs))


class FakeQuery:
    def __init__(self, data: str, chat_id=1, message_id=100):
        self.data = data
        self.message = FakeMessage(chat_id=chat_id, message_id=message_id)
        self.answered = False

    async def answer(self):
        self.answered = True


def update_commande(user_id=42) -> SimpleNamespace:
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id),
        effective_message=FakeMessage(),
        callback_query=None,
    )


def update_callback(data: str, user_id=42) -> SimpleNamespace:
    query = FakeQuery(data)
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id),
        effective_message=query.message,
        callback_query=query,
    )


def contexte(args=None, bot=None) -> SimpleNamespace:
    return SimpleNamespace(args=args or [], bot=bot or FakeBot())


def callbacks_du_markup(kwargs: dict) -> set[str]:
    markup = kwargs["reply_markup"]
    return {btn.callback_data for row in markup.inline_keyboard for btn in row}
