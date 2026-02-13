import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import main


def test_normalize_notification_text_repairs_mojibake() -> None:
    broken = "â Telegram-ÑÐ²ÐµÐ´Ð¾Ð¼Ð»ÐµÐ½Ð¸Ñ ÑÑÐ¿ÐµÑÐ½Ð¾ Ð¿Ð¾Ð´ÐºÐ»ÑÑÐµÐ½Ñ Ðº Ð²Ð°ÑÐµÐ¼Ñ Ð°ÐºÐºÐ°ÑÐ½ÑÑ."
    fixed = main._normalize_notification_text(broken)
    assert fixed == "✅ Telegram-уведомления успешно подключены к вашему аккаунту."


def test_normalize_notification_text_keeps_valid_text() -> None:
    text = "✅ Telegram-уведомления успешно подключены к вашему аккаунту."
    assert main._normalize_notification_text(text) == text


class DummyUser:
    def __init__(self, user_id: int) -> None:
        self.id = user_id


class DummyChat:
    def __init__(self, chat_id: int) -> None:
        self.id = chat_id


class DummySentMessage:
    def __init__(self, chat_id: int, message_id: int, text: str) -> None:
        self.chat = DummyChat(chat_id)
        self.message_id = message_id
        self.text = text
        self.edits: list[str] = []

    async def edit_text(self, text: str) -> None:
        self.edits.append(text)


class DummyMessage:
    def __init__(self, text: str, user_id: int | None) -> None:
        self.text = text
        self.from_user = DummyUser(user_id) if user_id is not None else None
        self.answers: list[str] = []
        self._next_message_id = 1

    async def answer(self, text: str) -> DummySentMessage:
        self.answers.append(text)
        sent = DummySentMessage(chat_id=self.from_user.id if self.from_user else 0, message_id=self._next_message_id, text=text)
        self._next_message_id += 1
        return sent


@pytest.mark.asyncio
async def test_rabbitmq_publisher_raises_if_not_connected() -> None:
    publisher = main.RabbitMQPublisher(url="amqp://guest:guest@localhost/", queue_name="q")

    with pytest.raises(RuntimeError):
        await publisher.publish_user_registration(request_id="req", token="token", telegram_user_id=123)


@pytest.mark.asyncio
async def test_rabbitmq_publisher_publishes_correct_payload() -> None:
    publisher = main.RabbitMQPublisher(url="amqp://guest:guest@localhost/", queue_name="q")
    mock_channel = SimpleNamespace()
    mock_exchange = AsyncMock()
    mock_channel.default_exchange = mock_exchange
    publisher._channel = mock_channel  # type: ignore[attr-defined]

    await publisher.publish_user_registration(request_id="req1", token="abc", telegram_user_id=42)

    assert mock_exchange.publish.await_count == 1
    call = mock_exchange.publish.await_args
    message = call.args[0]
    assert json.loads(message.body.decode("utf-8")) == {
        "request_id": "req1",
        "type": "telegram.link",
        "token": "abc",
        "telegram_user_id": 42,
    }
    assert call.kwargs["routing_key"] == "q"


@pytest.mark.asyncio
async def test_notification_consumer_parses_and_sends_message_without_buttons() -> None:
    bot = AsyncMock()
    consumer = main.RabbitMQNotificationConsumer(
        url="amqp://guest:guest@localhost/",
        queue_name="q",
        bot=bot,
    )

    class DummyIncomingMessage:
        def __init__(self, body: bytes) -> None:
            self.body = body

        async def __aenter__(self) -> "DummyIncomingMessage":  # pragma: no cover - trivial
            return self

        async def __aexit__(self, *_args: object) -> None:  # pragma: no cover - trivial
            return None

        def process(self) -> "DummyIncomingMessage":
            return self

    payload = {"telegram_user_id": 100, "text": "hello"}
    msg = DummyIncomingMessage(json.dumps(payload).encode("utf-8"))

    await consumer._on_message(msg)  # type: ignore[arg-type]

    bot.send_message.assert_awaited_once_with(chat_id=100, text="hello")


@pytest.mark.asyncio
async def test_notification_consumer_edits_processing_message_when_request_id_matches_pending() -> None:
    bot = AsyncMock()
    consumer = main.RabbitMQNotificationConsumer(
        url="amqp://guest:guest@localhost/",
        queue_name="q",
        bot=bot,
    )
    consumer._pending_timeout_seconds = 3600  # do not timeout during test

    await consumer.track_request(request_id="req-x", chat_id=100, message_id=50)

    class DummyIncomingMessage:
        def __init__(self, body: bytes) -> None:
            self.body = body

        async def __aenter__(self) -> "DummyIncomingMessage":  # pragma: no cover - trivial
            return self

        async def __aexit__(self, *_args: object) -> None:  # pragma: no cover - trivial
            return None

        def process(self) -> "DummyIncomingMessage":
            return self

    payload = {"request_id": "req-x", "telegram_user_id": 100, "text": "done"}
    msg = DummyIncomingMessage(json.dumps(payload).encode("utf-8"))

    await consumer._on_message(msg)  # type: ignore[arg-type]

    bot.edit_message_text.assert_awaited_once_with(chat_id=100, message_id=50, text="done")
    assert bot.send_message.await_count == 0


@pytest.mark.asyncio
async def test_notification_consumer_builds_inline_keyboard_when_buttons_present() -> None:
    bot = AsyncMock()
    consumer = main.RabbitMQNotificationConsumer(
        url="amqp://guest:guest@localhost/",
        queue_name="q",
        bot=bot,
    )

    class DummyIncomingMessage:
        def __init__(self, body: bytes) -> None:
            self.body = body

        async def __aenter__(self) -> "DummyIncomingMessage":  # pragma: no cover - trivial
            return self

        async def __aexit__(self, *_args: object) -> None:  # pragma: no cover - trivial
            return None

        def process(self) -> "DummyIncomingMessage":
            return self

    payload = {
        "telegram_user_id": 100,
        "text": "invite",
        "buttons": [
            {"type": "event_rsvp", "event_id": 1, "action": "ACCEPT"},
            {"type": "event_rsvp", "event_id": 1, "action": "DECLINE"},
        ],
    }
    msg = DummyIncomingMessage(json.dumps(payload).encode("utf-8"))

    await consumer._on_message(msg)  # type: ignore[arg-type]

    assert bot.send_message.await_count == 1
    kwargs = bot.send_message.await_args.kwargs
    assert kwargs["chat_id"] == 100
    assert kwargs["text"] == "invite"
    markup = kwargs["reply_markup"]
    assert markup is not None
    assert len(markup.inline_keyboard) == 1
    assert len(markup.inline_keyboard[0]) == 2


@pytest.mark.asyncio
async def test_handle_start_missing_from_user_does_nothing() -> None:
    message = DummyMessage(text="/start token", user_id=None)

    with patch.object(main, "rabbitmq_publisher", new=None):
        # Should not raise and not call publisher
        await main.handle_start(message)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_handle_start_without_token_sends_instruction() -> None:
    message = DummyMessage(text="/start", user_id=123)

    with patch.object(main, "rabbitmq_publisher", new=None):
        await main.handle_start(message)  # type: ignore[arg-type]

    assert len(message.answers) == 1
    assert "перейдите к боту по ссылке" in message.answers[0].lower()


@pytest.mark.asyncio
async def test_handle_start_with_token_publishes_to_rabbitmq_and_answers() -> None:
    message = DummyMessage(text="/start my-token", user_id=777)
    mock_publisher = AsyncMock()
    mock_consumer = AsyncMock()

    class DummyUuid:
        def __init__(self, hex_value: str) -> None:
            self.hex = hex_value

    with patch.object(main, "rabbitmq_publisher", new=mock_publisher), patch.object(
        main,
        "rabbitmq_notification_consumer",
        new=mock_consumer,
    ), patch.object(main.uuid, "uuid4", return_value=DummyUuid("req-1")):
        await main.handle_start(message)  # type: ignore[arg-type]

    # Отправили "processing" сообщение
    assert len(message.answers) == 1
    assert "⏳" in message.answers[0]

    mock_consumer.track_request.assert_awaited_once_with(
        request_id="req-1",
        chat_id=777,
        message_id=1,
    )
    mock_publisher.publish_user_registration.assert_awaited_once_with(
        request_id="req-1",
        token="my-token",
        telegram_user_id=777,
    )


@pytest.mark.asyncio
async def test_handle_start_broker_not_initialized_logs_error_and_does_not_crash() -> None:
    message = DummyMessage(text="/start token", user_id=123)

    with patch.object(main, "rabbitmq_publisher", new=None), patch.object(main, "rabbitmq_notification_consumer", new=None):
        await main.handle_start(message)  # type: ignore[arg-type]

    # Сначала отправили processing
    assert len(message.answers) == 1
    assert "⏳" in message.answers[0]


@pytest.mark.asyncio
async def test_handle_callback_publishes_event_rsvp() -> None:
    data = json.dumps({"type": "event_rsvp", "event_id": 10, "action": "ACCEPT"})
    from_user = DummyUser(999)

    class DummyCallbackMessage:
        def __init__(self, chat_id: int) -> None:
            self._chat_id = chat_id
            self.answers: list[DummySentMessage] = []

        async def answer(self, text: str) -> DummySentMessage:
            sent = DummySentMessage(chat_id=self._chat_id, message_id=len(self.answers) + 1, text=text)
            self.answers.append(sent)
            return sent

    class DummyCallback:
        def __init__(self) -> None:
            self.data = data
            self.from_user = from_user
            self.answered: list[str] = []
            self.message = DummyCallbackMessage(chat_id=from_user.id)

        async def answer(self, text: str) -> None:
            self.answered.append(text)

    dummy_callback = DummyCallback()
    mock_rsvp_publisher = AsyncMock()
    mock_consumer = AsyncMock()

    class DummyUuid:
        def __init__(self, hex_value: str) -> None:
            self.hex = hex_value

    with patch.object(main, "rabbitmq_rsvp_publisher", new=mock_rsvp_publisher), patch.object(
        main,
        "rabbitmq_notification_consumer",
        new=mock_consumer,
    ), patch.object(main.uuid, "uuid4", return_value=DummyUuid("req-2")):
        await main.handle_callback(dummy_callback)  # type: ignore[arg-type]

    mock_consumer.track_request.assert_awaited_once()
    mock_rsvp_publisher.publish_event_rsvp.assert_awaited_once_with(
        request_id="req-2",
        event_id=10,
        decision="ACCEPT",
        telegram_user_id=999,
    )
    assert dummy_callback.answered
