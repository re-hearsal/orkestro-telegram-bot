import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import main


class DummyUser:
    def __init__(self, user_id: int) -> None:
        self.id = user_id


class DummyMessage:
    def __init__(self, text: str, user_id: int | None) -> None:
        self.text = text
        self.from_user = DummyUser(user_id) if user_id is not None else None
        self.answers: list[str] = []

    async def answer(self, text: str) -> None:
        self.answers.append(text)


@pytest.mark.asyncio
async def test_rabbitmq_publisher_raises_if_not_connected() -> None:
    publisher = main.RabbitMQPublisher(url="amqp://guest:guest@localhost/", queue_name="q")

    with pytest.raises(RuntimeError):
        await publisher.publish_user_registration(token="token", telegram_user_id=123)


@pytest.mark.asyncio
async def test_rabbitmq_publisher_publishes_correct_payload() -> None:
    publisher = main.RabbitMQPublisher(url="amqp://guest:guest@localhost/", queue_name="q")
    mock_channel = SimpleNamespace()
    mock_exchange = AsyncMock()
    mock_channel.default_exchange = mock_exchange
    publisher._channel = mock_channel  # type: ignore[attr-defined]

    await publisher.publish_user_registration(token="abc", telegram_user_id=42)

    assert mock_exchange.publish.await_count == 1
    call = mock_exchange.publish.await_args
    message = call.args[0]
    assert json.loads(message.body.decode("utf-8")) == {"token": "abc", "telegram_user_id": 42}
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

    with patch.object(main, "rabbitmq_publisher", new=mock_publisher):
        await main.handle_start(message)  # type: ignore[arg-type]

    # Ответ пользователю отправлен
    assert len(message.answers) == 1
    assert "Телеграм-уведомления успешно подключены" in message.answers[0]

    # Сообщение о регистрации пользователя отправлено в брокер
    mock_publisher.publish_user_registration.assert_awaited_once_with(
        token="my-token",
        telegram_user_id=777,
    )


@pytest.mark.asyncio
async def test_handle_start_broker_not_initialized_logs_error_and_does_not_crash() -> None:
    message = DummyMessage(text="/start token", user_id=123)

    with patch.object(main, "rabbitmq_publisher", new=None):
        await main.handle_start(message)  # type: ignore[arg-type]

    # Ответ об успешной привязке уже отправлен до проверки брокера
    assert len(message.answers) == 1
    # При этом отсутствие брокера не приводит к исключению


@pytest.mark.asyncio
async def test_handle_callback_publishes_event_rsvp() -> None:
    data = json.dumps({"type": "event_rsvp", "event_id": 10, "action": "ACCEPT"})
    from_user = DummyUser(999)

    class DummyCallback:
        def __init__(self) -> None:
            self.data = data
            self.from_user = from_user
            self.answered: list[str] = []

        async def answer(self, text: str) -> None:
            self.answered.append(text)

    dummy_callback = DummyCallback()
    mock_rsvp_publisher = AsyncMock()

    with patch.object(main, "rabbitmq_rsvp_publisher", new=mock_rsvp_publisher):
        await main.handle_callback(dummy_callback)  # type: ignore[arg-type]

    mock_rsvp_publisher.publish_event_rsvp.assert_awaited_once_with(
        event_id=10,
        decision="ACCEPT",
        telegram_user_id=999,
    )
    assert dummy_callback.answered
