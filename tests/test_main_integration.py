import json
from unittest.mock import AsyncMock, patch

import pytest

import main


class DummyExchange:
    def __init__(self) -> None:
        self.published: list[tuple[bytes, str]] = []

    async def publish(self, message: object, routing_key: str) -> None:
        # message is aio_pika.Message; we only care about body
        self.published.append((message.body, routing_key))  # type: ignore[attr-defined]


class DummyQueue:
    def __init__(self) -> None:
        self._callback = None

    async def consume(self, callback) -> None:  # type: ignore[no-untyped-def]
        self._callback = callback

    async def push(self, body: bytes) -> None:
        # Simulate aio_pika.IncomingMessage minimal subset
        class DummyIncomingMessage:
            def __init__(self, body: bytes) -> None:
                self.body = body

            async def __aenter__(self) -> "DummyIncomingMessage":  # pragma: no cover - trivial
                return self

            async def __aexit__(self, *_args: object) -> None:  # pragma: no cover - trivial
                return None

            def process(self) -> "DummyIncomingMessage":
                return self

        if self._callback is not None:
            await self._callback(DummyIncomingMessage(body))


class DummyChannel:
    def __init__(self) -> None:
        self.default_exchange = DummyExchange()
        self.queues: dict[str, DummyQueue] = {}

    async def declare_queue(self, name: str, durable: bool = True) -> DummyQueue:  # pragma: no cover
        return self.queues.setdefault(name, DummyQueue())


class DummyConnection:
    def __init__(self, channel: DummyChannel) -> None:
        self._channel = channel

    async def channel(self) -> DummyChannel:
        return self._channel

    async def close(self) -> None:  # pragma: no cover - trivial
        return None


def _broker_unavailable(_url: str) -> None:
    raise ConnectionError


@pytest.mark.asyncio
async def test_publisher_and_consumer_integration_via_dummy_broker() -> None:
    channel = DummyChannel()
    connection = DummyConnection(channel=channel)

    async def fake_connect_robust(_url: str) -> DummyConnection:
        return connection

    bot = AsyncMock()

    with patch("main.aio_pika.connect_robust", new=fake_connect_robust):
        publisher = main.RabbitMQPublisher(url="amqp://guest:guest@localhost/", queue_name="reg_queue")
        await publisher.connect()

        consumer = main.RabbitMQNotificationConsumer(
            url="amqp://guest:guest@localhost/",
            queue_name="out_queue",
            bot=bot,
        )
        await consumer.start()

        # Находим очереди, созданные потребителем
        out_queue = channel.queues["out_queue"]

        # Публикуем сообщение через publisher и проверяем, что оно попадает в нужную очередь
        await publisher.publish_user_registration(token="t-1", telegram_user_id=555)
        assert channel.default_exchange.published, "Publisher did not publish any messages"

        body, routing_key = channel.default_exchange.published[0]
        assert routing_key == "reg_queue"
        assert json.loads(body.decode("utf-8")) == {"token": "t-1", "telegram_user_id": 555}

        # Теперь эмулируем сообщение от backend в очередь, которую слушает consumer,
        # и проверяем, что бот отправляет сообщение пользователю.
        backend_payload = {"telegram_user_id": 555, "text": "ok"}
        await out_queue.push(json.dumps(backend_payload).encode("utf-8"))

        bot.send_message.assert_awaited_once_with(chat_id=555, text="ok")


@pytest.mark.asyncio
async def test_publisher_connect_broker_failure_raises() -> None:
    with patch("main.aio_pika.connect_robust", new=_broker_unavailable):
        publisher = main.RabbitMQPublisher(url="amqp://guest:guest@localhost/", queue_name="q")

        with pytest.raises(ConnectionError):
            await publisher.connect()


@pytest.mark.asyncio
async def test_consumer_start_broker_failure_raises() -> None:
    bot = AsyncMock()

    with patch("main.aio_pika.connect_robust", new=_broker_unavailable):
        consumer = main.RabbitMQNotificationConsumer(
            url="amqp://guest:guest@localhost/",
            queue_name="q",
            bot=bot,
        )

        with pytest.raises(ConnectionError):
            await consumer.start()
