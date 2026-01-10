import asyncio
import json
import logging
import os

import aio_pika
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from dotenv import load_dotenv


logger = logging.getLogger(__name__)

router = Router()


class RabbitMQPublisher:
    def __init__(self, url: str, queue_name: str) -> None:
        self._url = url
        self._queue_name = queue_name
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._queue: aio_pika.abc.AbstractQueue | None = None

    async def connect(self) -> None:
        logger.info("Connecting to RabbitMQ at %s", self._url)
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel()
        self._queue = await self._channel.declare_queue(
            self._queue_name,
            durable=True,
        )
        logger.info("Connected to RabbitMQ, queue=%s", self._queue_name)

    async def publish_user_registration(self, *, token: str, telegram_user_id: int) -> None:
        if self._channel is None:
            raise RuntimeError

        payload = {"token": token, "telegram_user_id": telegram_user_id}
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        message = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        await self._channel.default_exchange.publish(
            message,
            routing_key=self._queue_name,
        )
        logger.info("Published user registration to RabbitMQ: %s", payload)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            logger.info("Closed RabbitMQ connection")


class RabbitMQNotificationConsumer:
    def __init__(self, url: str, queue_name: str, bot: Bot) -> None:
        self._url = url
        self._queue_name = queue_name
        self._bot = bot
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None

    async def start(self) -> None:
        logger.info("Connecting to RabbitMQ for incoming Telegram notifications at %s", self._url)
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel()
        queue = await self._channel.declare_queue(
            self._queue_name,
            durable=True,
        )
        await queue.consume(self._on_message)
        logger.info("Started consuming Telegram notifications from queue=%s", self._queue_name)

    async def _on_message(self, message: aio_pika.IncomingMessage) -> None:
        async with message.process():
            try:
                payload = json.loads(message.body.decode("utf-8"))
                chat_id = int(payload["telegram_user_id"])
                text = str(payload["text"])
            except Exception:
                logger.exception("Failed to parse incoming Telegram notification")
                return

            try:
                await self._bot.send_message(chat_id=chat_id, text=text)
            except Exception:
                logger.exception("Failed to send Telegram notification to chat_id=%s", chat_id)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            logger.info("Closed RabbitMQ notification consumer connection")


rabbitmq_publisher: RabbitMQPublisher | None = None
rabbitmq_notification_consumer: RabbitMQNotificationConsumer | None = None


@router.message(CommandStart())
async def handle_start(message: Message) -> None:
    user = message.from_user
    if user is None:
        logger.warning("Received /start from message without from_user")
        return

    user_id = user.id

    text = message.text or ""
    parts = text.split(maxsplit=1)
    token = parts[1].strip() if len(parts) > 1 else None

    if not token:
        await message.answer(
            ("Чтобы подключить Telegram-уведомления, перейдите к боту по ссылке из личного кабинета приложения."),
        )
        logger.warning("Received /start without token from telegram_user_id=%s", user_id)
        return

    await message.answer(
        (
            "✅ Телеграм-уведомления успешно подключены.\n\n"
            "Теперь важные уведомления будут приходить вам сюда, "
            "в личные сообщения от этого бота."
        ),
    )

    if rabbitmq_publisher is None:
        logger.error("RabbitMQ publisher is not initialized; cannot send user_id")
        return

    await rabbitmq_publisher.publish_user_registration(token=token, telegram_user_id=user_id)


@router.message()
async def handle_message(message: Message) -> None:
    await message.delete()

async def main() -> None:
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        msg = "BOT_TOKEN is not set in environment or .env file"
        raise RuntimeError(msg)

    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    rabbitmq_queue = os.getenv(
        "RABBITMQ_TELEGRAM_QUEUE",
        "telegram_notification_registrations",
    )
    rabbitmq_out_queue = os.getenv(
        "RABBITMQ_TELEGRAM_OUT_QUEUE",
        "telegram_bot_messages",
    )

    global rabbitmq_publisher
    rabbitmq_publisher = RabbitMQPublisher(
        url=rabbitmq_url,
        queue_name=rabbitmq_queue,
    )
    await rabbitmq_publisher.connect()

    bot = Bot(token=bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    global rabbitmq_notification_consumer
    rabbitmq_notification_consumer = RabbitMQNotificationConsumer(
        url=rabbitmq_url,
        queue_name=rabbitmq_out_queue,
        bot=bot,
    )
    await rabbitmq_notification_consumer.start()
    dp = Dispatcher()
    dp.include_router(router)

    try:
        logger.info("Starting Telegram bot polling")
        await dp.start_polling(bot)
    finally:
        logger.info("Shutting down Telegram bot")
        if rabbitmq_publisher is not None:
            await rabbitmq_publisher.close()
        if rabbitmq_notification_consumer is not None:
            await rabbitmq_notification_consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
