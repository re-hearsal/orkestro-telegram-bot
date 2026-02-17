import asyncio
import json
import logging
import os
import uuid

import aio_pika
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

from config import I18N_MESSAGES, RABBIT_CONTRACT, BotMessagesConfig


logger = logging.getLogger(__name__)

router = Router()

REQUEST_TYPE_TELEGRAM_LINK = RABBIT_CONTRACT.request_type_telegram_link
REQUEST_TYPE_EVENT_RSVP = RABBIT_CONTRACT.request_type_event_rsvp
TELEGRAM_BUTTON_TYPE_EVENT_RSVP = RABBIT_CONTRACT.button_type_event_rsvp


_MOJIBAKE_MARKERS = ("Ð", "Ñ", "â", "Â", "Ã")


def _cyrillic_char_count(text: str) -> int:
    return sum(1 for ch in text if ("А" <= ch <= "я") or ch in ("Ё", "ё"))


def _mojibake_char_count(text: str) -> int:
    return sum(text.count(marker) for marker in _MOJIBAKE_MARKERS)


def _normalize_notification_text(text: str) -> str:
    if not any(marker in text for marker in _MOJIBAKE_MARKERS):
        return text
    try:
        repaired = text.encode("latin-1").decode("utf-8")
    except UnicodeError:
        return text

    if _mojibake_char_count(repaired) < _mojibake_char_count(text) and _cyrillic_char_count(repaired) >= _cyrillic_char_count(text):
        return repaired
    return text


_user_locales: dict[int, str] = {}


def _normalize_locale(raw_locale: str | None) -> str:
    if not raw_locale:
        return "ru"
    locale = raw_locale.lower()
    if locale.startswith("en"):
        return "en"
    return "ru"


def _messages_for_locale(locale: str | None) -> BotMessagesConfig:
    normalized = _normalize_locale(locale)
    return I18N_MESSAGES.en if normalized == "en" else I18N_MESSAGES.ru


def _messages_for_chat(chat_id: int) -> BotMessagesConfig:
    return _messages_for_locale(_user_locales.get(chat_id))


def _extract_payload_locale(payload: dict[str, object]) -> str | None:
    raw_locale = payload.get("locale")
    if not isinstance(raw_locale, str):
        return None
    return _normalize_locale(raw_locale)


def _preferred_locale_for_user(user_id: int, telegram_language_code: str | None) -> str:
    cached = _user_locales.get(user_id)
    if cached is not None:
        return cached
    return _normalize_locale(telegram_language_code)


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

    async def publish_user_registration(
        self,
        *,
        request_id: str,
        token: str,
        telegram_user_id: int,
    ) -> None:
        if self._channel is None:
            raise RuntimeError

        payload = {
            "request_id": request_id,
            "type": REQUEST_TYPE_TELEGRAM_LINK,
            "token": token,
            "telegram_user_id": telegram_user_id,
        }
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        message = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type=RABBIT_CONTRACT.content_type,
            content_encoding=RABBIT_CONTRACT.content_encoding,
        )

        await self._channel.default_exchange.publish(
            message,
            routing_key=self._queue_name,
        )
        logger.info("Published user registration to RabbitMQ: %s", payload)

    async def publish_event_rsvp(
        self,
        *,
        request_id: str,
        event_id: int,
        decision: str,
        telegram_user_id: int,
    ) -> None:
        if self._channel is None:
            raise RuntimeError

        payload = {
            "request_id": request_id,
            "type": REQUEST_TYPE_EVENT_RSVP,
            "event_id": event_id,
            "decision": decision,
            "telegram_user_id": telegram_user_id,
        }
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        message = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type=RABBIT_CONTRACT.content_type,
            content_encoding=RABBIT_CONTRACT.content_encoding,
        )

        await self._channel.default_exchange.publish(
            message,
            routing_key=self._queue_name,
        )
        logger.info("Published event RSVP to RabbitMQ: %s", payload)

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
        self._pending: dict[str, tuple[int, int, str]] = {}
        self._pending_tasks: dict[str, asyncio.Task[None]] = {}
        self._pending_lock = asyncio.Lock()
        self._pending_timeout_seconds = int(os.getenv("PENDING_TIMEOUT_SECONDS", "20"))

    async def track_request(
        self,
        *,
        request_id: str,
        chat_id: int,
        message_id: int,
        locale: str = "ru",
    ) -> None:
        async with self._pending_lock:
            self._pending[request_id] = (chat_id, message_id, locale)
            existing = self._pending_tasks.pop(request_id, None)
            if existing is not None:
                existing.cancel()
            self._pending_tasks[request_id] = asyncio.create_task(self._timeout_request(request_id))

    async def _timeout_request(self, request_id: str) -> None:
        await asyncio.sleep(self._pending_timeout_seconds)
        async with self._pending_lock:
            pending = self._pending.pop(request_id, None)
            self._pending_tasks.pop(request_id, None)

        if pending is None:
            return
        chat_id, message_id, locale = pending
        texts = _messages_for_locale(locale)
        try:
            await self._bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=texts.timeout_waiting_backend,
            )
        except Exception:
            logger.exception("Failed to send timeout result for request_id=%s", request_id)

    async def _pop_pending(self, request_id: str) -> tuple[int, int, str] | None:
        async with self._pending_lock:
            pending = self._pending.pop(request_id, None)
            task = self._pending_tasks.pop(request_id, None)
            if task is not None:
                task.cancel()
            return pending

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
                text = _normalize_notification_text(str(payload["text"]))
                buttons = payload.get("buttons")
                request_id = payload.get("request_id")
                payload_locale = _extract_payload_locale(payload)
            except Exception:
                logger.exception("Failed to parse incoming Telegram notification")
                return

            if payload_locale is not None:
                _user_locales[chat_id] = payload_locale
            button_texts = _messages_for_locale(payload_locale) if payload_locale is not None else _messages_for_chat(chat_id)

            reply_markup = None
            if isinstance(buttons, list):
                inline_buttons: list[InlineKeyboardButton] = []
                for btn in buttons:
                    if not isinstance(btn, dict):
                        continue
                    if btn.get("type") != TELEGRAM_BUTTON_TYPE_EVENT_RSVP:
                        continue
                    event_id = btn.get("event_id")
                    action = btn.get("action")
                    if event_id is None or action is None:
                        continue
                    if action == "ACCEPT":
                        label = button_texts.button_label_accept
                    elif action == "DECLINE":
                        label = button_texts.button_label_decline
                    else:
                        label = str(action)
                    callback_data = json.dumps(
                        {"type": TELEGRAM_BUTTON_TYPE_EVENT_RSVP, "event_id": event_id, "action": action},
                        ensure_ascii=False,
                    )
                    inline_buttons.append(InlineKeyboardButton(text=label, callback_data=callback_data))
                if inline_buttons:
                    reply_markup = InlineKeyboardMarkup(
                        inline_keyboard=[inline_buttons],
                    )

            try:
                # If this message is a result for a pending request, try to edit the "processing" message.
                if request_id and reply_markup is None:
                    pending = await self._pop_pending(str(request_id))
                    if pending is not None:
                        pending_chat_id, pending_message_id, _pending_locale = pending
                        try:
                            await self._bot.edit_message_text(
                                chat_id=pending_chat_id,
                                message_id=pending_message_id,
                                text=text,
                            )
                        except Exception:
                            logger.exception(
                                "Failed to edit pending message for request_id=%s; falling back to send_message",
                                request_id,
                            )
                        else:
                            return

                if reply_markup is not None:
                    await self._bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        reply_markup=reply_markup,
                    )
                else:
                    await self._bot.send_message(
                        chat_id=chat_id,
                        text=text,
                    )
            except Exception:
                logger.exception("Failed to send Telegram notification to chat_id=%s", chat_id)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            logger.info("Closed RabbitMQ notification consumer connection")


rabbitmq_publisher: RabbitMQPublisher | None = None
rabbitmq_rsvp_publisher: RabbitMQPublisher | None = None
rabbitmq_notification_consumer: RabbitMQNotificationConsumer | None = None


@router.message(CommandStart())
async def handle_start(message: Message) -> None:
    user = message.from_user
    if user is None:
        logger.warning("Received /start from message without from_user")
        return

    user_id = user.id
    locale = _preferred_locale_for_user(user_id, getattr(user, "language_code", None))
    _user_locales[user_id] = locale
    texts = _messages_for_locale(locale)

    text = message.text or ""
    parts = text.split(maxsplit=1)
    token = parts[1].strip() if len(parts) > 1 else None

    if not token:
        await message.answer(
            texts.link_no_token_instruction,
        )
        logger.warning("Received /start without token from telegram_user_id=%s", user_id)
        return

    processing = await message.answer(texts.link_processing)

    if rabbitmq_publisher is None or rabbitmq_notification_consumer is None:
        logger.error("RabbitMQ is not initialized; cannot send user registration request")
        try:
            await processing.edit_text(texts.service_unavailable)
        except Exception:
            logger.exception("Failed to edit processing message")
        return

    request_id = uuid.uuid4().hex
    await rabbitmq_notification_consumer.track_request(
        request_id=request_id,
        chat_id=processing.chat.id,
        message_id=processing.message_id,
        locale=locale,
    )
    await rabbitmq_publisher.publish_user_registration(
        request_id=request_id,
        token=token,
        telegram_user_id=user_id,
    )


@router.message()
async def handle_message(message: Message) -> None:
    await message.delete()


@router.callback_query(F.data)
async def handle_callback(callback: CallbackQuery) -> None:
    if callback.data is None:
        return

    try:
        payload = json.loads(callback.data)
    except Exception:
        logger.exception("Failed to parse callback data")
        return

    if payload.get("type") != TELEGRAM_BUTTON_TYPE_EVENT_RSVP:
        return

    event_id = payload.get("event_id")
    decision = payload.get("action")
    user = callback.from_user
    if user is None or event_id is None or decision is None:
        return
    locale = _preferred_locale_for_user(user.id, getattr(user, "language_code", None))
    _user_locales[user.id] = locale
    texts = _messages_for_locale(locale)

    try:
        await callback.answer(texts.rsvp_callback_ack)
    except Exception:
        logger.exception("Failed to answer callback query")

    if callback.message is None:
        return

    processing = await callback.message.answer(texts.rsvp_processing)

    if rabbitmq_rsvp_publisher is None or rabbitmq_notification_consumer is None:
        logger.error("RabbitMQ is not initialized; cannot send RSVP request")
        try:
            await processing.edit_text(texts.service_unavailable)
        except Exception:
            logger.exception("Failed to edit processing message")
        return

    request_id = uuid.uuid4().hex
    await rabbitmq_notification_consumer.track_request(
        request_id=request_id,
        chat_id=processing.chat.id,
        message_id=processing.message_id,
        locale=locale,
    )
    await rabbitmq_rsvp_publisher.publish_event_rsvp(
        request_id=request_id,
        event_id=int(event_id),
        decision=str(decision),
        telegram_user_id=user.id,
    )


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
    rabbitmq_event_rsvp_queue = os.getenv(
        "RABBITMQ_EVENT_RSVP_QUEUE",
        "telegram_event_rsvp",
    )

    global rabbitmq_publisher
    rabbitmq_publisher = RabbitMQPublisher(
        url=rabbitmq_url,
        queue_name=rabbitmq_queue,
    )
    await rabbitmq_publisher.connect()

    global rabbitmq_rsvp_publisher
    rabbitmq_rsvp_publisher = RabbitMQPublisher(
        url=rabbitmq_url,
        queue_name=rabbitmq_event_rsvp_queue,
    )
    await rabbitmq_rsvp_publisher.connect()

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
        if rabbitmq_rsvp_publisher is not None:
            await rabbitmq_rsvp_publisher.close()
        if rabbitmq_notification_consumer is not None:
            await rabbitmq_notification_consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
