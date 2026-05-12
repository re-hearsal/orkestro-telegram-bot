import os
from dataclasses import dataclass
from typing import Final


def _env(key: str, default: str) -> str:
    value = os.getenv(key)
    return default if value is None or value == "" else value


@dataclass(frozen=True)
class RabbitContractConfig:
    content_type: str
    content_encoding: str
    request_type_telegram_link: str
    request_type_event_rsvp: str
    button_type_event_rsvp: str


@dataclass(frozen=True)
class BotMessagesConfig:
    link_no_token_instruction: str
    link_processing: str
    service_unavailable: str
    timeout_waiting_backend: str
    rsvp_callback_ack: str
    rsvp_processing: str
    button_label_accept: str
    button_label_decline: str


@dataclass(frozen=True)
class I18nBotMessagesConfig:
    ru: BotMessagesConfig
    en: BotMessagesConfig


RABBIT_CONTRACT: Final[RabbitContractConfig] = RabbitContractConfig(
    content_type=_env("RABBIT_CONTENT_TYPE", "application/json"),
    content_encoding=_env("RABBIT_CONTENT_ENCODING", "utf-8"),
    request_type_telegram_link=_env("RABBIT_REQUEST_TYPE_TELEGRAM_LINK", "telegram.link"),
    request_type_event_rsvp=_env("RABBIT_REQUEST_TYPE_EVENT_RSVP", "event.rsvp"),
    button_type_event_rsvp=_env("TELEGRAM_BUTTON_TYPE_EVENT_RSVP", "event_rsvp"),
)

MESSAGES: Final[BotMessagesConfig] = BotMessagesConfig(
    link_no_token_instruction=_env(
        "BOT_MSG_LINK_NO_TOKEN",
        "Чтобы подключить Telegram-уведомления, перейдите к боту по ссылке из личного кабинета приложения.",
    ),
    link_processing=_env("BOT_MSG_LINK_PROCESSING", "⏳ Подключаю Telegram-уведомления…"),
    service_unavailable=_env("BOT_MSG_SERVICE_UNAVAILABLE", "❌ Сервис временно недоступен. Попробуйте позже."),
    timeout_waiting_backend=_env(
        "BOT_MSG_TIMEOUT_WAITING_BACKEND",
        "❌ Не удалось получить ответ от сервера. Попробуйте ещё раз чуть позже.",
    ),
    rsvp_callback_ack=_env("BOT_MSG_RSVP_CALLBACK_ACK", "⏳ Отправляю ответ…"),
    rsvp_processing=_env("BOT_MSG_RSVP_PROCESSING", "⏳ Записываю ваш ответ…"),
    button_label_accept=_env("BOT_LABEL_RSVP_ACCEPT", "Я приду"),
    button_label_decline=_env("BOT_LABEL_RSVP_DECLINE", "Не смогу прийти"),
)

I18N_MESSAGES: Final[I18nBotMessagesConfig] = I18nBotMessagesConfig(
    ru=MESSAGES,
    en=BotMessagesConfig(
        link_no_token_instruction=_env(
            "BOT_MSG_LINK_NO_TOKEN_EN",
            "To connect Telegram notifications, open the bot from the link in your account settings.",
        ),
        link_processing=_env("BOT_MSG_LINK_PROCESSING_EN", "⏳ Connecting Telegram notifications..."),
        service_unavailable=_env(
            "BOT_MSG_SERVICE_UNAVAILABLE_EN", "❌ Service is temporarily unavailable. Please try again later."
        ),
        timeout_waiting_backend=_env(
            "BOT_MSG_TIMEOUT_WAITING_BACKEND_EN",
            "❌ Failed to get a response from the server. Please try again in a moment.",
        ),
        rsvp_callback_ack=_env("BOT_MSG_RSVP_CALLBACK_ACK_EN", "⏳ Sending your response..."),
        rsvp_processing=_env("BOT_MSG_RSVP_PROCESSING_EN", "⏳ Saving your response..."),
        button_label_accept=_env("BOT_LABEL_RSVP_ACCEPT_EN", "I will attend"),
        button_label_decline=_env("BOT_LABEL_RSVP_DECLINE_EN", "I cannot attend"),
    ),
)
