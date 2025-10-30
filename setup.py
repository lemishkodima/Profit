import contextlib
import asyncio
import logging
import csv
from typing import List, Optional, Dict, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters.command import Command
from aiogram.types import (
    ChatJoinRequest,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    FSInputFile,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# ─────────────────────────────────────────────────────────────────────────────
# НАЛАШТУВАННЯ
# ─────────────────────────────────────────────────────────────────────────────

# Токен бота, ID каналу та список адміністраторів
BOT_TOKEN: str = "6524445610:AAFyCvTHI9qpKajyXzNVTNP3GCPM9jWVvZ0"
TELEGRAM_CHANNEL_ID: int = -1001517003300
TELEGRAM_ADMINISTRATOR_IDS: List[int] = [402152266, 430692329]

# Google Sheets
GOOGLE_SHEETS_SPREADSHEET_ID: str = "1eam-jcAWOC54U6hoZmtmBcG4v7rzy--NtTHoZdDxLHA"
GOOGLE_SHEETS_USER_DATA_RANGE: str = "two!A:B"  # A: user_id, B: first_name

# Шлях до сервісного акаунта
GOOGLE_SERVICE_FILE: str = "maxim.json"

# Ініціалізація бота і диспетчера
telegram_bot: Bot = Bot(token=BOT_TOKEN)
bot_dispatcher: Dispatcher = Dispatcher()


# ─────────────────────────────────────────────────────────────────────────────
# FSM
# ─────────────────────────────────────────────────────────────────────────────

class BroadcastState(StatesGroup):
    """
    Стан, коли ми чекаємо контент для розсилки:
    - або текст
    - або фото з підписом
    """
    waiting_for_broadcast_content = State()


# ─────────────────────────────────────────────────────────────────────────────
# ХЕНДЛЕР: Автопідтвердження запиту на вступ у канал
# ─────────────────────────────────────────────────────────────────────────────

async def handle_channel_join_request(request: ChatJoinRequest, bot: Bot) -> None:
    """
    Після approve запросу на вступ у канал — шлемо юзеру кнопку Start.
    """
    approval_text: str = "Ваша заявка одобрена, для получения ссылки нажмите Start⬇️"

    start_button: KeyboardButton = KeyboardButton(text="Start")
    reply_keyboard: ReplyKeyboardMarkup = ReplyKeyboardMarkup(
        keyboard=[[start_button]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

    await bot.send_message(
        chat_id=request.from_user.id,
        text=approval_text,
        reply_markup=reply_keyboard,
    )


# ─────────────────────────────────────────────────────────────────────────────
# ХЕНДЛЕР: /broadcast
# ─────────────────────────────────────────────────────────────────────────────

@bot_dispatcher.message(Command("broadcast"))
async def command_broadcast(message: types.Message, state: FSMContext) -> None:
    """
    /broadcast — тільки для адмінів.
    Далі чекаємо повідомлення (текст або фото з підписом).
    """
    user_id: int = message.from_user.id

    if user_id not in TELEGRAM_ADMINISTRATOR_IDS:
        await message.answer("У вас нет разрешения использовать эту команду.")
        return

    await state.set_state(BroadcastState.waiting_for_broadcast_content)
    await message.answer(
        "Введите текст для рассылки ИЛИ пришлите фото с подписью.\n"
        "Можно использовать {{firstName}} для подстановки имени."
    )


# ─────────────────────────────────────────────────────────────────────────────
# ХЕНДЛЕР: Отримуємо контент для розсилки (текст/фото)
# ─────────────────────────────────────────────────────────────────────────────

@bot_dispatcher.message(BroadcastState.waiting_for_broadcast_content)
async def process_broadcast_content(message: types.Message, state: FSMContext) -> None:
    """
    Приймаємо контент від адміна і розсилаємо всім, хто в Google Sheets.
    Підтримка:
    - текст → send_message
    - фото + caption → send_photo
    """
    # Знімаємо state — щоб не ловити будь-що наступне як розсилку
    await state.clear()

    # 1. Визначаємо тип контенту
    is_photo_broadcast: bool = False
    photo_file_id: Optional[str] = None
    broadcast_template: str = ""

    if message.photo:
        # адмін прислав фото
        is_photo_broadcast = True
        photo_file_id = message.photo[-1].file_id  # найякісніше
        broadcast_template = message.caption or ""
    else:
        # адмін прислав текст
        broadcast_template = message.text or ""

    # 2. Підключаємося до Google Sheets
    google_credentials: Credentials = Credentials.from_service_account_file(GOOGLE_SERVICE_FILE)
    google_sheets_service = build("sheets", "v4", credentials=google_credentials).spreadsheets()

    sheet_data: Dict[str, Any] = google_sheets_service.values().get(
        spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
        range=GOOGLE_SHEETS_USER_DATA_RANGE,
    ).execute()

    users_data_list: List[List[str]] = sheet_data.get("values", [])

    send_results_list: List[Dict[str, Any]] = []

    # 3. Ітеруємось по всім юзерам
    for user_row_index, user_row in enumerate(users_data_list, start=2):
        # user_row = ["123456", "Dima"]
        if not user_row:
            continue

        raw_user_id: str = user_row[0]

        # беремо ім'я з таблиці, якщо є
        first_name: Optional[str] = None
        if len(user_row) > 1 and user_row[1].strip():
            first_name = user_row[1].strip()

        # конвертуємо user_id у int
        try:
            telegram_user_id: int = int(raw_user_id)
        except ValueError:
            logging.error(f"Некорректный user_id в строке {user_row_index}: {raw_user_id}")
            send_results_list.append(
                {
                    "Index": user_row_index,
                    "User ID": raw_user_id,
                    "Message ID": None,
                    "Status": "False (invalid user id)",
                }
            )
            continue

        # якщо імені нема — спробуємо дотягнути з Telegram
        last_name: Optional[str] = None
        if not first_name:
            try:
                chat_info = await telegram_bot.get_chat(chat_id=telegram_user_id)
                last_name = chat_info.last_name
            except Exception:
                # не критично
                pass

        display_name: str = first_name or last_name or ""

        # персоналізація
        personalized_message_text: str = broadcast_template.replace("{{firstName}}", display_name)

        # 4. Шлемо
        try:
            if is_photo_broadcast and photo_file_id:
                sent_message = await telegram_bot.send_photo(
                    chat_id=telegram_user_id,
                    photo=photo_file_id,
                    caption=personalized_message_text,
                    parse_mode="HTML",
                )
            else:
                sent_message = await telegram_bot.send_message(
                    chat_id=telegram_user_id,
                    text=personalized_message_text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )

            send_results_list.append(
                {
                    "Index": user_row_index,
                    "User ID": telegram_user_id,
                    "Message ID": sent_message.message_id,
                    "Status": "True",
                }
            )
        except Exception as send_error:
            logging.error(
                f"Не вдалося відправити повідомлення користувачу {telegram_user_id}: {send_error}"
            )
            send_results_list.append(
                {
                    "Index": user_row_index,
                    "User ID": telegram_user_id,
                    "Message ID": None,
                    "Status": f"False ({send_error.__class__.__name__})",
                }
            )

    # 5. Пишемо CSV
    csv_file_full_path: str = "broadcast_results.csv"
    with open(csv_file_full_path, "w", newline="", encoding="utf-8") as csv_file:
        csv_fieldnames: List[str] = ["Index", "User ID", "Message ID", "Status"]
        csv_writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
        csv_writer.writeheader()
        for result_record in send_results_list:
            csv_writer.writerow(result_record)

    # 6. Шлемо звіт адміну
    result_document: FSInputFile = FSInputFile(csv_file_full_path)
    await message.answer_document(
        document=result_document,
        caption="Рассылка завершена. Результаты сохранены в broadcast_results.csv",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ХЕНДЛЕР: "start" від юзера
# ─────────────────────────────────────────────────────────────────────────────

@bot_dispatcher.message(F.text.lower() == "start")
async def send_channel_invitation(message: types.Message) -> None:
    """
    Коли юзер пише "start" — шлемо йому кнопку входу і додаємо в Google Sheets.
    """
    invitation_text: str = (
        "Ваша заявка одобрена!\n\n"
        "Вступить в канал: https://t.me/+y6Vwv40gfJVmMGVi"
    )

    invitation_button: InlineKeyboardButton = InlineKeyboardButton(
        text="ВСТУПИТЬ",
        url="https://t.me/+y6Vwv40gfJVmMGVi",
    )
    invitation_keyboard: InlineKeyboardMarkup = InlineKeyboardMarkup(
        inline_keyboard=[[invitation_button]]
    )

    # додаємо юзера в таблицю
    user_id: int = message.from_user.id
    user_first_name: str = message.from_user.first_name or ""

    append_body: Dict[str, Any] = {"values": [[user_id, user_first_name]]}

    google_credentials: Credentials = Credentials.from_service_account_file(GOOGLE_SERVICE_FILE)
    google_sheets_service = build("sheets", "v4", credentials=google_credentials).spreadsheets()

    google_sheets_service.values().append(
        spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
        range=GOOGLE_SHEETS_USER_DATA_RANGE,
        valueInputOption="USER_ENTERED",
        body=append_body,
    ).execute()

    # шлемо повідомлення
    await message.answer(
        text=invitation_text,
        reply_markup=invitation_keyboard,
        disable_web_page_preview=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# ЗАПУСК БОТА
# ─────────────────────────────────────────────────────────────────────────────

async def run_bot() -> None:
    """
    Головна функція — реєстрація обробників та запуск polling.
    Тут МИ Й ВИДАЛЯЄМО WEBHOOK перед polling.
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - [%(levelname)s] - %(name)s - "
               "(%(filename)s:%(lineno)d) - %(message)s",
    )

    # 1. ОБОВʼЯЗКОВО! Видаляємо webhook, якщо він був налаштований раніше
    # інакше отримаємо:
    # TelegramConflictError: can't use getUpdates method while webhook is active
    await telegram_bot.delete_webhook(drop_pending_updates=True)

    # 2. Реєструємо обробник для join request у канал
    bot_dispatcher.chat_join_request.register(
        handle_channel_join_request,
        F.chat.id == TELEGRAM_CHANNEL_ID,
    )

    # 3. Запускаємо polling
    try:
        await bot_dispatcher.start_polling(
            telegram_bot,
            allowed_updates=bot_dispatcher.resolve_used_update_types(),
        )
    except Exception:
        logging.error("Під час роботи бота сталася помилка", exc_info=True)
    finally:
        await telegram_bot.session.close()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt, SystemExit):
        asyncio.run(run_bot())
