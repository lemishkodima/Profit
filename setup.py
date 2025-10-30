import contextlib
import asyncio
import logging
import csv

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
TELEGRAM_ADMINISTRATOR_IDS: list[int] = [402152266, 430692329]

# Налаштування Google Sheets
GOOGLE_SHEETS_SPREADSHEET_ID: str = "1eam-jcAWOC54U6hoZmtmBcG4v7rzy--NtTHoZdDxLHA"
# Стовпці: A — User ID, B — First Name
GOOGLE_SHEETS_USER_DATA_RANGE: str = "two!A:B"

# Ініціалізація об'єктів Bot і Dispatcher
telegram_bot: Bot = Bot(token=BOT_TOKEN)
bot_dispatcher: Dispatcher = Dispatcher()


# ─────────────────────────────────────────────────────────────────────────────
# FSM
# ─────────────────────────────────────────────────────────────────────────────

class BroadcastState(StatesGroup):
    """
    FSM-стан для введення контенту розсилки.
    Тут ми дозволяємо:
    - просто текст (message.text)
    - або фото з підписом (message.photo + message.caption)
    """
    waiting_for_broadcast_content = State()


# ─────────────────────────────────────────────────────────────────────────────
# ХЕНДЛЕР: Автопідпис на канал
# ─────────────────────────────────────────────────────────────────────────────

async def handle_channel_join_request(request: ChatJoinRequest, bot: Bot) -> None:
    """
    Автопідтвердження запиту на приєднання до каналу
    та відправка користувачу повідомлення з кнопкою Start.
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
# ХЕНДЛЕР: /broadcast (тільки для адмінів)
# ─────────────────────────────────────────────────────────────────────────────

@bot_dispatcher.message(Command("broadcast"))
async def command_broadcast(message: types.Message, state: FSMContext) -> None:
    """
    Обробник команди /broadcast.
    Доступно лише адміністраторам.
    Переводить у стан очікування контенту для розсилки.
    """
    user_id: int = message.from_user.id

    if user_id not in TELEGRAM_ADMINISTRATOR_IDS:
        await message.answer("У вас нет разрешения использовать эту команду.")
        return

    # Переходимо в стан, де чекаємо або текст, або фото з підписом
    await state.set_state(BroadcastState.waiting_for_broadcast_content)
    await message.answer(
        "Введите текст для рассылки \n"
        "ИЛИ пришлите фото с подписью (в подписи можно использовать {{firstName}})."
    )


# ─────────────────────────────────────────────────────────────────────────────
# ХЕНДЛЕР: отримали контент для розсилки
# ─────────────────────────────────────────────────────────────────────────────

@bot_dispatcher.message(BroadcastState.waiting_for_broadcast_content)
async def process_broadcast_content(message: types.Message, state: FSMContext) -> None:
    """
    Обробник, який приймає або:
    - чистий текст
    - або фото з підписом
    і робить розсилку всім користувачам з таблиці Google.
    Підтримується плейсхолдер {{firstName}}.
    """
    # Знімаємо стан — далі вже розсилка
    await state.clear()

    # 1. Визначаємо, що саме нам прислав адмін
    # --------------------------------------------------
    is_photo_broadcast: bool = False             # чи розсилка з фото
    photo_file_id: str | None = None            # file_id фото, якщо воно є
    broadcast_template: str = ""                 # текст, у якому будемо замінювати {{firstName}}

    # Якщо адмін надіслав фото з підписом
    if message.photo:
        is_photo_broadcast = True
        # беремо найбільше фото
        photo_file_id = message.photo[-1].file_id
        broadcast_template = message.caption or ""
    else:
        # інакше це проста текстова розсилка
        broadcast_template = message.text or ""

    # 2. Підключення до Google Sheets і читання списку користувачів
    # --------------------------------------------------
    google_credentials: Credentials = Credentials.from_service_account_file("maxim.json")
    google_sheets_service = build("sheets", "v4", credentials=google_credentials).spreadsheets()
    sheet_data: dict = google_sheets_service.values().get(
        spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
        range=GOOGLE_SHEETS_USER_DATA_RANGE,
    ).execute()

    # Список рядків з таблиці
    # Кожен рядок: [user_id, first_name?]
    users_data_list: list[list[str]] = sheet_data.get("values", [])

    # Сюди складатимемо результати відправки
    send_results_list: list[dict] = []

    # 3. Проходимо по кожному користувачу й відправляємо
    # --------------------------------------------------
    for user_row_index, user_row in enumerate(users_data_list, start=2):
        # user_row_index — це номер рядка в Google Sheets (починаємо з 2, бо 1 — це заголовок, якщо він є)
        if not user_row:
            # Порожній рядок — пропускаємо
            continue

        telegram_user_id_raw = user_row[0]

        # Ім'я з таблиці (може бути порожнім)
        first_name: str | None = None
        if len(user_row) > 1 and user_row[1].strip():
            first_name = user_row[1].strip()

        # Конвертуємо user_id у int
        try:
            telegram_user_id: int = int(telegram_user_id_raw)
        except ValueError:
            logging.error(f"Невірний user_id у рядку {user_row_index}: {telegram_user_id_raw}")
            send_results_list.append(
                {
                    "Index": user_row_index,
                    "User ID": telegram_user_id_raw,
                    "Message ID": None,
                    "Status": "False (invalid user id)",
                }
            )
            continue

        # Якщо в таблиці не вказано first name — пробуємо last_name через get_chat
        last_name: str | None = None
        if not first_name:
            try:
                chat_info = await telegram_bot.get_chat(chat_id=telegram_user_id)
                last_name = chat_info.last_name
            except Exception:
                # не критично, просто не знаємо ім'я
                pass

        # Ім'я для підстановки
        display_name: str = first_name or last_name or ""

        # Персоналізований текст
        personalized_message_text: str = broadcast_template.replace("{{firstName}}", display_name)

        # 4. Шлемо користувачу
        # --------------------------------------------------
        try:
            if is_photo_broadcast and photo_file_id:
                # Відправка фото з підписом
                # У підпису є ліміт (~1024 символи), якщо буде довше — телеграм відріже
                sent_message = await telegram_bot.send_photo(
                    chat_id=telegram_user_id,
                    photo=photo_file_id,
                    caption=personalized_message_text,
                    parse_mode="HTML",
                )
            else:
                # Відправка тексту
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
            # Якщо не вдалося відправити
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

    # 5. Запис результатів розсилки у CSV-файл
    # --------------------------------------------------
    csv_file_full_path: str = "broadcast_results.csv"
    with open(csv_file_full_path, "w", newline="", encoding="utf-8") as csv_file:
        csv_fieldnames: list[str] = ["Index", "User ID", "Message ID", "Status"]
        csv_writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
        csv_writer.writeheader()
        for result_record in send_results_list:
            csv_writer.writerow(result_record)

    # 6. Відправка CSV адміністратору (тому, хто запустив розсилку)
    # --------------------------------------------------
    result_document: FSInputFile = FSInputFile(csv_file_full_path)
    await message.answer_document(
        document=result_document,
        caption="Рассылка завершена. Результаты сохранены в broadcast_results.csv",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ХЕНДЛЕР: текст "start" від користувача
# ─────────────────────────────────────────────────────────────────────────────

@bot_dispatcher.message(F.text.lower() == "start")
async def send_channel_invitation(message: types.Message) -> None:
    """
    Обробник тексту 'start' — відправляє запрошення до каналу
    та додає користувача в Google Sheets.
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

    # Підготовка та відправка даних у Google Sheets
    user_id: int = message.from_user.id
    user_first_name: str = message.from_user.first_name or ""

    append_body: dict = {"values": [[user_id, user_first_name]]}

    google_credentials: Credentials = Credentials.from_service_account_file("maxim.json")
    google_sheets_service = build("sheets", "v4", credentials=google_credentials).spreadsheets()

    # Додаємо рядок у таблицю
    google_sheets_service.values().append(
        spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
        range=GOOGLE_SHEETS_USER_DATA_RANGE,
        valueInputOption="USER_ENTERED",
        body=append_body,
    ).execute()

    # Відправляємо користувачу кнопку
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
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - [%(levelname)s] - %(name)s - "
               "(%(filename)s:%(lineno)d) - %(message)s",
    )

    # Реєструємо обробник для join request у канал
    bot_dispatcher.chat_join_request.register(
        handle_channel_join_request,
        F.chat.id == TELEGRAM_CHANNEL_ID,
    )

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
