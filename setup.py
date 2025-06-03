import contextlib
import asyncio
import logging
import csv

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters.command import Command
from aiogram.types import (
    CallbackQuery,
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


# Токен бота, ID каналу та список адміністраторів
BOT_TOKEN = '6524445610:AAFyCvTHI9qpKajyXzNVTNP3GCPM9jWVvZ0'
TELEGRAM_CHANNEL_ID = -1001517003300
TELEGRAM_ADMINISTRATOR_IDS = [402152266, 430692329]

# Налаштування Google Sheets
GOOGLE_SHEETS_SPREADSHEET_ID = "1eam-jcAWOC54U6hoZmtmBcG4v7rzy--NtTHoZdDxLHA"
GOOGLE_SHEETS_USER_DATA_RANGE = "two!A:B"  # стовпці A: User ID, B: First Name


# Ініціалізація об'єктів Bot і Dispatcher
telegram_bot = Bot(token=BOT_TOKEN)
bot_dispatcher = Dispatcher()


class BroadcastState(StatesGroup):
    """FSM-стан для введення тексту розсилки."""
    waiting_for_broadcast_text = State()


async def handle_channel_join_request(request: ChatJoinRequest, bot: Bot):
    """
    Автопідтвердження запиту на приєднання до каналу
    та відправка користувачу повідомлення з кнопкою Start.
    """
    approval_text = "Ваша заявка одобрена, для получения ссылки нажмите Start⬇️"
    start_button = KeyboardButton(text='Start')
    reply_keyboard = ReplyKeyboardMarkup(
        keyboard=[[start_button]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await bot.send_message(
        chat_id=request.from_user.id,
        text=approval_text,
        reply_markup=reply_keyboard
    )
    await request.approve()


@bot_dispatcher.message(Command("broadcast"))
async def command_broadcast(message: types.Message, state: FSMContext):
    """
    Обробник команди /broadcast — доступно лише адміністраторам.
    Переходить у стан очікування тексту розсилки.
    """
    user_id = message.from_user.id
    if user_id not in TELEGRAM_ADMINISTRATOR_IDS:
        await message.answer("У вас нет разрешения использовать эту команду.")
        return

    await state.set_state(BroadcastState.waiting_for_broadcast_text)
    await message.answer("Введите текст для рассылки.")


@bot_dispatcher.message(BroadcastState.waiting_for_broadcast_text)
async def process_broadcast_text(message: types.Message, state: FSMContext):
    """
    Обробляє текст розсилки, персоналізує його за {{firstName}}
    і надсилає кожному користувачу з Google Sheets.
    """
    broadcast_template = message.text
    await state.clear()

    # Підключення до Google Sheets і читання даних
    google_credentials = Credentials.from_service_account_file("maxim.json")
    google_sheets_service = build('sheets', 'v4', credentials=google_credentials).spreadsheets()
    sheet_data = google_sheets_service.values().get(
        spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
        range=GOOGLE_SHEETS_USER_DATA_RANGE
    ).execute()
    users_data_list = sheet_data.get('values', [])

    send_results_list = []

    for user_row_index, user_row in enumerate(users_data_list, start=2):
        telegram_user_id = user_row[0]
        first_name = user_row[1] if len(user_row) > 1 and user_row[1].strip() else None

        # Якщо в таблиці не вказано first name — пробуємо last_name через get_chat
        last_name = None
        if not first_name:
            try:
                chat_info = await telegram_bot.get_chat(chat_id=int(telegram_user_id))
                last_name = chat_info.last_name
            except Exception:
                pass

        # Ім'я для підстановки
        display_name = first_name or last_name or ""

        # Персоналізований текст
        personalized_message_text = broadcast_template.replace("{{firstName}}", display_name)

        try:
            sent_message = await telegram_bot.send_message(
                chat_id=int(telegram_user_id),
                text=personalized_message_text,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            send_results_list.append({
                'Index':      user_row_index,
                'User ID':    telegram_user_id,
                'Message ID': sent_message.message_id,
                'Status':     'True'
            })
        except Exception as send_error:
            logging.error(f"Не вдалося відправити повідомлення користувачу {telegram_user_id}: {send_error}")
            send_results_list.append({
                'Index':      user_row_index,
                'User ID':    telegram_user_id,
                'Message ID': None,
                'Status':     'False'
            })

    # Запис результатів у CSV-файл
    csv_file_full_path = 'broadcast_results.csv'
    with open(csv_file_full_path, 'w', newline='', encoding='utf-8') as csv_file:
        csv_fieldnames = ['Index', 'User ID', 'Message ID', 'Status']
        csv_writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
        csv_writer.writeheader()
        for result_record in send_results_list:
            csv_writer.writerow(result_record)

    # Відправка CSV адміністратору
    result_document = FSInputFile(csv_file_full_path)
    await message.answer_document(
        document=result_document,
        caption="Рассылка завершена. Результаты сохранены в broadcast_results.csv"
    )


@bot_dispatcher.message(F.text.lower() == "start")
async def send_channel_invitation(message: types.Message):
    """
    Обробник тексту 'start' — відправляє запрошення до каналу
    та додає користувача в Google Sheets.
    """
    invitation_text = (
        "Ваша заявка одобрена!\n\n"
        "Вступить в канал: https://t.me/+4ia_jp8_1kAwNWFi"
    )
    invitation_button = InlineKeyboardButton(
        text='ВСТУПИТЬ',
        url='https://t.me/+4ia_jp8_1kAwNWFi'
    )
    invitation_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[invitation_button]]
    )

    # Підготовка та відправка даних у Google Sheets
    user_id = message.from_user.id
    user_first_name = message.from_user.first_name or ""
    append_body = {"values": [[user_id, user_first_name]]}

    google_credentials = Credentials.from_service_account_file("maxim.json")
    google_sheets_service = build('sheets', 'v4', credentials=google_credentials).spreadsheets()
    google_sheets_service.values().append(
        spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
        range=GOOGLE_SHEETS_USER_DATA_RANGE,
        valueInputOption="USER_ENTERED",
        body=append_body
    ).execute()

    await message.answer(
        text=invitation_text,
        reply_markup=invitation_keyboard,
        disable_web_page_preview=True
    )


async def run_bot():
    """
    Головна функція — реєстрація обробників та запуск polling.
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - [%(levelname)s] - %(name)s - "
               "(%(filename)s:%(lineno)d) - %(message)s"
    )

    bot_dispatcher.chat_join_request.register(
        handle_channel_join_request,
        F.chat.id == TELEGRAM_CHANNEL_ID
    )

    try:
        await bot_dispatcher.start_polling(
            telegram_bot,
            allowed_updates=bot_dispatcher.resolve_used_update_types()
        )
    except Exception:
        logging.error("Під час роботи бота сталася помилка", exc_info=True)
    finally:
        await telegram_bot.session.close()


if __name__ == '__main__':
    with contextlib.suppress(KeyboardInterrupt, SystemExit):
        asyncio.run(run_bot())
