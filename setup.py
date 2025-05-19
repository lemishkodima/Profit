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


# Токен вашого бота та ідентифікатор каналу й адміністраторів
BOT_TOKEN = '6524445610:AAFyCvTHI9qpKajyXzNVTNP3GCPM9jWVvZ0'
CHANNEL_ID = -1001517003300
ADMINISTRATOR_IDS = [402152266, 430692329]

# Ідентифікатори Google Sheets
SPREADSHEET_ID = "1eam-jcAWOC54U6hoZmtmBcG4v7rzy--NtTHoZdDxLHA"
USER_DATA_RANGE = "two!A:B"  # стовпці A: User ID, B: First Name


# Ініціалізація бота та диспетчера
telegram_bot = Bot(token=BOT_TOKEN)
dispatcher = Dispatcher()


class BroadcastForm(StatesGroup):
    """FSM для введення тексту розсилки."""
    waiting_for_message_text = State()


async def handle_chat_join_request(request: ChatJoinRequest, bot: Bot):
    """Автоматичне підтвердження заявки в канал та відправка кнопки Start."""
    welcome_text = "Ваша заявка одобрена, для получения ссылки нажмите Start⬇️"
    start_button = KeyboardButton(text='Start')
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[start_button]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await bot.send_message(
        chat_id=request.from_user.id,
        text=welcome_text,
        reply_markup=keyboard
    )
    await request.approve()


@dispatcher.message(Command("broadcast"))
async def command_broadcast(message: types.Message, state: FSMContext):
    """Команда /broadcast — лише для адміністраторів."""
    user_id = message.from_user.id
    if user_id not in ADMINISTRATOR_IDS:
        await message.answer("У вас нет разрешения использовать эту команду.")
        return

    await state.set_state(BroadcastForm.waiting_for_message_text)
    await message.answer("Введите текст для рассылки.")


@dispatcher.message(BroadcastForm.waiting_for_message_text)
async def process_broadcast_message(message: types.Message, state: FSMContext):
    """Обробляє текст розсилки та надсилає кожному користувачу з таблиці."""
    broadcast_template = message.text
    await state.clear()

    # Підключення до Google Sheets
    credentials = Credentials.from_service_account_file("maxim.json")
    sheets_service = build('sheets', 'v4', credentials=credentials).spreadsheets()
    sheet_values = sheets_service.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=USER_DATA_RANGE
    ).execute()
    users_data = sheet_values.get('values', [])

    send_results = []

    for row_index, row in enumerate(users_data, start=2):
        telegram_id = row[0]
        first_name = row[1] if len(row) > 1 and row[1].strip() else None

        # Якщо в таблиці немає імені — пробуємо отримати last_name через метод get_chat
        last_name = None
        if not first_name:
            try:
                chat = await telegram_bot.get_chat(chat_id=int(telegram_id))
                last_name = chat.last_name
            except Exception:
                pass

        # Формуємо ім’я для вставки
        display_name = first_name or last_name or ""

        # Персоналізація тексту
        personalized_text = broadcast_template.replace("{{firstName}}", display_name)

        try:
            sent_message = await telegram_bot.send_message(
                chat_id=int(telegram_id),
                text=personalized_text,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            send_results.append({
                'Index':      row_index,
                'User ID':    telegram_id,
                'Message ID': sent_message.message_id,
                'Status':     'True'
            })
        except Exception as error:
            logging.error(f"Не удалось отправить сообщение {telegram_id}: {error}")
            send_results.append({
                'Index':      row_index,
                'User ID':    telegram_id,
                'Message ID': None,
                'Status':     'False'
            })

    # Зберігаємо результати у CSV-файл
    csv_file_path = 'broadcast_results.csv'
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        fieldnames = ['Index', 'User ID', 'Message ID', 'Status']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for record in send_results:
            writer.writerow(record)

    # Відправляємо файл адміністратору
    document = FSInputFile(csv_file_path)
    await message.answer_document(
        document=document,
        caption="Рассылка завершена. Результаты сохранены в broadcast_results.csv"
    )


@dispatcher.message(F.text.lower() == "start")
async def send_channel_invite(message: types.Message):
    """Відправляє користувачу посилання на канал та зберігає його у таблицю."""
    invite_text = (
        "Ваша заявка одобрена!\n\n"
        "Вступить в канал: https://t.me/+4ia_jp8_1kAwNWFi"
    )
    invite_button = InlineKeyboardButton(
        text='ВСТУПИТЬ',
        url='https://t.me/+4ia_jp8_1kAwNWFi'
    )
    inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[[invite_button]])

    # Записуємо лише ID та ім'я користувача в таблицю
    user_id = message.from_user.id
    user_first_name = message.from_user.first_name or ""
    values_body = {"values": [[user_id, user_first_name]]}

    credentials = Credentials.from_service_account_file("maxim.json")
    sheets_service = build('sheets', 'v4', credentials=credentials).spreadsheets()
    sheets_service.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=USER_DATA_RANGE,
        valueInputOption="USER_ENTERED",
        body=values_body
    ).execute()

    await message.answer(
        text=invite_text,
        reply_markup=inline_keyboard,
        disable_web_page_preview=True
    )


async def main():
    """Запуск бота з обробкою заявок у канал."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - [%(levelname)s] - %(name)s - "
               "(%(filename)s:%(lineno)d) - %(message)s"
    )
    dispatcher.chat_join_request.register(
        handle_chat_join_request,
        F.chat.id == CHANNEL_ID
    )

    try:
        await dispatcher.start_polling(
            bot=telegram_bot,
            allowed_updates=dispatcher.resolve_used_update_types()
        )
    except Exception:
        logging.error("Під час роботи бота сталася помилка", exc_info=True)
    finally:
        await telegram_bot.session.close()


if __name__ == '__main__':
    with contextlib.suppress(KeyboardInterrupt, SystemExit):
        asyncio.run(main())
