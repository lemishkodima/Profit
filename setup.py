import contextlib
import asyncio
from aiogram.types import (
    CallbackQuery, ChatJoinRequest, InlineKeyboardButton, 
    InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, FSInputFile
)
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters.command import Command
import logging
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
import csv



BOT_TOKEN = '6524445610:AAFyCvTHI9qpKajyXzNVTNP3GCPM9jWVvZ0' 
CHANNEL_ID =  -1001517003300
ADMIN_ID = 402152266

ADMIN_IDS = [402152266, 430692329]

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


class Form(StatesGroup):
    Broadcast = State()  # Состояние для рассылки

async def approve_request(chat_join: ChatJoinRequest, bot: Bot):
    start_msg = "Ваша заявка одобрена, для получения ссылки нажмите Start⬇️"
    start_button = KeyboardButton(text='Start')
    markup = ReplyKeyboardMarkup(keyboard=[[start_button]], resize_keyboard=True, one_time_keyboard=True)
    await bot.send_message(chat_id=chat_join.from_user.id, text=start_msg, reply_markup=markup)
    await chat_join.approve()


@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message, state: FSMContext):
    """Команда /broadcast — лише для адмінів. Перехід у стан для вводу тексту розсилки."""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("У вас нет разрешения использовать эту команду.")
        return
    await state.set_state(Form.Broadcast)
    await message.answer("Введите текст для рассылки.")

@dp.message(Form.Broadcast)
async def process_broadcast(message: types.Message, state: FSMContext):
    """Обробляє введений текст і виконує розсилку всім з бази (Google Sheets)."""
    # Зчитування тексту розсилки
    broadcast_text = message.text
    # Вихід зі стану розсилки
    await state.clear()

    # Зчитування всіх користувачів з Google Sheets
    creds = Credentials.from_service_account_file("maxim.json")
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId="1eam-jcAWOC54U6hoZmtmBcG4v7rzy--NtTHoZdDxLHA",
        range="two!A:C"
    ).execute()
    users = result.get('values', [])

    # Список з результатами для CSV
    results = []

    for index, user in enumerate(users, start=2):
        try:
            user_id = user[0]
            # Надсилаємо повідомлення кожному
            sent_msg = await bot.send_message(chat_id=user_id, text=broadcast_text, disable_web_page_preview=True, parse_mode='HTML')

            # Зберігаємо user_id, message_id та статус успішної відправки
            results.append({
                'Index': index,
                'User ID': user_id,
                'Message ID': sent_msg.message_id,
                'Status': 'True'
            })
        except Exception as e:
            # Якщо виникає помилка, запишемо статус False та message_id = None
            results.append({
                'Index': index,
                'User ID': user[0],
                'Message ID': None,
                'Status': 'False'
            })
            logging.error(f"Не удалось отправить сообщение пользователю {user[0]}: {e}")

    # Збереження результатів у файл
    file_path = 'broadcast_results.csv'
    await save_results_to_csv(results, file_path)
    
    # Відправка файлу з результатами адміністратору (або тому, хто запустив /broadcast)
    document = FSInputFile(file_path)
    await message.answer_document(
        document,
        caption="Рассылка завершена. Результаты сохранены в broadcast_results.csv"
    )



@dp.message(F.text.lower() == "start")
async def send_channel_link(message: types.Message):
        msg = "Ваша заявка одобрена!\n\nВступить в канал: https://t.me/+4ia_jp8_1kAwNWFi"
        button = InlineKeyboardButton(text='ВСТУПИТЬ', url='https://t.me/+4ia_jp8_1kAwNWFi')
        markup = InlineKeyboardMarkup(inline_keyboard=[[button]])

        user_data = [message.from_user.id, message.from_user.username, message.from_user.first_name]
        append_data_to_sheet(user_data, "1eam-jcAWOC54U6hoZmtmBcG4v7rzy--NtTHoZdDxLHA", "A:C")

        await message.answer(text=msg, reply_markup=markup, disable_web_page_preview=True)
    
    
async def save_results_to_csv(results, file_path):
    """Зберігає результати розсилки у CSV-файл."""
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Index', 'User ID', 'Message ID', 'Status']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(result)

def append_data_to_sheet(user_data, spreadsheet_id, range_name):
    """Добавляет данные пользователя в Google таблицу."""
    creds = Credentials.from_service_account_file("maxim.json")
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    request = sheet.values().append(spreadsheetId=spreadsheet_id, 
                                    range=range_name, 
                                    valueInputOption="USER_ENTERED", 
                                    body={"values": [user_data]})
    response = request.execute()
    return response

async def start():
    logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s - [%(levelname)s] - %(name)s -"
                           "(%(filename)s.%(funcName)s(%(lineno)d) - %(message)s"
                    )
    dp.chat_join_request.register (approve_request, F.chat.id ==CHANNEL_ID)

    try:
     await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as ex:
     logging.error( exc_info=True)
    finally:
     await bot.session.close()


if __name__ == '__main__':
    with contextlib.suppress(KeyboardInterrupt, SystemExit):
        asyncio.run(start())

  

