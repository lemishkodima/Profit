import contextlib
import asyncio 
from aiogram.types import ChatJoinRequest, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram import Bot, Dispatcher, F
import logging
import contextlib
import asyncio
from aiogram.types import ChatJoinRequest, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram import Bot, Dispatcher, F
import logging
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


BOT_TOKEN = '6524445610:AAFyCvTHI9qpKajyXzNVTNP3GCPM9jWVvZ0' 
CHANNEL_ID =  -1001517003300
ADMIN_ID = 1889004772
async def approve_request(chat_join: ChatJoinRequest, bot: Bot):
    start_msg = "Ваша заявка одобрена, для получения ссылки нажмите Start⬇️"
    start_button = KeyboardButton(text='Start')
    markup = ReplyKeyboardMarkup(keyboard=[[start_button]], resize_keyboard=True, one_time_keyboard=True)

   user_data = [chat_join.from_user.id, chat_join.from_user.username, chat_join.from_user.first_name]
   append_data_to_sheet(user_data, "1J0PTN39cYH7IRH0P_IrrJ2QcGMl_nKDVTgT6CQ34WFY", "A:C")
   await bot.send_message(chat_id=chat_join.from_user.id, text=msg, reply_markup=markup, disable_web_page_preview=True)

@dp.message(F.text.lower() == "start")
async def send_channel_link(message: types.Message):
        msg = "Ваша заявка одобрена!\n\nВступить в канал: https://t.me/+I-1JiJsJRsw1Mjk6"
        button = InlineKeyboardButton(text='ВСТУПИТЬ', url='https://t.me/+I-1JiJsJRsw1Mjk6')
        markup = InlineKeyboardMarkup(inline_keyboard=[[button]])

        await message.answer(text=msg, reply_markup=markup, disable_web_page_preview=True)

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

  

