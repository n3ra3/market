import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from aiogram import Bot, Dispatcher, Router, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
import asyncio
import aiohttp
import os


def load_dotenv_file(env_path):
    """Load .env values without overriding real environment variables."""
    try:
        if not os.path.exists(env_path):
            return
        with open(env_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                if len(value) >= 2 and ((value[0] == '"' and value[-1] == '"') or (value[0] == "'" and value[-1] == "'")):
                    value = value[1:-1]
                os.environ.setdefault(key, value)
    except Exception:
        pass


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv_file(os.path.join(PROJECT_ROOT, ".env"))
TELEGRAM_TOKEN = os.getenv("MARKET_TELEGRAM_TOKEN")
CHAT_ID = os.getenv("MARKET_TELEGRAM_CHAT_ID")

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
router = Router()

def create_keyboard(lot_id):
    return InlineKeyboardMarkup().add(
        InlineKeyboardButton("✅ Купить", callback_data=f"approve:{lot_id}"),
        InlineKeyboardButton("❌ Отклонить", callback_data=f"reject:{lot_id}")
    )

async def send_telegram_notification(name, price, lot_id):
    keyboard = create_keyboard(lot_id)
    message = f"Новый лот:\nНазвание: {name}\nЦена: {price / 1000:.2f} USD"
    await bot.send_message(chat_id=CHAT_ID, text=message, reply_markup=keyboard)

@router.callback_query(lambda c: c.data.startswith("approve"))
async def approve_lot(callback: types.CallbackQuery):
    lot_id = callback.data.split(":")[1]
    await callback.answer("Покупка подтверждена!")
    await buy_lot(lot_id)

@router.callback_query(lambda c: c.data.startswith("reject"))
async def reject_lot(callback: types.CallbackQuery):
    lot_id = callback.data.split(":")[1]
    await callback.answer("Лот отклонён!")

async def buy_lot(lot_id):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://market.csgo.com/api/v2/get-money?key=ВАШ_API_КЛЮЧ') as response:
            balance = await response.json()
            if balance['money'] >= 10000:  # Пример проверки баланса
                async with session.get(f'https://market.csgo.com/api/v2/buy?key=ВАШ_API_КЛЮЧ&id={lot_id}&price=10000') as buy_response:
                    result = await buy_response.json()
                    print(result)

async def main():
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())