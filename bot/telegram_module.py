import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from aiogram import Bot, Dispatcher, Router, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
import asyncio
import aiohttp
import importlib.util
import os
# Load config by path to avoid import conflicts
TELEGRAM_TOKEN = None
CHAT_ID = None
try:
    cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.py")
    if os.path.exists(cfg_path):
        spec = importlib.util.spec_from_file_location("bot_config", cfg_path)
        cfg = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cfg)
        TELEGRAM_TOKEN = getattr(cfg, "TELEGRAM_TOKEN", None)
        CHAT_ID = getattr(cfg, "CHAT_ID", None)
    else:
        # fallback to environment
        TELEGRAM_TOKEN = os.getenv("MARKET_TELEGRAM_TOKEN")
        CHAT_ID = os.getenv("MARKET_TELEGRAM_CHAT_ID")
except Exception:
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