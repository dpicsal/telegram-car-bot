import os
import json
import logging
from datetime import datetime
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from dotenv import load_dotenv
import pytz
import aiohttp
import aiofiles

# Load environment variables
load_dotenv()
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Environment Variables
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_SHEETS_JSON = os.getenv("GOOGLE_SHEETS_JSON")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x]
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
DRIVERS_WORKSHEET_NAME = os.getenv("DRIVERS_WORKSHEET_NAME", "Drivers")
LOG_WORKSHEET_NAME = os.getenv("LOG_WORKSHEET_NAME", "Log")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
TIMEZONE = pytz.timezone(os.getenv("TIMEZONE", "Asia/Dubai"))

# Setup Google Sheets
creds_dict = json.loads(GOOGLE_SHEETS_JSON)
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
gs = gspread.authorize(creds)
sh = gs.open_by_key(SPREADSHEET_ID)

sheet_drivers = sh.worksheet(DRIVERS_WORKSHEET_NAME)
sheet_log = sh.worksheet(LOG_WORKSHEET_NAME)
sheet_cars = sh.worksheet("Cars")

# Setup Google Drive
service = build("drive", "v3", credentials=creds)
# Define Keyboard Layouts
MAIN_MENU = ReplyKeyboardMarkup([
    ["🚗 Take Car", "↩️ Return Car"],
    ["⬅️ Main Menu"]
], resize_keyboard=True)

ADMIN_MENU = ReplyKeyboardMarkup([
    ["➕ Add Car", "➖ Remove Car"],
    ["📋 Driver List"],
    ["📊 Status", "🔍 History"],
    ["⬅️ Main Menu"]
], resize_keyboard=True)

COMBINED_MENU = ReplyKeyboardMarkup([
    ["🚗 Take Car", "↩️ Return Car"],
    ["➕ Add Car", "➖ Remove Car"],
    ["📋 Driver List"],
    ["📊 Status", "🔍 History"],
    ["⬅️ Main Menu"]
], resize_keyboard=True)

# Decorators for Admins and Drivers
def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMINS:
            await update.message.reply_text("❌ Access denied. Admins only.")
            return
        return await func(update, context)
    return wrapper

def admin_or_driver(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        driver_ids = sheet_drivers.col_values(2)[1:]  # Skip header
        if user_id not in ADMINS and str(user_id) not in driver_ids:
            await update.message.reply_text("❌ Access denied. You are not registered.")
            return
        return await func(update, context)
    return wrapper

# /start command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    driver_ids = sheet_drivers.col_values(2)[1:]
    if user_id in ADMINS or str(user_id) in driver_ids:
        menu = COMBINED_MENU if user_id in ADMINS else MAIN_MENU
        await update.message.reply_text("Welcome! Choose an option:", reply_markup=menu)
    else:
        await update.message.reply_text("❌ Access denied. Requesting access from admin...")
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Approve", callback_data=f"approve|{user_id}|{update.effective_user.full_name}"),
             InlineKeyboardButton("❌ Reject", callback_data=f"reject|{user_id}")]
        ])
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"👤 Access Request:\nName: {update.effective_user.full_name}\nUsername: @{update.effective_user.username or 'N/A'}\nUser ID: {user_id}",
            reply_markup=buttons
        )

# Admin Panel Menu
@admin_only
async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🛠️ Admin Panel:", reply_markup=COMBINED_MENU)
# Helper: Upload Photo to Google Drive
async def upload_photo_to_drive(file_path, file_name):
    try:
        file_metadata = {
            "name": file_name,
            "parents": [GOOGLE_DRIVE_FOLDER_ID]
        }
        media = MediaFileUpload(file_path, mimetype="image/jpeg")
        uploaded_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()

        file_id = uploaded_file.get("id")
        if file_id:
            # Make file public
            service.permissions().create(
                fileId=file_id,
                body={"role": "reader", "type": "anyone"},
            ).execute()
            drive_link = f"https://drive.google.com/uc?id={file_id}"
            return drive_link
        return None
    except Exception as e:
        logger.error(f"Error uploading photo to Google Drive: {e}")
        return None

# Photo Message Handler
@admin_or_driver
async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user.first_name
        user_id = update.effective_user.id

        # Download the photo
        photo = update.message.photo[-1]  # Best quality
        file = await context.bot.get_file(photo.file_id)
        file_path = f"/tmp/{user}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"

        async with aiohttp.ClientSession() as session:
            async with session.get(file.file_path) as resp:
                if resp.status == 200:
                    f = await aiofiles.open(file_path, mode='wb')
                    await f.write(await resp.read())
                    await f.close()

        # Upload to Drive
        file_name = f"{user}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
        drive_link = await asyncio.to_thread(upload_photo_to_drive, file_path, file_name)

        if drive_link:
            # Find the latest action by this user in Log
            logs = sheet_log.get_all_records()
            logs.sort(key=lambda x: x["Timestamp"], reverse=True)
            for idx, log in enumerate(logs):
                if log.get("Driver Name") == user:
                    sheet_log.update_cell(idx + 2, 5, drive_link)  # Row index +2 (header), Column 5
                    break

            await update.message.reply_text(f"✅ Photo uploaded successfully!\n📎 [View Photo]({drive_link})", parse_mode="Markdown")
        else:
            await update.message.reply_text("❌ Failed to upload photo. Please try again later.")

    except Exception as e:
        logger.error(f"Error in handle_photo: {e}")
        await update.message.reply_text("❌ Error handling your photo. Please try again.")
# Retry Decorator for Google Sheets Operations
def retry_gsheet_operation(max_attempts=3, backoff_factor=2):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            attempt = 1
            while attempt <= max_attempts:
                try:
                    return await func(*args, **kwargs)
                except gspread.exceptions.APIError as e:
                    if attempt == max_attempts:
                        logger.error(f"Failed after {max_attempts} attempts: {e}")
                        raise
                    wait = backoff_factor ** attempt
                    logger.warning(f"API error, retrying after {wait}s: {e}")
                    time.sleep(wait)
                    attempt += 1
                except Exception as e:
                    logger.error(f"Unexpected error in {func.__name__}: {e}")
                    raise
            return None
        return wrapper
    return decorator

# Car Take Action
@retry_gsheet_operation()
@admin_or_driver
async def take_car_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user.first_name
        logs = sheet_log.get_all_records()
        logs.sort(key=lambda x: x["Timestamp"])

        all_cars = sheet_cars.col_values(1)[1:]  # Skip header
        last_action = {r["Car Plate"]: r["Action"] for r in logs}

        available_cars = [car for car in all_cars if last_action.get(car) != "out"]
        if not available_cars:
            await update.message.reply_text("🚫 No cars available right now.")
            return

        buttons = [[InlineKeyboardButton(f"{car} (Available)", callback_data=f"take|{car}")] for car in available_cars]
        await update.message.reply_text("Select a car to take:", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.error(f"Error in take_car_menu: {e}")
        await update.message.reply_text("❌ Error fetching available cars.")

# Car Return Action
@retry_gsheet_operation()
@admin_or_driver
async def return_car_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user.first_name
        logs = sheet_log.get_all_records()
        logs.sort(key=lambda x: x["Timestamp"])

        user_cars = [r["Car Plate"] for r in logs if r["Driver Name"] == user and r["Action"] == "out"]
        if not user_cars:
            await update.message.reply_text("✅ You have no cars to return.")
            return

        buttons = [[InlineKeyboardButton(f"Return {car}", callback_data=f"return|{car}")] for car in user_cars]
        await update.message.reply_text("Select a car to return:", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.error(f"Error in return_car_menu: {e}")
        await update.message.reply_text("❌ Error fetching cars to return.")
# Handle Approve/Reject Access Request
@retry_gsheet_operation()
async def handle_access_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action, user_id, *user_name_parts = query.data.split("|")
        user_id = int(user_id)
        user_name = user_name_parts[0] if user_name_parts else "Unknown"

        if action == "approve":
            sheet_drivers.append_row([user_name, str(user_id)])
            await query.edit_message_text(f"✅ Access approved for {user_name} (ID: {user_id})")
            await context.bot.send_message(user_id, "✅ Your access request was approved! You are now registered.")
        else:
            await query.edit_message_text(f"❌ Access rejected for {user_name} (ID: {user_id})")
            await context.bot.send_message(user_id, "❌ Your access request was rejected.")
    except Exception as e:
        logger.error(f"Error in handle_access_request: {e}")
        await query.edit_message_text("❌ Error processing access request.")

# Handle Car Take/Return Action
@retry_gsheet_operation()
async def on_car_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action, car_plate = query.data.split("|")
        user = update.effective_user.first_name

        timestamp_now = datetime.now(TIMEZONE)
        timestamp_storage = timestamp_now.strftime("%Y-%m-%d %H:%M")
        timestamp_display = timestamp_now.strftime("%d-%m-%Y, %I:%M %p")

        if action == "take":
            # Double check if the car is already taken
            logs = sheet_log.get_all_records()
            last_action = {r["Car Plate"]: r["Action"] for r in logs}
            if last_action.get(car_plate) == "out":
                await query.edit_message_text("🚫 Car is already in use.")
                return
            sheet_log.append_row([timestamp_storage, user, car_plate, "out"])
            await query.edit_message_text(f"✅ You took {car_plate} at {timestamp_display}")
        elif action == "return":
            sheet_log.append_row([timestamp_storage, user, car_plate, "in"])
            await query.edit_message_text(f"↩️ You returned {car_plate} at {timestamp_display}")
    except Exception as e:
        logger.error(f"Error in on_car_action: {e}")
        await query.edit_message_text("❌ Error processing car action.")
def main():
    try:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

        # Commands
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("admin", admin_menu))

        # Text Messages
        app.add_handler(MessageHandler(filters.Regex("^🚗 Take Car$"), take_car_menu))
        app.add_handler(MessageHandler(filters.Regex("^↩️ Return Car$"), return_car_menu))
        app.add_handler(MessageHandler(filters.Regex("^⬅️ Main Menu$"), start))
        
        # Photo Upload
        app.add_handler(MessageHandler(filters.PHOTO, handle_photo))

        # Callback Query Buttons
        app.add_handler(CallbackQueryHandler(handle_access_request, pattern="^(approve|reject)\\|"))
        app.add_handler(CallbackQueryHandler(on_car_action, pattern="^(take|return)\\|"))

        logging.info("Bot is running...")
        app.run_polling(timeout=10)

    except Exception as e:
        logger.error(f"Error starting bot: {e}")

if __name__ == "__main__":
    main()
