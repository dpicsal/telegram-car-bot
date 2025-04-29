import os
import json
import logging
import time
from datetime import datetime
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from dotenv import load_dotenv
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from io import BytesIO

# Load environment variables
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_SHEETS_JSON = os.getenv("GOOGLE_SHEETS_JSON")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
PHOTO_FOLDER_ID = os.getenv("PHOTO_FOLDER_ID")  # 📸 NEW: Photo Upload Google Drive Folder ID
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x]
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
DRIVERS_WORKSHEET_NAME = os.getenv("DRIVERS_WORKSHEET_NAME", "Drivers")
LOG_WORKSHEET_NAME = os.getenv("LOG_WORKSHEET_NAME", "Log")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Google Sheets
creds_dict = json.loads(GOOGLE_SHEETS_JSON)
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
gs = gspread.authorize(creds)
sh = gs.open_by_key(SPREADSHEET_ID)

# Initialize Worksheets
try:
    sheet_log = sh.worksheet(LOG_WORKSHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    sheet_log = sh.add_worksheet(title=LOG_WORKSHEET_NAME, rows=100, cols=4)
    sheet_log.append_row(["Timestamp", "Driver Name", "Car Plate", "Action"])

try:
    sheet_drivers = sh.worksheet(DRIVERS_WORKSHEET_NAME)
except gspread.exceptions.WorksheetNotFound:
    sheet_drivers = sh.add_worksheet(title=DRIVERS_WORKSHEET_NAME, rows=100, cols=2)
    sheet_drivers.append_row(["Name", "User ID"])

try:
    sheet_cars = sh.worksheet("Cars")
except gspread.exceptions.WorksheetNotFound:
    sheet_cars = sh.add_worksheet(title="Cars", rows=100, cols=1)
    sheet_cars.append_row(["Car Plate"])

# Timezone for Dubai
UAE_TZ = pytz.timezone("Asia/Dubai")

# Keyboards
MAIN_MENU = ReplyKeyboardMarkup([
    ["🚗 Take Car", "↩️ Return Car"],
    ["⬅️ Main Menu"]
], resize_keyboard=True)

ADMIN_MENU = ReplyKeyboardMarkup([
    ["➕ Add Car", "➖ Remove Car"],
    ["📋 Driver List", "🛠️ Admin Settings"],
    ["📊 Status", "🔍 History", "🧾 Generate Report"],
    ["⬅️ Main Menu"]
], resize_keyboard=True)

COMBINED_MENU = ReplyKeyboardMarkup([
    ["🚗 Take Car", "↩️ Return Car"],
    ["➕ Add Car", "➖ Remove Car"],
    ["📋 Driver List", "🛠️ Admin Settings"],
    ["📊 Status", "🔍 History", "🧾 Generate Report"],
    ["⬅️ Main Menu"]
], resize_keyboard=True)
# Retry decorator
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
                    logger.warning(f"Retrying after {wait}s: {e}")
                    time.sleep(wait)
                    attempt += 1
        return wrapper
    return decorator

# Admin-only decorator
def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id not in ADMINS:
            await update.message.reply_text("❌ Access Denied: Admins only.")
            return
        return await func(update, context)
    return wrapper

# Admin or Driver Access
def admin_or_driver(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        driver_ids = sheet_drivers.col_values(2)[1:]
        if user_id in ADMINS or str(user_id) in driver_ids:
            return await func(update, context)
        await update.message.reply_text("❌ Access denied. Please contact Admin.")
    return wrapper

# /start Command
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    driver_ids = sheet_drivers.col_values(2)[1:]
    if user_id in ADMINS:
        await update.message.reply_text("🛠️ Admin Menu:", reply_markup=COMBINED_MENU)
    elif str(user_id) in driver_ids:
        await update.message.reply_text("🚗 Driver Menu:", reply_markup=MAIN_MENU)
    else:
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Approve", callback_data=f"approve|{user_id}|{update.effective_user.full_name}")],
            [InlineKeyboardButton("❌ Reject", callback_data=f"reject|{user_id}")]
        ])
        await update.message.reply_text("🔒 Access Request Sent to Admin.", reply_markup=buttons)

# Take Car Menu
@admin_or_driver
async def take_car(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logs = sheet_log.get_all_records()
    last_action = {r["Car Plate"]: r["Action"] for r in logs}
    cars = sheet_cars.col_values(1)[1:]
    available = [c for c in cars if last_action.get(c) != "out"]
    if not available:
        await update.message.reply_text("🚫 No available cars.")
        return
    buttons = [[InlineKeyboardButton(f"🚗 {car}", callback_data=f"take|{car}")] for car in available]
    await update.message.reply_text("Select a car:", reply_markup=InlineKeyboardMarkup(buttons))

# Return Car Menu
@admin_or_driver
async def return_car(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user.first_name
    logs = sheet_log.get_all_records()
    last = {r["Car Plate"]: r for r in logs}
    user_cars = [cp for cp, rec in last.items() if rec["Action"] == "out" and rec["Driver Name"] == user]
    if not user_cars:
        await update.message.reply_text("✅ You have no cars to return.", reply_markup=MAIN_MENU)
        return
    buttons = [[InlineKeyboardButton(f"↩️ {c}", callback_data=f"return|{c}")] for c in user_cars]
    await update.message.reply_text("Select your car to return:", reply_markup=InlineKeyboardMarkup(buttons))

# Access Request Handling
async def access_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    action, user_id, name = query.data.split("|")
    user_id = int(user_id)
    if action == "approve":
        sheet_drivers.append_row([name, str(user_id)])
        await query.edit_message_text(f"✅ Approved: {name}")
        await context.bot.send_message(chat_id=user_id, text="✅ You are now registered! Use /start again.")
    else:
        await query.edit_message_text(f"❌ Rejected: {name}")
        await context.bot.send_message(chat_id=user_id, text="❌ Your request was rejected.")

# Take / Return Car Handler
async def car_action_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    action, plate = query.data.split("|")
    user = update.effective_user.first_name
    now = datetime.now(UAE_TZ)
    timestamp = now.strftime("%Y-%m-%d %H:%M")
    if action == "take":
        sheet_log.append_row([timestamp, user, plate, "out"])
        await query.edit_message_text(f"✅ Taken: {plate}")
    else:
        sheet_log.append_row([timestamp, user, plate, "in"])
        await query.edit_message_text(f"↩️ Returned: {plate}")
# Driver List Menu
@admin_only
async def driver_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    drivers = sheet_drivers.get_all_records()
    if not drivers:
        await update.message.reply_text("📋 No drivers registered.")
        return
    lines = [f"{d['Name']} - {d['User ID']}" for d in drivers]
    await update.message.reply_text("📋 Driver List:\n\n" + "\n".join(lines), reply_markup=ADMIN_MENU)

# Status Menu
@admin_only
async def status_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logs = sheet_log.get_all_records()
    last_action = {r["Car Plate"]: r["Action"] for r in logs}
    cars = sheet_cars.col_values(1)[1:]
    lines = []
    for car in cars:
        status = last_action.get(car, "✅ Available")
        label = "✅ Available" if status != "out" else "❌ Out"
        lines.append(f"{label} — {car}")
    await update.message.reply_text("📊 Car Status:\n\n" + "\n".join(lines), reply_markup=ADMIN_MENU)

# History Menu
@admin_only
async def history_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logs = sheet_log.get_all_records()
    if not logs:
        await update.message.reply_text("🔍 No history records found.", reply_markup=ADMIN_MENU)
        return
    latest = logs[-10:]
    lines = [f"{r['Timestamp']} - {r['Driver Name']} {'took' if r['Action']=='out' else 'returned'} {r['Car Plate']}" for r in latest]
    await update.message.reply_text("🔍 Recent History:\n\n" + "\n".join(lines), reply_markup=ADMIN_MENU)

# Handle Text Inputs (Add Car, Add Driver, Search History)
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    action = context.user_data.pop("await", None)
    text = update.message.text.strip()
    if action == "add_car":
        existing = sheet_cars.col_values(1)[1:]
        if text in existing:
            await update.message.reply_text("⚠️ Car already exists.", reply_markup=ADMIN_MENU)
        else:
            sheet_cars.append_row([text])
            await update.message.reply_text(f"✅ Car {text} added.", reply_markup=ADMIN_MENU)
    elif action == "add_driver":
        try:
            name, user_id = map(str.strip, text.split(","))
            sheet_drivers.append_row([name, user_id])
            await update.message.reply_text(f"✅ Driver {name} added.", reply_markup=ADMIN_MENU)
        except ValueError:
            await update.message.reply_text("❌ Format error. Use: Name, UserID", reply_markup=ADMIN_MENU)

# Admin Settings Panel (Placeholder if needed)
@admin_only
async def admin_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚙️ Admin Settings Panel (Coming Soon).", reply_markup=ADMIN_MENU)
import io
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Initialize PyDrive (for photo upload)
def init_drive():
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile("mycreds.txt")
    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
    gauth.SaveCredentialsFile("mycreds.txt")
    return GoogleDrive(gauth)

drive = None  # We'll initialize it inside handlers

# 📄 Generate PDF Report (filtered or full)
@admin_only
async def generate_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logs = sheet_log.get_all_records()
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    y = height - 50
    pdf.setFont("Helvetica", 12)
    pdf.drawString(50, y, "🚗 Car Usage Report")
    y -= 30
    for log in logs[-50:]:  # Last 50 records
        line = f"{log['Timestamp']} - {log['Driver Name']} {'Took' if log['Action']=='out' else 'Returned'} {log['Car Plate']}"
        pdf.drawString(50, y, line)
        y -= 20
        if y < 50:
            pdf.showPage()
            y = height - 50
    pdf.save()
    buffer.seek(0)
    await update.message.reply_document(document=buffer, filename="Car_Report.pdf")
    buffer.close()

# 📷 Upload Photo to Google Drive
@admin_only
async def upload_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global drive
    if drive is None:
        drive = init_drive()

    if update.message.photo:
        photo = await update.message.photo[-1].get_file()
        file_path = f"/tmp/{photo.file_id}.jpg"
        await photo.download_to_drive(file_path)

        upload = drive.CreateFile({'parents': [{'id': '1b5TzVdFbB6rHcnDEYpWP4WWLq2QahfFx'}]})  # Your Google Drive Folder ID
        upload.SetContentFile(file_path)
        upload.Upload()
        await update.message.reply_text("✅ Photo uploaded successfully to Google Drive!")
    else:
        await update.message.reply_text("⚠️ Please send a photo after the /upload_photo command.")

# ⏰ Snooze Access Request (Coming soon)
@admin_only
async def snooze_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏰ Snooze Request Option Coming Soon.")

# ➕ Multi Admin Support (dynamic update)
@admin_only
async def add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = "add_admin"
    await update.message.reply_text("➕ Send the new Admin's Telegram ID:", reply_markup=ADMIN_MENU)

async def text_add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        new_admin_id = int(text)
        if new_admin_id not in ADMINS:
            ADMINS.append(new_admin_id)
            await update.message.reply_text(f"✅ Admin ID {new_admin_id} added.")
        else:
            await update.message.reply_text(f"⚠️ Admin ID {new_admin_id} already exists.")
    except ValueError:
        await update.message.reply_text("❌ Please send a valid number (Telegram ID).")
from telegram.ext import ApplicationBuilder

def main():
    try:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

        # Regular Commands
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("admin", admin_menu))
        app.add_handler(CommandHandler("generate_pdf", generate_pdf))
        app.add_handler(CommandHandler("upload_photo", upload_photo))
        app.add_handler(CommandHandler("snooze", snooze_access))
        app.add_handler(CommandHandler("add_admin", add_admin))
        app.add_handler(CommandHandler("driver_list", driver_list))
        app.add_handler(CommandHandler("status", status_menu))
        app.add_handler(CommandHandler("history", history_menu))

        # Message/Text Handlers
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

        # Callback Query Handlers (for Approve/Reject, Car Actions)
        app.add_handler(CallbackQueryHandler(on_car_action, pattern="^(take|return)\\|"))
        app.add_handler(CallbackQueryHandler(handle_access_request, pattern="^(approve|reject)\\|"))
        app.add_handler(CallbackQueryHandler(handle_driver_action, pattern="^(remove_driver|add_driver)"))
        app.add_handler(CallbackQueryHandler(handle_remove_car_action, pattern="^remove_car\\|"))

        # If adding Admin ID manually (text)
        app.add_handler(MessageHandler(filters.Regex("^\\d{7,}$"), text_add_admin))  # 7+ digits

        logger.info("✅ Bot started successfully!")
        app.run_polling(timeout=10)
    except Exception as e:
        logger.error(f"❌ Error in main(): {e}")

if __name__ == "__main__":
    main()
