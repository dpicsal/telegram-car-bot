import os
import json
import logging
from datetime import datetime, timedelta
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
from dotenv import load_dotenv
import pytz
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table
from reportlab.lib.styles import getSampleStyleSheet
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import io

# Configure logging
load_dotenv()
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_SHEETS_JSON = os.getenv("GOOGLE_SHEETS_JSON")
GOOGLE_DRIVE_JSON = os.getenv("GOOGLE_DRIVE_JSON")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x]
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
DRIVERS_WORKSHEET_NAME = os.getenv("DRIVERS_WORKSHEET_NAME", "Drivers")
LOG_WORKSHEET_NAME = os.getenv("LOG_WORKSHEET_NAME", "Log")
MAINTENANCE_INTERVAL_DAYS = int(os.getenv("MAINTENANCE_INTERVAL_DAYS", "30"))

# Validate environment variables
if not all([TELEGRAM_TOKEN, GOOGLE_SHEETS_JSON, SPREADSHEET_ID, GOOGLE_DRIVE_JSON]):
    logger.error("Missing required environment variables")
    exit(1)

# Initialize Google Sheets
try:
    creds_dict = json.loads(GOOGLE_SHEETS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
    gs = gspread.authorize(creds)
    sh = gs.open_by_key(SPREADSHEET_ID)

    # Initialize worksheets
    def initialize_worksheet(name, headers, rows=100, cols=None):
        try:
            ws = sh.worksheet(name)
            data = ws.get_all_values()
            if not data or data[0] != headers:
                logger.warning(f"Worksheet '{name}' missing headers. Resetting...")
                ws.clear()
                ws.append_row(headers)
            return ws
        except gspread.exceptions.WorksheetNotFound:
            logger.warning(f"Worksheet '{name}' not found. Creating...")
            ws = sh.add_worksheet(title=name, rows=rows, cols=cols or len(headers))
            ws.append_row(headers)
            return ws

    sheet_log = initialize_worksheet(LOG_WORKSHEET_NAME, ["Timestamp", "Driver Name", "Car Plate", "Action"], cols=4)
    sheet_drivers = initialize_worksheet(DRIVERS_WORKSHEET_NAME, ["Name", "User ID"], cols=2)
    sheet_cars = initialize_worksheet("Cars", ["Car Plate"], cols=1)
    sheet_photos = initialize_worksheet("Photos", ["Timestamp", "Driver Name", "Car Plate", "File ID", "Drive Link"], cols=5)
    sheet_admins = initialize_worksheet("Admins", ["Name", "User ID"], cols=2)
    sheet_settings = initialize_worksheet("Settings", ["Setting", "Value"], cols=2)
    sheet_maintenance = initialize_worksheet("Maintenance", ["Car Plate", "Last Maintenance", "Next Maintenance"], cols=3)

    # Initialize settings if empty
    settings_data = sheet_settings.get_all_records()
    default_settings = {
        "photo_upload_enabled": "True",
        "snooze_1h": "True",
        "snooze_2h": "True",
        "snooze_24h": "True",
        "weekly_summary": "False"
    }
    if not settings_data:
        for k, v in default_settings.items():
            sheet_settings.append_row([k, v])
    else:
        # Update settings to include any missing defaults
        existing = {r["Setting"]: r["Value"] for r in settings_data}
        for k, v in default_settings.items():
            if k not in existing:
                sheet_settings.append_row([k, v])

except Exception as e:
    logger.error(f"Failed to initialize Google Sheets: {e}")
    exit(1)

# Initialize Google Drive
try:
    gauth = GoogleAuth()
    drive_creds_dict = json.loads(GOOGLE_DRIVE_JSON)
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(drive_creds_dict, scopes)
    drive = GoogleDrive(gauth)
except Exception as e:
    logger.error(f"Failed to initialize Google Drive: {e}")
    exit(1)

# Define keyboard layouts
MAIN_MENU = ReplyKeyboardMarkup([
    ["🚗 Take Car", "↩️ Return Car"],
    ["📸 Upload Photo", "🧾 My History"],
    ["⬅️ Main Menu"]
], resize_keyboard=True)

ADMIN_MENU = ReplyKeyboardMarkup([
    ["➕ Add Car", "➖ Remove Car"],
    ["📋 Driver List", "📊 Status"],
    ["🔍 History", "🧾 Generate Report"],
    ["➕ Add Admin", "➖ Remove Admin"],
    ["📄 List Admins", "⚙️ Settings"],
    ["🔄 Refresh Sheet"],
    ["⬅️ Main Menu"]
], resize_keyboard=True)

COMBINED_MENU = ReplyKeyboardMarkup([
    ["🚗 Take Car", "↩️ Return Car"],
    ["📸 Upload Photo", "🧾 My History"],
    ["➕ Add Car", "➖ Remove Car"],
    ["📋 Driver List", "📊 Status"],
    ["🔍 History", "🧾 Generate Report"],
    ["➕ Add Admin", "➖ Remove Admin"],
    ["📄 List Admins", "⚙️ Settings"],
    ["🔄 Refresh Sheet"],
    ["⬅️ Main Menu"]
], resize_keyboard=True)

# Define UAE time zone (UTC+4)
UAE_TZ = pytz.timezone("Asia/Dubai")

# Initialize scheduler
scheduler = AsyncIOScheduler(timezone=UAE_TZ)

# Retry decorator for Google Sheets operations
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

# Decorator for admin-only access
def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        admin_ids = [str(r["User ID"]) for r in sheet_admins.get_all_records()]
        if str(user_id) not in admin_ids:
            await update.message.reply_text("❌ Access denied. Admin privileges required.")
            return
        return await func(update, context)
    return wrapper

# Decorator for admin or driver access
def admin_or_driver(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        try:
            driver_ids = [str(id) for id in sheet_drivers.col_values(2)[1:]]
            admin_ids = [str(r["User ID"]) for r in sheet_admins.get_all_records()]
        except Exception as e:
            logger.error(f"Error fetching IDs: {e}")
            await update.message.reply_text("❌ Server error. Please try again later.")
            return
        if str(user_id) not in admin_ids and str(user_id) not in driver_ids:
            await update.message.reply_text("❌ Access denied. You are not registered.")
            return
        return await func(update, context)
    return wrapper

# PDF generation helper
def generate_pdf(title, data, headers):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    elements = []
    styles = getSampleStyleSheet()
    
    elements.append(Paragraph(title, styles["Title"]))
    elements.append(Spacer(1, 12))
    
    table_data = [headers] + data
    table = Table(table_data)
    table.setStyle([
        ('BACKGROUND', (0, 0), (-1, 0), '#d5dae6'),
        ('TEXTCOLOR', (0, 0), (-1, 0), '#000000'),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), '#f5f5f5'),
        ('GRID', (0, 0), (-1, -1), 1, '#000000'),
    ])
    elements.append(table)
    
    doc.build(elements)
    buffer.seek(0)
    return buffer

# Handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        driver_ids = [str(id) for id in sheet_drivers.col_values(2)[1:]]
        admin_ids = [str(r["User ID"]) for r in sheet_admins.get_all_records()]
    except Exception as e:
        logger.error(f"Error in start handler: {e}")
        await update.message.reply_text("❌ Server error. Please try again later.")
        return
    if str(user_id) in admin_ids or str(user_id) in driver_ids:
        menu = COMBINED_MENU if str(user_id) in admin_ids else MAIN_MENU
        await update.message.reply_text("Welcome! Choose an option:", reply_markup=menu)
    else:
        await update.message.reply_text("❌ Access denied. Requesting access from admin...")
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Approve", callback_data=f"approve|{user_id}|{update.effective_user.full_name}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"reject|{user_id}"),
                InlineKeyboardButton("⏰ Snooze", callback_data=f"snooze|{user_id}|{update.effective_user.full_name}")
            ]
        ])
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"👤 Access Request:\n"
                    f"Name: {update.effective_user.full_name}\n"
                    f"Username: @{update.effective_user.username or 'N/A'}\n"
                    f"User ID: {user_id}"
                ),
                reply_markup=buttons
            )
        except Exception as e:
            logger.error(f"Error sending access request: {e}")
            await update.message.reply_text("❌ Failed to send access request. Please try again.")

@admin_only
async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🛠️ Admin Panel:", reply_markup=COMBINED_MENU)

@admin_only
async def add_car_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = "add_car"
    await update.message.reply_text("➕ Send me the new CAR PLATE to add:", reply_markup=ADMIN_MENU)

@admin_only
async def remove_car_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        all_cars = sheet_cars.col_values(1)[1:]
        if not all_cars:
            return await update.message.reply_text("🚫 No cars available to remove.", reply_markup=ADMIN_MENU)
        logs = sheet_log.get_all_records()
        last_action = {r["Car Plate"]: r["Action"] for r in logs}
        buttons = []
        for car in all_cars:
            if last_action.get(car) == "out":
                buttons.append([InlineKeyboardButton(f"{car} (In Use)", callback_data="noop")])
            else:
                buttons.append([InlineKeyboardButton(f"{car} - Remove", callback_data=f"remove_car|{car}")])
        await update.message.reply_text("Select a car to remove:", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.error(f"Error in remove_car_prompt: {e}")
        await update.message.reply_text("❌ Error fetching cars to remove.", reply_markup=ADMIN_MENU)

@admin_only
async def add_driver_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = "add_driver"
    await update.message.reply_text("➕ Send the DRIVER NAME and TELEGRAM USER ID (comma-separated):", reply_markup=ADMIN_MENU)

@retry_gsheet_operation()
@admin_only
async def driver_list_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        drivers = sheet_drivers.get_all_records()
        buttons = [
            [InlineKeyboardButton(f"{d['Name']} (ID: {d['User ID']}) - Remove", callback_data=f"remove_driver|{d['Name']}")]
            for d in drivers
        ]
        buttons.append([InlineKeyboardButton("➕ Add Driver", callback_data="add_driver")])
        await update.message.reply_text(
            "📋 Driver List (select to remove or add):",
            reply_markup=InlineKeyboardMarkup(buttons) if drivers else InlineKeyboardMarkup([[InlineKeyboardButton("➕ Add Driver", callback_data="add_driver")]])
        )
    except Exception as e:
        logger.error(f"Error in driver_list_menu: {e}")
        await update.message.reply_text("❌ Error fetching driver list.", reply_markup=ADMIN_MENU)

@retry_gsheet_operation()
@admin_only
async def status_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logs = sheet_log.get_all_records()
        logs.sort(key=lambda x: x["Timestamp"])
        last = {}
        car_driver = {}
        for r in logs:
            car_plate = str(r["Car Plate"]).strip().upper()
            last[car_plate] = r["Action"]
            if r["Action"] == "out":
                car_driver[car_plate] = r["Driver Name"]
        all_cars = sheet_cars.col_values(1)[1:]
        status_lines = []
        for car in all_cars:
            normalized_car = str(car).strip().upper()
            state = last.get(normalized_car, "✅ Available")
            label = f"❌ Out (Driver: {car_driver.get(normalized_car, 'Unknown')})" if state == "out" else "✅ Available"
            status_lines.append(f"{label} — {car}")
        await update.message.reply_text(f"📊 Current Car Status:\n\n{'\n'.join(status_lines) or 'No cars found.'}", reply_markup=ADMIN_MENU)
    except Exception as e:
        logger.error(f"Error in status_menu: {e}")
        await update.message.reply_text("❌ Error fetching status.", reply_markup=ADMIN_MENU)

@retry_gsheet_operation()
@admin_only
async def history_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logs = sheet_log.get_all_records()
        latest = logs[-10:]
        lines = [
            f'{datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ).strftime("%d-%m-%Y, %I:%M %p")} - {r["Driver Name"]} {"took" if r["Action"] == "out" else "returned"} {r["Car Plate"]}'
            for r in latest
        ]
        await update.message.reply_text("🔍 Latest History:\n\n" + "\n".join(lines) or "No history.", reply_markup=ADMIN_MENU)
        context.user_data["await"] = "search_logs"
        await update.message.reply_text(
            "🔍 Enter the car plate and date to search logs (format: CAR_PLATE, DD-MM-YYYY):",
            reply_markup=ADMIN_MENU
        )
    except Exception as e:
        logger.error(f"Error in history_menu: {e}")
        await update.message.reply_text("❌ Error fetching history.", reply_markup=ADMIN_MENU)

async def back_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    admin_ids = [str(r["User ID"]) for r in sheet_admins.get_all_records()]
    menu = COMBINED_MENU if str(user_id) in admin_ids else MAIN_MENU
    await update.message.reply_text("🔙 Back to main menu:", reply_markup=menu)

@retry_gsheet_operation()
@admin_or_driver
async def take_car_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user.first_name
        user_id = update.effective_user.id
        logs = sheet_log.get_all_records()
        logs.sort(key=lambda x: x["Timestamp"])
        last_action = {r["Car Plate"]: r["Action"] for r in logs}
        user_cars = [cp for cp, action in last_action.items() if action == "out" and logs[list(last_action.keys()).index(cp)]["Driver Name"] == user]
        if user_cars:
            menu = COMBINED_MENU if str(user_id) in [str(r["User ID"]) for r in sheet_admins.get_all_records()] else MAIN_MENU
            return await update.message.reply_text(f"🚫 You already have a car ({user_cars[0]}).", reply_markup=menu)
        all_cars = sheet_cars.col_values(1)[1:]
        if not all_cars:
            return await update.message.reply_text("🚫 No cars available.", reply_markup=menu)
        available_cars = [car for car in all_cars if last_action.get(str(car).strip().upper()) != "out"]
        if not available_cars:
            return await update.message.reply_text("🚫 No cars available to take.", reply_markup=menu)
        buttons = [[InlineKeyboardButton(f"{car} (Available)", callback_data=f"take|{car}")] for car in available_cars]
        await update.message.reply_text("Select a car to take:", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.error(f"Error in take_car_menu: {e}")
        await update.message.reply_text("❌ Error fetching available cars.", reply_markup=COMBINED_MENU if str(user_id) in [str(r["User ID"]) for r in sheet_admins.get_all_records()] else MAIN_MENU)

@retry_gsheet_operation()
@admin_or_driver
async def return_car_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user.first_name
        user_id = update.effective_user.id
        logs = sheet_log.get_all_records()
        last = {r["Car Plate"]: r for r in logs}
        user_cars = [cp for cp, rec in last.items() if rec["Action"] == "out" and rec["Driver Name"] == user]
        if not user_cars:
            menu = COMBINED_MENU if str(user_id) in [str(r["User ID"]) for r in sheet_admins.get_all_records()] else MAIN_MENU
            return await update.message.reply_text("✅ You have no cars to return.", reply_markup=menu)
        buttons = [[InlineKeyboardButton(c, callback_data=f"return|{c}")] for c in user_cars]
        await update.message.reply_text("Select a car to return:", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.error(f"Error in return_car_menu: {e}")
        await update.message.reply_text("❌ Error fetching cars to return.", reply_markup=COMBINED_MENU if str(user_id) in [str(r["User ID"]) for r in sheet_admins.get_all_records()] else MAIN_MENU)

@retry_gsheet_operation()
@admin_or_driver
async def my_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user.first_name
        logs = sheet_log.get_all_records()
        user_logs = [r for r in logs if r["Driver Name"] == user]
        if not user_logs:
            return await update.message.reply_text("🧾 No history found for you.", reply_markup=MAIN_MENU)
        
        data = [
            [
                datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ).strftime("%d-%m-%Y, %I:%M %p"),
                r["Car Plate"],
                "Took" if r["Action"] == "out" else "Returned"
            ]
            for r in user_logs
        ]
        pdf_buffer = generate_pdf(f"History for {user}", data, ["Timestamp", "Car Plate", "Action"])
        await update.message.reply_document(
            document=pdf_buffer,
            filename=f"{user}_history.pdf",
            caption="🧾 Your usage history."
        )
    except Exception as e:
        logger.error(f"Error in my_history: {e}")
        await update.message.reply_text("❌ Error generating history report.")

@retry_gsheet_operation()
@admin_only
async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = "report_filter"
    buttons = [
        [InlineKeyboardButton("All Time", callback_data="report|all")],
        [InlineKeyboardButton("Last 7 Days", callback_data="report|7")],
        [InlineKeyboardButton("Last 30 Days", callback_data="report|30")],
        [InlineKeyboardButton("Custom Date (DD-MM-YYYY)", callback_data="report|custom")]
    ]
    await update.message.reply_text("🧾 Select report period:", reply_markup=InlineKeyboardMarkup(buttons))

@retry_gsheet_operation()
@admin_or_driver
async def upload_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    settings = {r["Setting"]: r["Value"] for r in sheet_settings.get_all_records()}
    if settings.get("photo_upload_enabled", "True") != "True":
        return await update.message.reply_text("📸 Photo upload is disabled by admin.", reply_markup=MAIN_MENU)
    
    context.user_data["await"] = "upload_photo"
    await update.message.reply_text("📸 Please upload a photo (fuel meter, car condition):", reply_markup=MAIN_MENU)

@retry_gsheet_operation()
@admin_only
async def add_admin_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = "add_admin"
    await update.message.reply_text("➕ Send the ADMIN NAME and TELEGRAM USER ID (comma-separated):", reply_markup=ADMIN_MENU)

@retry_gsheet_operation()
@admin_only
async def remove_admin_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        admins = sheet_admins.get_all_records()
        if len(admins) <= 1:
            return await update.message.reply_text("⚠️ Cannot remove the last admin.", reply_markup=ADMIN_MENU)
        buttons = [
            [InlineKeyboardButton(f"{a['Name']} (ID: {a['User ID']}) - Remove", callback_data=f"remove_admin|{a['User ID']}")]
            for a in admins
        ]
        await update.message.reply_text("Select an admin to remove:", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.error(f"Error in remove_admin_prompt: {e}")
        await update.message.reply_text("❌ Error fetching admins.", reply_markup=ADMIN_MENU)

@retry_gsheet_operation()
@admin_only
async def list_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        admins = sheet_admins.get_all_records()
        lines = [f"{a['Name']} (ID: {a['User ID']})" for a in admins]
        await update.message.reply_text(f"📄 Current Admins:\n\n{'\n'.join(lines) or 'No admins.'}", reply_markup=ADMIN_MENU)
    except Exception as e:
        logger.error(f"Error in list_admins: {e}")
        await update.message.reply_text("❌ Error fetching admin list.", reply_markup=ADMIN_MENU)

@retry_gsheet_operation()
@admin_only
async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    settings = {r["Setting"]: r["Value"] for r in sheet_settings.get_all_records()}
    buttons = [
        [InlineKeyboardButton(
            f"{'✅' if settings.get('photo_upload_enabled', 'True') == 'True' else '❌'} Photo Upload",
            callback_data="toggle_photo_upload"
        )],
        [InlineKeyboardButton(
            f"{'✅' if settings.get('weekly_summary', 'False') == 'True' else '❌'} Weekly Summary",
            callback_data="toggle_weekly_summary"
        )],
        [InlineKeyboardButton("Set Snooze Times", callback_data="set_snooze")]
    ]
    await update.message.reply_text("⚙️ Settings:", reply_markup=InlineKeyboardMarkup(buttons))

@retry_gsheet_operation()
@admin_only
async def refresh_sheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        global sheet_log, sheet_drivers, sheet_cars, sheet_photos, sheet_admins, sheet_settings, sheet_maintenance
        sheet_log = sh.worksheet(LOG_WORKSHEET_NAME)
        sheet_drivers = sh.worksheet(DRIVERS_WORKSHEET_NAME)
        sheet_cars = sh.worksheet("Cars")
        sheet_photos = sh.worksheet("Photos")
        sheet_admins = sh.worksheet("Admins")
        sheet_settings = sh.worksheet("Settings")
        sheet_maintenance = sh.worksheet("Maintenance")
        await update.message.reply_text("🔄 Google Sheets data refreshed.", reply_markup=ADMIN_MENU)
    except Exception as e:
        logger.error(f"Error in refresh_sheet: {e}")
        await update.message.reply_text("❌ Error refreshing sheets.", reply_markup=ADMIN_MENU)

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
            await context.bot.send_message(user_id, "✅ Your access request was approved!", reply_markup=MAIN_MENU)
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"✅ New driver {user_name} (ID: {user_id}) approved."
            )
        elif action == "reject":
            await query.edit_message_text(f"❌ Access rejected for {user_name} (ID: {user_id})")
            await context.bot.send_message(user_id, "❌ Your access request was rejected.")
        elif action == "snooze":
            buttons = [
                [InlineKeyboardButton("1 Hour", callback_data=f"snooze_time|{user_id}|{user_name}|1")],
                [InlineKeyboardButton("2 Hours", callback_data=f"snooze_time|{user_id}|{user_name}|2")],
                [InlineKeyboardButton("24 Hours", callback_data=f"snooze_time|{user_id}|{user_name}|24")]
            ]
            await query.edit_message_text(f"⏰ Select snooze duration for {user_name}:", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.error(f"Error in handle_access_request: {e}")
        await query.edit_message_text("❌ Error processing request.")

@retry_gsheet_operation()
async def handle_snooze_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        _, user_id, user_name, hours = query.data.split("|")
        user_id = int(user_id)
        hours = int(hours)
        settings = {r["Setting"]: r["Value"] for r in sheet_settings.get_all_records()}
        if settings.get(f"snooze_{hours}h", "True") != "True":
            await query.edit_message_text(f"⏰ Snooze for {hours} hours is disabled.")
            return
        remind_time = datetime.now(UAE_TZ) + timedelta(hours=hours)
        context.bot_data.setdefault("snoozed_requests", []).append({
            "user_id": user_id,
            "user_name": user_name,
            "remind_time": remind_time,
            "chat_id": query.message.chat_id,
            "message_id": query.message.message_id
        })
        await query.edit_message_text(f"⏰ Snoozed access request for {user_name} until {remind_time.strftime('%d-%m-%Y, %I:%M %p')}")
    except Exception as e:
        logger.error(f"Error in handle_snooze_time: {e}")
        await query.edit_message_text("❌ Error processing snooze.")

@retry_gsheet_operation()
async def handle_driver_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action, *params = query.data.split("|")
        if action == "remove_driver":
            driver_name = params[0]
            vals = sheet_drivers.col_values(1)[1:]
            if driver_name not in vals:
                await query.edit_message_text(f"⚠️ Driver {driver_name} not found.")
                return
            row = vals.index(driver_name) + 2
            sheet_drivers.delete_rows(row, row)
            await query.edit_message_text(f"✅ Driver {driver_name} removed.")
        elif action == "add_driver":
            await query.message.reply_text("➕ Send the DRIVER NAME and TELEGRAM USER ID (comma-separated):", reply_markup=ADMIN_MENU)
            context.user_data["await"] = "add_driver"
    except Exception as e:
        logger.error(f"Error in handle_driver_action: {e}")
        await query.edit_message_text("❌ Error processing driver action.")

@retry_gsheet_operation()
async def handle_remove_car_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action, car_plate = query.data.split("|")
        if action != "remove_car":
            return
        logs = sheet_log.get_all_records()
        last_action = {r["Car Plate"]: r["Action"] for r in logs}
        if last_action.get(car_plate) == "out":
            await query.edit_message_text(f"⚠️ Car {car_plate} is in use.")
            return
        all_cars = sheet_cars.col_values(1)
        if car_plate not in all_cars:
            await query.edit_message_text(f"⚠️ Car {car_plate} not found.")
            return
        row = all_cars.index(car_plate) + 1
        sheet_cars.delete_rows(row, row)
        await query.edit_message_text(f"✅ Car {car_plate} removed.")
    except Exception as e:
        logger.error(f"Error in handle_remove_car_action: {e}")
        await query.edit_message_text("❌ Error removing car.")

@retry_gsheet_operation()
async def handle_admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action, user_id = query.data.split("|")
        if action == "remove_admin":
            admins = sheet_admins.get_all_records()
            if len(admins) <= 1:
                await query.edit_message_text("⚠️ Cannot remove the last admin.")
                return
            admin_ids = [str(a["User ID"]) for a in admins]
            if user_id not in admin_ids:
                await query.edit_message_text("⚠️ Admin not found.")
                return
            row = admin_ids.index(user_id) + 2
            sheet_admins.delete_rows(row, row)
            await query.edit_message_text(f"✅ Admin {user_id} removed.")
    except Exception as e:
        logger.error(f"Error in handle_admin_action: {e}")
        await query.edit_message_text("❌ Error processing admin action.")

@retry_gsheet_operation()
async def handle_report_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        _, period = query.data.split("|")
        logs = sheet_log.get_all_records()
        now = datetime.now(UAE_TZ)
        
        if period == "7":
            start_date = now - timedelta(days=7)
            logs = [r for r in logs if datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ) >= start_date]
        elif period == "30":
            start_date = now - timedelta(days=30)
            logs = [r for r in logs if datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ) >= start_date]
        elif period == "custom":
            context.user_data["await"] = "report_custom_date"
            await query.message.reply_text("🧾 Enter the date (DD-MM-YYYY):", reply_markup=ADMIN_MENU)
            return
        
        data = [
            [
                datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ).strftime("%d-%m-%Y, %I:%M %p"),
                r["Driver Name"],
                r["Car Plate"],
                "Took" if r["Action"] == "out" else "Returned"
            ]
            for r in logs
        ]
        title = f"Full Usage Report ({'All Time' if period == 'all' else f'Last {period} Days'})"
        pdf_buffer = generate_pdf(title, data, ["Timestamp", "Driver Name", "Car Plate", "Action"])
        await query.message.reply_document(
            document=pdf_buffer,
            filename=f"full_report_{period}.pdf",
            caption="🧾 Full usage report."
        )
    except Exception as e:
        logger.error(f"Error in handle_report_action: {e}")
        await query.edit_message_text("❌ Error generating report.")

@retry_gsheet_operation()
async def handle_settings_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action = query.data
        settings = {r["Setting"]: r["Value"] for r in sheet_settings.get_all_records()}
        if action == "toggle_photo_upload":
            new_value = "False" if settings.get("photo_upload_enabled", "True") == "True" else "True"
            sheet_settings.update_cell(
                [r for r in sheet_settings.get_all_records() if r["Setting"] == "photo_upload_enabled"][0]["row"],
                2,
                new_value
            )
            await query.edit_message_text(f"📸 Photo upload {'enabled' if new_value == 'True' else 'disabled'}.")
        elif action == "toggle_weekly_summary":
            new_value = "False" if settings.get("weekly_summary", "False") == "True" else "True"
            sheet_settings.update_cell(
                [r for r in sheet_settings.get_all_records() if r["Setting"] == "weekly_summary"][0]["row"],
                2,
                new_value
            )
            await query.edit_message_text(f"🧾 Weekly summary {'enabled' if new_value == 'True' else 'disabled'}.")
        elif action == "set_snooze":
            buttons = [
                [InlineKeyboardButton(
                    f"{'✅' if settings.get('snooze_1h', 'True') == 'True' else '❌'} 1 Hour",
                    callback_data="toggle_snooze_1h"
                )],
                [InlineKeyboardButton(
                    f"{'✅' if settings.get('snooze_2h', 'True') == 'True' else '❌'} 2 Hours",
                    callback_data="toggle_snooze_2h"
                )],
                [InlineKeyboardButton(
                    f"{'✅' if settings.get('snooze_24h', 'True') == 'True' else '❌'} 24 Hours",
                    callback_data="toggle_snooze_24h"
                )]
            ]
            await query.message.reply_text("⏰ Set snooze times:", reply_markup=InlineKeyboardMarkup(buttons))
        elif action.startswith("toggle_snooze_"):
            hours = action.split("_")[-1]
            new_value = "False" if settings.get(f"snooze_{hours}", "True") == "True" else "True"
            sheet_settings.update_cell(
                [r for r in sheet_settings.get_all_records() if r["Setting"] == f"snooze_{hours}"][0]["row"],
                2,
                new_value
            )
            await query.edit_message_text(f"⏰ Snooze {hours} {'enabled' if new_value == 'True' else 'disabled'}.")
    except Exception as e:
        logger.error(f"Error in handle_settings_action: {e}")
        await query.edit_message_text("❌ Error updating settings.")

@retry_gsheet_operation()
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    action = context.user_data.pop("await", None)
    text = update.message.text.strip()
    try:
        if action == "add_car":
            text = text.strip().upper()
            existing = [plate.strip().upper() for plate in sheet_cars.col_values(1)[1:]]
            if text in existing:
                return await update.message.reply_text("⚠️ That plate already exists.", reply_markup=ADMIN_MENU)
            sheet_cars.append_row([text])
            sheet_maintenance.append_row([text, "", ""])
            return await update.message.reply_text(f"✅ Car {text} added.", reply_markup=ADMIN_MENU)
        if action == "add_driver":
            name, user_id = map(str.strip, text.split(","))
            sheet_drivers.append_row([name, user_id])
            await update.message.reply_text(f"✅ Driver {name} added.", reply_markup=ADMIN_MENU)
            await driver_list_menu(update, context)
        if action == "add_admin":
            name, user_id = map(str.strip, text.split(","))
            sheet_admins.append_row([name, user_id])
            await update.message.reply_text(f"✅ Admin {name} added.", reply_markup=ADMIN_MENU)
        if action == "search_logs":
            car_plate, date_str = map(str.strip, text.split(","))
            car_plate = car_plate.upper()
            search_date = datetime.strptime(date_str, "%d-%m-%Y").date()
            logs = sheet_log.get_all_records()
            filtered_logs = [
                log for log in logs
                if log["Car Plate"].upper() == car_plate and
                datetime.strptime(log["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ).date() == search_date
            ]
            lines = [
                f'{datetime.strptime(log["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ).strftime("%d-%m-%Y, %I:%M %p")} - {log["Driver Name"]} {"took" if log["Action"] == "out" else "returned"} {log["Car Plate"]}'
                for log in filtered_logs
            ]
            await update.message.reply_text(
                f"🔍 Search Results for {car_plate} on {date_str}:\n\n{'\n'.join(lines) or 'No records found.'}",
                reply_markup=ADMIN_MENU
            )
        if action == "report_custom_date":
            search_date = datetime.strptime(text, "%d-%m-%Y").astimezone(UAE_TZ)
            logs = sheet_log.get_all_records()
            filtered_logs = [
                r for r in logs
                if datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ).date() == search_date.date()
            ]
            data = [
                [
                    datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ).strftime("%d-%m-%Y, %I:%M %p"),
                    r["Driver Name"],
                    r["Car Plate"],
                    "Took" if r["Action"] == "out" else "Returned"
                ]
                for r in filtered_logs
            ]
            pdf_buffer = generate_pdf(f"Full Usage Report ({text})", data, ["Timestamp", "Driver Name", "Car Plate", "Action"])
            await update.message.reply_document(
                document=pdf_buffer,
                filename=f"full_report_{text}.pdf",
                caption=f"🧾 Full usage report for {text}."
            )
    except Exception as e:
        logger.error(f"Error in text_handler: {e}")
        await update.message.reply_text("❌ An error occurred.", reply_markup=ADMIN_MENU)

@retry_gsheet_operation()
async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "await" not in context.user_data or context.user_data["await"] != "upload_photo":
        return
    try:
        photo = update.message.photo[-1]
        user = update.effective_user.first_name
        ts = datetime.now(UAE_TZ)
        ts_storage = ts.strftime("%Y-%m-%d %H:%M")
        
        # Prompt for car plate
        context.user_data["await"] = "photo_car_plate"
        context.user_data["photo_file_id"] = photo.file_id
        await update.message.reply_text("📸 Enter the car plate for this photo:", reply_markup=MAIN_MENU)
    except Exception as e:
        logger.error(f"Error in photo_handler: {e}")
        await update.message.reply_text("❌ Error processing photo.", reply_markup=MAIN_MENU)

@retry_gsheet_operation()
async def photo_car_plate_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "await" not in context.user_data or context.user_data["await"] != "photo_car_plate":
        return
    try:
        car_plate = update.message.text.strip().upper()
        user = update.effective_user.first_name
        ts = datetime.now(UAE_TZ)
        ts_storage = ts.strftime("%Y-%m-%d %H:%M")
        file_id = context.user_data.pop("photo_file_id", None)
        if not file_id:
            return await update.message.reply_text("❌ No photo found.", reply_markup=MAIN_MENU)
        
        # Upload to Google Drive
        file = await context.bot.get_file(file_id)
        file_bytes = await file.download_as_bytearray()
        drive_file = drive.CreateFile({'title': f"{car_plate}_{ts_storage}.jpg"})
        drive_file.SetContentString(file_bytes)
        drive_file.Upload()
        drive_link = drive_file['alternateLink']
        
        # Log to Photos worksheet
        sheet_photos.append_row([ts_storage, user, car_plate, file_id, drive_link])
        await update.message.reply_text(f"✅ Photo for {car_plate} uploaded.", reply_markup=MAIN_MENU)
    except Exception as e:
        logger.error(f"Error in photo_car_plate_handler: {e}")
        await update.message.reply_text("❌ Error uploading photo.", reply_markup=MAIN_MENU)

@retry_gsheet_operation()
async def on_car_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action, plate = query.data.split("|")
        user = update.effective_user.first_name
        user_id = update.effective_user.id
        ts = datetime.now(UAE_TZ)
        ts_display = ts.strftime("%d-%m-%Y, %I:%M %p")
        ts_storage = ts.strftime("%Y-%m-%d %H:%M")
        normalized_plate = str(plate).strip().upper()
        
        if action == "take":
            logs = sheet_log.get_all_records()
            logs.sort(key=lambda x: x["Timestamp"])
            last_action = {r["Car Plate"]: r["Action"] for r in logs}
            user_cars = [cp for cp, rec in last_action.items() if rec == "out" and logs[list(last_action.keys()).index(cp)]["Driver Name"] == user]
            if user_cars:
                menu = COMBINED_MENU if str(user_id) in [str(r["User ID"]) for r in sheet_admins.get_all_records()] else MAIN_MENU
                await query.edit_message_text(f"🚫 You already have a car ({user_cars[0]}).", reply_markup=menu)
                return
            if last_action.get(normalized_plate) == "out":
                await query.edit_message_text(f"⚠️ Car {plate} is already in use.")
                return
            sheet_log.append_row([ts_storage, user, plate, "out"])
            await query.edit_message_text(f"✅ You took {plate} at {ts_display}")
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"🚗 {plate} taken by {user} at {ts_display}"
            )
        else:
            sheet_log.append_row([ts_storage, user, plate, "in"])
            await query.edit_message_text(f"↩️ You returned {plate} at {ts_display}")
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"✅ {plate} returned by {user} at {ts_display}"
            )
            # Update maintenance schedule
            maintenance_records = sheet_maintenance.get_all_records()
            for record in maintenance_records:
                if record["Car Plate"] == plate:
                    last_maintenance = record["Last Maintenance"] or ts_storage
                    next_maintenance = (
                        datetime.strptime(last_maintenance, "%Y-%m-%d %H:%M").astimezone(UAE_TZ) + 
                        timedelta(days=MAINTENANCE_INTERVAL_DAYS)
                    ).strftime("%Y-%m-%d %H:%M")
                    sheet_maintenance.update_cell(
                        maintenance_records.index(record) + 2,
                        3,
                        next_maintenance
                    )
                    break
    except Exception as e:
        logger.error(f"Error in on_car_action: {e}")
        await query.edit_message_text("❌ Error processing car action.")

@retry_gsheet_operation()
async def check_snoozed_requests(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(UAE_TZ)
    snoozed = context.bot_data.get("snoozed_requests", [])
    for req in snoozed[:]:
        if now >= req["remind_time"]:
            buttons = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Approve", callback_data=f"approve|{req['user_id']}|{req['user_name']}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"reject|{req['user_id']}"),
                    InlineKeyboardButton("⏰ Snooze", callback_data=f"snooze|{req['user_id']}|{req['user_name']}")
                ]
            ])
            await context.bot.send_message(
                chat_id=req["chat_id"],
                text=(
                    f"⏰ Reminder: Access Request\n"
                    f"Name: {req['user_name']}\n"
                    f"User ID: {req['user_id']}"
                ),
                reply_markup=buttons
            )
            snoozed.remove(req)
    context.bot_data["snoozed_requests"] = snoozed

@retry_gsheet_operation()
async def send_weekly_summary(context: ContextTypes.DEFAULT_TYPE):
    settings = {r["Setting"]: r["Value"] for r in sheet_settings.get_all_records()}
    if settings.get("weekly_summary", "False") != "True":
        return
    try:
        logs = sheet_log.get_all_records()
        start_date = datetime.now(UAE_TZ) - timedelta(days=7)
        logs = [r for r in logs if datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ) >= start_date]
        data = [
            [
                datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ).strftime("%d-%m-%Y, %I:%M %p"),
                r["Driver Name"],
                r["Car Plate"],
                "Took" if r["Action"] == "out" else "Returned"
            ]
            for r in logs
        ]
        pdf_buffer = generate_pdf("Weekly Usage Summary", data, ["Timestamp", "Driver Name", "Car Plate", "Action"])
        await context.bot.send_document(
            chat_id=ADMIN_CHAT_ID,
            document-mod=pdf_buffer,
            filename="weekly_summary.pdf",
            caption="🧾 Weekly usage summary."
        )
    except Exception as e:
        logger.error(f"Error in send_weekly_summary: {e}")

@retry_gsheet_operation()
async def send_monthly_stats(context: ContextTypes.DEFAULT_TYPE):
    try:
        logs = sheet_log.get_all_records()
        start_date = datetime.now(UAE_TZ) - timedelta(days=30)
        logs = [r for r in logs if datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ) >= start_date]
        
        # Calculate stats
        driver_counts = {}
        car_counts = {}
        for r in logs:
            if r["Action"] == "out":
                driver_counts[r["Driver Name"]] = driver_counts.get(r["Driver Name"], 0) + 1
                car_counts[r["Car Plate"]] = car_counts.get(r["Car Plate"], 0) + 1
        
        top_drivers = sorted(driver_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        top_cars = sorted(car_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        
        lines = [
            "📊 Monthly Stats",
            "Top Drivers:",
            *[f"{d}: {c} trips" for d, c in top_drivers],
            "Most Used Cars:",
            *[f"{c}: {u} uses" for c, u in top_cars]
        ]
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text="\n".join(lines)
        )
    except Exception ase:
        logger.error(f"Error in send_monthly_stats: {e}")

@retry_gsheet_operation()
async def check_maintenance_reminders(context: ContextTypes.DEFAULT_TYPE):
    try:
        maintenance_records = sheet_maintenance.get_all_records()
        now = datetime.now(UAE_TZ)
        for record in maintenance_records:
            if record["Next Maintenance"]:
                next_maintenance = datetime.strptime(record["Next Maintenance"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ)
                if now >= next_maintenance:
                    await context.bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=f"🛠 Maintenance due for {record['Car Plate']}!"
                    )
                    # Update next maintenance
                    sheet_maintenance.update_cell(
                        maintenance_records.index(record) + 2,
                        2,
                        now.strftime("%Y-%m-%d %H:%M")
                    )
                    sheet_maintenance.update_cell(
                        maintenance_records.index(record) + 2,
                        3,
                        (now + timedelta(days=MAINTENANCE_INTERVAL_DAYS)).strftime("%Y-%m-%d %H:%M")
                    )
    except Exception as e:
        logger.error(f"Error in check_maintenance_reminders: {e}")

def main():
    try:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("myhistory", my_history))
        app.add_handler(MessageHandler(filters.Regex("^🛠️ Admin Panel$"), admin_menu))
        app.add_handler(MessageHandler(filters.Regex("^➕ Add Car$"), add_car_prompt))
        app.add_handler(MessageHandler(filters.Regex("^➖ Remove Car$"), remove_car_prompt))
        app.add_handler(MessageHandler(filters.Regex("^📋 Driver List$"), driver_list_menu))
        app.add_handler(MessageHandler(filters.Regex(r"^(⬅️ Main Menu|⬅\ufe0f Main Menu)$"), back_to_main_menu))
        app.add_handler(MessageHandler(filters.Regex("^🚗 Take Car$"), take_car_menu))
        app.add_handler(MessageHandler(filters.Regex("^↩️ Return Car$"), return_car_menu))
        app.add_handler(MessageHandler(filters.Regex("^📊 Status$"), status_menu))
        app.add_handler(MessageHandler(filters.Regex("^🔍 History$"), history_menu))
        app.add_handler(MessageHandler(filters.Regex("^🧾 Generate Report$"), generate_report))
        app.add_handler(MessageHandler(filters.Regex("^📸 Upload Photo$"), upload_photo))
        app.add_handler(MessageHandler(filters.Regex("^➕ Add Admin$"), add_admin_prompt))
        app.add_handler(MessageHandler(filters.Regex("^➖ Remove Admin$"), remove_admin_prompt))
        app.add_handler(MessageHandler(filters.Regex("^📄 List Admins$"), list_admins))
        app.add_handler(MessageHandler(filters.Regex("^⚙️ Settings$"), settings_menu))
        app.add_handler(MessageHandler(filters.Regex("^🔄 Refresh Sheet$"), refresh_sheet))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
        app.add_handler(MessageHandler(filters.PHOTO, photo_handler))
        app.add_handler(CallbackQueryHandler(on_car_action, pattern="^(take|return)\\|"))
        app.add_handler(CallbackQueryHandler(handle_access_request, pattern="^(approve|reject|snooze)\\|"))
        app.add_handler(CallbackQueryHandler(handle_snooze_time, pattern="^snooze_time\\|"))
        app.add_handler(CallbackQueryHandler(handle_driver_action, pattern="^(remove_driver|add_driver)"))
        app.add_handler(CallbackQueryHandler(handle_remove_car_action, pattern="^remove_car\\|"))
        app.add_handler(CallbackQueryHandler(handle_admin_action, pattern="^remove_admin\\|"))
        app.add_handler(CallbackQueryHandler(handle_report_action, pattern="^report\\|"))
        app.add_handler(CallbackQueryHandler(handle_settings_action, pattern="^(toggle_photo_upload|toggle_weekly_summary|set_snooze|toggle_snooze_.*)"))
        
        # Schedule tasks
        scheduler.add_job(check_snoozed_requests, 'interval', minutes=1, args=[app])
        scheduler.add_job(send_weekly_summary, CronTrigger(day_of_week='sun', hour=9), args=[app])
        scheduler.add_job(send_monthly_stats, CronTrigger(day=1, hour=9), args=[app])
        scheduler.add_job(check_maintenance_reminders, 'interval', hours=24, args=[app])
        scheduler.start()
        
        logger.info("Bot started.")
        app.run_polling(timeout=10)
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()
