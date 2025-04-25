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
from dotenv import load_dotenv
import pytz

# Configure logging
load_dotenv()
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_SHEETS_JSON = os.getenv("GOOGLE_SHEETS_JSON")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x]
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
DRIVERS_WORKSHEET_NAME = os.getenv("DRIVERS_WORKSHEET_NAME", "Drivers")
LOG_WORKSHEET_NAME = os.getenv("LOG_WORKSHEET_NAME", "Log")

# Validate environment variables
if not all([TELEGRAM_TOKEN, GOOGLE_SHEETS_JSON, SPREADSHEET_ID]):
    logger.error("Missing one of TELEGRAM_BOT_TOKEN, GOOGLE_SHEETS_JSON, or SPREADSHEET_ID")
    exit(1)

# Initialize Google Sheets
try:
    creds_dict = json.loads(GOOGLE_SHEETS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
    gs = gspread.authorize(creds)
    sh = gs.open_by_key(SPREADSHEET_ID)

    # Initialize worksheets
    try:
        sheet_log = sh.worksheet(LOG_WORKSHEET_NAME)
        # Verify headers
        log_data = sheet_log.get_all_values()
        required_headers = ["Timestamp", "Driver Name", "Car Plate", "Action"]
        if not log_data or log_data[0] != required_headers:
            logger.warning(f"Log worksheet '{LOG_WORKSHEET_NAME}' missing headers. Resetting them...")
            sheet_log.clear()
            sheet_log.append_row(required_headers)
    except gspread.exceptions.WorksheetNotFound:
        logger.warning(f"Log worksheet '{LOG_WORKSHEET_NAME}' not found. Creating it...")
        sheet_log = sh.add_worksheet(title=LOG_WORKSHEET_NAME, rows=100, cols=4)
        sheet_log.append_row(["Timestamp", "Driver Name", "Car Plate", "Action"])

    try:
        sheet_drivers = sh.worksheet(DRIVERS_WORKSHEET_NAME)
        # Check and create headers if missing
        driver_data = sheet_drivers.get_all_values()
        if not driver_data or driver_data[0] != ["Name", "User ID"]:
            logger.warning(f"Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' missing headers. Creating them...")
            sheet_drivers.clear()
            sheet_drivers.append_row(["Name", "User ID"])
    except gspread.exceptions.WorksheetNotFound:
        logger.warning(f"Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' not found. Creating it...")
        sheet_drivers = sh.add_worksheet(title=DRIVERS_WORKSHEET_NAME, rows=100, cols=2)
        sheet_drivers.append_row(["Name", "User ID"])

    sheet_cars = sh.worksheet("Cars")
except gspread.exceptions.WorksheetNotFound as e:
    logger.error(f"Worksheet not found: {e}")
    exit(1)
except Exception as e:
    logger.error(f"Failed to initialize Google Sheets: {e}")
    exit(1)

# Define keyboard layouts
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

# Define UAE time zone (UTC+4)
UAE_TZ = pytz.timezone("Asia/Dubai")

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
        if user_id not in ADMINS:
            await update.message.reply_text("❌ Access denied. Admin privileges required.")
            return
        return await func(update, context)
    return wrapper

# Decorator for admin or driver access
def admin_or_driver(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        try:
            driver_ids = sheet_drivers.col_values(2)[1:]
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' not found")
            await update.message.reply_text(f"❌ Error: Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' not found in Google Sheets.")
            return
        except Exception as e:
            logger.error(f"Error fetching driver IDs: {e}")
            await update.message.reply_text("❌ Server error. Please try again later.")
            return
        # Corrected logic: Deny access only if user is neither an admin nor a driver
        if user_id not in ADMINS and str(user_id) not in driver_ids:
            logger.debug(f"User {user_id} denied access (not in ADMINS: {user_id not in ADMINS}, not in driver_ids: {str(user_id) not in driver_ids})")
            await update.message.reply_text("❌ Access denied. You are not registered.")
            return
        logger.debug(f"User {user_id} granted access (in ADMINS: {user_id in ADMINS}, in driver_ids: {str(user_id) in driver_ids})")
        return await func(update, context)
    return wrapper

# Handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        driver_ids = sheet_drivers.col_values(2)[1:]
        logger.debug(f"Driver IDs fetched: {driver_ids}")
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' not found in start handler")
        await update.message.reply_text(f"❌ Error: Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' not found in Google Sheets.")
        return
    except Exception as e:
        logger.error(f"Error in start handler: {e}")
        await update.message.reply_text("❌ Server error. Please try again later.")
        return
    if user_id in ADMINS or str(user_id) in driver_ids:
        logger.debug(f"User {user_id} is authorized (admin: {user_id in ADMINS}, driver: {str(user_id) in driver_ids})")
        # Admins get the combined menu, drivers get the main menu
        menu = COMBINED_MENU if user_id in ADMINS else MAIN_MENU
        await update.message.reply_text("Welcome! Choose an option:", reply_markup=menu)
    else:
        logger.debug(f"User {user_id} is not authorized. Sending access request.")
        await update.message.reply_text("❌ Access denied. Requesting access from admin...")
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Approve", callback_data=f"approve|{user_id}|{update.effective_user.full_name}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"reject|{user_id}")
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
        all_cars = sheet_cars.col_values(1)[1:]  # Skip header
        if not all_cars:
            return await update.message.reply_text("🚫 No cars available to remove.", reply_markup=ADMIN_MENU)
        
        # Check which cars are in use
        logs = sheet_log.get_all_records()
        last_action = {r["Car Plate"]: r["Action"] for r in logs}
        
        buttons = []
        for car in all_cars:
            if last_action.get(car) == "out":
                # Car is in use, show without remove button
                buttons.append([InlineKeyboardButton(f"{car} (In Use)", callback_data="noop")])
            else:
                # Car is available, show with remove button
                buttons.append([InlineKeyboardButton(f"{car} - Remove", callback_data=f"remove_car|{car}")])
        
        await update.message.reply_text("Select a car to remove (cars in use cannot be removed):", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.error(f"Error in remove_car_prompt: {e}")
        await update.message.reply_text("❌ Error fetching cars to remove. Please try again.", reply_markup=ADMIN_MENU)

@admin_only
async def add_driver_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["await"] = "add_driver"
    await update.message.reply_text("➕ Send the DRIVER NAME and TELEGRAM USER ID (comma-separated, e.g., John Doe, 123456789):", reply_markup=ADMIN_MENU)

@retry_gsheet_operation()
@admin_only
async def driver_list_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Fetch all data as a list of lists to validate structure
        driver_data = sheet_drivers.get_all_values()
        if not driver_data:
            buttons = [[InlineKeyboardButton("➕ Add Driver", callback_data="add_driver")]]
            return await update.message.reply_text(
                "📋 No drivers found in the system (worksheet is empty).",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        
        # Check for headers
        if len(driver_data) < 1 or not driver_data[0]:
            return await update.message.reply_text("❌ Error: Drivers worksheet is empty or missing headers. Expected headers: 'Name', 'User ID'.", reply_markup=ADMIN_MENU)
        
        headers = driver_data[0]
        if "Name" not in headers or "User ID" not in headers:
            return await update.message.reply_text("❌ Error: Drivers worksheet missing required headers. Expected: 'Name', 'User ID'.", reply_markup=ADMIN_MENU)
        
        # Fetch records using get_all_records
        drivers = sheet_drivers.get_all_records()
        buttons = [
            [InlineKeyboardButton(f"{d['Name']} (ID: {d['User ID']}) - Remove", callback_data=f"remove_driver|{d['Name']}")]
            for d in drivers
        ]
        # Add the "Add Driver" button
        buttons.append([InlineKeyboardButton("➕ Add Driver", callback_data="add_driver")])
        
        await update.message.reply_text(
            "📋 Driver List (select to remove or add a driver):",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' not found")
        await update.message.reply_text(f"❌ Error: Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' not found in Google Sheets. Please check the spreadsheet configuration.", reply_markup=ADMIN_MENU)
    except gspread.exceptions.APIError as e:
        error_details = e.response.json() if hasattr(e.response, 'json') else str(e)
        logger.error(f"Google Sheets API error: {error_details}")
        if "quota" in error_details.lower():
            await update.message.reply_text("❌ Error: Google Sheets API quota exceeded. Please try again later or contact the admin.", reply_markup=ADMIN_MENU)
        else:
            await update.message.reply_text("❌ Error fetching driver list due to API issue. Please try again later.", reply_markup=ADMIN_MENU)
    except ValueError as e:
        logger.error(f"ValueError in driver_list_menu: {e}")
        await update.message.reply_text("❌ Error: Drivers worksheet data is malformed. Ensure it has proper headers ('Name', 'User ID') and data rows.", reply_markup=ADMIN_MENU)
    except Exception as e:
        logger.error(f"Unexpected error in driver_list_menu: {e}")
        await update.message.reply_text("❌ Error fetching driver list. Please try again or contact the admin.", reply_markup=ADMIN_MENU)

@retry_gsheet_operation()
@admin_only
async def status_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logs = sheet_log.get_all_records()
        # Sort logs by timestamp to ensure the most recent action is last
        logs.sort(key=lambda x: x["Timestamp"])
        # Normalize car plates and build last_action dictionary with driver info
        last = {}
        car_driver = {}  # Track which driver has each car
        for r in logs:
            car_plate = str(r["Car Plate"]).strip().upper()
            last[car_plate] = r["Action"]
            if r["Action"] == "out":
                car_driver[car_plate] = r["Driver Name"]
        logger.debug(f"Raw Log data: {logs}")
        logger.debug(f"Last actions for cars in status_menu: {last}")
        logger.debug(f"Car to driver mapping in status_menu: {car_driver}")
        
        all_cars = sheet_cars.col_values(1)[1:]
        logger.debug(f"Raw Cars data: {all_cars}")
        status_lines = []
        for car in all_cars:
            normalized_car = str(car).strip().upper()
            state = last.get(normalized_car, "✅ Available")
            if state == "out":
                driver = car_driver.get(normalized_car, "Unknown")
                label = f"❌ Out (Driver: {driver})"
            else:
                label = "✅ Available"
            status_lines.append(f"{label} — {car}")
            logger.debug(f"Car {car} (normalized: {normalized_car}) status: {label}")
        
        status_text = "\n".join(status_lines) or "No cars found."
        await update.message.reply_text(f"📊 Current Car Status:\n\n{status_text}", reply_markup=ADMIN_MENU)
    except Exception as e:
        logger.error(f"Error in status_menu: {e}")
        await update.message.reply_text("❌ Error fetching status. Please try again.", reply_markup=ADMIN_MENU)

@retry_gsheet_operation()
@admin_only
async def history_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Fetch all data as a list of lists to validate structure
        log_data = sheet_log.get_all_values()
        if not log_data:
            return await update.message.reply_text("🔍 No history records found (worksheet is empty).", reply_markup=ADMIN_MENU)
        
        # Check for headers
        if len(log_data) < 1 or not log_data[0]:
            return await update.message.reply_text("❌ Error: Log worksheet is empty or missing headers. Expected headers: 'Timestamp', 'Driver Name', 'Car Plate', 'Action'.", reply_markup=ADMIN_MENU)
        
        headers = log_data[0]
        required_headers = ["Timestamp", "Driver Name", "Car Plate", "Action"]
        if not all(h in headers for h in required_headers):
            return await update.message.reply_text("❌ Error: Log worksheet missing required headers. Expected: 'Timestamp', 'Driver Name', 'Car Plate', 'Action'.", reply_markup=ADMIN_MENU)
        
        # Fetch records using get_all_records
        logs = sheet_log.get_all_records()
        if not logs:
            return await update.message.reply_text("🔍 No history records found (no data rows).", reply_markup=ADMIN_MENU)
        
        # Show the latest 10 entries with the new timestamp format
        latest = logs[-10:]
        lines = [
            f'{datetime.strptime(r["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ).strftime("%d-%m-%Y, %I:%M %p")} - {r["Driver Name"]} {"took" if r["Action"] == "out" else "returned"} {r["Car Plate"]}'
            for r in latest
        ]
        await update.message.reply_text("🔍 Latest History:\n\n" + "\n".join(lines), reply_markup=ADMIN_MENU)
        
        # Prompt for search
        context.user_data["await"] = "search_logs"
        await update.message.reply_text(
            "🔍 Enter the car plate and date to search logs (format: CAR_PLATE, DD-MM-YYYY)\n"
            "Example: 1111111, 25-04-2025",
            reply_markup=ADMIN_MENU
        )
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Log worksheet '{LOG_WORKSHEET_NAME}' not found")
        await update.message.reply_text(f"❌ Error: Log worksheet '{LOG_WORKSHEET_NAME}' not found in Google Sheets. Please check the spreadsheet configuration.", reply_markup=ADMIN_MENU)
    except gspread.exceptions.APIError as e:
        error_details = e.response.json() if hasattr(e.response, 'json') else str(e)
        logger.error(f"Google Sheets API error: {error_details}")
        if "quota" in error_details.lower():
            await update.message.reply_text("❌ Error: Google Sheets API quota exceeded. Please try again later or contact the admin.", reply_markup=ADMIN_MENU)
        else:
            await update.message.reply_text("❌ Error fetching history due to API issue. Please try again later.", reply_markup=ADMIN_MENU)
    except ValueError as e:
        logger.error(f"ValueError in history_menu: {e}")
        await update.message.reply_text("❌ Error: Log worksheet data is malformed. Ensure it has proper headers ('Timestamp', 'Driver Name', 'Car Plate', 'Action') and data rows.", reply_markup=ADMIN_MENU)
    except Exception as e:
        logger.error(f"Error in history_menu: {e}")
        await update.message.reply_text("❌ Error fetching history. Please try again or contact the admin.", reply_markup=ADMIN_MENU)

async def back_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.debug(f"Back to main menu triggered by user {update.effective_user.id}")
    user_id = update.effective_user.id
    try:
        driver_ids = sheet_drivers.col_values(2)[1:]
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' not found")
        await update.message.reply_text(f"❌ Error: Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' not found in Google Sheets.")
        return
    except Exception as e:
        logger.error(f"Error fetching driver IDs in back_to_main_menu: {e}")
        await update.message.reply_text("❌ Server error. Please try again later.")
        return
    # Show COMBINED_MENU for admins, MAIN_MENU for drivers
    menu = COMBINED_MENU if user_id in ADMINS else MAIN_MENU
    await update.message.reply_text("🔙 Back to main menu:", reply_markup=menu)

@retry_gsheet_operation()
@admin_or_driver
async def take_car_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user.first_name
        user_id = update.effective_user.id
        # Fetch and sort logs
        logs = sheet_log.get_all_records()
        logs.sort(key=lambda x: x["Timestamp"])
        logger.debug(f"Raw Log data in take_car_menu: {logs}")
        
        # Build last_action dictionary with driver info
        last_action = {}
        car_driver = {}  # Track which driver has each car
        for r in logs:
            car_plate = str(r["Car Plate"]).strip().upper()
            last_action[car_plate] = r["Action"]
            if r["Action"] == "out":
                car_driver[car_plate] = r["Driver Name"]
        logger.debug(f"Last actions for cars: {last_action}")
        logger.debug(f"Car to driver mapping: {car_driver}")
        
        # Check if the current driver already has a car
        user_cars = [cp for cp, action in last_action.items() if action == "out" and car_driver.get(cp) == user]
        logger.debug(f"Driver {user} has cars: {user_cars}")
        if user_cars:
            menu = COMBINED_MENU if user_id in ADMINS else MAIN_MENU
            return await update.message.reply_text(f"🚫 You already have a car ({user_cars[0]}). Please return it before taking another.", reply_markup=menu)
        
        all_cars = sheet_cars.col_values(1)[1:]  # Skip header
        if not all_cars:
            menu = COMBINED_MENU if user_id in ADMINS else MAIN_MENU
            return await update.message.reply_text("🚫 No cars available in the system.", reply_markup=menu)
        
        # Filter only available cars
        available_cars = [car for car in all_cars if last_action.get(str(car).strip().upper()) != "out"]
        if not available_cars:
            menu = COMBINED_MENU if user_id in ADMINS else MAIN_MENU
            return await update.message.reply_text("🚫 No cars available to take right now.", reply_markup=menu)
        
        buttons = []
        for car in available_cars:
            buttons.append([InlineKeyboardButton(f"{car} (Available)", callback_data=f"take|{car}")])
            logger.debug(f"Car {car} (normalized: {str(car).strip().upper()}) is available.")
        
        await update.message.reply_text("Select a car to take:", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.error(f"Error in take_car_menu: {e}")
        menu = COMBINED_MENU if user_id in ADMINS else MAIN_MENU
        await update.message.reply_text("❌ Error fetching available cars. Please try again.", reply_markup=menu)

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
            menu = COMBINED_MENU if user_id in ADMINS else MAIN_MENU
            return await update.message.reply_text("✅ You have no cars to return.", reply_markup=menu)
        buttons = [[InlineKeyboardButton(c, callback_data=f"return|{c}")] for c in user_cars]
        await update.message.reply_text("Select a car to return:", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.error(f"Error in return_car_menu: {e}")
        menu = COMBINED_MENU if user_id in ADMINS else MAIN_MENU
        await update.message.reply_text("❌ Error fetching cars to return. Please try again.", reply_markup=menu)

@retry_gsheet_operation()
async def handle_access_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action, user_id, *user_name_parts = query.data.split("|")
        user_id = int(user_id)
        user_name = user_name_parts[0] if user_name_parts else "Unknown"
        if action == "approve":
            # Add the new driver to the Drivers worksheet
            sheet_drivers.append_row([user_name, str(user_id)])
            await query.edit_message_text(f"✅ Access approved for {user_name} (ID: {user_id})")
            await context.bot.send_message(user_id, "✅ Your access request was approved! You are now a driver.", reply_markup=MAIN_MENU)
            # Send notification to admin chat
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"✅ New driver {user_name} (ID: {user_id}) has been approved."
            )
        else:
            await query.edit_message_text(f"❌ Access rejected for {user_name} (ID: {user_id})")
            await context.bot.send_message(user_id, "❌ Your access request was rejected.")
    except Exception as e:
        logger.error(f"Error in handle_access_request: {e}")
        await query.edit_message_text("❌ Error processing request.")

@retry_gsheet_operation()
async def handle_driver_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action, *params = query.data.split("|")
        if action == "remove_driver":
            driver_name = params[0]
            # Fetch the first column (driver names)
            driver_data = sheet_drivers.get_all_values()
            if len(driver_data) <= 1:  # Only headers or empty
                await query.edit_message_text("⚠️ No drivers to remove.")
                return
            vals = sheet_drivers.col_values(1)[1:]  # Skip header
            if not vals or driver_name not in vals:
                await query.edit_message_text(f"⚠️ Driver {driver_name} not found.")
                return
            # Find the row number (adding 1 to account for header)
            row = vals.index(driver_name) + 2
            sheet_drivers.delete_rows(row, row)  # Use delete_rows for single row
            await query.edit_message_text(f"✅ Driver {driver_name} removed.")
        elif action == "add_driver":
            await query.message.reply_text("➕ Send the DRIVER NAME and TELEGRAM USER ID (comma-separated, e.g., John Doe, 123456789):", reply_markup=ADMIN_MENU)
            context.user_data["await"] = "add_driver"
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' not found")
        await query.edit_message_text(f"❌ Error: Drivers worksheet '{DRIVERS_WORKSHEET_NAME}' not found in Google Sheets.")
    except gspread.exceptions.APIError as e:
        error_details = e.response.json() if hasattr(e.response, 'json') else str(e)
        logger.error(f"Google Sheets API error in handle_driver_action: {error_details}")
        if "quota" in error_details.lower():
            await query.edit_message_text("❌ Error: Google Sheets API quota exceeded. Please try again later.")
        else:
            await query.edit_message_text("❌ Error: Failed to process driver action due to API issue. Please try again later.")
    except ValueError as e:
        logger.error(f"ValueError in handle_driver_action: {e}")
        await query.edit_message_text("❌ Error: Drivers worksheet data is malformed. Please check the worksheet.")
    except Exception as e:
        logger.error(f"Unexpected error in handle_driver_action: {e}")
        await query.edit_message_text("❌ Error processing driver action. Please try again or contact the admin.")

@retry_gsheet_operation()
async def handle_remove_car_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action, car_plate = query.data.split("|")
        if action != "remove_car":
            return
        # Check if the car is in use
        logs = sheet_log.get_all_records()
        last_action = {r["Car Plate"]: r["Action"] for r in logs}
        if last_action.get(car_plate) == "out":
            await query.edit_message_text(f"⚠️ Car {car_plate} is currently in use. It can only be removed after being returned.")
            return
        # Fetch all car plates
        all_cars = sheet_cars.col_values(1)
        if car_plate not in all_cars:
            await query.edit_message_text(f"⚠️ Car {car_plate} not found.")
            return
        # Find the row number (adding 1 to account for header)
        row = all_cars.index(car_plate) + 1
        sheet_cars.delete_rows(row, row)  # Use delete_rows for single row
        await query.edit_message_text(f"✅ Car {car_plate} removed.")
    except gspread.exceptions.WorksheetNotFound:
        logger.error("Cars worksheet not found")
        await query.edit_message_text("❌ Error: Cars worksheet not found in Google Sheets.")
    except gspread.exceptions.APIError as e:
        error_details = e.response.json() if hasattr(e.response, 'json') else str(e)
        logger.error(f"Google Sheets API error in handle_remove_car_action: {error_details}")
        if "quota" in error_details.lower():
            await query.edit_message_text("❌ Error: Google Sheets API quota exceeded. Please try again later.")
        else:
            await query.edit_message_text("❌ Error: Failed to remove car due to API issue. Please try again later.")
    except ValueError as e:
        logger.error(f"ValueError in handle_remove_car_action: {e}")
        await query.edit_message_text("❌ Error: Cars worksheet data is malformed. Please check the worksheet.")
    except Exception as e:
        logger.error(f"Unexpected error in handle_remove_car_action: {e}")
        await query.edit_message_text("❌ Error removing car. Please try again or contact the admin.")

@retry_gsheet_operation()
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    action = context.user_data.pop("await", None)
    text = update.message.text.strip()
    try:
        if action == "add_car":
            existing = sheet_cars.col_values(1)[1:]
            if text in existing:
                return await update.message.reply_text("⚠️ That plate already exists.", reply_markup=ADMIN_MENU)
            sheet_cars.append_row([text])
            return await update.message.reply_text(f"✅ Car {text} added.", reply_markup=ADMIN_MENU)
        if action == "add_driver":
            try:
                name, user_id = map(str.strip, text.split(","))
                sheet_drivers.append_row([name, user_id])
                await update.message.reply_text(f"✅ Driver {name} added with ID {user_id}.", reply_markup=ADMIN_MENU)
                # Refresh the driver list
                await driver_list_menu(update, context)
            except ValueError:
                await update.message.reply_text("❌ Invalid format. Use: Name, TelegramUserID (e.g., John Doe, 123456789)", reply_markup=ADMIN_MENU)
        if action == "search_logs":
            try:
                # Parse the input: expected format "CAR_PLATE, DD-MM-YYYY"
                car_plate, date_str = map(str.strip, text.split(","))
                car_plate = car_plate.upper()
                search_date = datetime.strptime(date_str, "%d-%m-%Y").date()
                
                # Fetch logs and filter by car plate and date
                logs = sheet_log.get_all_records()
                filtered_logs = [
                    log for log in logs
                    if log["Car Plate"].upper() == car_plate and
                    datetime.strptime(log["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ).date() == search_date
                ]
                
                if not filtered_logs:
                    return await update.message.reply_text(f"🔍 No records found for car {car_plate} on {date_str}.", reply_markup=ADMIN_MENU)
                
                # Format the filtered logs with the new timestamp format
                lines = [
                    f'{datetime.strptime(log["Timestamp"], "%Y-%m-%d %H:%M").astimezone(UAE_TZ).strftime("%d-%m-%Y, %I:%M %p")} - {log["Driver Name"]} {"took" if log["Action"] == "out" else "returned"} {log["Car Plate"]}'
                    for log in filtered_logs
                ]
                await update.message.reply_text(f"🔍 Search Results for {car_plate} on {date_str}:\n\n" + "\n".join(lines), reply_markup=ADMIN_MENU)
            except ValueError as e:
                logger.error(f"Error parsing search input: {e}")
                await update.message.reply_text(
                    "❌ Invalid format. Please use: CAR_PLATE, DD-MM-YYYY\n"
                    "Example: 1111111, 25-04-2025",
                    reply_markup=ADMIN_MENU
                )
            except Exception as e:
                logger.error(f"Error in search_logs: {e}")
                await update.message.reply_text("❌ Error searching logs. Please try again.", reply_markup=ADMIN_MENU)
    except Exception as e:
        logger.error(f"Error in text_handler: {e}")
        await update.message.reply_text("❌ An error occurred. Please try again.", reply_markup=ADMIN_MENU)

@retry_gsheet_operation()
async def on_car_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        action, plate = query.data.split("|")
        user = update.effective_user.first_name
        user_id = update.effective_user.id
        # Get current time in UAE time zone
        ts = datetime.now(UAE_TZ)
        # Format the timestamp for display
        ts_display = ts.strftime("%d-%m-%Y, %I:%M %p")
        # Format the timestamp for storage in the Log worksheet
        ts_storage = ts.strftime("%Y-%m-%d %H:%M")
        normalized_plate = str(plate).strip().upper()
        
        if action == "take":
            # Double-check if the driver already has a car
            logs = sheet_log.get_all_records()
            logs.sort(key=lambda x: x["Timestamp"])
            last_action = {r["Car Plate"]: r["Action"] for r in logs}
            user_cars = [cp for cp, rec in last_action.items() if rec == "out" and logs[list(last_action.keys()).index(cp)]["Driver Name"] == user]
            if user_cars:
                menu = COMBINED_MENU if user_id in ADMINS else MAIN_MENU
                await query.edit_message_text(f"🚫 You already have a car ({user_cars[0]}). Please return it before taking another.", reply_markup=menu)
                return
            # Double-check if the car is already in use
            if last_action.get(normalized_plate) == "out":
                menu = COMBINED_MENU if user_id in ADMINS else MAIN_MENU
                await query.edit_message_text(f"⚠️ Car {plate} is already in use by another driver.", reply_markup=menu)
                return
            # Log the take action
            sheet_log.append_row([ts_storage, user, plate, "out"])
            logger.debug(f"Logged take action: {user} took {plate} at {ts_storage}")
            await query.edit_message_text(f"✅ You took {plate} at {ts_display}")
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"🚗 {plate} taken by {user} at {ts_display}"
            )
            logger.debug(f"Notification sent to admin chat {ADMIN_CHAT_ID}: {plate} taken by {user}")
        else:
            sheet_log.append_row([ts_storage, user, plate, "in"])
            logger.debug(f"Logged return action: {user} returned {plate} at {ts_storage}")
            await query.edit_message_text(f"↩️ You returned {plate} at {ts_display}")
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"✅ {plate} returned by {user} at {ts_display}"
            )
            logger.debug(f"Notification sent to admin chat {ADMIN_CHAT_ID}: {plate} returned by {user}")
    except Exception as e:
        logger.error(f"Error in on_car_action: {e}")
        await query.edit_message_text("❌ Error processing car action.")

def main():
    try:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(MessageHandler(filters.Regex("^🛠️ Admin Panel$"), admin_menu))
        app.add_handler(MessageHandler(filters.Regex("^➕ Add Car$"), add_car_prompt))
        app.add_handler(MessageHandler(filters.Regex("^➖ Remove Car$"), remove_car_prompt))
        app.add_handler(MessageHandler(filters.Regex("^📋 Driver List$"), driver_list_menu))
        app.add_handler(MessageHandler(filters.Regex(r"^(⬅️ Main Menu|⬅\ufe0f Main Menu)$"), back_to_main_menu))
        app.add_handler(MessageHandler(filters.Regex("^🚗 Take Car$"), take_car_menu))
        app.add_handler(MessageHandler(filters.Regex("^↩️ Return Car$"), return_car_menu))
        app.add_handler(MessageHandler(filters.Regex("^📊 Status$"), status_menu))
        app.add_handler(MessageHandler(filters.Regex("^🔍 History$"), history_menu))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
        app.add_handler(CallbackQueryHandler(on_car_action, pattern="^(take|return)\\|"))
        app.add_handler(CallbackQueryHandler(handle_access_request, pattern="^(approve|reject)\\|"))
        app.add_handler(CallbackQueryHandler(handle_driver_action, pattern="^(remove_driver|add_driver)"))
        app.add_handler(CallbackQueryHandler(handle_remove_car_action, pattern="^remove_car\\|"))
        logger.info("Bot started.")
        app.run_polling(timeout=10)
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()"# Comment for redeploy" 
"# Comment for redeploy" 
"# Comment for redeploy" 
"# Comment for redeploy" 
