import os
import json
import logging
from datetime import datetime, timedelta
import time
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
import pytz
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
import tempfile

# Configure logging
load_dotenv()
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_SHEETS_JSON = os.getenv("GOOGLE_SHEETS_JSON")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x]
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
DRIVERS_WORKSHEET_NAME = os.getenv("DRIVERS_WORKSHEET_NAME", "Drivers")
LOG_WORKSHEET_NAME = os.getenv("LOG_WORKSHEET_NAME", "Log")
CARS_WORKSHEET_NAME = os.getenv("CARS_WORKSHEET_NAME", "Cars")
MAINTENANCE_WORKSHEET_NAME = os.getenv("MAINTENANCE_WORKSHEET_NAME", "Maintenance")
PHOTOS_WORKSHEET_NAME = os.getenv("PHOTOS_WORKSHEET_NAME", "Photos")

# Validate environment variables
if not all([TELEGRAM_TOKEN, GOOGLE_SHEETS_JSON, SPREADSHEET_ID]):
    logger.error("Missing required environment variables")
    exit(1)

# Initialize Google Services
try:
    creds_dict = json.loads(GOOGLE_SHEETS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
    gs = gspread.authorize(creds)
    sh = gs.open_by_key(SPREADSHEET_ID)
    
    # Initialize Google Drive service
    drive_creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)
    drive_service = build('drive', 'v3', credentials=drive_creds)

    # Initialize worksheets
    def init_worksheet(name, headers):
        try:
            ws = sh.worksheet(name)
            current_headers = ws.row_values(1)
            if current_headers != headers:
                ws.clear()
                ws.append_row(headers)
            return ws
        except gspread.exceptions.WorksheetNotFound:
            logger.warning(f"Worksheet '{name}' not found. Creating it...")
            ws = sh.add_worksheet(title=name, rows=100, cols=len(headers))
            ws.append_row(headers)
            return ws

    sheet_log = init_worksheet(LOG_WORKSHEET_NAME, ["Timestamp", "Driver Name", "Driver ID", "Car Plate", "Action"])
    sheet_drivers = init_worksheet(DRIVERS_WORKSHEET_NAME, ["Name", "User ID", "Is Admin"])
    sheet_cars = init_worksheet(CARS_WORKSHEET_NAME, ["Plate", "Model", "Year", "Last Maintenance", "Next Maintenance"])
    sheet_maintenance = init_worksheet(MAINTENANCE_WORKSHEET_NAME, ["Car Plate", "Date", "Type", "Notes", "Mileage"])
    sheet_photos = init_worksheet(PHOTOS_WORKSHEET_NAME, ["Timestamp", "Car Plate", "Driver ID", "Photo URL", "Type", "Notes"])

except Exception as e:
    logger.error(f"Failed to initialize Google services: {e}")
    exit(1)

# Define UAE time zone (UTC+4)
UAE_TZ = pytz.timezone("Asia/Dubai")

# Keyboard layouts
MAIN_MENU = ReplyKeyboardMarkup([
    ["🚗 Take Car", "↩️ Return Car"],
    ["📸 Upload Photo", "📄 My History"],
    ["⬅️ Main Menu"]
], resize_keyboard=True)

ADMIN_MENU = ReplyKeyboardMarkup([
    ["➕ Add Car", "➖ Remove Car"],
    ["👥 Driver List", "👑 Admin Management"],
    ["📊 Status", "🔍 History", "🧾 Generate Report"],
    ["🛠 Maintenance", "⬅️ Main Menu"]
], resize_keyboard=True)

COMBINED_MENU = ReplyKeyboardMarkup([
    ["🚗 Take Car", "↩️ Return Car"],
    ["📸 Upload Photo", "📄 My History"],
    ["➕ Add Car", "➖ Remove Car"],
    ["👥 Driver List", "👑 Admin Management"],
    ["📊 Status", "🔍 History", "🧾 Generate Report"],
    ["🛠 Maintenance", "⬅️ Main Menu"]
], resize_keyboard=True)

PHOTO_TYPES = [
    ["⛽ Fuel Meter", "🛠 Condition"],
    ["📝 Notes", "⬅️ Main Menu"]
]

ADMIN_MANAGEMENT = ReplyKeyboardMarkup([
    ["➕ Add Admin", "➖ Remove Admin"],
    ["📄 List Admins", "⬅️ Main Menu"]
], resize_keyboard=True)

REPORT_TYPES = ReplyKeyboardMarkup([
    ["📊 Driver Report", "📈 Full Report"],
    ["🗓 Weekly Summary", "⬅️ Main Menu"]
], resize_keyboard=True)

# Utility functions
def is_admin(user_id):
    try:
        drivers = sheet_drivers.get_all_records()
        return any(d['User ID'] == str(user_id) and d.get('Is Admin', '').lower() == 'true' for d in drivers) or user_id in ADMINS
    except Exception as e:
        logger.error(f"Error checking admin status: {e}")
        return False

async def notify_admins(context: ContextTypes.DEFAULT_TYPE, message: str):
    try:
        drivers = sheet_drivers.get_all_records()
        admin_ids = [int(d['User ID']) for d in drivers if d.get('Is Admin', '').lower() == 'true']
        all_admins = set(ADMINS + admin_ids)
        
        for admin_id in all_admins:
            try:
                await context.bot.send_message(chat_id=admin_id, text=message)
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {e}")
    except Exception as e:
        logger.error(f"Error in notify_admins: {e}")

def upload_to_drive(file_path, file_name, mime_type='image/jpeg'):
    try:
        file_metadata = {
            'name': file_name,
            'parents': [DRIVE_FOLDER_ID]
        }
        media = MediaFileUpload(file_path, mimetype=mime_type)
        file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,webViewLink'
        ).execute()
        return file.get('webViewLink')
    except Exception as e:
        logger.error(f"Error uploading to Drive: {e}")
        return None

# PDF Generation functions
async def generate_driver_report(driver_id, driver_name):
    try:
        # Create a file-like buffer to receive PDF data
        buffer = io.BytesIO()
        
        # Create the PDF object
        doc = SimpleDocTemplate(buffer, pagesize=letter)
        styles = getSampleStyleSheet()
        
        # Get driver's history
        logs = sheet_log.get_all_records()
        driver_logs = [log for log in logs if str(log.get('Driver ID')) == str(driver_id)]
        driver_logs.sort(key=lambda x: x['Timestamp'], reverse=True)
        
        # Prepare data for table
        data = [['Date', 'Car Plate', 'Action']]
        for log in driver_logs[-30:]:  # Last 30 entries
            date = datetime.strptime(log['Timestamp'], "%Y-%m-%d %H:%M").strftime("%d-%m-%Y %H:%M")
            data.append([date, log['Car Plate'], 'Taken' if log['Action'] == 'out' else 'Returned'])
        
        # Create elements
        elements = []
        elements.append(Paragraph(f"Driver History Report: {driver_name}", styles['Title']))
        elements.append(Spacer(1, 12))
        
        if data:
            table = Table(data)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(table)
        else:
            elements.append(Paragraph("No history found", styles['BodyText']))
        
        doc.build(elements)
        buffer.seek(0)
        return buffer
    except Exception as e:
        logger.error(f"Error generating driver report: {e}")
        return None

async def generate_full_report():
    try:
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter)
        styles = getSampleStyleSheet()
        
        # Get all logs
        logs = sheet_log.get_all_records()
        logs.sort(key=lambda x: x['Timestamp'], reverse=True)
        
        # Prepare data
        data = [['Date', 'Driver', 'Car Plate', 'Action']]
        for log in logs[-100:]:  # Last 100 entries
            date = datetime.strptime(log['Timestamp'], "%Y-%m-%d %H:%M").strftime("%d-%m-%Y %H:%M")
            data.append([date, log['Driver Name'], log['Car Plate'], 'Taken' if log['Action'] == 'out' else 'Returned'])
        
        # Create elements
        elements = []
        elements.append(Paragraph("Full Car Usage Report", styles['Title']))
        elements.append(Spacer(1, 12))
        
        if data:
            table = Table(data)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(table)
        else:
            elements.append(Paragraph("No history found", styles['BodyText']))
        
        # Add maintenance info
        elements.append(Spacer(1, 24))
        elements.append(Paragraph("Maintenance Schedule", styles['Heading2']))
        
        maintenance_data = [['Car Plate', 'Last Maintenance', 'Next Maintenance']]
        cars = sheet_cars.get_all_records()
        for car in cars:
            maintenance_data.append([car['Plate'], car.get('Last Maintenance', 'N/A'), car.get('Next Maintenance', 'N/A')])
        
        if len(maintenance_data) > 1:
            table = Table(maintenance_data)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            elements.append(table)
        
        doc.build(elements)
        buffer.seek(0)
        return buffer
    except Exception as e:
        logger.error(f"Error generating full report: {e}")
        return None

# Handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_name = update.effective_user.full_name
    
    try:
        # Check if user exists in drivers sheet
        drivers = sheet_drivers.get_all_records()
        driver_ids = [d['User ID'] for d in drivers]
        
        if str(user_id) in driver_ids or user_id in ADMINS:
            menu = COMBINED_MENU if is_admin(user_id) else MAIN_MENU
            await update.message.reply_text(f"Welcome back, {user_name}! Choose an option:", reply_markup=menu)
        else:
            await update.message.reply_text("❌ Access denied. Requesting access from admin...")
            
            buttons = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Approve", callback_data=f"approve|{user_id}|{user_name}"),
                    InlineKeyboardButton("⏰ Snooze", callback_data=f"snooze|{user_id}|{user_name}"),
                    InlineKeyboardButton("❌ Reject", callback_data=f"reject|{user_id}")
                ]
            ])
            
            await notify_admins(
                context,
                f"👤 Access Request:\nName: {user_name}\nUsername: @{update.effective_user.username or 'N/A'}\nUser ID: {user_id}",
                reply_markup=buttons
            )
    except Exception as e:
        logger.error(f"Error in start handler: {e}")
        await update.message.reply_text("❌ Server error. Please try again later.")

async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin privileges required.")
        return
    await update.message.reply_text("🛠️ Admin Panel:", reply_markup=ADMIN_MENU)

async def admin_management(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin privileges required.")
        return
    await update.message.reply_text("👑 Admin Management:", reply_markup=ADMIN_MANAGEMENT)

async def add_admin_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin privileges required.")
        return
    context.user_data["await"] = "add_admin"
    await update.message.reply_text("➕ Send the DRIVER NAME and TELEGRAM USER ID to make admin (comma-separated, e.g., John Doe, 123456789):")

async def remove_admin_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin privileges required.")
        return
    
    try:
        drivers = sheet_drivers.get_all_records()
        admins = [d for d in drivers if d.get('Is Admin', '').lower() == 'true']
        
        if not admins:
            await update.message.reply_text("ℹ️ No admins found.")
            return
        
        buttons = []
        for admin in admins:
            buttons.append([
                InlineKeyboardButton(
                    f"{admin['Name']} (ID: {admin['User ID']})",
                    callback_data=f"remove_admin|{admin['User ID']}"
                )
            ])
        
        await update.message.reply_text(
            "Select an admin to remove:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except Exception as e:
        logger.error(f"Error in remove_admin_prompt: {e}")
        await update.message.reply_text("❌ Error fetching admin list.")

async def list_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin privileges required.")
        return
    
    try:
        drivers = sheet_drivers.get_all_records()
        admins = [d for d in drivers if d.get('Is Admin', '').lower() == 'true']
        
        if not admins:
            await update.message.reply_text("ℹ️ No admins found.")
            return
        
        admin_list = "\n".join([f"• {d['Name']} (ID: {d['User ID']})" for d in admins])
        await update.message.reply_text(f"👑 Current Admins:\n\n{admin_list}")
    except Exception as e:
        logger.error(f"Error in list_admins: {e}")
        await update.message.reply_text("❌ Error fetching admin list.")

async def generate_report_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin privileges required.")
        return
    await update.message.reply_text("🧾 Select report type:", reply_markup=REPORT_TYPES)

async def generate_driver_report_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin privileges required.")
        return
    
    context.user_data["await"] = "driver_report"
    await update.message.reply_text("📊 Enter driver ID to generate their report:")

async def generate_full_report_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Admin privileges required.")
        return
    
    await update.message.reply_text("⏳ Generating full report...")
    report = await generate_full_report()
    
    if report:
        await update.message.reply_document(
            document=InputFile(report, filename="full_report.pdf"),
            caption="📈 Full Car Usage Report"
        )
    else:
        await update.message.reply_text("❌ Failed to generate report.")

async def my_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_name = update.effective_user.full_name
    
    await update.message.reply_text("⏳ Generating your history report...")
    report = await generate_driver_report(user_id, user_name)
    
    if report:
        await update.message.reply_document(
            document=InputFile(report, filename="my_history.pdf"),
            caption=f"📄 Your Driving History - {user_name}"
        )
    else:
        await update.message.reply_text("❌ Failed to generate your history report.")

async def upload_photo_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # Check if user has a car currently checked out
    try:
        logs = sheet_log.get_all_records()
        logs.sort(key=lambda x: x['Timestamp'])
        last_action = {r['Car Plate']: r['Action'] for r in logs}
        car_driver = {r['Car Plate']: r['Driver ID'] for r in logs if r['Action'] == 'out'}
        
        user_cars = [cp for cp, driver_id in car_driver.items() if str(driver_id) == str(user_id) and last_action.get(cp) == 'out']
        
        if not user_cars:
            await update.message.reply_text("ℹ️ You don't have any cars checked out to upload photos for.")
            return
        
        context.user_data["photo_car"] = user_cars[0]
        await update.message.reply_text(
            f"📸 Upload photo for car {user_cars[0]}. Select photo type:",
            reply_markup=ReplyKeyboardMarkup(PHOTO_TYPES, resize_keyboard=True)
        )
    except Exception as e:
        logger.error(f"Error in upload_photo_menu: {e}")
        await update.message.reply_text("❌ Error checking your car status.")

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_name = update.effective_user.full_name
    
    if 'photo_car' not in context.user_data:
        await update.message.reply_text("❌ Please select a car first from the menu.")
        return
    
    car_plate = context.user_data['photo_car']
    photo_type = context.user_data.get('photo_type', 'Unknown')
    
    try:
        # Download the photo
        photo_file = await update.message.photo[-1].get_file()
        with tempfile.NamedTemporaryFile(suffix='.jpg') as temp_file:
            await photo_file.download_to_drive(temp_file.name)
            
            # Upload to Google Drive
            file_name = f"{car_plate}_{photo_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
            photo_url = upload_to_drive(temp_file.name, file_name)
            
            if photo_url:
                # Log the photo in the spreadsheet
                sheet_photos.append_row([
                    datetime.now(UAE_TZ).strftime("%Y-%m-%d %H:%M"),
                    car_plate,
                    user_id,
                    photo_url,
                    photo_type,
                    context.user_data.get('photo_notes', '')
                ])
                
                await update.message.reply_text(
                    f"✅ Photo uploaded successfully for {car_plate}!\n"
                    f"Type: {photo_type}\n"
                    f"URL: {photo_url}",
                    reply_markup=MAIN_MENU if not is_admin(user_id) else COMBINED_MENU
                )
                
                # Notify admins
                await notify_admins(
                    context,
                    f"📸 New photo uploaded by {user_name} for {car_plate}\n"
                    f"Type: {photo_type}\n"
                    f"URL: {photo_url}"
                )
            else:
                await update.message.reply_text("❌ Failed to upload photo to Drive.")
    except Exception as e:
        logger.error(f"Error handling photo: {e}")
        await update.message.reply_text("❌ Failed to process photo.")

async def set_photo_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "⛽ Fuel Meter":
        context.user_data['photo_type'] = 'Fuel'
    elif text == "🛠 Condition":
        context.user_data['photo_type'] = 'Condition'
    elif text == "📝 Notes":
        context.user_data['await'] = 'photo_notes'
        await update.message.reply_text("✏️ Please enter notes about the photo:")
        return
    
    await update.message.reply_text("📸 Now please send the photo:")

# [Previous handlers like take_car_menu, return_car_menu, etc. remain the same]
# [Add the new handlers to the application in the main function]

def main():
    try:
        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        
        # Basic commands
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("myhistory", my_history))
        
        # Menu handlers
        app.add_handler(MessageHandler(filters.Regex("^🛠️ Admin Panel$"), admin_menu))
        app.add_handler(MessageHandler(filters.Regex("^👑 Admin Management$"), admin_management))
        app.add_handler(MessageHandler(filters.Regex("^➕ Add Admin$"), add_admin_prompt))
        app.add_handler(MessageHandler(filters.Regex("^➖ Remove Admin$"), remove_admin_prompt))
        app.add_handler(MessageHandler(filters.Regex("^📄 List Admins$"), list_admins))
        app.add_handler(MessageHandler(filters.Regex("^🧾 Generate Report$"), generate_report_menu))
        app.add_handler(MessageHandler(filters.Regex("^📈 Full Report$"), generate_full_report_handler))
        app.add_handler(MessageHandler(filters.Regex("^📊 Driver Report$"), generate_driver_report_handler))
        app.add_handler(MessageHandler(filters.Regex("^📄 My History$"), my_history))
        app.add_handler(MessageHandler(filters.Regex("^📸 Upload Photo$"), upload_photo_menu))
        
        # Photo handlers
        app.add_handler(MessageHandler(
            filters.PHOTO & ~filters.COMMAND, handle_photo
        ))
        app.add_handler(MessageHandler(
            filters.Regex("^(⛽ Fuel Meter|🛠 Condition|📝 Notes)$"), set_photo_type
        ))
        
        # Text handlers
        app.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, text_handler
        ))
        
        # Callback handlers
        app.add_handler(CallbackQueryHandler(handle_access_request, pattern="^(approve|reject|snooze)\\|"))
        app.add_handler(CallbackQueryHandler(handle_driver_action, pattern="^(remove_driver|add_driver)"))
        app.add_handler(CallbackQueryHandler(handle_remove_car_action, pattern="^remove_car\\|"))
        app.add_handler(CallbackQueryHandler(on_car_action, pattern="^(take|return)\\|"))
        app.add_handler(CallbackQueryHandler(handle_remove_admin, pattern="^remove_admin\\|"))
        
        logger.info("Bot started with all features")
        app.run_polling()
        
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()
