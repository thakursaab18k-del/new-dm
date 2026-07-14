import asyncio
import logging
import os
import time
import re
import traceback
from logging.handlers import RotatingFileHandler

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError,
    UnauthorizedError, ChatWriteForbiddenError,
    AuthKeyUnregisteredError, AuthKeyDuplicatedError
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

# MongoDB Async Driver
from motor.motor_asyncio import AsyncIOMotorClient
# For Render Port Binding
from aiohttp import web

# ==========================================
# CONFIGURATION (Loaded from Environment Variables for Render)
# ==========================================
API_ID = int(os.environ.get('API_ID', 36922726))
API_HASH = os.environ.get('API_HASH', 'add2fcabcee55013e2c1a9775c033f03')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8873455036:AAHKI3O8MKxCJmBZnrQbmGe_CrJVmP44aak')
ADMIN_ID = int(os.environ.get('ADMIN_ID', 1746356733))
MONGO_URI = os.environ.get('MONGO_URI', "mongodb://localhost:27017") # Atlas URI on Render
PORT = int(os.environ.get('PORT', 8080)) # Render will provide this automatically

# Folders
os.makedirs("downloads", exist_ok=True)
os.makedirs("logs", exist_ok=True)

# --- REAL LOGGING SYSTEM ---
logger = logging.getLogger("MultiAccountManager")
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

file_handler = RotatingFileHandler("logs/bot_logs.txt", maxBytes=5*1024*1024, backupCount=1)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# --- MONGODB SETUP ---
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['telethon_manager']

accounts_col = db['accounts']
ad_config_col = db['ad_config']
bot_stats_col = db['bot_stats']
auth_users_col = db['auth_users']

async def setup_db():
    try:
        if not await ad_config_col.find_one({"_id": 1}):
            await ad_config_col.insert_one({
                "_id": 1, "status": "paused", "msgs": [], "interval": 300, 
                "last_run": 0, "target_type": "all", "target_ids": [], "acc_cooldown": 5 
            })
        if not await bot_stats_col.find_one({"_id": 1}):
            await bot_stats_col.insert_one({"_id": 1, "total_sent": 0, "total_failed": 0, "total_joined": 0})
        if not await auth_users_col.find_one({"_id": ADMIN_ID}):
            await auth_users_col.insert_one({"_id": ADMIN_ID})
        logger.info("MongoDB Database initialized successfully.")
    except Exception as e:
        logger.error(f"MongoDB Setup Error: {e}")

# --- GLOBALS ---
bot = TelegramClient('master_bot', API_ID, API_HASH)
active_clients = {}  
bot_username = "Ads"

# --- DB HELPERS ---
async def update_stats(sent=0, failed=0, joined=0):
    try:
        await bot_stats_col.update_one(
            {"_id": 1}, 
            {"$inc": {"total_sent": sent, "total_failed": failed, "total_joined": joined}}
        )
    except Exception as e:
        logger.error(f"Failed to update stats: {e}")

async def is_authorized(user_id):
    user = await auth_users_col.find_one({"_id": user_id})
    return bool(user)

# --- AUTO JOIN HANDLER ---
async def auto_join_handler(event):
    try:
        if event.message.sender and getattr(event.message.sender, 'bot', False):
            client = event.client
            links_to_join = set()
            if event.message.buttons:
                for row in event.message.buttons:
                    for btn in row:
                        if hasattr(btn, 'url') and btn.url and ('t.me/' in btn.url or 'telegram.me/' in btn.url):
                            links_to_join.add(btn.url)
            if event.message.text:
                matches = re.findall(r'(https?://)?(t\.me|telegram\.me)/(joinchat/|\+)?([\w-]+)', event.message.text)
                for match in matches:
                    links_to_join.add(f"https://t.me/{match[2]}{match[3]}")

            for url in links_to_join:
                try:
                    clean_url = re.sub(r'[^a-zA-Z0-9_/+:-]', '', url)
                    if '+' in clean_url or 'joinchat' in clean_url:
                        hash_code = clean_url.split('/')[-1].replace('+', '').split('?')[0]
                        await client(ImportChatInviteRequest(hash_code))
                    else:
                        username = clean_url.split('/')[-1].replace('@', '').split('?')[0]
                        await client(JoinChannelRequest(username))
                    await update_stats(joined=1)
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Auto-join error: {e}")

# --- HEALTH CHECKER ---
async def load_and_verify_clients():
    global bot_username
    try:
        me = await bot.get_me()
        bot_username = f"@{me.username}" if me.username else "@Tecxo"
    except Exception:
        pass
        
    accounts = await accounts_col.find({}).to_list(length=None)
    logger.info(f"Initializing {len(accounts)} accounts from MongoDB...")
    
    for acc in accounts:
        phone = acc['_id']
        session_str = acc.get('session_string')
        try:
            client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                active_clients[phone] = client
                client.add_event_handler(auto_join_handler, events.NewMessage(incoming=True))
                await accounts_col.update_one({"_id": phone}, {"$set": {"status": "active"}})
            else:
                raise AuthKeyUnregisteredError("Unauthorized")
        except Exception as e:
            await accounts_col.update_one({"_id": phone}, {"$set": {"status": "dead"}})

# --- PARALLEL BROADCAST ENGINE ---
async def process_broadcast_for_account(client, phone, messages, target_type, specific_ids, config):
    acc_sent, acc_failed = 0, 0
    targets = []
    try:
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            if target_type == 'groups' and dialog.is_group:
                targets.append(dialog)
            elif target_type == 'channels' and dialog.is_channel and not dialog.is_group:
                targets.append(dialog)
            elif target_type == 'all' and not (dialog.is_user and getattr(dialog.entity, 'bot', False)):
                targets.append(dialog)
    except Exception:
        return 0, 0

    for dialog in targets:
        db_config = await ad_config_col.find_one({"_id": 1})
        if db_config and db_config['status'] != 'active': break
        for msg in messages:
            try:
                if msg.get('media'):
                    await client.send_file(dialog.id, msg['media'], caption=msg['text'])
                else:
                    await client.send_message(dialog.id, msg['text'])
                acc_sent += 1
                await asyncio.sleep(0.3) 
            except FloodWaitError as e:
                acc_failed += 1
                await asyncio.sleep(e.seconds) 
            except Exception:
                acc_failed += 1
    return acc_sent, acc_failed

async def spammer_engine():
    while True:
        try:
            config = await ad_config_col.find_one({"_id": 1})
            if config and config.get('status') == 'active':
                messages = config.get('msgs', [])
                interval = config.get('interval', 300)
                if time.time() - config.get('last_run', 0) >= interval and messages and active_clients:
                    tasks = [
                        process_broadcast_for_account(client, phone, messages, config.get('target_type', 'all'), [], config)
                        for phone, client in active_clients.items()
                    ]
                    results = await asyncio.gather(*tasks)
                    total_sent = sum(r[0] for r in results)
                    total_failed = sum(r[1] for r in results)
                    await update_stats(sent=total_sent, failed=total_failed)
                    await ad_config_col.update_one({"_id": 1}, {"$set": {"last_run": time.time()}})
        except Exception:
            pass
        await asyncio.sleep(5)

# ==========================================
# UI & BOT COMMANDS
# ==========================================
async def get_dashboard_text():
    total_hosted = await accounts_col.count_documents({})
    active_accs = len(active_clients)
    config = await ad_config_col.find_one({"_id": 1})
    stats = await bot_stats_col.find_one({"_id": 1})
    
    is_set = "Set 🟢" if config.get('msgs') else "Not Set 🔴"
    ad_status = "Active ▶️" if config.get('status') == 'active' else "Paused ⏸"
    
    return (
        f"╰_╯ **{bot_username} Ads DASHBOARD** ❞\n\n"
        f"**Server Analytics:**\n"
        f"• Hosted: `{total_hosted}` | Online: `{active_accs}` 🟢\n\n"
        f"**Ad Configuration:**\n"
        f"• Message: **{is_set}** | Interval: `{config.get('interval')}s`\n"
        f"• Status: **{ad_status}**\n\n"
        f"**Global Statistics:**\n"
        f"• Sent: `{stats.get('total_sent')}` | Failed: `{stats.get('total_failed')}`\n"
    )

def dashboard_buttons():
    return [
        [Button.inline("Add Accounts", b"add_account"), Button.inline("My Accounts", b"my_accounts")],
        [Button.inline("Set Ad Message", b"set_ad"), Button.inline("Start Ads ▶️", b"start_ads")],
        [Button.inline("Stop Ads ⏸", b"stop_ads"), Button.inline("Close ❌", b"close_menu")]
    ]

@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    if await is_authorized(event.sender_id):
        await event.respond(await get_dashboard_text(), buttons=dashboard_buttons())

@bot.on(events.CallbackQuery(data=b"main_menu"))
async def back_to_main(event):
    await event.edit(await get_dashboard_text(), buttons=dashboard_buttons())

@bot.on(events.CallbackQuery(data=b"close_menu"))
async def close_menu(event):
    await event.delete()

# --- 1. ADD ACCOUNT ---
@bot.on(events.CallbackQuery(data=b"add_account"))
async def add_account_handler(event):
    sender = event.sender_id
    await event.delete()
    async with bot.conversation(sender, timeout=300) as conv:
        await conv.send_message("Enter phone number with country code (e.g., +1234567890):")
        resp = await conv.get_response()
        phone = resp.text.strip().replace(" ", "")
        
        temp_client = TelegramClient(StringSession(), API_ID, API_HASH)
        await temp_client.connect()
        try:
            send_code = await temp_client.send_code_request(phone)
            await conv.send_message(f"✅ OTP sent to `{phone}`. Enter OTP (put spaces between numbers e.g., 1 2 3 4 5):")
            otp = (await conv.get_response()).text.replace(" ", "").strip()
            
            try:
                await temp_client.sign_in(phone, otp, phone_code_hash=send_code.phone_code_hash)
            except SessionPasswordNeededError:
                await conv.send_message("🔐 Enter 2FA Password:")
                pw = (await conv.get_response()).text.strip()
                await temp_client.sign_in(password=pw)
                
            session_string = temp_client.session.save()
            await accounts_col.update_one({"_id": phone}, {"$set": {"session_string": session_string, "status": "active"}}, upsert=True)
            active_clients[phone] = temp_client
            temp_client.add_event_handler(auto_join_handler, events.NewMessage(incoming=True))
            
            await conv.send_message(f"🎉 **Account {phone} hosted!**", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
        except Exception as e:
            await conv.send_message(f"❌ **Error:** {str(e)}", buttons=[[Button.inline("Back 🔙", b"main_menu")]])

# --- 2. MY ACCOUNTS ---
@bot.on(events.CallbackQuery(data=b"my_accounts"))
async def my_accounts_handler(event):
    accounts = await accounts_col.find({}).to_list(length=None)
    text = "╰_╯ **HOSTED ACCOUNTS** ❞\n\n"
    for i, acc in enumerate(accounts, 1):
        icon = "🟢" if acc.get('status') == 'active' else "🔴"
        text += f"`{i}.` 📱 {acc['_id']} [{icon}]\n"
    await event.edit(text, buttons=[[Button.inline("Back 🔙", b"main_menu")]])

# --- 3. SET AD MESSAGE (Fixed cut-off code) ---
@bot.on(events.CallbackQuery(data=b"set_ad"))
async def set_ad_handler(event):
    sender = event.sender_id
    await event.delete()
    async with bot.conversation(sender, timeout=600) as conv:
        await conv.send_message("`Send your ad message now (Text, Photo, or Video):`")
        m = await conv.get_response()
        media_path = None
        
        if m.media:
            msg_loader = await conv.send_message("⏳ Downloading media, please wait...")
            media_path = await m.download_media(file="downloads/")
            await msg_loader.delete()
            
        new_msg = {"text": m.text or "", "media": media_path}
        await ad_config_col.update_one({"_id": 1}, {"$set": {"msgs": [new_msg]}})
        await conv.send_message("✅ Ad message updated successfully!", buttons=[[Button.inline("Back 🔙", b"main_menu")]])

# --- 4. START/STOP ADS ---
@bot.on(events.CallbackQuery(data=b"start_ads"))
async def start_ads(event):
    await ad_config_col.update_one({"_id": 1}, {"$set": {"status": "active"}})
    await event.answer("▶️ Broadcasting Started!", alert=True)
    await back_to_main(event)

@bot.on(events.CallbackQuery(data=b"stop_ads"))
async def stop_ads(event):
    await ad_config_col.update_one({"_id": 1}, {"$set": {"status": "paused"}})
    await event.answer("⏸ Broadcasting Paused!", alert=True)
    await back_to_main(event)

# ==========================================
# RENDER DUMMY WEB SERVER & MAIN LOOP
# ==========================================
async def web_server():
    """Dummy web server to satisfy Render's port binding requirement."""
    async def handle(request):
        return web.Response(text="MultiAccountManager Bot is running smoothly on Render!")
    
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Web server started on port {PORT}")

async def main():
    await bot.start(bot_token=BOT_TOKEN)
    await setup_db()
    await load_and_verify_clients()
    
    # Start background processes
    asyncio.create_task(spammer_engine())
    asyncio.create_task(web_server()) 
    
    logger.info("Bot is fully operational!")
    await bot.run_until_disconnected()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
