# ==========================================
# PRODUCTION-GRADE TELETHON MANAGER BOT
# Optimized for 50-100 Accounts with MongoDB
# Blazing Fast Parallel Processing Engine
# Features: Mass Broadcast, Mass Join, Mass Report (Message/User), DB Backup
# ==========================================

import asyncio
import logging
import os
import time
import re
import traceback
import json
from logging.handlers import RotatingFileHandler

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError,
    UnauthorizedError, ChatWriteForbiddenError, UserAlreadyParticipantError,
    UserBannedInChannelError, InviteHashExpiredError, InviteHashInvalidError,
    ChannelPrivateError, AuthKeyUnregisteredError, AuthKeyDuplicatedError
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.account import ReportPeerRequest
from telethon.tl.functions.messages import ReportRequest # For reporting specific messages
from telethon.tl.types import (
    InputReportReasonSpam, InputReportReasonFake, 
    InputReportReasonViolence, InputReportReasonPornography, 
    InputReportReasonOther
)

# MongoDB Async Driver & Render Web Server
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web

# --- CONFIGURATION (Load from Environment Variables for Render) ---
API_ID = int(os.environ.get('API_ID', 36922726))
API_HASH = os.environ.get('API_HASH', 'add2fcabcee55013e2c1a9775c033f03')
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8873455036:AAHKI3O8MKxCJmBZnrQbmGe_CrJVmP44aak')
ADMIN_ID = int(os.environ.get('ADMIN_ID', 1746356733))  # Primary Admin

MONGO_URI = os.environ.get('MONGO_URI', "mongodb://localhost:27017") # Atlas URI on Render
PORT = int(os.environ.get('PORT', 8080)) # Render automatically assigns this

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
        # Default Ad Config
        if not await ad_config_col.find_one({"_id": 1}):
            await ad_config_col.insert_one({
                "_id": 1, 
                "status": "paused", 
                "msgs": [], 
                "interval": 300, 
                "last_run": 0, 
                "target_type": "all", 
                "target_ids": [], 
                "acc_cooldown": 5 
            })
            
        # Default Bot Stats
        if not await bot_stats_col.find_one({"_id": 1}):
            await bot_stats_col.insert_one({"_id": 1, "total_sent": 0, "total_failed": 0, "total_joined": 0})
            
        # Default Admin Auth
        if not await auth_users_col.find_one({"_id": ADMIN_ID}):
            await auth_users_col.insert_one({"_id": ADMIN_ID})
            
        logger.info("MongoDB Database initialized successfully.")
    except Exception as e:
        logger.error(f"MongoDB Setup Error: {e}")

# --- GLOBALS ---
bot = TelegramClient('master_bot', API_ID, API_HASH)
active_clients = {}  # {'+91...': TelegramClient}
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

# --- ADVANCED BACKGROUND AUTO-JOIN HANDLER ---
async def auto_join_handler(event):
    """Monitors incoming bot replies for restricted group join links"""
    try:
        if event.message.sender and getattr(event.message.sender, 'bot', False):
            client = event.client
            links_to_join = set()
            
            if event.message.buttons:
                for row in event.message.buttons:
                    for btn in row:
                        if hasattr(btn, 'url') and btn.url:
                            if 't.me/' in btn.url or 'telegram.me/' in btn.url:
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
                    logger.info(f"Auto-joined restricted channel: {clean_url}")
                    await update_stats(joined=1)
                except Exception as e:
                    logger.debug(f"Auto-join failed for {clean_url}: {e}")
    except Exception as e:
        logger.error(f"Auto-join handler error: {e}")

# --- HEALTH CHECKER & CLIENT LOADER ---
async def load_and_verify_clients():
    global bot_username
    try:
        me = await bot.get_me()
        bot_username = f"@{me.username}" if me.username else "@Tecxo"
    except Exception as e:
        logger.warning(f"Failed to fetch bot username: {e}")
        
    cursor = accounts_col.find({})
    accounts = await cursor.to_list(length=None)
    logger.info(f"Initializing {len(accounts)} accounts from MongoDB...")
    
    loaded, dead = 0, 0
    
    for acc in accounts:
        phone = acc['_id']
        session_str = acc['session_string']
        try:
            client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
            await client.connect()
            
            if await client.is_user_authorized():
                await client.get_me()
                active_clients[phone] = client
                client.add_event_handler(auto_join_handler, events.NewMessage(incoming=True))
                await accounts_col.update_one({"_id": phone}, {"$set": {"status": "active"}})
                loaded += 1
                logger.info(f"[+] Loaded and verified: {phone}")
            else:
                raise AuthKeyUnregisteredError("Session no longer authorized")
                
        except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, UnauthorizedError):
            logger.warning(f"[-] Dead session detected for {phone}. Marking as dead.")
            await accounts_col.update_one({"_id": phone}, {"$set": {"status": "dead"}})
            dead += 1
        except Exception as e:
            logger.error(f"[-] Failed to load {phone}: {e}")
            await accounts_col.update_one({"_id": phone}, {"$set": {"status": "dead"}})
            dead += 1
            
    logger.info(f"Initialization complete. Active: {loaded}, Dead: {dead}")

# --- HIGH-PERFORMANCE PARALLEL BROADCAST ENGINE ---
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
            elif target_type == 'private' and dialog.is_user and not getattr(dialog.entity, 'bot', False):
                targets.append(dialog)
            elif target_type == 'all' and not (dialog.is_user and getattr(dialog.entity, 'bot', False)):
                targets.append(dialog)
            elif target_type == 'specific_groups' and dialog.is_group and dialog.id in specific_ids:
                targets.append(dialog)
            elif target_type == 'specific_channels' and dialog.is_channel and not dialog.is_group and dialog.id in specific_ids:
                targets.append(dialog)
    except Exception as e:
        logger.error(f"[{phone}] Error fetching dialogs: {e}")
        return acc_sent, acc_failed

    for dialog in targets:
        db_config = await ad_config_col.find_one({"_id": 1})
        if db_config and db_config['status'] != 'active':
            break

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
            except (UnauthorizedError, ChatWriteForbiddenError):
                acc_failed += 1
            except Exception as e:
                acc_failed += 1
                
    return acc_sent, acc_failed

async def spammer_engine():
    logger.info("🚀 Global Ads Engine Started...")
    while True:
        try:
            config = await ad_config_col.find_one({"_id": 1})
            if not config:
                await asyncio.sleep(5)
                continue
                
            status = config.get('status')
            messages = config.get('msgs', [])
            interval = config.get('interval', 300)
            last_run = config.get('last_run', 0)
            target_type = config.get('target_type', 'all')
            specific_ids = set(config.get('target_ids', []))
            
            current_time = time.time()
            
            if status == 'active' and (current_time - last_run >= interval):
                if not messages or not active_clients:
                    await asyncio.sleep(5)
                    continue
                    
                logger.info(f"Starting FULLY PARALLEL broadcast cycle for {len(active_clients)} accounts.")
                
                tasks = [
                    process_broadcast_for_account(client, phone, messages, target_type, specific_ids, config)
                    for phone, client in active_clients.items()
                ]
                
                results = await asyncio.gather(*tasks)
                
                total_cycle_sent = sum(r[0] for r in results)
                total_cycle_failed = sum(r[1] for r in results)
                
                await update_stats(sent=total_cycle_sent, failed=total_cycle_failed)
                
                await ad_config_col.update_one({"_id": 1}, {"$set": {"last_run": time.time()}})
                logger.info(f"Broadcast cycle finished. Sent: {total_cycle_sent}, Failed: {total_cycle_failed}.")
                
        except Exception as e:
            logger.error(f"Engine Error: {traceback.format_exc()}")
            
        await asyncio.sleep(5)

# ==========================================
# UI & BOT COMMANDS
# ==========================================

async def get_dashboard_text():
    try:
        total_hosted = await accounts_col.count_documents({})
        active_accs = len(active_clients)
        dead_accs = total_hosted - active_accs
        
        config = await ad_config_col.find_one({"_id": 1})
        status = config.get('status', 'paused')
        msgs = config.get('msgs', [])
        interval = config.get('interval', 300)
        acc_cooldown = config.get('acc_cooldown', 5)
        
        stats = await bot_stats_col.find_one({"_id": 1})
        tot_sent = stats.get('total_sent', 0)
        tot_failed = stats.get('total_failed', 0)
        tot_joined = stats.get('total_joined', 0)
        
        is_set = "Set 🟢" if msgs else "Not Set 🔴"
        ad_status = "Active ▶️" if status == 'active' else "Paused ⏸"
        
        text = (
            f"╰_╯ **{bot_username} Ads DASHBOARD** ❞\n\n"
            f"**Server Analytics (MongoDB):**\n"
            f"• Hosted Accounts: `{total_hosted}/100`\n"
            f"• Online Accounts: `{active_accs}` 🟢\n"
            f"• Dead Sessions: `{dead_accs}` 🔴\n\n"
            f"**Ad Configuration:**\n"
            f"• Ad Message: **{is_set}**\n"
            f"• Cycle Interval: `{interval}s`\n"
            f"• Wait Time: `{acc_cooldown}s`\n"
            f"• Advertising Status: **{ad_status}**\n\n"
            f"**Global Statistics:**\n"
            f"• Total Sent Ads: `{tot_sent}`\n"
            f"• Failed Ads: `{tot_failed}`\n"
            f"• Successful Joins: `{tot_joined}`\n\n"
            f"╰_╯ Choose an action below to continue ❞"
        )
        return text
    except Exception as e:
        logger.error(f"Dashboard generation error: {e}")
        return "❌ Error loading dashboard."

def dashboard_buttons():
    return [
        [Button.inline("Add Accounts", b"add_account"), Button.inline("My Accounts", b"my_accounts")],
        [Button.inline("Set Ad Message", b"set_ad"), Button.inline("Set Time Interval", b"set_time")],
        [Button.inline("Start Ads ▶️", b"start_ads"), Button.inline("Stop Ads ⏸", b"stop_ads")],
        [Button.inline("Delete Accounts", b"del_accounts"), Button.inline("Join Link 🔗", b"join_all")],
        [Button.inline("Mass Report ⚠️", b"mass_report"), Button.inline("Acc Cooldown ⏳", b"set_acc_cooldown")],
        [Button.inline("Download DB 💾", b"download_db"), Button.inline("Logs 📋", b"view_logs")],
        [Button.inline("Manage Access 🔐", b"manage_auth"), Button.inline("Close ❌", b"close_menu")]
    ]

@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    try:
        if not await is_authorized(event.sender_id):
            return await event.respond("🚫 **You are not authorized to use this bot.**")
        msg = await get_dashboard_text()
        await event.respond(msg, buttons=dashboard_buttons())
    except Exception as e:
        logger.error(f"Start command error: {e}")

@bot.on(events.CallbackQuery(data=b"main_menu"))
async def back_to_main(event):
    try:
        if not await is_authorized(event.sender_id):
            return await event.answer("🚫 Unauthorized!", alert=True)
        msg = await get_dashboard_text()
        await event.edit(msg, buttons=dashboard_buttons())
    except Exception as e:
        logger.error(f"Main menu callback error: {e}")

@bot.on(events.CallbackQuery(data=b"close_menu"))
async def close_menu(event):
    await event.delete()

# --- 1. ADD ACCOUNT ---
@bot.on(events.CallbackQuery(data=b"add_account"))
async def add_account_handler(event):
    sender = event.sender_id
    await event.delete()
    
    async with bot.conversation(sender, timeout=300) as conv:
        text = (
            "╰_╯ **HOST NEW ACCOUNT** ❞\n\n"
            "Secure Account Hosting via MongoDB\n\n"
            "Enter your phone number with country code:\n\n"
            "`Example: +1234567890 ❞`\n\n"
            "Your data is encrypted and secure"
        )
        await conv.send_message(text, buttons=[[Button.inline("Back 🔙", b"main_menu")]])
        
        try:
            resp = await conv.get_response()
            if resp.text == '/start' or resp.text.lower() == 'back': return
            
            phone = resp.text.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
            if not phone.startswith('+') and phone.isdigit():
                phone = '+' + phone
            
            temp_client = TelegramClient(StringSession(), API_ID, API_HASH)
            await temp_client.connect()
            
            send_code = await temp_client.send_code_request(phone)
            await conv.send_message(f"✅ OTP sent to `{phone}`.\n\n**Enter OTP:** (If OTP is 12345, type it with spaces like `1 2 3 4 5`).")
            otp = (await conv.get_response()).text.replace(" ", "").strip()
            
            try:
                await temp_client.sign_in(phone, otp, phone_code_hash=send_code.phone_code_hash)
            except SessionPasswordNeededError:
                await conv.send_message("🔐 Two-Step Verification is active. **Enter Password:**")
                pw = (await conv.get_response()).text.strip()
                await temp_client.sign_in(password=pw)
            
            session_string = temp_client.session.save()
            await accounts_col.update_one(
                {"_id": phone}, 
                {"$set": {"session_string": session_string, "status": "active"}}, 
                upsert=True
            )
            
            active_clients[phone] = temp_client
            temp_client.add_event_handler(auto_join_handler, events.NewMessage(incoming=True))
            
            logger.info(f"New account hosted: {phone}")
            await conv.send_message(f"🎉 **Account {phone} successfully hosted!**", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            
        except Exception as e:
            logger.error(f"Add account failed: {e}")
            await conv.send_message(f"❌ **Error:** {str(e)}", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            try: await temp_client.disconnect()
            except: pass

# --- 2. MY ACCOUNTS ---
@bot.on(events.CallbackQuery(data=b"my_accounts"))
async def my_accounts_handler(event):
    try:
        accounts = await accounts_col.find({}).to_list(length=None)
        if not accounts:
            text = "╰_╯ **NO ACCOUNTS HOSTED** ❞\n\nAdd an account to start broadcasting!"
            return await event.edit(text, buttons=[[Button.inline("Add Account 📱", b"add_account"), Button.inline("Back 🔙", b"main_menu")]])
        
        text = "╰_╯ **HOSTED ACCOUNTS** ❞\n\n"
        for i, acc in enumerate(accounts, 1):
            phone = acc['_id']
            status = acc.get('status', 'dead')
            icon = "🟢" if status == 'active' else "🔴"
            text += f"`{i}.` 📱 {phone} [{icon}]\n"
        
        text += "\n*(🟢 = Online, 🔴 = Dead/Needs Relogin)*"
        await event.edit(text, buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"My accounts error: {e}")

# --- 3. SET AD MESSAGE ---
@bot.on(events.CallbackQuery(data=b"set_ad"))
async def set_ad_handler(event):
    sender = event.sender_id
    await event.delete()
    
    try:
        config = await ad_config_col.find_one({"_id": 1})
        msgs = config.get('msgs', [])
        current_ad = msgs[0]['text'][:30] + "..." if msgs and msgs[0].get('text') else ("Media Ad" if msgs else "No message set yet.")
        
        async with bot.conversation(sender, timeout=600) as conv:
            text = (
                "╰_╯ **SET YOUR AD MESSAGE** ❞\n\n"
                f"**Current Ad Message:** ❞\n`{current_ad}`\n\n"
                "`Send your ad message now (Text, Photo, or Sticker): ❞`"
            )
            await conv.send_message(text, buttons=[[Button.inline("Cancel", b"main_menu")]])
            
            m = await conv.get_response()
            
            media_path = None
            if m.media:
                msg_loader = await conv.send_message("⏳ Downloading media, please wait...")
                media_path = await m.download_media(file="downloads/")
                await msg_loader.delete()
                
            saved_messages = [{"text": m.text if m.text else "", "media": media_path}]
            
            tgt_text = "🎯 **Choose Target Audience:**"
            tgt_btns = [
                [Button.inline("All Groups", b"tg_groups"), Button.inline("All Channels", b"tg_channels")],
                [Button.inline("Specific Groups", b"tg_spcgroups"), Button.inline("Everything", b"tg_all")]
            ]
            tgt_msg = await conv.send_message(tgt_text, buttons=tgt_btns)
            tgt_resp = await conv.wait_event(events.CallbackQuery())
            choice = tgt_resp.data.decode().split('_')[1]
            await tgt_msg.delete()
            
            target_type = choice
            target_ids = []
            
            if choice == 'spcgroups':
                target_type = 'specific_groups'
                client = list(active_clients.values())[0] if active_clients else None
                if client:
                    dialogs = [d async for d in client.iter_dialogs() if d.is_group]
                    if dialogs:
                        msg_text = "🎯 **Select Groups:**\n\n"
                        inline_buttons = []
                        row = []
                        for i, d in enumerate(dialogs[:50], 1):
                            msg_text += f"`{i}.` {d.title[:30]}\n"
                            row.append(Button.inline(str(i), f"sel_{d.id}"))
                            if len(row) == 5:
                                inline_buttons.append(row)
                                row = []
                        if row: inline_buttons.append(row)
                        inline_buttons.append([Button.inline("✅ DONE", b"sel_done")])
                        
                        sel_msg = await conv.send_message(msg_text, buttons=inline_buttons)
                        selected_specific_ids = set()
                        while True:
                            s_resp = await conv.wait_event(events.CallbackQuery())
                            if s_resp.data == b"sel_done":
                                await s_resp.answer("Saved!")
                                await sel_msg.delete()
                                break
                            elif s_resp.data.startswith(b"sel_"):
                                d_id = int(s_resp.data.decode().split('_')[1])
                                if d_id in selected_specific_ids:
                                    selected_specific_ids.remove(d_id)
                                    await s_resp.answer("Removed!", alert=False)
                                else:
                                    selected_specific_ids.add(d_id)
                                    await s_resp.answer("Added! ✅", alert=False)
                        target_ids = list(selected_specific_ids)

            await ad_config_col.update_one(
                {"_id": 1}, 
                {"$set": {"msgs": saved_messages, "target_type": target_type, "target_ids": target_ids}}
            )
                     
            logger.info("Ad message updated.")
            await conv.send_message("✅ **Ad Message Set!**", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"Set ad error: {traceback.format_exc()}")
        await bot.send_message(sender, "❌ Error setting ad.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])

# --- 4. SET TIME INTERVALS ---
@bot.on(events.CallbackQuery(pattern=b"set_time|set_acc_cooldown"))
async def set_times_handler(event):
    sender = event.sender_id
    action = event.data.decode()
    await event.delete()
    
    try:
        config = await ad_config_col.find_one({"_id": 1})
        if action == "set_time":
            current_val = config.get('interval', 300)
            col = "interval"
            text = f"╰_╯ **SET INTERVAL** ❞\n\n__Current:__ `{current_val}`s\n\n`Send number in seconds:`"
        else:
            current_val = config.get('acc_cooldown', 5)
            col = "acc_cooldown"
            text = f"╰_╯ **SET COOLDOWN** ❞\n\n__Current:__ `{current_val}`s\n\n`Send number in seconds:`"

        async with bot.conversation(sender, timeout=120) as conv:
            msg = await conv.send_message(text, buttons=[[Button.inline("Cancel", b"main_menu")]])
            m = await conv.get_response()
            try:
                new_val = int(m.text.strip())
                await ad_config_col.update_one({"_id": 1}, {"$set": {col: new_val}})
                await msg.delete()
                await conv.send_message(f"✅ Updated to **{new_val} seconds**.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            except ValueError:
                await conv.send_message("❌ Invalid number.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"Set time error: {e}")

# --- 5. START / STOP ADS ---
@bot.on(events.CallbackQuery(pattern=b"start_ads|stop_ads"))
async def toggle_ads(event):
    try:
        new_status = 'active' if event.data == b"start_ads" else 'paused'
        await ad_config_col.update_one({"_id": 1}, {"$set": {"status": new_status}})
        await event.answer(f"Ads {new_status.title()}!", alert=True)
        await back_to_main(event)
    except Exception as e:
        logger.error(f"Toggle ads error: {e}")

# --- 6. DELETE ACCOUNTS ---
@bot.on(events.CallbackQuery(data=b"del_accounts"))
async def del_accounts_handler(event):
    try:
        accounts = await accounts_col.find({}).to_list(length=None)
        if not accounts:
            return await event.answer("No accounts!", alert=True)
            
        text = "╰_╯ **DELETE ACCOUNTS** ❞\nClick on an account to safely remove."
        buttons = []
        for acc in accounts:
            phone = acc['_id']
            icon = "🟢" if acc.get('status') == 'active' else "🔴"
            buttons.append([Button.inline(f"🗑 {phone} [{icon}]", f"rmacc_{phone}".encode())])
        buttons.append([Button.inline("Back 🔙", b"main_menu")])
        await event.edit(text, buttons=buttons)
    except Exception as e:
        logger.error(f"Delete menu error: {e}")

@bot.on(events.CallbackQuery(pattern=rb"rmacc_(.*)"))
async def process_delete(event):
    try:
        phone = event.data.decode().replace('rmacc_', '')
        
        if phone in active_clients:
            try:
                await active_clients[phone].log_out()
                await active_clients[phone].disconnect()
            except Exception as e:
                logger.error(f"Error logging out {phone}: {e}")
            finally:
                del active_clients[phone]
                
        await accounts_col.delete_one({"_id": phone})
        await event.answer("Account Deleted!", alert=True)
        await del_accounts_handler(event)
    except Exception as e:
        logger.error(f"Delete process error: {e}")

# --- 7. ULTIMATE GLOBAL JOIN LINK ---
@bot.on(events.CallbackQuery(data=b"join_all"))
async def join_all_handler(event):
    sender = event.sender_id
    await event.delete()
    
    if not active_clients:
        return await bot.send_message(sender, "❌ No active accounts hosted.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])

    async with bot.conversation(sender, timeout=300) as conv:
        await conv.send_message(
            "╰_╯ **GLOBAL JOIN LINK** ❞\n\nAll accounts will join simultaneously.\n`Send links separated by spaces/newlines: ❞`",
            buttons=[[Button.inline("Cancel", b"main_menu")]]
        )
        resp = await conv.get_response()
        if resp.text == '/start' or resp.text.lower() == 'back': return
        
        raw_links = [l.strip() for l in resp.text.split() if l.strip()]
        links = [l for l in raw_links if 't.me/' in l or 'telegram.me/' in l]
        
        if not links:
            return await conv.send_message("❌ No valid links found.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
        
        state = {"total_links": len(links), "total_accs": len(active_clients), "processed_accs": 0, "success": 0, "already": 0, "failed": 0, "running": True}
        ui_msg = await conv.send_message(f"🚀 **Initializing Join Engine...**")
        
        async def ui_updater():
            while state["running"]:
                try:
                    pct = int((state["processed_accs"] / state["total_accs"]) * 100) if state["total_accs"] > 0 else 0
                    bar = "█" * (pct // 10) + "░" * (10 - (pct // 10))
                    text = (
                        f"╰_╯ **LIVE JOIN PROGRESS** ❞\n\n"
                        f"**Progress:** [{bar}] {pct}%\n"
                        f"• Accounts Processed: `{state['processed_accs']}/{state['total_accs']}`\n"
                        f"• Successful Joins: `✅ {state['success']}`\n"
                        f"• Failed/Limits: `❌ {state['failed']}`\n\n"
                        f"*(Processing at Hyper Speed...)*"
                    )
                    await ui_msg.edit(text)
                except Exception:
                    pass
                await asyncio.sleep(2)
                
        ui_task = asyncio.create_task(ui_updater())

        async def process_account(client, phone):
            for link in links:
                try:
                    if '+' in link or 'joinchat' in link:
                        h_code = link.split('/')[-1].replace('+', '').split('?')[0]
                        await client(ImportChatInviteRequest(h_code))
                    else:
                        uname = link.split('/')[-1].replace('@', '').split('?')[0]
                        await client(JoinChannelRequest(uname))
                        
                    state["success"] += 1
                    await asyncio.sleep(0.2) 
                    
                except UserAlreadyParticipantError:
                    state["already"] += 1
                except FloodWaitError as e:
                    remaining = state["total_links"] - (state["success"] + state["already"] + state["failed"])
                    state["failed"] += remaining
                    break 
                except Exception as e:
                    state["failed"] += 1
                    
            state["processed_accs"] += 1

        sem = asyncio.Semaphore(40) 
        
        async def bounded_process(client, phone):
            async with sem:
                await process_account(client, phone)

        tasks = [bounded_process(c, p) for p, c in active_clients.items()]
        await asyncio.gather(*tasks)
        
        state["running"] = False
        await ui_task 
        await update_stats(joined=state["success"])
        
        final_text = (
            f"╰_╯ **JOIN PROCESS COMPLETED** ❞\n\n"
            f"📊 **Final Report:**\n"
            f"✅ Joined: `{state['success']}` | ⏭️ Skipped: `{state['already']}` | ❌ Failed: `{state['failed']}`\n"
        )
        await ui_msg.edit(final_text, buttons=[[Button.inline("Back 🔙", b"main_menu")]])

# --- 8. NEW FEATURE: ADVANCED MASS REPORT ENGINE ⚠️ ---
@bot.on(events.CallbackQuery(data=b"mass_report"))
async def mass_report_handler(event):
    sender = event.sender_id
    await event.delete()
    
    if not active_clients:
        return await bot.send_message(sender, "❌ No active accounts available to report from.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])

    async with bot.conversation(sender, timeout=300) as conv:
        # STEP 1: Get Target
        await conv.send_message(
            "╰_╯ **ADVANCED MASS REPORT ENGINE** ❞\n\n"
            "⚠️ **Warning:** Abuse of this feature may lead to account bans.\n\n"
            "`Send the Target (Username, Chat ID, or Message Link):\n"
            "(e.g., @Scammer, -1001234567, or https://t.me/karanerainfo/45735) ❞`",
            buttons=[[Button.inline("Cancel", b"main_menu")]]
        )
        
        resp = await conv.get_response()
        if resp.text == '/start' or resp.text.lower() == 'back': return
        
        # Parse Target Input with detailed logging
        target_input = resp.text.strip()
        clean_target = target_input.split('?')[0].replace('https://', '').replace('http://', '').replace('t.me/', '').replace('telegram.me/', '').strip()
        
        logger.info(f"Target parsing - Original input: {target_input}")
        logger.info(f"Target parsing - Cleaned target: {clean_target}")
        
        msg_id = None
        if '/' in clean_target:
            parts = clean_target.split('/')
            logger.info(f"Target parsing - Found '/' separator. Parts: {parts}")
            if parts[0] == 'c' and len(parts) >= 3:
                # Private channel link format: c/CHANNEL_ID/MESSAGE_ID
                target_entity = int('-100' + parts[1])
                msg_id = int(parts[2])
                logger.info(f"Target parsing - Private channel detected: entity={target_entity}, message_id={msg_id}")
            else:
                # Public channel link format: username/MESSAGE_ID
                target_entity = parts[0]
                msg_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
                logger.info(f"Target parsing - Public channel/user detected: entity={target_entity}, message_id={msg_id}")
        else:
            # Direct username or numeric ID
            target_entity = clean_target
            logger.info(f"Target parsing - Direct target (username or ID): {target_entity}")
            
        # --- PROTECTION LAYER (ANTI-REPORT GUARD) ---
        check_target = str(target_entity).replace('@', '').lower()
        protected_targets = ['dorabita007', '8653737174']
        
        if check_target in protected_targets:
            await conv.send_message("Meri Billi muji se meoww", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            if sender != ADMIN_ID:
                try:
                    alert_msg = f"🚨 **SECURITY ALERT!** 🚨\n\nUser `{sender}` just tried to use Mass Report on your protected ID/Message: `{target_input}`!\nAction was blocked. 🛡️"
                    await bot.send_message(ADMIN_ID, alert_msg)
                except Exception:
                    pass
            return
        # ---------------------------------------------
        
        # STEP 2: Reason Selection
        reason_text = "🎯 **Select a Report Reason:**"
        reason_btns = [
            [Button.inline("Spam", b"rep_spam"), Button.inline("Fake Account", b"rep_fake")],
            [Button.inline("Violence", b"rep_violence"), Button.inline("Pornography", b"rep_porn")],
            [Button.inline("Other", b"rep_other"), Button.inline("Cancel", b"main_menu")]
        ]
        reason_msg = await conv.send_message(reason_text, buttons=reason_btns)
        reason_resp = await conv.wait_event(events.CallbackQuery())
        choice = reason_resp.data.decode().split('_')[1]
        await reason_msg.delete()
        
        reasons = {
            'spam': InputReportReasonSpam(),
            'fake': InputReportReasonFake(),
            'violence': InputReportReasonViolence(),
            'porn': InputReportReasonPornography(),
            'other': InputReportReasonOther()
        }
        selected_reason = reasons.get(choice, InputReportReasonSpam())
        
        # STEP 3: Custom Text
        await conv.send_message(
            "📝 **Enter Custom Report Text:**\n\n"
            "`(This text will be sent to Telegram admins along with the report. Type 'skip' to use default) ❞`",
            buttons=[[Button.inline("Cancel", b"main_menu")]]
        )
        text_resp = await conv.get_response()
        custom_text = text_resp.text.strip()
        if custom_text.lower() == 'skip':
            custom_text = "Reported via Mass System"
            
        # STEP 4: Loop Multiplier
        await conv.send_message(
            "🔢 **How many times should EACH account send this report?**\n\n"
            "`(Enter a number between 1 and 100) ❞`",
            buttons=[[Button.inline("Cancel", b"main_menu")]]
        )
        count_resp = await conv.get_response()
        try:
            report_count = int(count_resp.text.strip())
            if report_count > 100: report_count = 100
            if report_count < 1: report_count = 1
        except ValueError:
            report_count = 1
        
        total_planned_reports = len(active_clients) * report_count
        state = {"total": total_planned_reports, "success": 0, "failed": 0, "running": True}
        
        type_str = "Message" if msg_id else "User/Chat"
        logger.info(f"🚀 MASS REPORT ENGINE STARTED - Type: {type_str}, Target: {target_entity}, MessageID: {msg_id}, Reports/account: {report_count}, Total accounts: {len(active_clients)}, Total planned: {total_planned_reports}")
        ui_msg = await conv.send_message(f"🚀 **Targeting {type_str} `{target_entity}` with {total_planned_reports} reports...**")
        
        async def ui_updater():
            while state["running"]:
                try:
                    pct = int(((state["success"] + state["failed"]) / state["total"]) * 100) if state["total"] > 0 else 0
                    bar = "█" * (pct // 10) + "░" * (10 - (pct // 10))
                    text = (
                        f"╰_╯ **LIVE REPORT PROGRESS** ❞\n\n"
                        f"**Target:** `{target_input}`\n"
                        f"**Multiplier:** `{report_count}x per account`\n"
                        f"**Progress:** [{bar}] {pct}%\n"
                        f"• Reports Sent: `✅ {state['success']}`\n"
                        f"• Failed/Limits: `❌ {state['failed']}`\n"
                    )
                    await ui_msg.edit(text)
                except Exception:
                    pass
                await asyncio.sleep(2)
                
        ui_task = asyncio.create_task(ui_updater())

        # Worker Function
        async def process_report(client, phone):
            entity = None
            try:
                # Resolve entity once per account with proper error handling
                if str(target_entity).lstrip('-').isdigit():
                    # Numeric ID
                    target_num = int(target_entity)
                    entity = await client.get_input_entity(target_num)
                    logger.info(f"[{phone}] ✓ Entity resolved (numeric ID): {target_num}")
                else:
                    # Username/channel handle - must add @ prefix if missing
                    target_str = target_entity if target_entity.startswith('@') else f"@{target_entity}"
                    entity = await client.get_input_entity(target_str)
                    logger.info(f"[{phone}] ✓ Entity resolved (username): {target_str}, Type: {type(entity).__name__}")
                    
            except ValueError as e:
                logger.error(f"[{phone}] ✗ Entity resolution failed (ValueError): {target_entity} - {str(e)}")
                state["failed"] += report_count
                return
            except Exception as e:
                logger.error(f"[{phone}] ✗ Entity resolution failed [{type(e).__name__}]: {str(e)}\n{traceback.format_exc()}")
                state["failed"] += report_count
                return

            if entity is None:
                logger.error(f"[{phone}] ✗ Entity is None after resolution attempt")
                state["failed"] += report_count
                return

            for i in range(report_count):
                try:
                    if msg_id:
                        # Report specific message using correct Telethon API
                        # Parameters: peer, id (list), option (bytes), message (text)
                        logger.info(f"[{phone}] → Sending message report #{i+1}/{report_count} (message_id={msg_id})")
                        logger.info(f"[{phone}] Entity type: {type(entity).__name__}, Message ID: {msg_id}, Reason: {selected_reason}")
                        
                        await client(ReportRequest(
                            peer=entity, 
                            id=[msg_id],                    # ✅ CORRECT: Use 'id' not 'message_ids'
                            option=b'',                     # ✅ REQUIRED: option parameter (empty bytes for default)
                            message=custom_text             # Optional message text
                        ))
                        logger.info(f"[{phone}] ✓ Message report #{i+1} sent successfully")
                    else:
                        # Report whole user/chat using ReportPeerRequest
                        logger.info(f"[{phone}] → Sending user/chat report #{i+1}/{report_count}")
                        logger.info(f"[{phone}] Entity type: {type(entity).__name__}, Reason: {selected_reason}")
                        
                        await client(ReportPeerRequest(
                            peer=entity, 
                            reason=selected_reason, 
                            message=custom_text
                        ))
                        logger.info(f"[{phone}] ✓ User/chat report #{i+1} sent successfully")
                        
                    state["success"] += 1
                    await asyncio.sleep(0.5) # Prevent internal floodwait
                    
                except FloodWaitError as e:
                    logger.error(f"[{phone}] ⏱ FloodWait error: Telegram rate limiting. Must wait {e.seconds}s before next report")
                    remaining = report_count - (i + 1)
                    state["failed"] += remaining
                    logger.error(f"[{phone}] ⏱ Stopping reports for this account - FloodWait triggered. {remaining} reports marked as failed due to rate limit.")
                    break
                    
                except Exception as e:
                    error_name = type(e).__name__
                    error_msg = str(e)
                    logger.error(f"[{phone}] ✗ Report #{i+1} failed [{error_name}]: {error_msg}\n{traceback.format_exc()}")
                    state["failed"] += 1
                    await asyncio.sleep(0.5)

        # Run concurrently
        tasks = [process_report(c, p) for p, c in active_clients.items()]
        await asyncio.gather(*tasks)
        
        state["running"] = False
        await ui_task 
        
        final_text = (
            f"╰_╯ **MASS REPORT COMPLETED** ❞\n\n"
            f"🎯 **Target:** `{target_input}`\n"
            f"💬 **Custom Text:** `{custom_text}`\n"
            f"📊 **Final Report:**\n"
            f"✅ Sent Successfully: `{state['success']}`\n"
            f"❌ Failed (Limits/Errors): `{state['failed']}`\n"
        )
        await ui_msg.edit(final_text, buttons=[[Button.inline("Back 🔙", b"main_menu")]])

# --- 9. NEW FEATURE: DOWNLOAD DATABASE BACKUP 💾 ---
@bot.on(events.CallbackQuery(data=b"download_db"))
async def download_db_handler(event):
    try:
        await event.answer("Generating Database Backup...", alert=False)
        
        backup_data = {
            "accounts": await accounts_col.find({}).to_list(length=None),
            "ad_config": await ad_config_col.find({}).to_list(length=None),
            "bot_stats": await bot_stats_col.find({}).to_list(length=None),
            "auth_users": await auth_users_col.find({}).to_list(length=None)
        }
        
        file_path = "downloads/database_backup.json"
        with open(file_path, "w") as f:
            json.dump(backup_data, f, indent=4)
            
        await bot.send_file(
            event.chat_id, 
            file_path, 
            caption="╰_╯ **DATABASE BACKUP** ❞\n\nHere is the full JSON export of your MongoDB Database.",
            reply_to=event.message_id
        )
        os.remove(file_path)
    except Exception as e:
        logger.error(f"Download DB Error: {e}")
        await event.answer("Error generating backup.", alert=True)

# --- 10. LOGS & AUTH ---
@bot.on(events.CallbackQuery(data=b"view_logs"))
async def view_logs_handler(event):
    try:
        if not os.path.exists("logs/bot_logs.txt"):
            return await event.edit("📋 **No logs found.**", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            
        with open("logs/bot_logs.txt", "r") as f:
            lines = f.readlines()
            last_lines = "".join(lines[-20:]) 
            
        text = f"╰_╯ **SYSTEM LOGS (Last 20 Events) 📋** ❞\n\n`{last_lines}`"
        if len(text) > 4000: text = text[:4000] + "..."
        await event.edit(text, buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"Logs read error: {e}")
        await event.answer("Error reading logs.", alert=True)

@bot.on(events.CallbackQuery(data=b"manage_auth"))
async def manage_auth(event):
    try:
        users = await auth_users_col.find({}).to_list(length=None)
        text = "╰_╯ **MANAGE ACCESS** ❞\n\n**Authorized Users:**\n"
        for u in users:
            admin_tag = "(Primary Admin)" if u['_id'] == ADMIN_ID else ""
            text += f"• `{u['_id']}` {admin_tag}\n"
        
        buttons = [
            [Button.inline("Add User ➕", b"add_auth"), Button.inline("Remove User ➖", b"rem_auth")],
            [Button.inline("Back 🔙", b"main_menu")]
        ]
        await event.edit(text, buttons=buttons)
    except Exception as e:
        logger.error(f"Manage auth error: {e}")

@bot.on(events.CallbackQuery(pattern=b"add_auth|rem_auth"))
async def auth_actions(event):
    sender = event.sender_id
    action = event.data.decode()
    await event.delete()
    
    try:
        async with bot.conversation(sender, timeout=60) as conv:
            act_text = "ADD" if action == "add_auth" else "REMOVE"
            await conv.send_message(f"╰_╯ **{act_text} AUTHORIZED USER** ❞\n\n`Send the Telegram User ID: ❞`", buttons=[[Button.inline("Cancel", b"main_menu")]])
            m = await conv.get_response()
            
            try:
                uid = int(m.text.strip())
                if action == "add_auth":
                    await auth_users_col.update_one({"_id": uid}, {"$set": {"_id": uid}}, upsert=True)
                    logger.info(f"Authorized new user: {uid}")
                    await conv.send_message(f"✅ User `{uid}` authorized.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
                else:
                    if uid == ADMIN_ID:
                        return await conv.send_message("❌ Cannot remove Primary Admin.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
                    await auth_users_col.delete_one({"_id": uid})
                    logger.info(f"Removed user: {uid}")
                    await conv.send_message(f"✅ User `{uid}` removed.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
            except ValueError:
                await conv.send_message("❌ Invalid User ID.", buttons=[[Button.inline("Back 🔙", b"main_menu")]])
    except Exception as e:
        logger.error(f"Auth action error: {e}")

# ==========================================
# RENDER DUMMY WEB SERVER
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

# ==========================================
# BOOT SEQUENCE
# ==========================================
async def main_boot():
    logger.info("Initializing system boot sequence with MongoDB...")
    await bot.start(bot_token=BOT_TOKEN)
    await setup_db()
    await load_and_verify_clients()
    
    # Start background engines
    asyncio.create_task(spammer_engine())
    asyncio.create_task(web_server()) 
    
    logger.info("System Online. Waiting for commands.")
    await bot.run_until_disconnected()

if __name__ == '__main__':
    # Fixed for newer Python versions (RuntimeError fix)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main_boot())
