import os
import asyncio
from dotenv import load_dotenv
from metaapi_cloud_sdk import MetaApi
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import TypeNotFoundError, AuthKeyDuplicatedError, ServerError, FloodWaitError
import time
import datetime
import aiohttp
import logging
import pytz  # Add import for timezone handling

# Set up logging - only show warnings and errors by default
logging.basicConfig(level=logging.WARNING, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger('keyroom_manager')

# Create a special logger just for message events
msg_logger = logging.getLogger('message_events')
msg_logger.setLevel(logging.INFO)

# Load environment variables from .env file
load_dotenv()

# MetaApi credentials
METAAPI_TOKEN = os.getenv("METAAPI_TOKEN")
METAAPI_ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID")

# Telegram credentials
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
FORWARD_CHANNEL_ID = os.getenv("FORWARD_CHANNEL_ID")  # e.g. "-1002357820440"

# Path to save Telegram session
SESSION_FILE = os.path.join(os.path.dirname(__file__), 'telegram_session.txt')

def load_session_string():
    """Load the session string from file if it exists."""
    try:
        if os.path.exists(SESSION_FILE):
            with open(SESSION_FILE, 'r') as f:
                return f.read().strip()
    except Exception as e:
        logger.error(f"Error reading session file: {e}")
    return None

def save_session_string(session_str):
    """Save the session string to file."""
    try:
        with open(SESSION_FILE, 'w') as f:
            f.write(session_str)
        msg_logger.info("Session string saved successfully")
    except Exception as e:
        logger.error(f"Error saving session string: {e}")

def format_delta(delta, ref_price):
    """Round delta to the same number of decimals as ref_price."""
    try:
        ref_str = str(ref_price)
        if '.' in ref_str:
            decimals = len(ref_str.split('.')[1])
        else:
            decimals = 0
        return round(delta, decimals)
    except Exception:
        return round(delta, 2)

async def send_telegram_message(client, target_entity, message: str, reply_to=None):
    """Send a message to Telegram, optionally replying to another message.
    Returns the sent message object."""
    sent_message = await client.send_message(
        target_entity, 
        message, 
        parse_mode="markdown",
        reply_to=reply_to
    )
    
    # Log the message being sent - extract first line for cleaner logs
    first_line = message.split('\n')[0] if '\n' in message else message
    msg_logger.info(f"Sent to Telegram: {first_line[:50]}...")
    
    return sent_message

async def generate_status_message(current_positions, current_pending_orders, daily_closed_positions, daily_points, cancelled_orders=None):
    """Generate a status message showing the current trading day overview."""
    # Get current time in UTC
    now_utc = datetime.datetime.now(pytz.UTC)
    
    # Format the date and time with timezone
    date_str = now_utc.strftime("%d %b %Y")
    time_str = now_utc.strftime("%H:%M:%S %Z")  # Added %Z for timezone abbreviation
    
    # Header with last update timestamp
    message = f"📊 **TRADING OVERVIEW** 📊\n"
    message += f"📅 __{date_str}__ | ⏱️ Last update: __{time_str}__\n\n"
    
    # Active Positions Section
    message += f"📌 **ACTIVE POSITIONS ({len(current_positions)})**\n"
    if current_positions:
        for pos_id, (open_price, tp, sl, trade_type, symbol, _) in current_positions.items():
            action = "BUY" if "buy" in trade_type.lower() else "SELL"
            message += f"__{symbol} {action} | ID: {pos_id}__\n"
    else:
        message += "-\n"
    
    message += "\n"
    
    # Pending Orders Section
    message += f"⏳ **PENDING ORDERS ({len(current_pending_orders)})**\n"
    if current_pending_orders:
        for order_id, (price, tp, sl, trade_type, symbol) in current_pending_orders.items():
            if "BUY_LIMIT" in trade_type:
                action = "BUY LIMIT"
            elif "BUY_STOP" in trade_type:
                action = "BUY STOP"
            elif "SELL_LIMIT" in trade_type:
                action = "SELL LIMIT"
            elif "SELL_STOP" in trade_type:
                action = "SELL STOP"
            else:
                action = trade_type
                
            message += f"__{symbol} {action} | ID: {order_id}__\n"
    else:
        message += "-\n"
    
    message += "\n"
    
    # Today's Closed Positions
    message += f"🏁 **TODAY'S CLOSED POSITIONS ({len(daily_closed_positions)})**\n"
    if daily_closed_positions:
        for pos_data in daily_closed_positions:
            symbol = pos_data.get('symbol', 'Unknown')
            points = pos_data.get('points', 0)
            reason = pos_data.get('reason', 'Unknown')
            pos_id = pos_data.get('id', 'Unknown')
                
            message += f"__{symbol} | Points: {points} | ID: {pos_id}__\n"
    else:
        message += "-\n"
    
    message += "\n"
    
    # Cancelled Orders Section
    if cancelled_orders:
        message += f"🚫 **TODAY'S CANCELLED ORDERS ({len(cancelled_orders)})**\n"
        for order in cancelled_orders:
            symbol = order.get('symbol', 'Unknown')
            order_id = order.get('id', 'Unknown')
            order_type = order.get('type', 'Unknown')
            price = order.get('price', 'Unknown')

            if "BUY_LIMIT" in order_type:
                action = "BUY LIMIT"
            elif "BUY_STOP" in order_type:
                action = "BUY STOP"
            elif "SELL_LIMIT" in order_type:
                action = "SELL LIMIT"
            elif "SELL_STOP" in order_type:
                action = "SELL STOP"
            else:
                action = order_type
            
            message += f"__{symbol} {action} | ID: {order_id}__\n"
        
        message += "\n"
    
    # Daily Performance Summary
    emoji = "🟢" if daily_points > 0 else "🔴" if daily_points < 0 else "⚪️"
    message += f"{emoji} **TOTAL POINTS TODAY: {round(daily_points, 5)}**\n"
    
    return message

async def update_pinned_message(client, target_entity, status_message, pinned_message_id=None):
    """Update the pinned status message or create and pin a new one."""
    try:
        if pinned_message_id:
            # Update existing pinned message
            await client.edit_message(target_entity, pinned_message_id, status_message, parse_mode="markdown")
            return pinned_message_id
        else:
            # Create and pin new message
            sent_message = await client.send_message(target_entity, status_message, parse_mode="markdown")
            await client.pin_message(target_entity, sent_message)
            msg_logger.info("New status message pinned")
            return sent_message.id
    except Exception as e:
        logger.error(f"Error updating pinned message: {e}")
        
        # Only create a new message if it's specifically because the message doesn't exist
        if "message to edit not found" in str(e).lower() or "message not found" in str(e).lower():
            try:
                sent_message = await client.send_message(target_entity, status_message, parse_mode="markdown")
                await client.pin_message(target_entity, sent_message)
                msg_logger.info("Created new pinned message after previous was not found")
                return sent_message.id
            except Exception as inner_e:
                logger.error(f"Failed to create new pinned message: {inner_e}")
                return None
        return pinned_message_id  # Return the old ID so we can try again next time

async def run_monitor(telegram_client, target_entity):
    # Create MetaApi connection and wait for synchronization
    api = MetaApi(METAAPI_TOKEN)
    
    while True:
        try:
            account = await api.metatrader_account_api.get_account(METAAPI_ACCOUNT_ID)
            connection = account.get_streaming_connection()

            msg_logger.info("Connecting to MetaApi terminal...")
            await connection.connect()
            await connection.wait_synchronized()
            msg_logger.info("Connected and synchronized with MT terminal")
            
            # Initialize tracking variables
            history_storage = connection.history_storage
            last_positions = {}
            position_messages = {}
            last_pending_orders = {}
            pending_order_messages = {}
            triggered_pending_map = {}
            processed_orders = set()
            linked_orders = set()
            pending_disappeared_orders = {}
            order_processing_delay = 3
            pinned_message_id = None
            last_status_update_time = 0
            min_update_interval = 2
            today = datetime.date.today()
            daily_closed_positions = []
            daily_points = 0
            cancelled_orders = []  # New list to track cancelled orders

            # Main monitoring loop
            while True:
                try:
                    # Check if day has changed, reset daily stats if needed
                    current_day = datetime.date.today()
                    if current_day != today:
                        daily_closed_positions = []
                        daily_points = 0
                        cancelled_orders = []  # Reset cancelled orders for new day
                        today = current_day
                        pinned_message_id = None  # Force creation of a new pinned message for the new day
                        msg_logger.info(f"New day started: {today}. Reset daily statistics.")
                    
                    # Get terminal state
                    terminal_state = connection.terminal_state
                    positions = getattr(terminal_state, 'positions', [])
                    orders = terminal_state.orders  # Get pending orders
                    
                    # Process positions
                    current_positions = {}
                    # Build dictionary of current positions
                    for pos in positions:
                        pos_id = pos['id'] if isinstance(pos, dict) else getattr(pos, 'id', None)
                        if not pos_id:
                            continue
                            
                        # Extract position details
                        new_open_price = pos.get('openPrice', None) if isinstance(pos, dict) else getattr(pos, 'openPrice', None)
                        new_tp = pos.get('takeProfit', None) if isinstance(pos, dict) else getattr(pos, 'takeProfit', None)
                        new_sl = pos.get('stopLoss', None) if isinstance(pos, dict) else getattr(pos, 'stopLoss', None)
                        new_trade_type = pos.get('type', 'N/A') if isinstance(pos, dict) else getattr(pos, 'type', 'N/A')
                        new_symbol = pos.get('symbol', 'N/A') if isinstance(pos, dict) else getattr(pos, 'symbol', 'N/A')
                        new_order_id = pos.get('orderId', None) if isinstance(pos, dict) else getattr(pos, 'orderId', None)
                        new_symbol = new_symbol.replace('.s', '')
                        
                        # If we have seen this position before, preserve its initial values
                        if pos_id in last_positions:
                            old_price, old_tp, old_sl, old_type, old_symbol, old_order_id = last_positions[pos_id]
                            current_positions[pos_id] = (old_price, old_tp, old_sl, old_type, old_symbol, old_order_id or new_order_id)
                        else:
                            current_positions[pos_id] = (new_open_price, new_tp, new_sl, new_trade_type, new_symbol, new_order_id)
                    
                    # Process pending orders
                    current_pending_orders = {}
                    for order in orders:
                        order_id = order['id'] if isinstance(order, dict) else getattr(order, 'id', None)
                        if not order_id:
                            continue
                        
                        price = order.get('openPrice', None) if isinstance(order, dict) else getattr(order, 'openPrice', None)
                        tp = order.get('takeProfit', None) if isinstance(order, dict) else getattr(order, 'takeProfit', None)
                        sl = order.get('stopLoss', None) if isinstance(order, dict) else getattr(order, 'stopLoss', None)
                        trade_type = order.get('type', 'N/A') if isinstance(order, dict) else getattr(order, 'type', 'N/A')
                        symbol = order.get('symbol', 'N/A') if isinstance(order, dict) else getattr(order, 'symbol', 'N/A')
                        symbol = symbol.replace('.s', '')
                        
                        current_pending_orders[order_id] = (price, tp, sl, trade_type, symbol)
                    
                    # Process closed positions 
                    closed_positions = [pid for pid in last_positions if pid not in current_positions]
                    for pos_id in closed_positions:
                        # No sleep needed here - we'll process immediately
                        # but be aware the closing info might need a moment to appear in history
                        closed_deals = [
                            deal for deal in history_storage.deals 
                            if deal.get("positionId") == pos_id and deal.get("entryType") == "DEAL_ENTRY_OUT"
                        ]
                        
                        # If we don't find the closing deal immediately, wait a moment and try once more
                        if not closed_deals:
                            # Short wait to allow history to update if needed
                            await asyncio.sleep(0.1)
                            closed_deals = [
                                deal for deal in history_storage.deals 
                                if deal.get("positionId") == pos_id and deal.get("entryType") == "DEAL_ENTRY_OUT"
                            ]
                        
                        if closed_deals:
                            closing_deal = closed_deals[0]
                            closing_price = closing_deal.get("price")
                            open_price, tp, sl, trade_type, symbol, _ = last_positions.get(pos_id, (None, None, None, None, "N/A", None))
                            if closing_price is not None and open_price is not None:
                                # Calculate delta based on trade direction
                                if "buy" in trade_type.lower():
                                    delta = closing_price - open_price
                                    # For a BUY: if closing_price is within 5% of the range up to TP:
                                    if tp is not None and closing_price >= open_price + 0.95*(tp - open_price):
                                        reason = "Closed via TP"
                                    # For a BUY: if closing_price is within 5% of the range down to SL:
                                    elif sl is not None and closing_price <= open_price - 0.95*(open_price - sl):
                                        reason = "Closed via SL"
                                    else:
                                        reason = "Manual Closing"
                                else:  # SELL trade
                                    delta = open_price - closing_price
                                    # For a SELL: if closing_price is within 5% of the range down to TP:
                                    if tp is not None and closing_price <= open_price - 0.95*(open_price - tp):
                                        reason = "Closed via TP"
                                    # For a SELL: if closing_price is within 5% of the range up to SL:
                                    elif sl is not None and closing_price >= open_price + 0.95*(sl - open_price):
                                        reason = "Closed via SL"
                                    else:
                                        reason = "Manual Closing"
                                
                                delta = format_delta(delta, open_price)
                                
                                # Track daily statistics
                                daily_closed_positions.append({
                                    'id': pos_id,
                                    'symbol': symbol,
                                    'points': delta,
                                    'reason': reason
                                })
                                daily_points += delta
                                
                                # Format the message based on reason and delta
                                if reason == "Closed via TP":
                                    message = (
                                        f"**🤑 TP {symbol}**\n"
                                        f"__ID: {pos_id}__\n\n"
                                        f"💰 Closing Price: {closing_price}\n 📊 Points: {delta}"
                                    )
                                elif reason == "Closed via SL":
                                    message = (
                                        f"**⛔️ SL {symbol}**\n"
                                        f"__ID: {pos_id}__\n\n"
                                        f"💰 Closing Price: {closing_price}\n 📊 Points: {delta}"
                                    )
                                else:
                                    # Choose emoji based on whether delta is positive or negative
                                    header_emoji = "✅" if delta > 0 else "❌"
                                    message = (
                                        f"**{header_emoji} CLOSE {symbol}**\n"
                                        f"__ID: {pos_id}__\n\n"
                                        f"💰 Closing Price: {closing_price}\n 📊 Points: {delta}"
                                    )
                                
                                # Get the message ID of the original open position to reply to it
                                reply_to_message = position_messages.get(pos_id)
                                await send_telegram_message(telegram_client, target_entity, message, reply_to=reply_to_message)
                                
                                # Remove the message ID from tracking as position is now closed
                                if pos_id in position_messages:
                                    del position_messages[pos_id]
                            else:
                                await send_telegram_message(telegram_client, target_entity,
                                    f"🔴 **CLOSE {symbol}**\nID: {pos_id}\n\nMissing price data.")
                        else:
                            open_price, tp, sl, trade_type, symbol, _ = last_positions.get(pos_id, (None, None, None, None, "N/A", None))
                            await send_telegram_message(telegram_client, target_entity,
                                f"🔴 **CLOSE {symbol}**\nID: {pos_id}\n\nNo closing deal found.")
                        
                        # Update the pinned status message after each position closes
                        status_message = await generate_status_message(
                            current_positions, current_pending_orders, daily_closed_positions, daily_points)
                        pinned_message_id = await update_pinned_message(
                            telegram_client, target_entity, status_message, pinned_message_id)
                    
                    # Find newly disappeared orders and queue them for delayed processing
                    disappeared_orders = [order_id for order_id in last_pending_orders if order_id not in current_pending_orders]
                    update_needed = False  # Flag to track if we need to update the pinned message
                    
                    for order_id in disappeared_orders:
                        # Skip if already processed or already in queue
                        if order_id in processed_orders or order_id in pending_disappeared_orders:
                            continue
                        
                        # Check if this order has immediately become a position (no need for delay)
                        if order_id in current_positions:
                            # Handle it immediately as a triggered order
                            price, tp, sl, trade_type, symbol = last_pending_orders.get(order_id, (None, None, None, None, "N/A"))
                            msg_logger.info(f"Order {order_id} was immediately detected as a position")
                            
                            # Link the pending order message to the position
                            if order_id in pending_order_messages and order_id not in linked_orders:
                                triggered_pending_map[order_id] = pending_order_messages[order_id]
                                linked_orders.add(order_id)
                                
                                # Since this order is now a position, send a "triggered" message
                                position_data = current_positions[order_id]
                                open_price, tp, sl, trade_type, symbol, _ = position_data
                                action = "BUY" if "buy" in trade_type.lower() else "SELL"
                                market_emoji = "📈" if action == "BUY" else "📉"
                                
                                message = (
                                    f"**{market_emoji} TRIGGERED {action} {symbol}**\n"
                                    f"__ID: {order_id}__\n\n"
                                    f"💵 Entry Price: {open_price}\n"
                                    f"⛔️ SL: `{sl}`   ✅ TP: `{tp}`\n\n"
                                )
                                
                                # Reply to the original pending order message
                                reply_to = pending_order_messages.get(order_id)
                                sent_message = await send_telegram_message(telegram_client, target_entity, message, reply_to=reply_to)
                                
                                # Store the message for when the position closes
                                position_messages[order_id] = sent_message
                            
                            # Mark as processed
                            processed_orders.add(order_id)
                        else:
                            # Add to delayed queue with current timestamp
                            price, tp, sl, trade_type, symbol = last_pending_orders.get(order_id, (None, None, None, None, "N/A"))
                            pending_disappeared_orders[order_id] = (time.time(), (price, tp, sl, trade_type, symbol))
                            msg_logger.info(f"Order {order_id} disappeared, will check if triggered after {order_processing_delay}s delay")
                        
                        # Set flag that we need to update the pinned message
                        update_needed = True
                    
                    # Process orders in the delayed queue that have waited long enough
                    current_time = time.time()
                    orders_to_process = [order_id for order_id, (timestamp, _) in pending_disappeared_orders.items() 
                                        if current_time - timestamp >= order_processing_delay]
                    
                    # Process orders that have waited the required delay time
                    for order_id in orders_to_process:
                        # Skip if already processed
                        if order_id in processed_orders:
                            continue
                        
                        order_data = pending_disappeared_orders[order_id][1]
                        price, tp, sl, trade_type, symbol = order_data
                        
                        # KEY LOGIC: Check if this order's ID now exists as a position ID
                        # This is how we know the pending order was triggered
                        triggered = order_id in current_positions
                        
                        if triggered:
                            msg_logger.info(f"Order {order_id} ({symbol}) was triggered")
                            
                            # Link the pending order message to the position for future reference
                            if order_id in pending_order_messages and order_id not in linked_orders:
                                triggered_pending_map[order_id] = pending_order_messages[order_id]
                                linked_orders.add(order_id)
                                
                                # Since this order is now a position, we need to send a "triggered" message
                                position_data = current_positions[order_id]
                                open_price, tp, sl, trade_type, symbol, _ = position_data
                                action = "BUY" if "buy" in trade_type.lower() else "SELL"
                                market_emoji = "📈" if action == "BUY" else "📉"
                                
                                message = (
                                    f"**{market_emoji} TRIGGERED {action} {symbol}**\n"
                                    f"__ID: {order_id}__\n\n"
                                    f"💵 Entry Price: {open_price}\n"
                                    f"⛔️ SL: `{sl}`   ✅ TP: `{tp}`\n\n"
                                )
                                
                                # Reply to the original pending order message
                                reply_to = pending_order_messages.get(order_id)
                                sent_message = await send_telegram_message(telegram_client, target_entity, message, reply_to=reply_to)
                                
                                # Store the message for when the position closes
                                position_messages[order_id] = sent_message
                        else:
                            # If not triggered after waiting, it was canceled
                            msg_logger.info(f"Order {order_id} ({symbol}) was canceled")
                            
                            # Add to cancelled orders list
                            cancelled_orders.append({
                                'id': order_id,
                                'symbol': symbol,
                                'type': trade_type,
                                'price': price
                            })
                            
                            message = (
                                f"**🚫 CANCELED ORDER {symbol}**\n"
                                f"__ID: {order_id}__\n\n"
                                f"Order was canceled before being triggered"
                            )
                            
                            # Reply to the original pending order message
                            reply_to = pending_order_messages.get(order_id)
                            await send_telegram_message(telegram_client, target_entity, message, reply_to=reply_to)
                            
                            # Remove from pending tracking since it's closed
                            if order_id in pending_order_messages:
                                del pending_order_messages[order_id]
                        
                        # Mark as processed and remove from queue
                        processed_orders.add(order_id)
                        del pending_disappeared_orders[order_id]
                        
                        # Set flag that we need to update the pinned message
                        update_needed = True
                    
                    # Process new positions (that were not from pending orders)
                    new_positions = [pid for pid in current_positions if pid not in last_positions]
                    for pos_id in new_positions:
                        # Skip positions that came from pending orders - we already handled them
                        if pos_id in linked_orders:
                            continue
                        
                        # If this position ID matches a currently pending order, it's a triggered order
                        # Handle it immediately without waiting for the order to disappear
                        if pos_id in current_pending_orders:
                            msg_logger.info(f"New position {pos_id} matches a current pending order - handling as triggered")
                            
                            # Link the pending order message to the position
                            if pos_id in pending_order_messages and pos_id not in linked_orders:
                                triggered_pending_map[pos_id] = pending_order_messages[pos_id]
                                linked_orders.add(pos_id)
                                
                                # Get the position data
                                open_price, tp, sl, trade_type, symbol, _ = current_positions[pos_id]
                                action = "BUY" if "buy" in trade_type.lower() else "SELL"
                                market_emoji = "📈" if action == "BUY" else "📉"
                                
                                # Create and send triggered message
                                message = (
                                    f"**{market_emoji} TRIGGERED {action} {symbol}**\n"
                                    f"__ID: {pos_id}__\n\n"
                                    f"💵 Entry Price: {open_price}\n"
                                    f"⛔️ SL: `{sl}`   ✅ TP: `{tp}`\n\n"
                                )
                                
                                # Reply to the original pending order message
                                reply_to = pending_order_messages.get(pos_id)
                                sent_message = await send_telegram_message(telegram_client, target_entity, message, reply_to=reply_to)
                                
                                # Store the message for when the position closes
                                position_messages[pos_id] = sent_message
                                
                                # Mark as processed to avoid duplicate messages
                                processed_orders.add(pos_id)
                                continue
                        
                        # If this position ID matches any pending order we're tracking (but was just triggered)
                        if pos_id in pending_order_messages and pos_id not in linked_orders:
                            msg_logger.info(f"New position {pos_id} matches a known pending order - handling as triggered")
                            
                            # Link the pending order message to the position
                            triggered_pending_map[pos_id] = pending_order_messages[pos_id]
                            linked_orders.add(pos_id)
                            
                            # Get the position data
                            open_price, tp, sl, trade_type, symbol, _ = current_positions[pos_id]
                            action = "BUY" if "buy" in trade_type.lower() else "SELL"
                            market_emoji = "📈" if action == "BUY" else "📉"
                            
                            # Create and send triggered message
                            message = (
                                f"**{market_emoji} TRIGGERED {action} {symbol}**\n"
                                f"__ID: {pos_id}__\n\n"
                                f"💵 Entry Price: {open_price}\n"
                                f"⛔️ SL: `{sl}`   ✅ TP: `{tp}`\n\n"
                            )
                            
                            # Reply to the original pending order message
                            reply_to = pending_order_messages.get(pos_id)
                            sent_message = await send_telegram_message(telegram_client, target_entity, message, reply_to=reply_to)
                            
                            # Store the message for when the position closes
                            position_messages[pos_id] = sent_message
                            
                            # Mark as processed to avoid duplicate messages
                            processed_orders.add(pos_id)
                            continue
                        
                        # For direct market orders (not from pending)
                        open_price, tp, sl, trade_type, symbol, _ = current_positions[pos_id]
                        action = "BUY" if "buy" in trade_type.lower() else "SELL"
                        market_emoji = "📈" if action == "BUY" else "📉"
                        
                        message = (
                            f"**{market_emoji} {action} {symbol}**\n"
                            f"__ID: {pos_id}__\n\n"
                            f"💵 Entry Price: {open_price}\n"
                            f"⛔️ SL: `{sl}`   ✅ TP: `{tp}`\n\n"
                        )
                        
                        # Store the message ID for later use when position closes
                        sent_message = await send_telegram_message(telegram_client, target_entity, message)
                        position_messages[pos_id] = sent_message
                        
                        # Set flag that we need to update the pinned message
                        update_needed = True
                    
                    # Send messages for new pending orders
                    new_pending_orders = [order_id for order_id in current_pending_orders if order_id not in last_pending_orders]
                    for order_id in new_pending_orders:
                        # Skip if already processed or if already tracking as a position
                        if order_id in processed_orders or order_id in position_messages:
                            continue
                        
                        price, tp, sl, trade_type, symbol = current_pending_orders[order_id]
                        
                        # Determine order type and appropriate emoji
                        order_emoji = "🔷"  # Default pending order emoji
                        if "BUY_LIMIT" in trade_type:
                            action = "BUY LIMIT"
                            order_emoji = "🔹"
                        elif "BUY_STOP" in trade_type:
                            action = "BUY STOP"
                            order_emoji = "🔹"
                        elif "SELL_LIMIT" in trade_type:
                            action = "SELL LIMIT"
                            order_emoji = "🔸"
                        elif "SELL_STOP" in trade_type:
                            action = "SELL STOP"
                            order_emoji = "🔸"
                        else:
                            action = trade_type
                        
                        message = (
                            f"**{order_emoji} PENDING {action} {symbol}**\n"
                            f"__ID: {order_id}__\n\n"
                            f"💵 Trigger Price: {price}\n"
                            f"⛔️ SL: `{sl}`   ✅ TP: `{tp}`\n\n"
                        )
                        
                        # Store the message ID for later use
                        sent_message = await send_telegram_message(telegram_client, target_entity, message)
                        pending_order_messages[order_id] = sent_message
                        msg_logger.info(f"Sent message for new pending order {order_id}")
                        
                        # Set flag that we need to update the pinned message
                        update_needed = True

                    # Update the pinned status message if needed and not too frequent
                    current_time = time.time()
                    if (pinned_message_id is None or 
                        update_needed and current_time - last_status_update_time >= min_update_interval):
                        
                        status_message = await generate_status_message(
                            current_positions, current_pending_orders, daily_closed_positions, 
                            daily_points, cancelled_orders)
                        
                        new_pinned_id = await update_pinned_message(
                            telegram_client, target_entity, status_message, pinned_message_id)
                        
                        # Only update the pinned message ID if we got a valid ID back
                        if new_pinned_id:
                            pinned_message_id = new_pinned_id
                            last_status_update_time = current_time
                    
                    # Save state for next cycle
                    last_positions = current_positions
                    last_pending_orders = current_pending_orders
                    await asyncio.sleep(0.1)
                    
                    # Periodically clean up processed_orders to avoid memory leaks
                    if len(processed_orders) > 1000:
                        relevant_orders = set(pending_order_messages.keys()).union(linked_orders)
                        processed_orders = processed_orders.intersection(relevant_orders)
                        
                except TypeNotFoundError as tl_err:
                    # Specific handling for Telethon TypeNotFoundError
                    logger.warning(f"Telegram protocol error, reconnecting: {tl_err}")
                    
                    # Try to safely disconnect and reconnect the Telegram client
                    try:
                        await telegram_client.disconnect()
                    except Exception as disconnect_err:
                        logger.error(f"Error disconnecting Telegram client: {disconnect_err}")
                    
                    # Wait before reconnecting
                    await asyncio.sleep(5)
                    
                    try:
                        await telegram_client.connect()
                        # Get the target entity again
                        target_entity = await telegram_client.get_entity(int(FORWARD_CHANNEL_ID))
                        logger.info("Telegram client reconnected.")
                    except Exception as reconnect_err:
                        logger.error(f"Failed to reconnect Telegram client: {reconnect_err}")
                        # Break out of the inner loop to fully restart connections
                        break
                    
                except (AuthKeyDuplicatedError, ServerError, FloodWaitError) as tg_err:
                    # Handle other Telegram-specific errors
                    logger.warning(f"Telegram error: {tg_err}")
                    # Break out to restart connections
                    break
                    
                except aiohttp.ClientError as http_err:
                    # Handle aiohttp errors (network issues)
                    logger.warning(f"Network error: {http_err}")
                    await asyncio.sleep(5)
                    # Continue to retry in the current loop
                    
                except Exception as inner_e:
                    logger.error(f"Error in monitoring loop: {inner_e}")
                    # For other unexpected errors, break out to restart connections
                    break
                    
        except Exception as outer_e:
            logger.error(f"MetaAPI connection error: {outer_e}")
            
        # Clean up MetaAPI connection before retrying
        try:
            if 'connection' in locals() and connection:
                await connection.close()
                msg_logger.info("Closed MetaAPI connection")
        except Exception as cleanup_err:
            logger.error(f"Error closing MetaAPI connection: {cleanup_err}")
            
        # Wait before reconnecting
        logger.warning("Waiting 5 seconds before reconnecting...")
        await asyncio.sleep(5)
        msg_logger.info("Attempting to reconnect to MetaAPI...")

async def main():
    session_string = load_session_string()
    telegram_client = None
    
    # Outer loop for the entire program
    while True:
        try:
            # Create a new client if needed
            if not telegram_client:
                telegram_client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
                msg_logger.info("Connecting to Telegram...")
                await telegram_client.connect()
                
                if not await telegram_client.is_user_authorized():
                    msg_logger.info("No valid session found. Starting new authorization...")
                    await telegram_client.start()
                    new_session_str = telegram_client.session.save()
                    save_session_string(new_session_str)
                    msg_logger.info("New Telegram session created and saved for future use")
                else:
                    msg_logger.info("Telegram client connected and authorized")
            
            # Get the target entity
            target_entity = await telegram_client.get_entity(int(FORWARD_CHANNEL_ID))
            
            # Run the monitor
            await run_monitor(telegram_client, target_entity)
            
        except TypeNotFoundError as tl_err:
            logger.warning(f"Telegram protocol error in main loop: {tl_err}")
            
            # Clean up and recreate telegram client
            try:
                if telegram_client:
                    await telegram_client.disconnect()
                    telegram_client = None
            except:
                pass
            
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            
            # Clean up resources
            try:
                if telegram_client:
                    await telegram_client.disconnect()
                    telegram_client = None
            except:
                pass
        
        # Wait before retrying
        logger.warning("Restarting in 15 seconds...")
        await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        msg_logger.info("Program terminated by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
