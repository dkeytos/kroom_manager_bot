import os
import asyncio
from dotenv import load_dotenv
from metaapi_cloud_sdk import MetaApi
from telethon import TelegramClient
from telethon.sessions import StringSession
import time

# Load environment variables from .env file
load_dotenv()

# MetaApi credentials
METAAPI_TOKEN = os.getenv("METAAPI_TOKEN")
METAAPI_ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID")

# Telegram credentials – always create a new session every run
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
FORWARD_CHANNEL_ID = os.getenv("FORWARD_CHANNEL_ID")  # e.g. "-1002357820440"

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

async def send_telegram_message(client, target_entity, message: str):
    await client.send_message(target_entity, message, parse_mode="markdown")
    print("Sent message to Telegram.")

async def run_monitor(telegram_client, target_entity):
    # Create MetaApi connection and wait for synchronization
    api = MetaApi(METAAPI_TOKEN)
    account = await api.metatrader_account_api.get_account(METAAPI_ACCOUNT_ID)
    connection = account.get_streaming_connection()

    print("Connecting to MetaApi terminal...")
    await connection.connect()
    await connection.wait_synchronized()
    print("Connected and synchronized with the terminal.\n")
    
    history_storage = connection.history_storage
    # Track active positions with initial values: { pos_id: (open_price, tp, sl, trade_type, symbol) }
    last_positions = {}

    while True:
        try:
            terminal_state = connection.terminal_state
            positions = getattr(terminal_state, 'positions', [])
            
            current_positions = {}
            # Build a dictionary of current positions while preserving the initial TP & SL
            for pos in positions:
                pos_id = pos['id'] if isinstance(pos, dict) else getattr(pos, 'id', None)
                if not pos_id:
                    continue
                new_open_price = pos.get('openPrice', None) if isinstance(pos, dict) else getattr(pos, 'openPrice', None)
                new_tp = pos.get('takeProfit', None) if isinstance(pos, dict) else getattr(pos, 'takeProfit', None)
                new_sl = pos.get('stopLoss', None) if isinstance(pos, dict) else getattr(pos, 'stopLoss', None)
                new_trade_type = pos.get('type', 'N/A') if isinstance(pos, dict) else getattr(pos, 'type', 'N/A')
                new_symbol = pos.get('symbol', 'N/A') if isinstance(pos, dict) else getattr(pos, 'symbol', 'N/A')
                # Remove ".s" from the symbol
                new_symbol = new_symbol.replace('.s', '')
                
                # If we have seen this position before, preserve its initial values.
                if pos_id in last_positions:
                    current_positions[pos_id] = last_positions[pos_id]
                else:
                    current_positions[pos_id] = (new_open_price, new_tp, new_sl, new_trade_type, new_symbol)
            
            # Process closed positions (ones in last_positions but missing in current_positions)
            closed_positions = [pid for pid in last_positions if pid not in current_positions]
            for pos_id in closed_positions:
                # Let’s give a short pause before processing a closed position
                time.sleep(0.5)
                closed_deals = [
                    deal for deal in history_storage.deals 
                    if deal.get("positionId") == pos_id and deal.get("entryType") == "DEAL_ENTRY_OUT"
                ]
                if closed_deals:
                    closing_deal = closed_deals[0]
                    closing_price = closing_deal.get("price")
                    open_price, tp, sl, trade_type, symbol = last_positions.get(pos_id, (None, None, None, None, "N/A"))
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
                        
                        # Set header based on closing reason
                        if reason == "Closed via TP":
                            header = "**🤑 TP HIT SIGNAL**"
                        elif reason == "Closed via SL":
                            header = "**🚫 SL HIT SIGNAL**"
                        else:
                            header = "**✅ ACTION - CLOSE MARKET**"
                        
                        message = (
                            f"{header}\n"
                            f"Order ID: {pos_id}\n"
                            f"Symbol: **{symbol}**\n\n"
                            f"💰 Closing Price: {closing_price}\n"
                            f"📊 Points: {delta}\n\n"
                            f"Position closed! Review your trades."
                        )
                        await send_telegram_message(telegram_client, target_entity, message)
                    else:
                        await send_telegram_message(telegram_client, target_entity,
                            f"🔴 **CLOSE SIGNAL** 📉\nOrder ID: {pos_id}\nSymbol: **{symbol}**\nMissing price data.")
                else:
                    await send_telegram_message(telegram_client, target_entity,
                        f"🔴 **CLOSE SIGNAL** 📉\nOrder ID: {pos_id}\nSymbol: **{symbol}**\nNo closing deal found.")
            
            # Send open signal for new positions
            new_positions = [pid for pid in current_positions if pid not in last_positions]
            if new_positions:
                for pos_id in new_positions:
                    open_price, tp, sl, trade_type, symbol = current_positions.get(pos_id, (None, None, None, None, "N/A"))
                    action = "BUY" if "buy" in trade_type.lower() else "SELL"
                    market_emoji = "📈" if action == "BUY" else "📉"
                    header = f"**{market_emoji} ACTION - {action} MARKET {market_emoji}**"
                    message = (
                        f"{header}\n"
                        f"Order ID: {pos_id}\n"
                        f"Symbol: **{symbol}**\n\n"
                        f"💵 Entry Price: {open_price}\n"
                        f"🛑 SL: `{sl}`   ✅ TP: `{tp}`\n\n"
                        f"Stay alert and manage your risk!"
                    )
                    await send_telegram_message(telegram_client, target_entity, message)
            
            # Save the current positions so that initial TP and SL are preserved
            last_positions = current_positions
            await asyncio.sleep(0.5)
        except Exception as inner_e:
            print("Error in monitoring loop:", inner_e)
            raise inner_e

async def main():
    # Always create a new session
    telegram_client = TelegramClient(StringSession(), API_ID, API_HASH)
    print("Connecting to Telegram...")
    await telegram_client.connect()
    if not await telegram_client.is_user_authorized():
        await telegram_client.start()
        new_session_str = telegram_client.session.save()
        print("New Telegram session created. Save this session for future use:")
        print(new_session_str)
    else:
        print("Telegram client connected and authorized.")
    
    # Retrieve the channel entity using its numeric ID
    target_entity = await telegram_client.get_entity(int(FORWARD_CHANNEL_ID))
    
    # Outer loop: reconnect if the monitoring loop fails
    while True:
        try:
            await run_monitor(telegram_client, target_entity)
        except Exception as e:
            error_message = f"Monitoring error: {e}. Reconnecting in 5 seconds..."
            print(error_message)
            await send_telegram_message(telegram_client, target_entity, error_message)
            await asyncio.sleep(5)
            print("Reconnecting to MetaApi terminal...")

if __name__ == "__main__":
    asyncio.run(main())
