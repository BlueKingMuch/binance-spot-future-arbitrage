import logging
import argparse
import time
import json
import asyncio
import websockets
import math
import yaml
from datetime import datetime, timedelta
from binance.async_client import AsyncClient
from binance.exceptions import BinanceAPIException
from decimal import Decimal, ROUND_DOWN

# --- KONFIGURATION LADEN ---
# Lade API-Keys aus keys.yaml
try:
    with open("keys.yaml") as f:
        keys = yaml.load(f, Loader=yaml.FullLoader)
    API_KEY = keys['API_KEY']
    API_SECRET = keys['API_SECRET']
except (FileNotFoundError, KeyError):
    print("FEHLER: 'keys.yaml' nicht gefunden oder fehlerhaft. Bitte die 'keys.yaml.example' kopieren, umbenennen und die API-Keys eintragen.")
    exit()

# Lade alle anderen Konfigurationen aus config.yaml
try:
    with open("config.yaml") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
except FileNotFoundError:
    print("FEHLER: 'config.yaml' nicht gefunden. Stelle sicher, dass die Datei im selben Verzeichnis liegt.")
    exit()

# --- HAUPTKONFIGURATION ---
LIVE_TRADING = config['LIVE_TRADING']

# --- STRATEGIE- & RISIKOMANAGEMENT-PARAMETER ---
cfg_strat = config['strategy']
SPOT_QUOTE_CURRENCY = cfg_strat['SPOT_QUOTE_CURRENCY']
FUTURE_QUOTE_CURRENCY = cfg_strat['FUTURE_QUOTE_CURRENCY']
ENTRY_THRESHOLD_FACTOR = cfg_strat['ENTRY_THRESHOLD_FACTOR']
EXIT_THRESHOLD_FACTOR = cfg_strat['EXIT_THRESHOLD_FACTOR']
PERCENTAGE_TRADE_AMOUNT_LIQUIDITY = cfg_strat['PERCENTAGE_TRADE_AMOUNT_LIQUIDITY']
MIN_TRADE_AMOUNT_USDC = cfg_strat['MIN_TRADE_AMOUNT_USDC']
MAX_TRADE_AMOUNT_USDC = cfg_strat['MAX_TRADE_AMOUNT_USDC']
MAX_CAPITAL_ALLOCATION_PERCENT = cfg_strat['MAX_CAPITAL_ALLOCATION_PERCENT']
MAX_PLAUSIBLE_SPREAD = cfg_strat['MAX_PLAUSIBLE_SPREAD']
MAX_INTERNAL_SPREAD = cfg_strat['MAX_INTERNAL_SPREAD']
MAX_HOLDING_HOURS = cfg_strat['MAX_HOLDING_HOURS']
MAX_TIME_DIFFERENCE = timedelta(milliseconds=cfg_strat['MAX_TIME_DIFFERENCE_MS'])
FORCE_EXIT_BEFORE_DELIST_SECONDS = cfg_strat['FORCE_EXIT_BEFORE_DELIST_SECONDS']
MIN_TRADE_LIFETIME_SECONDS = cfg_strat['MIN_TRADE_LIFETIME_SECONDS']

# --- MAINTENANCE KONFIGURATION ---
cfg_maint = config['maintenance']
ENABLE_MAINTENANCE = cfg_maint['ENABLE']
CAPITAL_DIFFERENCE_THRESHOLD_PERCENT = cfg_maint['CAPITAL_DIFFERENCE_THRESHOLD_PERCENT']
MARGIN_DUST_THRESHOLD_USDT = cfg_maint['MARGIN_DUST_THRESHOLD_USDT']
BNB_MIN_PER_ACCOUNT = cfg_maint['BNB_MIN_PER_ACCOUNT']
BNB_BUY_AMOUNT_USDC = cfg_maint['BNB_BUY_AMOUNT_USDC']
MIN_BNB_TRANSFER_AMOUNT = cfg_maint['MIN_BNB_TRANSFER_AMOUNT']

# --- SYSTEM & API ---
cfg_sys = config['system']
RE_DISCOVERY_INTERVAL_HOURS = cfg_sys['RE_DISCOVERY_INTERVAL_HOURS']
SYSTEM_TASK_INTERVAL_SECONDS = cfg_sys['SYSTEM_TASK_INTERVAL_SECONDS']
DEBUG_TASK_INTERVAL_SECONDS = cfg_sys['DEBUG_TASK_INTERVAL_SECONDS']
FUTURES_WS_URL = cfg_sys['FUTURES_WS_URL']
SPOT_WS_BASE_URL = cfg_sys['SPOT_WS_BASE_URL']
BLACKLISTED_BASE_ASSETS = set(cfg_sys['BLACKLISTED_BASE_ASSETS'])


# --- LOGGING SETUP ---
log_file_name = f"arbitrage_bot_{'live' if LIVE_TRADING else 'simulation'}.log"
file_handler = logging.FileHandler(log_file_name, encoding='utf-8')
stream_handler = logging.StreamHandler()
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',handlers=[file_handler, stream_handler],encoding='utf-8')


# --- SHARED STATE & GLOBALE VARIABLEN ---
client = None
trade_execution_lock = asyncio.Lock()
book_tickers = {}
tracked_opportunities = {}
debug_top_opportunities = {}
spot_stream_task = None
known_bases = set()
SPOT_EXCHANGE_INFO = {}
FUTURE_EXCHANGE_INFO = {}
account_state_cache = {"total_equity": 0.0,"available_capital": 0.0}
margin_interest_rates = {}
futures_funding_rates = {}
account_state_cache = {
    "margin_account": {},
    "futures_account": {},
    "btc_price": 0.0,
    "total_equity": 0.0,
    "available_capital": 0.0,
    "last_update": None
}

class Opportunity:
    def __init__(self, base_asset, direction, entry_spread, spot_prices, future_prices):
        self.base_asset, self.direction, self.entry_time = base_asset, direction, datetime.now()
        self.trade_id = f"{base_asset}-{self.entry_time.strftime('%Y%m%d-%H%M%S%f')}"
        self.entry_spread_factor = entry_spread
        self.peak_spread_factor = entry_spread
        try:
            self.low_conv_spread_factor = (spot_prices['ask'] / future_prices['bid']) if direction == 'spot_high' else (future_prices['ask'] / spot_prices['bid'])
        except ZeroDivisionError:
            self.low_conv_spread_factor = 1.0
        self.peak_conv_spread_factor = self.low_conv_spread_factor
        self.entry_spot_prices, self.entry_future_prices = spot_prices, future_prices
        self.status = "new"
        self.spot_order_id = None
        self.future_order_id = None


        self.spot_entry_fill = None
        self.future_entry_fill = None
        self.executed_spot_qty_net = 0.0
        self.realized_entry_spread_factor = 0.0

        spot_liq_usd = (spot_prices['bid_qty'] * spot_prices['bid']) if direction == 'spot_high' else (spot_prices['ask_qty'] * spot_prices['ask'])
        future_liq_usd = (future_prices['ask_qty'] * future_prices['ask']) if direction == 'spot_high' else (future_prices['bid_qty'] * future_prices['bid'])

        logging.info(f"[ENTRY DETECTED] id={self.trade_id} dir={direction} spread={entry_spread:.5f} spot_prices=[B:{spot_prices['bid']} A:{spot_prices['ask']}] future_prices=[B:{future_prices['bid']} A:{future_prices['ask']}] spot_liq={spot_liq_usd:,.0f}$ future_liq={future_liq_usd:,.0f}$")

    def update_spreads(self, potential_spread, convergence_spread):
        if potential_spread > self.peak_spread_factor: self.peak_spread_factor = potential_spread
        if convergence_spread < self.low_conv_spread_factor: self.low_conv_spread_factor = convergence_spread
        if convergence_spread > self.peak_conv_spread_factor: self.peak_conv_spread_factor = convergence_spread

# --- HILFSFUNKTIONEN ---

def compute_vwap_and_fees(fills: list, quote_currency: str):
    if not fills:
        return 0.0, 0.0

    total_qty = 0.0
    total_quote = 0.0
    total_fee = 0.0

    for f in fills:
        qty = float(f.get("qty", 0))
        price = float(f.get("price", 0))
        commission = float(f.get("commission", 0))
        commission_asset = f.get("commissionAsset")

        total_qty += qty
        total_quote += qty * price

        if commission > 0:
            if commission_asset != quote_currency:
                total_fee += commission * price
            else:
                total_fee += commission

    vwap = total_quote / total_qty if total_qty > 0 else 0
    return vwap, total_fee

def compute_futures_vwap_and_fees(trades: list):
    if not trades: return 0.0, 0.0, 0.0
    total_qty, total_quote, total_fee, realized_pnl = 0.0, 0.0, 0.0, 0.0
    for t in trades:
        qty, price, commission = float(t.get("qty", 0)), float(t.get("price", 0)), float(t.get("commission", 0))
        realized_pnl += float(t.get("realizedPnl", 0))
        total_qty += qty
        total_quote += qty * price
        total_fee += commission
    return (total_quote / total_qty if total_qty > 0 else 0), total_fee, realized_pnl

def get_fees_from_fills(fills: list, quote_currency: str) -> float:
    total_fee = 0.0
    if not fills: return 0.0
    for fill in fills:
        commission = float(fill.get('commission', 0))
        if commission > 0:
            if fill.get('commissionAsset') != quote_currency:
                price = float(fill.get('price', 0))
                total_fee += commission * price
            else:
                total_fee += commission
    return total_fee


def recalculate_account_metrics():
    global account_state_cache
    try:
        margin_account = account_state_cache.get("margin_account", {})
        future_account = account_state_cache.get("futures_account", {})
        btc_price = account_state_cache.get("btc_price", 0.0)

        if not margin_account or not future_account or btc_price == 0.0:
            return 

        margin_free_usdc = float(next((a['free'] for a in margin_account.get('userAssets', []) if a['asset'] == SPOT_QUOTE_CURRENCY), "0"))
        
        margin_net_asset_btc = float(margin_account.get('totalNetAssetOfBtc', 0.0))
        margin_equity_usdt = margin_net_asset_btc * btc_price

        total_wallet_balance = float(future_account.get('totalWalletBalance', "0"))
        total_unrealized_pnl = float(future_account.get('totalUnrealizedProfit', "0"))
        futures_equity = total_wallet_balance + total_unrealized_pnl
        
        futures_available_margin = float(future_account.get('availableBalance', "0"))
        
        total_equity = margin_equity_usdt + futures_equity
        available_capital = margin_free_usdc + futures_available_margin

        account_state_cache["total_equity"] = total_equity
        account_state_cache["available_capital"] = available_capital
        account_state_cache["last_update"] = datetime.now()

    except Exception as e:
        logging.error(f"Fehler bei der Neuberechnung der Account-Metriken: {e}")


async def initial_account_sync(client: AsyncClient):
    global account_state_cache
    logging.info("Führe initialen Account-Sync durch...")
    try:
        async with asyncio.timeout(15):
            margin_account_task = client.get_margin_account()
            future_account_task = client.futures_account()
            btc_price_task = client.get_symbol_ticker(symbol=f"BTC{FUTURE_QUOTE_CURRENCY}")
            
            margin_account, future_account, btc_price_ticker = await asyncio.gather(
                margin_account_task, future_account_task, btc_price_task
            )

        account_state_cache["margin_account"] = margin_account
        account_state_cache["futures_account"] = future_account
        account_state_cache["btc_price"] = float(btc_price_ticker['price'])
        
        recalculate_account_metrics()
        logging.info("Initialer Account-Sync erfolgreich. Cache ist befüllt.")
        await log_account_state()

    except Exception as e:
        logging.critical(f"Konnte initialen Account-Status nicht abrufen. Beende Bot. Fehler: {e}")
        exit()


async def log_account_state():
    INTERNAL_CURRENCY = FUTURE_QUOTE_CURRENCY
    cache = account_state_cache
    
    margin_equity_usdt = cache['total_equity'] - (float(cache['futures_account'].get('totalWalletBalance', 0)) + float(cache['futures_account'].get('totalUnrealizedProfit', 0)))
    margin_free_usdc = float(next((a['free'] for a in cache['margin_account'].get('userAssets', []) if a['asset'] == SPOT_QUOTE_CURRENCY), "0"))
    futures_equity = cache['total_equity'] - margin_equity_usdt
    futures_available_margin = float(cache['futures_account'].get('availableBalance', "0"))

    log_message = (
        f"\n--- Account State (Cache, Stand: {cache['last_update'].strftime('%H:%M:%S') if cache['last_update'] else 'N/A'}) ---\n"
        f"Spot Margin: Equity: {margin_equity_usdt:,.2f} | Free (USDC): {margin_free_usdc:,.2f}\n"
        f"Futures:     Equity: {futures_equity:,.2f} | Available Margin: {futures_available_margin:,.2f}\n"
        f"TOTAL EQUITY: {cache['total_equity']:,.2f} {INTERNAL_CURRENCY}\n"
        f"---------------------------------------------------"
    )
    logging.info(log_message)

# --- Funktionen zum Aktualisieren der Zins- und Funding-Raten ---
async def update_interest_rates(client: AsyncClient):
    global margin_interest_rates
    logging.info("Aktualisiere Margin-Zinssätze...")
    try:
        cross_margin_data = await client.get_cross_margin_data()
        new_rates = {item.get('coin'): float(item.get('dailyInterest', 0.0)) / 24 for item in cross_margin_data}
        margin_interest_rates = new_rates
        logging.info(f"Erfolgreich {len(margin_interest_rates)} Margin-Zinssätze aktualisiert.")
    except Exception as e: logging.error(f"Fehler beim Abrufen der Margin-Zinssätze: {e}")

async def update_funding_rates(client: AsyncClient):
    global futures_funding_rates
    logging.info("Aktualisiere Futures Funding Rates...")
    try:
        all_rates = await client.futures_funding_rate()
        new_rates = {rate_info.get('symbol'): {'lastFundingRate': float(rate_info.get('lastFundingRate', 0.0)),'markPrice': float(rate_info.get('markPrice', 0.0)), 'nextFundingTime': int(rate_info.get('nextFundingTime', 0))} for rate_info in all_rates}
        futures_funding_rates = new_rates
        btc_rate = futures_funding_rates.get(f'BTC{FUTURE_QUOTE_CURRENCY}')
        if btc_rate: logging.info(f"Erfolgreich {len(futures_funding_rates)} Funding Rates aktualisiert. Bsp: BTC Rate = {btc_rate['lastFundingRate']:.8f}")
        else: logging.info(f"Erfolgreich {len(futures_funding_rates)} Funding Rates aktualisiert.")
    except Exception as e: logging.error(f"Fehler beim Abrufen der Funding Rates: {e}")


def adjust_quantity_to_rules(quantity, rules, rounding_direction='down'):
    try:
        step_size_key = 'stepSize' if 'stepSize' in rules else 'lotSize'
        step_size = Decimal(str(rules[step_size_key]))
        if step_size <= 0: return float(Decimal(str(quantity)))
        qty = Decimal(str(quantity))
        if rounding_direction == 'down': adj = (qty // step_size) * step_size
        elif rounding_direction == 'up': adj = Decimal(str(math.ceil(qty / step_size))) * step_size
        else: raise ValueError("rounding_direction muss 'up' oder 'down' sein.")
        decimals = max(0, -step_size.as_tuple().exponent)
        return float(adj.quantize(Decimal(1).scaleb(-decimals), rounding=ROUND_DOWN))
    except Exception as e:
        logging.error(f"adjust_quantity_to_rules() failed: {e}. Returning original quantity={quantity}")
        try: return float(quantity)
        except: return 0.0

async def wait_for_order_fill(client, symbol, order_id, is_futures=False, max_attempts=40, poll_interval=0.5):
    for _ in range(max_attempts):
        try:
            order_status = await client.futures_get_order(symbol=symbol, orderId=order_id) if is_futures else await client.get_margin_order(symbol=symbol, orderId=order_id, isIsolated='FALSE')
            status = order_status.get('status')
            if status == 'FILLED': logging.info(f"Order {order_id} for {symbol} was filled."); return order_status
            if status in ('CANCELED', 'EXPIRED', 'REJECTED'): logging.error(f"Order {order_id} for {symbol} failed with status: {status}"); return None
            await asyncio.sleep(poll_interval)
        except BinanceAPIException as e:
            if getattr(e, "code", None) == -2013:
                try:
                    trades = await client.futures_account_trades(symbol=symbol, orderId=order_id) if is_futures else [t for t in await client.get_margin_trades(symbol=symbol, limit=100, isIsolated='FALSE') if str(t.get('orderId')) == str(order_id)]
                    if trades:
                        total_qty = sum(float(t.get('qty', 0)) for t in trades)
                        if total_qty > 0:
                            total_quote = sum(float(t.get('qty', 0)) * float(t.get('price', 0)) for t in trades)
                            avg_price = total_quote / total_qty
                            logging.info(f"Fallback reconstructed order {order_id} for {symbol}: qty={total_qty} avg_price={avg_price}")
                            return {'status': 'FILLED', 'executedQty': str(total_qty), 'cummulativeQuoteQty': str(total_quote), 'avgPrice': str(avg_price), 'fills': trades}
                except Exception as inner_ex: logging.error(f"Error during fallback trade fetch for order {order_id}: {inner_ex}")
            else: logging.error(f"API Error while checking order {order_id}: {e}")
            await asyncio.sleep(poll_interval)
        except Exception as e: logging.error(f"Unexpected error in wait_for_order_fill for order {order_id}: {e}"); await asyncio.sleep(poll_interval)
    logging.error(f"Timeout waiting for order {order_id} to fill."); return None


def get_symbol_filters_safe(exchange_info_symbols, target_symbol):
    if not exchange_info_symbols: return None
    return next((s.get('filters') for s in exchange_info_symbols if s.get('symbol') == target_symbol), None)

def force_exit_pair(base_asset, reason="Unknown"):
    if base_asset in tracked_opportunities:
        opp = tracked_opportunities[base_asset]
        if LIVE_TRADING and opp.status == 'open': opp.status = "closing"; asyncio.create_task(close_trade_task(opp))
        else: logging.warning(f"[EXIT - FORCED (SIM)] Closing trade on {base_asset} due to {reason}. id={opp.trade_id} dur={(datetime.now() - opp.entry_time).total_seconds():.1f}s"); del tracked_opportunities[base_asset]

async def get_valid_pairs(client):
    try:
        async with asyncio.timeout(10):
            futures_info_task, margin_info_task, spot_info_task = client.futures_exchange_info(), client.get_margin_all_pairs(), client.get_exchange_info()
            futures_info, margin_info, spot_info = await asyncio.gather(futures_info_task, margin_info_task, spot_info_task)
        global SPOT_EXCHANGE_INFO, FUTURE_EXCHANGE_INFO
        FUTURE_EXCHANGE_INFO, SPOT_EXCHANGE_INFO = futures_info, spot_info
        future_symbols = {s['symbol'] for s in futures_info['symbols'] if s.get('status') == 'TRADING'}
        spot_symbols = {p['symbol'] for p in margin_info if p.get('isMarginTrade')}
        future_bases = {s.replace(FUTURE_QUOTE_CURRENCY, '') for s in future_symbols if s.endswith(FUTURE_QUOTE_CURRENCY)}
        spot_bases = {s.replace(SPOT_QUOTE_CURRENCY, '') for s in spot_symbols if s.endswith(SPOT_QUOTE_CURRENCY)}
        return {b for b in future_bases.intersection(spot_bases) if b not in BLACKLISTED_BASE_ASSETS}
    except Exception as e: logging.error(f"Failed to get valid pairs from API: {e}"); return set()

# --- ASYNCHRONE HANDELS-TASKS ---
async def execute_trade_task(opp, trade_value_usd):
    async with trade_execution_lock:
        try:
            opp.status = "executing"
            start_time = time.time()
            base_asset = opp.base_asset
            spot_symbol = f"{base_asset}{SPOT_QUOTE_CURRENCY}"
            future_symbol = f"{base_asset}{FUTURE_QUOTE_CURRENCY}"

            future_rules = get_symbol_filters_safe(FUTURE_EXCHANGE_INFO.get('symbols'), future_symbol)
            spot_rules = get_symbol_filters_safe(SPOT_EXCHANGE_INFO.get('symbols'), spot_symbol)

            if not future_rules or not spot_rules:
                logging.error(f"Trade {opp.trade_id} aborted: Could not find exchange rules for {spot_symbol} or {future_symbol}.")
                if opp.base_asset in tracked_opportunities: del tracked_opportunities[opp.base_asset]
                return

            try:
                future_qty_rules = next(f for f in future_rules if f['filterType'] in ['LOT_SIZE', 'MARKET_LOT_SIZE'])
                spot_qty_rules = next(f for f in spot_rules if f['filterType'] == 'LOT_SIZE')
            except StopIteration:
                logging.error(f"Trade {opp.trade_id} aborted: Could not find LOT_SIZE filter for symbols.")
                if opp.base_asset in tracked_opportunities: del tracked_opportunities[opp.base_asset]
                return

            approx_price = (opp.entry_spot_prices['ask'] + opp.entry_future_prices['bid']) / 2
            target_quantity = trade_value_usd / approx_price
            
            spot_step = float(spot_qty_rules['stepSize'])
            future_step = float(future_qty_rules['stepSize'])

            if future_step > spot_step:
                base_qty = adjust_quantity_to_rules(target_quantity, future_qty_rules, rounding_direction='down')
            else:
                base_qty = adjust_quantity_to_rules(target_quantity, spot_qty_rules, rounding_direction='down')

            spot_quantity = base_qty
            future_quantity = base_qty
            
            if not spot_quantity or not future_quantity:
                logging.error(f"Trade {opp.trade_id} aborted: zero quantity after rounding.")
                if opp.base_asset in tracked_opportunities: del tracked_opportunities[opp.base_asset]
                return
            await log_account_state()
            logging.info(
                f"[EXECUTE OPEN] id={opp.trade_id} dir={opp.direction} "
                f"consistent_qty={spot_quantity} notional≈{spot_quantity * approx_price:.2f} {SPOT_QUOTE_CURRENCY}"
            )

            spot_order_success, future_order_success = False, False
            spot_resp_full, future_fill = None, None

            # --- LEGS PARALLEL ERÖFFNEN ---
            if opp.direction == 'spot_high':
                spot_task = client.create_margin_order(symbol=spot_symbol, side='SELL', type='MARKET', quantity=spot_quantity, sideEffectType='AUTO_BORROW_REPAY', newOrderRespType='FULL')
                future_task = client.futures_create_order(symbol=future_symbol, side='BUY', type='MARKET', quantity=future_quantity)
            else: # 'future_high' Logik
                spot_task = client.create_margin_order(symbol=spot_symbol, side='BUY', type='MARKET', quantity=spot_quantity, sideEffectType='AUTO_BORROW_REPAY', newOrderRespType='FULL')
                future_task = client.futures_create_order(symbol=future_symbol, side='SELL', type='MARKET', quantity=future_quantity)
            
            # asyncio.gather mit return_exceptions=True verwenden
            spot_result, future_result = await asyncio.gather(spot_task, future_task, return_exceptions=True)

            # --- ERGEBNISSE AUSWERTEN ---

            # Spot-Leg auswerten
            if isinstance(spot_result, Exception):
                logging.error(f"Spot leg failed for {opp.trade_id}: {spot_result}")
            else:
                spot_order_success = True
                spot_resp_full = spot_result
                opp.spot_order_id = spot_resp_full['orderId']
                opp.spot_entry_fill = spot_resp_full
                total_executed_qty = float(spot_resp_full.get('executedQty', 0))
                total_commission_in_base = sum(
                    float(fill['commission']) for fill in spot_resp_full.get('fills', [])
                    if fill.get('commissionAsset') == opp.base_asset
                )
                opp.executed_spot_qty_net = total_executed_qty - total_commission_in_base
                logging.info(f"Spot-Fill-Analyse für {opp.trade_id}: Brutto={total_executed_qty}, Gebühren={total_commission_in_base}, Netto={opp.executed_spot_qty_net}")

            # Future-Leg auswerten
            if isinstance(future_result, Exception):
                logging.error(f"Future leg (initial creation) failed for {opp.trade_id}: {future_result}")
            else:
                try:
                    future_fill = await wait_for_order_fill(client, future_symbol, future_result['orderId'], is_futures=True)
                    if future_fill:
                        future_order_success = True
                        opp.future_order_id = future_result['orderId']
                        opp.future_entry_fill = future_fill
                    else:
                        logging.error(f"Future order for {opp.trade_id} did not fill in time.")
                except Exception as e:
                    logging.error(f"Error waiting for future fill for {opp.trade_id}: {e}")

            if spot_order_success and future_order_success:
                # Erfolgsfall: Beide Legs wurden ausgeführt
                latency_ms = (time.time() - start_time) * 1000
                exec_spot_price, spot_entry_fee = compute_vwap_and_fees(opp.spot_entry_fill.get("fills", []), SPOT_QUOTE_CURRENCY)
                exec_future_price = float(future_fill['avgPrice'])

                if opp.direction == 'spot_high':
                    realized_spread = exec_spot_price / exec_future_price if exec_future_price != 0 else 0
                else:
                    realized_spread = exec_future_price / exec_spot_price if exec_spot_price != 0 else 0
                opp.realized_entry_spread_factor = realized_spread

                logging.info(
                    f"[EXECUTION ANALYSIS] id={opp.trade_id} latency={latency_ms:.0f}ms "
                    f"realized_spread={realized_spread:.5f} "
                    f"fees: spot={spot_entry_fee:.4f}, future=pending"
                )
                opp.status = "open"

            elif spot_order_success and not future_order_success:
                # Rollback-Fall: Spot erfolgreich, Future fehlgeschlagen
                logging.warning(f"ROLLBACK: Future leg failed. Closing spot leg for {opp.trade_id}.")
                try:
                    rollback_qty = opp.executed_spot_qty_net
                    if opp.direction == 'spot_high': # Wir haben geshortet (SELL), also zurückkaufen (BUY)
                        await client.create_margin_order(symbol=spot_symbol, side='BUY', type='MARKET', quantity=rollback_qty, sideEffectType='AUTO_REPAY')
                    else: # Wir haben gelongt (BUY), also verkaufen (SELL)
                        await client.create_margin_order(symbol=spot_symbol, side='SELL', type='MARKET', quantity=rollback_qty, sideEffectType='AUTO_REPAY')
                    logging.info(f"Rollback of spot leg for {opp.trade_id} successful.")
                except Exception as rb_e:
                    logging.critical(f"SPOT LEG ROLLBACK FAILED for {opp.trade_id}. MANUAL INTERVENTION REQUIRED. Error: {rb_e}")
                
                if opp.base_asset in tracked_opportunities: del tracked_opportunities[opp.base_asset]
                return

            elif not spot_order_success and future_order_success:
                # Fehlender Rollback hinzugefügt
                logging.warning(f"ROLLBACK: Spot leg failed. Closing future leg for {opp.trade_id}.")
                try:
                    rollback_qty = float(future_fill['executedQty'])
                    if opp.direction == 'spot_high': # Future war BUY, also für Rollback SELL
                        await client.futures_create_order(symbol=future_symbol, side='SELL', type='MARKET', quantity=rollback_qty)
                    else: # Future war SELL, also für Rollback BUY
                        await client.futures_create_order(symbol=future_symbol, side='BUY', type='MARKET', quantity=rollback_qty)
                    logging.info(f"Rollback of future leg for {opp.trade_id} successful.")
                except Exception as rb_e:
                    logging.critical(f"FUTURE LEG ROLLBACK FAILED for {opp.trade_id}. MANUAL INTERVENTION REQUIRED. Error: {rb_e}")

                if opp.base_asset in tracked_opportunities: del tracked_opportunities[opp.base_asset]
                return
            
            else: # Beide sind fehlgeschlagen
                logging.error(f"Both legs failed to execute for {opp.trade_id}. No rollback needed.")
                if opp.base_asset in tracked_opportunities: del tracked_opportunities[opp.base_asset]
                return

        except Exception as e:
            logging.error(f"Critical error in execute_trade_task for {opp.trade_id}: {e}")
            if opp.base_asset in tracked_opportunities: del tracked_opportunities[opp.base_asset]


async def close_trade_task(opp):
    async with trade_execution_lock:
        try:
            trade_duration = (datetime.now() - opp.entry_time).total_seconds()
            if trade_duration < MIN_TRADE_LIFETIME_SECONDS:
                await asyncio.sleep(MIN_TRADE_LIFETIME_SECONDS - trade_duration)

            opp.status = "closing"
            spot_symbol = f"{opp.base_asset}{SPOT_QUOTE_CURRENCY}"
            future_symbol = f"{opp.base_asset}{FUTURE_QUOTE_CURRENCY}"
            logging.info(f"[EXECUTE CLOSE] id={opp.trade_id}")

            # --- SCHLIESSUNGSVERSUCH ---
            spot_target_qty = opp.executed_spot_qty_net
            future_target_qty = float(opp.future_entry_fill.get('executedQty', 0)) if opp.future_entry_fill else 0.0

            if not (spot_target_qty > 0 and future_target_qty > 0):
                raise Exception(f"Cannot close trade {opp.trade_id}, invalid entry fill data.")

            spot_rules = get_symbol_filters_safe(SPOT_EXCHANGE_INFO.get('symbols'), spot_symbol)
            future_rules = get_symbol_filters_safe(FUTURE_EXCHANGE_INFO.get('symbols'), future_symbol)
            if not spot_rules or not future_rules:
                raise Exception(f"Cannot close trade {opp.trade_id}, exchange rules not found.")
            
            spot_qty_rules = next(f for f in spot_rules if f['filterType'] == 'LOT_SIZE')
            future_qty_rules = next(f for f in future_rules if f['filterType'] in ['LOT_SIZE', 'MARKET_LOT_SIZE'])

            # Intelligente Rundung basierend auf der Trade-Richtung
            if opp.direction == 'spot_high': # Short-Position (Sell -> Buy)
                spot_rounding, spot_side, future_side = 'up', 'BUY', 'SELL'
            else: # Long-Position (Buy -> Sell)
                spot_rounding, spot_side, future_side = 'down', 'SELL', 'BUY'

            spot_qty_to_close = adjust_quantity_to_rules(spot_target_qty, spot_qty_rules, rounding_direction=spot_rounding)
            future_qty_to_close = adjust_quantity_to_rules(future_target_qty, future_qty_rules, rounding_direction='down')

            logging.info(f"Closing amounts for {opp.trade_id}:")
            logging.info(f"  -> Spot:   Target={spot_target_qty:.8f}, Side={spot_side}, Rounded ({spot_rounding}) -> {spot_qty_to_close:.8f}")
            logging.info(f"  -> Future: Target={future_target_qty:.8f}, Side={future_side}, Rounded (down) -> {future_qty_to_close:.8f}")

            spot_resp, future_resp = None, None
            try:
                spot_task = client.create_margin_order(symbol=spot_symbol, side=spot_side, type='MARKET', quantity=spot_qty_to_close, sideEffectType='AUTO_REPAY')
                future_task = client.futures_create_order(symbol=future_symbol, side=future_side, type='MARKET', quantity=future_qty_to_close)
                spot_resp, future_resp = await asyncio.gather(spot_task, future_task)
                logging.info(f"Primary close orders for {opp.trade_id} submitted successfully.")
            
            except BinanceAPIException as e:
                logging.warning(f"Primary close attempt failed for {opp.trade_id}: {e}. Starting failsafe for Spot leg.")
                # --- FAILSAFE-MECHANISMUS ---
                try:
                    margin_account = await client.get_margin_account()
                    actual_position = 0.0
                    for asset in margin_account.get('userAssets', []):
                        if asset.get('asset') == opp.base_asset:
                            actual_position = float(asset.get('borrowed', 0.0)) if opp.direction == 'spot_high' else float(asset.get('free', 0.0))
                            break
                    
                    if actual_position <= 0:
                        raise Exception("Failsafe could not find an open position in margin account.")

                    failsafe_qty = adjust_quantity_to_rules(actual_position, spot_qty_rules, rounding_direction=spot_rounding)
                    logging.info(f"Failsafe: Found actual position of {actual_position:.8f}. Closing with {failsafe_qty:.8f}.")
                    spot_resp = await client.create_margin_order(symbol=spot_symbol, side=spot_side, type='MARKET', quantity=failsafe_qty, sideEffectType='AUTO_REPAY')
                    if not future_resp: # Future-Order auch erneut versuchen, falls sie fehlgeschlagen ist
                         future_resp = await client.futures_create_order(symbol=future_symbol, side=future_side, type='MARKET', quantity=future_qty_to_close)

                except Exception as failsafe_e:
                    logging.critical(f"FAILSAFE FAILED for {opp.trade_id}. MANUAL INTERVENTION REQUIRED. Error: {failsafe_e}")
                    
            
            # --- PNL-ANALYSE ---
            spot_close_fill, future_close_fill = None, None
            if spot_resp and spot_resp.get('orderId'):
                spot_close_fill = await wait_for_order_fill(client, spot_symbol, spot_resp['orderId'])
            if future_resp and future_resp.get('orderId'):
                future_close_fill = await wait_for_order_fill(client, future_symbol, future_resp['orderId'], is_futures=True)

            if spot_close_fill and future_close_fill and opp.spot_entry_fill:
                spot_entry_notional = float(opp.spot_entry_fill.get('cummulativeQuoteQty', 0))
                spot_exit_notional = float(spot_close_fill.get('cummulativeQuoteQty', 0))

                if opp.direction == "spot_high": # Entry war SELL (Geld erhalten), Exit war BUY (Geld ausgegeben)
                    spot_pnl = spot_entry_notional - spot_exit_notional
                else: # 'future_high', Entry war BUY (Geld ausgegeben), Exit war SELL (Geld erhalten)
                    spot_pnl = spot_exit_notional - spot_entry_notional
                
                _, spot_entry_fee = compute_vwap_and_fees(opp.spot_entry_fill.get("fills", []), SPOT_QUOTE_CURRENCY)
                _, spot_exit_fee = compute_vwap_and_fees(spot_close_fill.get("fills", []), SPOT_QUOTE_CURRENCY)
                total_spot_fees = spot_entry_fee + spot_exit_fee
                
                start_time_ms = int(opp.entry_time.timestamp() * 1000)
                end_time_ms = int(datetime.now().timestamp() * 1000)
                
                trades = await client.futures_account_trades(symbol=future_symbol, startTime=start_time_ms)
                relevant_order_ids = {opp.future_order_id, (future_close_fill.get("orderId") if future_close_fill else None)}
                trades = [t for t in trades if t.get("orderId") in relevant_order_ids]
                fut_vwap, fut_fee, realized_future_pnl = compute_futures_vwap_and_fees(trades)
                
                income_history = await client.futures_income_history(symbol=future_symbol, incomeType='FUNDING_FEE', startTime=start_time_ms, endTime=end_time_ms)
                total_funding_fee = sum(float(i['income']) for i in income_history)
                
                total_net_pnl = spot_pnl + realized_future_pnl + total_funding_fee - (total_spot_fees + fut_fee)
                
                logging.info(
                    f"[TRADE SUMMARY] id={opp.trade_id} "
                    f"PnL: spot={spot_pnl:+.4f}, future={realized_future_pnl:+.4f}, funding={total_funding_fee:+.4f} "
                    f"fees: spot={total_spot_fees:.4f}, future={fut_fee:.4f} "
                    f"TOTAL_NET_PNL={total_net_pnl:+.4f} {SPOT_QUOTE_CURRENCY}"
                )
                await log_account_state()
            else:
                logging.error(f"Could not retrieve all fill data for PnL analysis of {opp.trade_id}.")

        except Exception as e:
            logging.error(f"Severe error in close_trade_task for {opp.trade_id}: {e}", exc_info=True)
        finally:
            if opp.base_asset in tracked_opportunities: del tracked_opportunities[opp.base_asset]

# --- MAINTENANCE FUNKTIONEN ---

async def handle_margin_dust(client: AsyncClient, margin_data: dict):
    logging.info("--- (M1) Starte Behandlung von Margin-Staub (direkt zu BNB) ---")
    assets_to_convert = []
    
    for asset in margin_data.get('userAssets', []):
        asset_name = asset.get('asset')
        total_amount = float(asset.get('free', 0.0)) + float(asset.get('locked', 0.0))

        if total_amount > 0 and asset_name not in ['USDC', 'BTC', 'ETH', 'BNB']:
            try:
                free_amount = float(asset.get('free', 0.0))
                if free_amount <= 0:
                    continue
                
                ticker = await client.get_symbol_ticker(symbol=f"{asset_name}USDT")
                price = float(ticker['price'])
                
                value_in_usdt = total_amount * price
                if 0 < value_in_usdt < MARGIN_DUST_THRESHOLD_USDT:
                    assets_to_convert.append(asset_name)
                    
            except BinanceAPIException:
                continue

    if not assets_to_convert:
        logging.info("Kein konvertierbarer Staub im Margin-Konto gefunden.")
        return

    logging.info(f"Versuche folgenden Staub zu BNB zu konvertieren: {', '.join(assets_to_convert)}")
    
    successful_conversions = []
    failed_conversions = []

    for asset_name in assets_to_convert:
        try:
            await client.transfer_margin_dust(asset=asset_name)
            logging.info(f"  -> Staub von '{asset_name}' erfolgreich zu BNB konvertiert.")
            successful_conversions.append(asset_name)
        except BinanceAPIException as e:
            logging.error(f"  -> Fehler bei der Konvertierung von '{asset_name}': {e}")
            failed_conversions.append(asset_name)

    logging.info("Staub-Behandlung im Margin-Konto abgeschlossen.")
    if successful_conversions:
        logging.info(f"Erfolgreich konvertiert: {', '.join(successful_conversions)}")
    if failed_conversions:
        logging.warning(f"Fehlgeschlagene Konvertierungen: {', '.join(failed_conversions)}")

async def ensure_sufficient_bnb(client: AsyncClient, margin_data: dict, futures_data: dict) -> bool:
    logging.info("--- (M2) Prüfe, ob ausreichend BNB vorhanden ist ---")
    margin_bnb_free = next((float(a.get('free', 0.0)) for a in margin_data.get('userAssets', []) if a.get('asset') == 'BNB'), 0.0)
    futures_bnb_free = next((float(a.get('walletBalance', 0.0)) for a in futures_data.get('assets', []) if a.get('asset') == 'BNB'), 0.0)
    total_bnb = margin_bnb_free + futures_bnb_free
    total_required_bnb = BNB_MIN_PER_ACCOUNT * 2

    logging.info(f"Gesamter BNB-Bestand: {total_bnb:.8f}. Benötigtes Minimum: {total_required_bnb:.8f}")

    if total_bnb < total_required_bnb:
        logging.warning("BNB-Bestand ist unter dem Minimum. Versuche, BNB zu kaufen.")
        margin_usdc_free = next((float(a.get('free', 0.0)) for a in margin_data.get('userAssets', []) if a.get('asset') == 'USDC'), 0.0)

        if margin_usdc_free < BNB_BUY_AMOUNT_USDC:
            logging.error(f"Nicht genügend USDC ({margin_usdc_free:.2f}) im Margin-Konto, um BNB für {BNB_BUY_AMOUNT_USDC} USDC zu kaufen. Breche ab.")
            return False
        try:
            logging.info(f"Kaufe BNB für {BNB_BUY_AMOUNT_USDC} USDC im Margin-Konto (Market Order)...")
            await client.create_margin_order(symbol='BNBUSDC', side=AsyncClient.SIDE_BUY, type=AsyncClient.ORDER_TYPE_MARKET, quoteOrderQty=BNB_BUY_AMOUNT_USDC)
            logging.info("BNB-Kaufauftrag erfolgreich platziert.")
            return True
        except BinanceAPIException as e:
            logging.error(f"Fehler beim Kauf von BNB: {e}")
            return False
    else:
        logging.info("BNB-Bestand ist ausreichend.")
        return False

async def handle_futures_pnl(client: AsyncClient, futures_data: dict):
    logging.info("--- (M3) Starte PNL-Behandlung (BNFCR <-> BNB) ---")
    bnfcr_balance = next((float(a.get('walletBalance', 0.0)) for a in futures_data.get('assets', []) if a.get('asset') == 'BNFCR'), 0.0)
    if abs(bnfcr_balance) < 0.00001:
        logging.info("Kein nennenswertes BNFCR-Guthaben/Defizit gefunden.")
        return
    try:
        if bnfcr_balance > 0:
            logging.info(f"Positives BNFCR-Guthaben gefunden: {bnfcr_balance}. Konvertiere zu BNB.")
            quote = await client.futures_v1_post_convert_get_quote(fromAsset='BNFCR', toAsset='BNB', fromAmount=str(bnfcr_balance))
            action_log = "Konvertierung von BNFCR zu BNB"
        else:
            amount_to_cover = abs(bnfcr_balance)
            logging.info(f"Negatives BNFCR-Guthaben gefunden: {bnfcr_balance}. Gleiche mit BNB aus.")
            quote = await client.futures_v1_post_convert_get_quote(fromAsset='BNB', toAsset='BNFCR', toAmount=str(amount_to_cover))
            action_log = "Ausgleich von BNFCR mit BNB"

        quote_id = quote.get('quoteId')
        if not quote_id:
            logging.error(f"Konnte keine gültige Quote für BNFCR<->BNB erhalten: {quote}")
            return
        await client.futures_v1_post_convert_accept_quote(quoteId=quote_id)
        logging.info(f"{action_log} erfolgreich.")
    except BinanceAPIException as e:
        logging.error(f"Fehler bei der PNL-Behandlung: {e}")

async def balance_bnb_between_accounts(client: AsyncClient, margin_data: dict, futures_data: dict):
    logging.info("--- (M4) Gleiche BNB auf 50/50-Verteilung an ---")
    try:
        margin_bnb_free = next((float(a.get('free', 0.0)) for a in margin_data.get('userAssets', []) if a.get('asset') == 'BNB'), 0.0)
        futures_bnb_free = next((float(a.get('walletBalance', 0.0)) for a in futures_data.get('assets', []) if a.get('asset') == 'BNB'), 0.0)

        total_bnb = margin_bnb_free + futures_bnb_free
        target_per_account = total_bnb / 2.0

        logging.info(f"Gesamt-BNB: {total_bnb:.8f} -> Ziel pro Konto: {target_per_account:.8f}")
        logging.info(f"Aktuell: Margin={margin_bnb_free:.8f}, Futures={futures_bnb_free:.8f}")

        margin_diff = target_per_account - margin_bnb_free
        amount_to_transfer = round(abs(margin_diff), 6)

        if amount_to_transfer < MIN_BNB_TRANSFER_AMOUNT:
            logging.info("BNB-Verteilung ist bereits ausgeglichen (Transfermenge zu gering).")
            return

        if margin_diff > 0:
            logging.info(f"Transferiere {amount_to_transfer:.8f} BNB von Futures zu Margin.")
            await client.make_universal_transfer(type='UMFUTURE_MARGIN', asset='BNB', amount=amount_to_transfer)
            logging.info("BNB-Transfer zu Margin erfolgreich.")
        elif margin_diff < 0:
            logging.info(f"Transferiere {amount_to_transfer:.8f} BNB von Margin zu Futures.")
            await client.make_universal_transfer(type='MARGIN_UMFUTURE', asset='BNB', amount=amount_to_transfer)
            logging.info("BNB-Transfer zu Futures erfolgreich.")

    except BinanceAPIException as e:
        logging.error(f"Fehler beim BNB-Ausgleich: {e}")

async def get_total_margin_assets_in_usdt(client: AsyncClient, margin_data: dict) -> float:
    try:
        total_asset_of_btc = float(margin_data.get('totalAssetOfBtc', 0.0))
        if total_asset_of_btc > 0:
            ticker = await client.get_symbol_ticker(symbol="BTCUSDT")
            price = float(ticker['price'])
            return total_asset_of_btc * price
        return 0.0
    except (BinanceAPIException, Exception) as e:
        logging.error(f"Konnte Margin-Gesamtwert nicht berechnen: {e}")
        return 0.0

async def balance_capital_between_accounts(client: AsyncClient, margin_data: dict, futures_data: dict):
    logging.info("--- (M5) Starte finalen Kapitalausgleich (USDC) ---")
    margin_total_value_usdt = await get_total_margin_assets_in_usdt(client, margin_data)
    futures_collateral_usdt = float(futures_data.get('totalWalletBalance', 0.0))

    if margin_total_value_usdt == 0 and futures_collateral_usdt == 0:
        logging.warning("Beide Kontowerte sind 0, überspringe Kapitalausgleich.")
        return

    total_capital = margin_total_value_usdt + futures_collateral_usdt
    difference_usdt = margin_total_value_usdt - futures_collateral_usdt
    threshold_usdt = total_capital * (CAPITAL_DIFFERENCE_THRESHOLD_PERCENT / 100.0)

    logging.info(f"Gesamtkapital: {total_capital:.2f} USDT")
    logging.info(f"Margin-Wert: {margin_total_value_usdt:.2f} USDT | Futures-Collateral: {futures_collateral_usdt:.2f} USDT")
    logging.info(f"Differenz (Margin - Futures): {difference_usdt:.2f} USDT")
    logging.info(f"Ausgleichsschwelle ({CAPITAL_DIFFERENCE_THRESHOLD_PERCENT}%): {threshold_usdt:.2f} USDT")

    if abs(difference_usdt) > threshold_usdt:
        try:
            usdc_transfer_amount = round(abs(difference_usdt) / 2.0, 4)
            if difference_usdt > 0:
                margin_usdc_balance = next((float(a.get('free', 0.0)) for a in margin_data.get('userAssets', []) if a.get('asset') == 'USDC'), 0.0)
                amount = min(usdc_transfer_amount, margin_usdc_balance)
                if amount > 1:
                    logging.info(f"Balance weicht ab. Transferiere {amount} USDC von Margin zu Futures.")
                    await client.make_universal_transfer(type='MARGIN_UMFUTURE', asset='USDC', amount=amount)
                    logging.info("Kapitalausgleich erfolgreich.")
                else: logging.warning(f"Nicht genug freies USDC im Margin-Konto für Transfer ({margin_usdc_balance:.2f} verfügbar).")
            else:
                futures_usdc_balance = next((float(a.get('walletBalance', 0.0)) for a in futures_data.get('assets', []) if a.get('asset') == 'USDC'), 0.0)
                amount = min(usdc_transfer_amount, futures_usdc_balance)
                if amount > 1:
                    logging.info(f"Balance weicht ab. Transferiere {amount} USDC von Futures zu Margin.")
                    await client.make_universal_transfer(type='UMFUTURE_MARGIN', asset='USDC', amount=amount)
                    logging.info("Kapitalausgleich erfolgreich.")
                else: logging.warning(f"Nicht genug freies USDC im Futures-Konto für Transfer ({futures_usdc_balance:.2f} verfügbar).")
        except BinanceAPIException as e: logging.error(f"Fehler beim Kapitalausgleich: {e}")
    else: logging.info("Kapitalverteilung ist innerhalb der Toleranz.")

async def run_maintenance_sequence(client: AsyncClient):
    logging.info("================ Starte Maintenance Sequenz ================")
    try:
        async def fetch_data():
            return await client.get_margin_account(), await client.futures_account()

        margin_data, futures_data = await fetch_data()
        await handle_margin_dust(client, margin_data)
        await asyncio.sleep(3)

        margin_data, futures_data = await fetch_data()
        bought_bnb = await ensure_sufficient_bnb(client, margin_data, futures_data)
        if bought_bnb:
            logging.info("BNB wurde gekauft, warte 5s und rufe Daten erneut ab.")
            await asyncio.sleep(5)
            margin_data, futures_data = await fetch_data()

        await handle_futures_pnl(client, futures_data)
        await asyncio.sleep(3)

        margin_data, futures_data = await fetch_data()
        await balance_bnb_between_accounts(client, margin_data, futures_data)
        await asyncio.sleep(3)

        margin_data, futures_data = await fetch_data()
        await balance_capital_between_accounts(client, margin_data, futures_data)

    except BinanceAPIException as e:
        logging.error(f"Ein Fehler bei der Binance API ist während der Maintenance aufgetreten: {e}")
    except Exception as e:
        logging.error(f"Ein unerwarteter Fehler ist während der Maintenance aufgetreten: {e}")

    logging.info("================ Maintenance Sequenz beendet ================")


# --- HAUPT-LOGIKFUNKTIONEN ---
def check_single_exit(base_asset, spot_prices, future_prices, convergence_spread):
    opp = tracked_opportunities.get(base_asset)
    if not opp or opp.status != "open": return

    if datetime.now() - opp.entry_time > timedelta(hours=MAX_HOLDING_HOURS):
        logging.warning(f"[EXIT - TIMEOUT] id={opp.trade_id} dur={(datetime.now() - opp.entry_time).total_seconds():.1f}s")
        if LIVE_TRADING:
            opp.status = "closing"
            asyncio.create_task(close_trade_task(opp))
        else: del tracked_opportunities[base_asset]
        return

    if convergence_spread <= EXIT_THRESHOLD_FACTOR:
        logging.info(f"[EXIT - CONVERGENCE] Closing trade on {base_asset}. id={opp.trade_id} spread={convergence_spread:.5f}")
        if LIVE_TRADING:
            opp.status = "closing"
            asyncio.create_task(close_trade_task(opp))
        else: del tracked_opportunities[base_asset]
        return

def check_single_entry(base_asset, spot_prices, future_prices, debug_enabled):
    if base_asset in tracked_opportunities: return
    spot, future = spot_prices, future_prices
    if not (spot['ask'] > 0 and spot['bid'] > 0 and future['ask'] > 0 and future['bid'] > 0): return
    if (spot['ask'] / spot['bid'] - 1) > MAX_INTERNAL_SPREAD or (future['ask'] / future['bid'] - 1) > MAX_INTERNAL_SPREAD: return
    spot_mid, future_mid = (spot['bid'] + spot['ask']) / 2, (future['bid'] + future['ask']) / 2
    if abs(spot_mid / future_mid - 1) > MAX_PLAUSIBLE_SPREAD: return

    # --- DYNAMISCHE THRESHOLD BERECHNUNG ---
    base_asset_hourly_rate = margin_interest_rates.get(base_asset, 0.0)
    quote_asset_hourly_rate = margin_interest_rates.get(SPOT_QUOTE_CURRENCY, 0.0)
    cost_factor_spot_high_interest = base_asset_hourly_rate * MAX_HOLDING_HOURS
    cost_factor_future_high_interest = quote_asset_hourly_rate * MAX_HOLDING_HOURS

    future_symbol = f"{base_asset}{FUTURE_QUOTE_CURRENCY}"
    funding_info = futures_funding_rates.get(future_symbol)
    estimated_funding_factor = 0.0

    if funding_info:
        num_funding_events = math.ceil(MAX_HOLDING_HOURS / 8.0)
        last_rate = funding_info.get('lastFundingRate', 0.0)
        estimated_funding_factor = last_rate * num_funding_events

    total_cost_factor_spot_high = cost_factor_spot_high_interest + estimated_funding_factor
    dynamic_threshold_spot_high = ENTRY_THRESHOLD_FACTOR + total_cost_factor_spot_high

    total_cost_factor_future_high = cost_factor_future_high_interest - estimated_funding_factor
    dynamic_threshold_future_high = ENTRY_THRESHOLD_FACTOR + total_cost_factor_future_high

    spread_spot_high = spot['bid'] / future['ask']
    spread_future_high = future['bid'] / spot['ask']

    if debug_enabled:
        global debug_top_opportunities
        ratio_spot_high = spread_spot_high / dynamic_threshold_spot_high if dynamic_threshold_spot_high > 0 else 0
        ratio_future_high = spread_future_high / dynamic_threshold_future_high if dynamic_threshold_future_high > 0 else 0

        if ratio_spot_high > ratio_future_high:
            best_ratio, direction, current_spread = ratio_spot_high, 'spot_high', spread_spot_high
        else:
            best_ratio, direction, current_spread = ratio_future_high, 'future_high', spread_future_high

        spot_liq = (spot['bid_qty'] * spot['bid']) if direction == 'spot_high' else (spot['ask_qty'] * spot['ask'])
        future_liq = (future['ask_qty'] * future['ask']) if direction == 'spot_high' else (future['bid_qty'] * future['bid'])

        if base_asset in debug_top_opportunities:
            if best_ratio > debug_top_opportunities[base_asset].get('threshold_ratio', 0):
                debug_top_opportunities[base_asset].update({'threshold_ratio': best_ratio,'direction': direction,'actual_spread': current_spread,'spot_liq': spot_liq,'future_liq': future_liq})
        else:
            if len(debug_top_opportunities) < 5 or best_ratio > min((d.get('threshold_ratio', 0) for d in debug_top_opportunities.values()), default=0):
                debug_top_opportunities[base_asset] = {'threshold_ratio': best_ratio,'direction': direction,'actual_spread': current_spread,'spot_liq': spot_liq,'future_liq': future_liq}
                if len(debug_top_opportunities) > 5:
                    worst_asset = min(debug_top_opportunities, key=lambda k: debug_top_opportunities[k].get('threshold_ratio', 0))
                    del debug_top_opportunities[worst_asset]

    direction, entry_spread = None, 0.0
    if spread_spot_high > dynamic_threshold_spot_high:
        direction, entry_spread = 'spot_high', spread_spot_high
    elif spread_future_high > dynamic_threshold_future_high:
        direction, entry_spread = 'future_high', spread_future_high

    if direction:
        cost_log = (f"interest_cost={cost_factor_spot_high_interest:.6f}, funding_cost={estimated_funding_factor:.6f}") if direction == 'spot_high' else (f"interest_cost={cost_factor_future_high_interest:.6f}, funding_gain={-estimated_funding_factor:.6f}")
        used_threshold = dynamic_threshold_spot_high if direction == 'spot_high' else dynamic_threshold_future_high
        logging.info(f"Dynamic Threshold MET for {base_asset} ({direction}): {used_threshold:.6f} (Base: {ENTRY_THRESHOLD_FACTOR}, {cost_log})")

        spot_liq_usd = (spot['bid_qty'] * spot['bid']) if direction == 'spot_high' else (spot['ask_qty'] * spot['ask'])
        future_liq_usd = (future['ask_qty'] * future['ask']) if direction == 'spot_high' else (future['bid_qty'] * future['bid'])
        available_liquidity = min(spot_liq_usd, future_liq_usd)
        potential_trade_size = min(available_liquidity * PERCENTAGE_TRADE_AMOUNT_LIQUIDITY, MAX_TRADE_AMOUNT_USDC)
        
        total_equity, available_capital = account_state_cache["total_equity"], account_state_cache["available_capital"]
        if total_equity <= 0: return

        max_allowed_used_capital = total_equity * (MAX_CAPITAL_ALLOCATION_PERCENT / 100)
        current_used_capital = total_equity - available_capital
        remaining_allocatable_capital = max(0, max_allowed_used_capital - current_used_capital)
        final_trade_size = min(potential_trade_size, remaining_allocatable_capital)

        if final_trade_size < MIN_TRADE_AMOUNT_USDC: 
            logging.info(f"final_trade_size ({final_trade_size}) < MIN_TRADE_AMOUNT_USDC ({MIN_TRADE_AMOUNT_USDC}) for {base_asset}, skipping.")
            return

        opp = Opportunity(base_asset, direction, entry_spread, spot, future)
        tracked_opportunities[base_asset] = opp

        if LIVE_TRADING:
            if not trade_execution_lock.locked():
                asyncio.create_task(execute_trade_task(opp, final_trade_size))
            else:
                logging.info(f"Skipping trade {opp.trade_id} as another trade is in execution or maintenance is active.")
                del tracked_opportunities[base_asset]


async def process_price_update(payload, market_type, debug_enabled):
    try:
        symbol = payload.get('s')
        if not symbol or 'b' not in payload: return

        event_time = None
        if isinstance(payload, dict):
            for key in ('E', 'T'):
                if key in payload:
                    try: event_time = datetime.fromtimestamp(int(payload[key]) / 1000.0); break
                    except (ValueError, TypeError): continue
        timestamp = event_time if event_time else datetime.now()
        book_tickers[symbol] = {'bid': float(payload['b']), 'bid_qty': float(payload['B']), 'ask': float(payload['a']), 'ask_qty': float(payload['A']), 'market': market_type, 'timestamp': timestamp}

        if symbol.endswith(SPOT_QUOTE_CURRENCY): base_asset, counterpart_symbol = symbol.replace(SPOT_QUOTE_CURRENCY, ''), f"{symbol.replace(SPOT_QUOTE_CURRENCY, '')}{FUTURE_QUOTE_CURRENCY}"
        elif symbol.endswith(FUTURE_QUOTE_CURRENCY): base_asset, counterpart_symbol = symbol.replace(FUTURE_QUOTE_CURRENCY, ''), f"{symbol.replace(FUTURE_QUOTE_CURRENCY, '')}{SPOT_QUOTE_CURRENCY}"
        else: return

        if counterpart_symbol not in book_tickers: return

        updated_data, counterpart_data = book_tickers[symbol], book_tickers[counterpart_symbol]
        if abs(updated_data['timestamp'] - counterpart_data['timestamp']) > MAX_TIME_DIFFERENCE: return

        spot_prices, future_prices = (updated_data, counterpart_data) if updated_data['market'] == 'spot' else (counterpart_data, updated_data)

        if base_asset in tracked_opportunities:
            opp = tracked_opportunities[base_asset]
            potential_spread = (spot_prices['bid'] / future_prices['ask']) if opp.direction == 'spot_high' else (future_prices['bid'] / spot_prices['ask'])
            convergence_spread = (spot_prices['ask'] / future_prices['bid']) if opp.direction == 'spot_high' else (future_prices['ask'] / spot_prices['bid'])
            opp.update_spreads(potential_spread, convergence_spread)
            check_single_exit(base_asset, spot_prices, future_prices, convergence_spread)
        else:
            check_single_entry(base_asset, spot_prices, future_prices, debug_enabled)

    except (KeyError, TypeError, ValueError, ZeroDivisionError): pass
    except Exception as e: logging.error(f"Unerwarteter Fehler in process_price_update: {e}")


# --- SYSTEM-TASKS ---
async def periodic_system_tasks(client: AsyncClient):

    while True:
        try:
            await check_and_run_maintenance(client)
            await asyncio.sleep(SYSTEM_TASK_INTERVAL_SECONDS)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"Fehler in periodic_system_tasks: {e}")
            await asyncio.sleep(60)
            
async def btc_price_stream_handler(client: AsyncClient):
    base_ws_url = "wss://stream.binance.com:9443/ws"
    url = f"{base_ws_url}/btcusdt@ticker"
    while True:
        try:
            async with websockets.connect(url) as websocket:
                logging.info("BTC-Preis-Stream verbunden.")
                async for message in websocket:
                    data = json.loads(message)
                    if 'c' in data: 
                        account_state_cache['btc_price'] = float(data['c'])
                        if account_state_cache.get("margin_account"):
                            recalculate_account_metrics()
        except Exception as e:
            logging.error(f"Fehler im BTC-Preis-Stream: {e}. Verbinde neu in 5s...")
            await asyncio.sleep(5)
            
async def spot_user_stream_handler(client: AsyncClient):
    base_ws_url = "wss://stream.binance.com:9443/ws"
    
    while True:
        try:
            listen_key = await client.margin_stream_get_listen_key()
            url = f"{base_ws_url}/{listen_key}"
            
            async def keepalive():
                while True:
                    await asyncio.sleep(30 * 60) 
                    await client.margin_stream_keepalive(listenKey=listen_key)

            keepalive_task = asyncio.create_task(keepalive())
            
            async with websockets.connect(url) as websocket:
                logging.info("Spot/Margin User Data Stream verbunden.")
                async for message in websocket:
                    data = json.loads(message)
                    if data.get('e') == 'outboundAccountPosition':
                        asset_map = {asset['asset']: asset for asset in account_state_cache['margin_account']['userAssets']}
                        
                        for asset_update in data.get('B', []):
                            asset_symbol = asset_update.get('a')
                            if asset_symbol in asset_map:
                                asset_map[asset_symbol]['free'] = asset_update.get('f')
                                asset_map[asset_symbol]['locked'] = asset_update.get('l')
                            else:
                                new_asset = {
                                    'asset': asset_symbol, 'free': asset_update.get('f'), 'locked': asset_update.get('l'),
                                    'borrowed': '0.0', 'interest': '0.0', 'netAsset': '0.0'
                                }
                                account_state_cache['margin_account']['userAssets'].append(new_asset)
                                
                        recalculate_account_metrics()
        except asyncio.CancelledError:
            break 
        except Exception as e:
            logging.error(f"Fehler im Spot User Stream: {e}. Verbinde neu in 5s...")
        finally:
            if 'keepalive_task' in locals() and not keepalive_task.done():
                keepalive_task.cancel()
            await asyncio.sleep(5)

async def futures_user_stream_handler(client: AsyncClient):
    futures_ws_user_url = FUTURES_WS_URL.split('/stream?streams=')[0] 
    while True:
        try:
            listen_key = await client.futures_stream_get_listen_key()
            url = f"{futures_ws_user_url}/{listen_key}"
            
            async def keepalive():
                while True:
                    await asyncio.sleep(30 * 60) # Alle 30 Minuten
                    await client.futures_stream_keepalive(listenKey=listen_key)

            keepalive_task = asyncio.create_task(keepalive())

            async with websockets.connect(url) as websocket:
                logging.info("Futures User Data Stream verbunden.")
                async for message in websocket:
                    data = json.loads(message)
                    if data.get('e') == 'ACCOUNT_UPDATE':
                        update_data = data.get('a', {})
                        for balance_update in update_data.get('B', []):
                            asset_symbol = balance_update.get('a')
                            for asset in account_state_cache['futures_account']['assets']:
                                if asset['asset'] == asset_symbol:
                                    asset['walletBalance'] = balance_update.get('wb')
                                    break

                        total_pnl = 0.0
                        for pos_update in update_data.get('P', []):
                            for i, pos in enumerate(account_state_cache['futures_account']['positions']):
                                if pos['symbol'] == pos_update.get('s'):
                                    pos['unrealizedProfit'] = pos_update.get('up')
                                    pos['positionAmt'] = pos_update.get('pa')
                                    break
                            else: 
                                 account_state_cache['futures_account']['positions'].append(pos_update)

                        for pos in account_state_cache['futures_account']['positions']:
                            total_pnl += float(pos.get('unrealizedProfit', 0.0))

                        account_state_cache['futures_account']['totalUnrealizedProfit'] = str(total_pnl)
                        recalculate_account_metrics()
        except Exception as e:
            logging.error(f"Fehler im Futures User Stream: {e}. Verbinde neu in 5s...")
        finally:
            if 'keepalive_task' in locals() and not keepalive_task.done():
                keepalive_task.cancel()
            await asyncio.sleep(5)

async def check_and_run_maintenance(client: AsyncClient):
    if not ENABLE_MAINTENANCE or tracked_opportunities: return
    try:
        margin_data, futures_data = await client.get_margin_account(), await client.futures_account()
        margin_total_value_usdt = await get_total_margin_assets_in_usdt(client, margin_data)
        futures_collateral_usdt = float(futures_data.get('totalWalletBalance', 0.0))
        if margin_total_value_usdt == 0 and futures_collateral_usdt == 0: return
        total_capital = margin_total_value_usdt + futures_collateral_usdt
        difference_usdt = margin_total_value_usdt - futures_collateral_usdt
        threshold_usdt = total_capital * (CAPITAL_DIFFERENCE_THRESHOLD_PERCENT / 100.0)
        logging.info(f"Maintenance Check: Differenz={difference_usdt:.2f} USDT, Schwelle={threshold_usdt:.2f} USDT")
        if abs(difference_usdt) > threshold_usdt:
            logging.warning("Kapital-Imbalance überschreitet Schwelle. Starte Maintenance...")
            await trade_execution_lock.acquire()
            logging.info("Maintenance Lock aktiviert.")
            try:
                await run_maintenance_sequence(client)
            finally:
                trade_execution_lock.release()
                logging.info("Maintenance Lock freigegeben.")
        else:
            logging.info("Maintenance Check: Kapitalbalance ist in Ordnung.")
    except Exception as e: logging.error(f"Fehler im Maintenance Check: {e}")



async def debug_logger():
    while True:
        try:
            await log_account_state()
            open_legs = [opp for opp in tracked_opportunities.values() if opp.status == "open"]
            if open_legs:
                log_lines = ["\n--- Offene Legs ---"]
                for opp in open_legs:
                    runtime = datetime.now() - opp.entry_time
                    runtime_str = str(timedelta(seconds=int(runtime.total_seconds())))
                    spot_symbol, future_symbol = f"{opp.base_asset}{SPOT_QUOTE_CURRENCY}", f"{opp.base_asset}{FUTURE_QUOTE_CURRENCY}"
                    spot_now, future_now = book_tickers.get(spot_symbol), book_tickers.get(future_symbol)
                    if not spot_now or not future_now:
                        log_lines.append(f"  - {opp.base_asset:<7} | Laufzeit: {runtime_str} | Warte auf Preisdaten...")
                        continue
                    current_spread = (spot_now['ask'] / future_now['bid']) if opp.direction == 'spot_high' else (future_now['ask'] / spot_now['bid'])
                    realized_entry_spread_str = f"{opp.realized_entry_spread_factor:.5f}" if opp.realized_entry_spread_factor > 0 else "N/A"
                    log_lines.append(f"  - SYMBOL: {opp.base_asset} ({opp.direction}), Laufzeit: {runtime_str}, SPREADS: Entry={realized_entry_spread_str} | Aktuell={current_spread:.5f} | High={opp.peak_conv_spread_factor:.5f} | Low={opp.low_conv_spread_factor:.5f}")
                logging.info("\n".join(log_lines))


            if debug_top_opportunities:
                sorted_opportunities = sorted(debug_top_opportunities.items(), key=lambda item: item[1].get('threshold_ratio', 0), reverse=True)
                header = "\n--- Top 5 Potential Opportunities (sorted by profitability) ---\n"
                header += f"  {'Symbol':<10} {'Direction':<12} | {'Dist. Thr.':<12} | {'Actual Spread':<15} | {'Liq (Spot / Future)'}\n"
                header += "-" * 95
                log_lines = [header]

                for asset, data in sorted_opportunities:
                    direction_str = f"({data.get('direction', 'N/A')})"
                    
                    distance_percent = (data.get('threshold_ratio', 0) - 1.0) * 100
                    distance_str = f"{distance_percent:+.2f}%"

                    actual_spread_str = f"{data.get('actual_spread', 0):.5f}"
                    
                    liq_str = f"${data.get('spot_liq', 0):>8,.0f} / ${data.get('future_liq', 0):>8,.0f}"

                    log_lines.append(f"  {asset:<10} {direction_str:<12} | {distance_str:<12} | {actual_spread_str:<15} | {liq_str}")

                logging.info("\n".join(log_lines))
                debug_top_opportunities.clear()

            await asyncio.sleep(DEBUG_TASK_INTERVAL_SECONDS)

        except asyncio.CancelledError: 
            break
        except Exception as e:
            logging.error(f"Error in debug logger: {e}"); await asyncio.sleep(60)

async def stream_handler(url, market_type, debug_enabled):
    while True:
        try:
            async with websockets.connect(url) as websocket:
                logging.info(f"Successfully connected to {market_type.upper()} stream.")
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        payload_list = data if isinstance(data, list) else [data.get('data', data)]
                        for payload in payload_list:
                             await process_price_update(payload, market_type, debug_enabled)
                    except (json.JSONDecodeError): pass
        except asyncio.CancelledError: break
        except Exception as e:
            logging.error(f"WebSocket error on {market_type.upper()} stream: {e}. Reconnecting in 5s..."); await asyncio.sleep(5)


async def discovery_scheduler(client, args):
    global known_bases, spot_stream_task, BLACKLISTED_BASE_ASSETS
    while True:
        try:
            logging.info("Scheduler: Checking for new/delisted pairs, schedule, interest & funding rates...")
            await update_interest_rates(client)
            await update_funding_rates(client)

            try:
                async with asyncio.timeout(10): delist_schedule = await client.get_margin_delist_schedule()
                if delist_schedule:
                    for item in delist_schedule:
                        if (datetime.fromtimestamp(item['delistTime'] / 1000) - datetime.now()).total_seconds() <= FORCE_EXIT_BEFORE_DELIST_SECONDS:
                            assets_to_check = set(item.get('crossMarginAssets', [])) | {sym.replace(SPOT_QUOTE_CURRENCY, '') for sym in item.get('isolatedMarginSymbols', [])}
                            for asset in assets_to_check:
                                if asset not in BLACKLISTED_BASE_ASSETS:
                                    logging.warning(f"RISK MGMT: Asset '{asset}' is scheduled for delisting. Adding to blacklist and forcing exit.")
                                    BLACKLISTED_BASE_ASSETS.add(asset)
                                    force_exit_pair(asset, "Scheduled for Delisting")
            except Exception as e: logging.error(f"Scheduler: Could not fetch delist schedule: {e}")

            latest_bases = await get_valid_pairs(client)
            if latest_bases and (latest_bases - known_bases):
                logging.info(f"Found {len(latest_bases - known_bases)} new/changed pairs. Restarting spot stream.")
                known_bases = latest_bases
                if spot_stream_task and not spot_stream_task.done():
                    spot_stream_task.cancel(); await spot_stream_task
                spot_stream_names = [f"{base.lower()}{SPOT_QUOTE_CURRENCY.lower()}@bookTicker" for base in known_bases]
                if spot_stream_names:
                    dynamic_spot_ws_url = SPOT_WS_BASE_URL + '/'.join(spot_stream_names)
                    spot_stream_task = asyncio.create_task(stream_handler(dynamic_spot_ws_url, 'spot', args.debug))
                    logging.info(f"Now tracking {len(known_bases)} spot pairs.")
            else: logging.info("Scheduler: No new pairs found.")
            await asyncio.sleep(RE_DISCOVERY_INTERVAL_HOURS * 3600)
        except asyncio.CancelledError: break
        except Exception as e:
            logging.error(f"Error in discovery_scheduler: {e}. Retrying in 1 hour."); await asyncio.sleep(3600)

async def run_main(args):
    global known_bases, spot_stream_task, client
    tasks = []
    try:
        client = await AsyncClient.create(API_KEY, API_SECRET)
        logging.info("Performing initial account state cache, interest and funding rate fetch...")
        await initial_account_sync(client)
        await update_interest_rates(client)
        await update_funding_rates(client)

        logging.info("Initial Discovery: Fetching current valid pairs...")
        known_bases = await get_valid_pairs(client)
        logging.info(f"Initially found {len(known_bases)} pairs.")

        tasks.append(asyncio.create_task(btc_price_stream_handler(client)))
        tasks.append(asyncio.create_task(spot_user_stream_handler(client)))
        tasks.append(asyncio.create_task(futures_user_stream_handler(client)))
        tasks.append(asyncio.create_task(stream_handler(FUTURES_WS_URL, 'future', args.debug)))
        if known_bases:
            spot_stream_names = [f"{base.lower()}{SPOT_QUOTE_CURRENCY.lower()}@bookTicker" for base in known_bases]
            dynamic_spot_ws_url = SPOT_WS_BASE_URL + '/'.join(spot_stream_names)
            spot_stream_task = asyncio.create_task(stream_handler(dynamic_spot_ws_url, 'spot', args.debug))
            tasks.append(spot_stream_task)
        
        tasks.append(asyncio.create_task(discovery_scheduler(client, args)))
        tasks.append(asyncio.create_task(periodic_system_tasks(client)))

        if args.debug:
            logging.info("Debug mode enabled: periodic summary will be logged.")
            tasks.append(asyncio.create_task(debug_logger()))

        await asyncio.gather(*tasks)
        
    except asyncio.CancelledError:
        logging.info("Shutdown sequence initiated.")
    finally:
        all_running_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if all_running_tasks:
            for task in all_running_tasks: task.cancel()
            await asyncio.gather(*all_running_tasks, return_exceptions=True)
        if client: await client.close_connection(); logging.info("Client session closed successfully.")
if __name__ == "__main__":
    if "YOUR_API_KEY" in API_KEY or API_KEY == "":
        logging.error("Please enter your API Key and Secret in keys.yaml")
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('--debug', action='store_true', help='Enable periodic summary.')
        args = parser.parse_args()
        try:
            asyncio.run(run_main(args))
        except KeyboardInterrupt:
            logging.info("Logger stopped by user (Ctrl+C).")
