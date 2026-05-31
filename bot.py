"""
╔══════════════════════════════════════════════════════╗
║   BYBIT TRIANGULAR ARBITRAGE BOT — AGGRESSIVE MODE   ║
║   Target: 10+ trades/hour                            ║
║   Safety: Auto-pause on 3 consecutive losses         ║
║   Speed:  WebSocket + 30 parallel workers            ║
║   Alerts: Every trade + hourly report                ║
╚══════════════════════════════════════════════════════╝
"""

import os, asyncio, logging, time, hmac, hashlib, json
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx, websockets
from telegram import Bot
from telegram.constants import ParseMode

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════
TELEGRAM_TOKEN     = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]
BYBIT_API_KEY      = os.environ["BYBIT_API_KEY"]
BYBIT_SECRET       = os.environ["BYBIT_SECRET"]

TRADE_AMOUNT_USDT  = float(os.getenv("TRADE_AMOUNT_USDT",  "20"))
MIN_PROFIT_PCT     = float(os.getenv("MIN_PROFIT_PCT",     "0.08"))  # aggressive — low threshold
BYBIT_TAKER_FEE    = float(os.getenv("TAKER_FEE",          "0.1"))   # 0.1% per leg
MAX_TRADES_DAY     = int(os.getenv("MAX_TRADES_DAY",        "1000"))
SCAN_WORKERS       = int(os.getenv("SCAN_WORKERS",          "30"))
COOLDOWN_SECS      = float(os.getenv("COOLDOWN_SECS",       "0.8"))  # aggressive cooldown
MAX_CONSEC_LOSSES  = int(os.getenv("MAX_CONSEC_LOSSES",     "3"))    # pause after 3 losses
PAUSE_SECS         = int(os.getenv("PAUSE_SECS",            "300"))  # pause 5 mins
HOURLY_REPORT      = os.getenv("HOURLY_REPORT", "true").lower() == "true"

BASE_URL   = "https://api.bybit.com"
WS_PUBLIC  = "wss://stream.bybit.com/v5/public/spot"

# ── Expanded coin list ────────────────────────────────
BASE_ASSETS = ["USDT", "BTC", "ETH", "USDC"]
FOCUS_COINS = [
    "BTC","ETH","SOL","XRP","BNB","ADA","DOGE","MATIC",
    "DOT","LINK","AVAX","UNI","ATOM","LTC","BCH","NEAR",
    "APT","ARB","OP","SUI","TRX","TON","SHIB","PEPE",
    "WIF","BONK","RENDER","INJ","SEI","WLD","FET","AGIX",
    "OCEAN","ROSE","EGLD","MINA","ENJ","SAND","MANA","AXS",
    "CHZ","GALA","IMX","LDO","AAVE","CRV","COMP","MKR",
    "SUSHI","1INCH","FIL","FLOW","ZIL","ONE","CELO","GRT",
    "SNX","BAL","YFI","ALPHA","BAND","KAVA","LUNA","LUNC",
    "GMT","GST","STEPN","STG","MAGIC","HFT","HOOK","ACH"
]

# ── State ─────────────────────────────────────────────
orderbook:      dict = {}
symbol_info:    dict = {}
triangles:      list = []
trade_cooldown: dict = {}
is_paused:      bool = False
pause_until:    float = 0

session_stats = {
    "trades": 0, "profit_usdt": 0.0,
    "wins": 0, "losses": 0, "consec_losses": 0,
    "start_time": time.time(), "best_trade": 0.0,
    "scans": 0, "opps": 0, "pauses": 0
}
hourly_stats  = {"profit": 0.0, "trades": 0, "wins": 0, "losses": 0}
last_hr_report = time.time()
is_trading     = asyncio.Lock()

# ══════════════════════════════════════════════════════
#  BYBIT AUTH
# ══════════════════════════════════════════════════════
def bybit_sign(params: dict) -> str:
    sorted_params = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(BYBIT_SECRET.encode(), sorted_params.encode(), hashlib.sha256).hexdigest()

def bybit_headers(params: dict) -> dict:
    ts = str(int(time.time() * 1000))
    params["api_key"]   = BYBIT_API_KEY
    params["timestamp"] = ts
    params["sign"]      = bybit_sign(params)
    return {"Content-Type": "application/json"}

async def bybit_get(client, path, params=None):
    p = params or {}
    ts = str(int(time.time() * 1000))
    p.update({"api_key": BYBIT_API_KEY, "timestamp": ts})
    p["sign"] = bybit_sign(p)
    try:
        r = await client.get(f"{BASE_URL}{path}", params=p, timeout=8)
        if r.status_code == 200:
            d = r.json()
            if d.get("retCode") == 0:
                return d
            log.warning(f"Bybit GET {path}: {d.get('retMsg')}")
    except Exception as e:
        log.error(f"GET {path}: {e}")
    return None

async def bybit_post(client, path, body: dict):
    ts   = str(int(time.time() * 1000))
    body.update({"api_key": BYBIT_API_KEY, "timestamp": ts})
    body["sign"] = bybit_sign(body)
    try:
        r = await client.post(f"{BASE_URL}{path}", json=body, timeout=8)
        if r.status_code == 200:
            d = r.json()
            if d.get("retCode") == 0:
                return d
            log.warning(f"Bybit POST {path}: {d.get('retMsg')}")
    except Exception as e:
        log.error(f"POST {path}: {e}")
    return None

# ══════════════════════════════════════════════════════
#  INSTRUMENTS
# ══════════════════════════════════════════════════════
async def load_instruments(client):
    # Public endpoint — no auth needed, avoids IP restriction issues
    try:
        r = await client.get(
            f"{BASE_URL}/v5/market/instruments-info",
            params={"category": "spot", "limit": "1000"},
            timeout=15
        )
        if r.status_code != 200:
            log.error(f"Instruments HTTP {r.status_code}: {r.text[:200]}")
            return
        data = r.json()
        if data.get("retCode") != 0:
            log.error(f"Instruments API error: {data.get('retMsg')}")
            return
    except Exception as e:
        log.error(f"Failed to load instruments: {e}")
        return
    for inst in data.get("result", {}).get("list", []) or []:
        sym   = inst["symbol"]
        base  = inst["baseCoin"]
        quote = inst["quoteCoin"]
        lot   = inst.get("lotSizeFilter", {})
        symbol_info[sym] = {
            "base":    base,
            "quote":   quote,
            "lot_sz":  float(lot.get("basePrecision",  "0.00001")),
            "min_qty": float(lot.get("minOrderQty",    "0.00001")),
            "min_amt": float(lot.get("minOrderAmt",    "1")),
        }
    log.info(f"Loaded {len(symbol_info)} Bybit instruments")

def round_lot(qty, lot_sz):
    if lot_sz == 0: return qty
    p = len(str(lot_sz).rstrip("0").split(".")[-1]) if "." in str(lot_sz) else 0
    return round(round(qty / lot_sz) * lot_sz, p)

# ══════════════════════════════════════════════════════
#  TRIANGLE DISCOVERY
# ══════════════════════════════════════════════════════
def discover_triangles():
    found   = []
    sym_set = set(symbol_info.keys())

    def find_pair(f, t):
        # Bybit format: BASEUSDT, BTCETH etc
        if f"{t}{f}" in sym_set: return f"{t}{f}", "BUY"
        if f"{f}{t}" in sym_set: return f"{f}{t}", "SELL"
        return None, None

    for a in BASE_ASSETS:
        for b in FOCUS_COINS:
            if b == a: continue
            for c in FOCUS_COINS:
                if c in (a, b): continue
                s1, d1 = find_pair(a, b)
                s2, d2 = find_pair(b, c)
                s3, d3 = find_pair(c, a)
                if s1 and s2 and s3:
                    found.append([
                        {"sym": s1, "side": d1, "from": a, "to": b},
                        {"sym": s2, "side": d2, "from": b, "to": c},
                        {"sym": s3, "side": d3, "from": c, "to": a},
                    ])

    seen, dedup = set(), []
    for tri in found:
        key = tuple(s["sym"] for s in tri)
        if key not in seen:
            seen.add(key)
            dedup.append(tri)

    log.info(f"Discovered {len(dedup)} unique triangles")
    return dedup

# ══════════════════════════════════════════════════════
#  WEBSOCKET PRICE FEED
# ══════════════════════════════════════════════════════
async def ws_price_feed():
    needed = set()
    for tri in triangles:
        for s in tri:
            needed.add(s["sym"])

    # Bybit: subscribe to orderbook level 1
    args    = [f"orderbook.1.{s}" for s in needed]
    batches = [args[i:i+100] for i in range(0, len(args), 100)]

    async def handle(batch):
        while True:
            try:
                async with websockets.connect(WS_PUBLIC, ping_interval=20) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": batch}))
                    async for msg in ws:
                        d    = json.loads(msg)
                        if d.get("op") == "subscribe": continue
                        topic = d.get("topic", "")
                        if not topic.startswith("orderbook"): continue
                        sym  = topic.split(".")[-1]
                        data = d.get("data", {})
                        bids = data.get("b", [])
                        asks = data.get("a", [])
                        if bids and asks:
                            orderbook[sym] = {
                                "bid": float(bids[0][0]),
                                "ask": float(asks[0][0]),
                                "ts":  time.time()
                            }
            except Exception as e:
                log.warning(f"WS error: {e} — reconnecting")
                await asyncio.sleep(1)

    await asyncio.gather(*[handle(b) for b in batches])

# ══════════════════════════════════════════════════════
#  PROFIT CALCULATOR
# ══════════════════════════════════════════════════════
def calc_profit(triangle, amount):
    fee = 1 - (BYBIT_TAKER_FEE / 100)
    for step in triangle:
        sym = step["sym"]
        if sym not in orderbook: return 0.0, -999.0
        ob  = orderbook[sym]
        if time.time() - ob.get("ts", 0) > 1.5: return 0.0, -999.0
        bid, ask = ob["bid"], ob["ask"]
        if bid <= 0 or ask <= 0: return 0.0, -999.0
        if step["side"] == "BUY":
            amount = (amount / ask) * fee
        else:
            amount = (amount * bid) * fee
    pct = ((amount - TRADE_AMOUNT_USDT) / TRADE_AMOUNT_USDT) * 100
    return amount, pct

# ══════════════════════════════════════════════════════
#  PRE-EXECUTION SANITY CHECK
#  Re-calculates profit RIGHT before placing orders
#  Aborts if profit dropped below threshold
# ══════════════════════════════════════════════════════
def pre_execution_check(triangle) -> tuple[bool, float]:
    end, pct = calc_profit(triangle, TRADE_AMOUNT_USDT)
    if pct < MIN_PROFIT_PCT:
        return False, pct
    return True, pct

# ══════════════════════════════════════════════════════
#  PARALLEL SCANNER
# ══════════════════════════════════════════════════════
async def scan_chunk(chunk, results):
    now = time.time()
    for tri in chunk:
        key = "→".join(s["sym"] for s in tri)
        if now - trade_cooldown.get(key, 0) < COOLDOWN_SECS: continue
        end, pct = calc_profit(tri, TRADE_AMOUNT_USDT)
        if pct >= MIN_PROFIT_PCT:
            results.append((pct, end, tri, key))

async def find_opportunities():
    session_stats["scans"] += 1
    cs  = max(1, len(triangles) // SCAN_WORKERS)
    cks = [triangles[i:i+cs] for i in range(0, len(triangles), cs)]
    res = []
    await asyncio.gather(*[scan_chunk(c, res) for c in cks])
    if not res: return []
    session_stats["opps"] += len(res)
    res.sort(key=lambda x: x[0], reverse=True)
    return res  # return ALL opportunities ranked by profit

# ══════════════════════════════════════════════════════
#  EXECUTE TRIANGLE
# ══════════════════════════════════════════════════════
async def execute_triangle(triangle, expected_pct, bot):
    global is_paused, pause_until

    async with is_trading:
        # Pre-execution check — prices may have moved
        still_good, current_pct = pre_execution_check(triangle)
        if not still_good:
            log.info(f"Aborted — profit dropped to {current_pct:.4f}%")
            return False

        async with httpx.AsyncClient(timeout=10) as client:
            amount = TRADE_AMOUNT_USDT
            legs   = []
            ok     = True

            for i, step in enumerate(triangle):
                sym    = step["sym"]
                side   = step["side"]
                info   = symbol_info.get(sym, {})
                lot_sz = info.get("lot_sz", 0.00001)
                min_qty= info.get("min_qty", 0.00001)
                min_amt= info.get("min_amt", 1.0)

                if side == "BUY":
                    ask = orderbook.get(sym, {}).get("ask", 0)
                    if ask <= 0: ok = False; break
                    qty = round_lot(amount / ask, lot_sz)
                    if qty < min_qty or qty * ask < min_amt:
                        ok = False; break
                    body = {
                        "category": "spot", "symbol": sym,
                        "side": "Buy", "orderType": "Market", "qty": str(qty)
                    }
                else:
                    qty = round_lot(amount, lot_sz)
                    if qty < min_qty:
                        ok = False; break
                    body = {
                        "category": "spot", "symbol": sym,
                        "side": "Sell", "orderType": "Market", "qty": str(qty)
                    }

                result = await bybit_post(client, "/v5/order/create", body)
                if not result: ok = False; break

                await asyncio.sleep(0.12)

                # Get fill
                ord_id = result.get("result", {}).get("orderId", "")
                fill   = await bybit_get(client, "/v5/order/realtime", {
                    "category": "spot", "orderId": ord_id
                })

                if fill:
                    fd      = (fill.get("result", {}).get("list") or [{}])[0]
                    fill_sz = float(fd.get("cumExecQty",  qty))
                    fill_px = float(fd.get("avgPrice",    0) or 0)
                    fee     = float(fd.get("cumExecFee",  0) or 0)
                    if side == "BUY":
                        amount = fill_sz - abs(fee)
                    elif fill_px > 0:
                        amount = fill_sz * fill_px - abs(fee)

                legs.append(f"  Leg {i+1}: {step['side']} {sym} ✅")

            # ── PnL ───────────────────────────────────
            profit_usdt = amount - TRADE_AMOUNT_USDT
            pct_real    = (profit_usdt / TRADE_AMOUNT_USDT) * 100
            win         = profit_usdt > 0

            session_stats["trades"]      += 1
            session_stats["profit_usdt"] += profit_usdt
            hourly_stats["profit"]       += profit_usdt
            hourly_stats["trades"]       += 1

            if win:
                session_stats["wins"]       += 1
                session_stats["consec_losses"] = 0
                hourly_stats["wins"]        += 1
                if profit_usdt > session_stats["best_trade"]:
                    session_stats["best_trade"] = profit_usdt
            else:
                session_stats["losses"]     += 1
                session_stats["consec_losses"] += 1
                hourly_stats["losses"]      += 1

            # ── Auto-pause on 3 consecutive losses ────
            if session_stats["consec_losses"] >= MAX_CONSEC_LOSSES:
                is_paused   = True
                pause_until = time.time() + PAUSE_SECS
                session_stats["pauses"] += 1
                await send_msg(bot, f"""
⏸ *AUTO-PAUSE TRIGGERED*
━━━━━━━━━━━━━━━━━━━━━
{MAX_CONSEC_LOSSES} consecutive losses detected.
Pausing for `{PAUSE_SECS//60} minutes` to protect capital.
Will resume at `{datetime.fromtimestamp(pause_until, tz=timezone.utc).strftime('%H:%M UTC')}`
━━━━━━━━━━━━━━━━━━━━━
""")
                session_stats["consec_losses"] = 0

            path  = " → ".join([s["from"] for s in triangle] + [triangle[0]["from"]])
            emoji = "✅" if win else "🔴"

            await send_msg(bot, f"""
{emoji} *{'WIN' if win else 'LOSS'} — ARB TRADE*
━━━━━━━━━━━━━━━━━━━━━
🔄 `{path}`
💵 `${TRADE_AMOUNT_USDT:.4f}` → `${amount:.4f}`
📊 `{pct_real:+.4f}%` (`${profit_usdt:+.5f}`)
{''.join(chr(10)+l for l in legs)}
📈 Session: `${session_stats['profit_usdt']:+.5f}`
✅`{session_stats['wins']}` ❌`{session_stats['losses']}` | Hour: `${hourly_stats['profit']:+.5f}`
⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}
""")
            return win

# ══════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════
async def send_msg(bot, text):
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=text,
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        log.error(f"Telegram: {e}")

async def send_hourly_report(bot):
    global last_hr_report
    wr    = (session_stats["wins"] / max(session_stats["trades"], 1)) * 100
    hr_wr = (hourly_stats["wins"] / max(hourly_stats["trades"], 1)) * 100
    await send_msg(bot, f"""
⏱ *HOURLY REPORT*
━━━━━━━━━━━━━━━━━━━━━
🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}

📊 *This Hour*
  Trades:   `{hourly_stats['trades']}`
  P&L:      `${hourly_stats['profit']:+.5f}`
  Win Rate: `{hr_wr:.1f}%`

📈 *Session Total*
  Trades:   `{session_stats['trades']}`
  P&L:      `${session_stats['profit_usdt']:+.5f}`
  Win Rate: `{wr:.1f}%`
  Best:     `${session_stats['best_trade']:.5f}`
  Pauses:   `{session_stats['pauses']}`
  Scans:    `{session_stats['scans']:,}`
━━━━━━━━━━━━━━━━━━━━━
""")
    # Reset hourly
    hourly_stats["profit"]  = 0.0
    hourly_stats["trades"]  = 0
    hourly_stats["wins"]    = 0
    hourly_stats["losses"]  = 0
    last_hr_report = time.time()

# ══════════════════════════════════════════════════════
#  MAIN TRADING LOOP
# ══════════════════════════════════════════════════════
async def trading_loop(bot):
    global triangles, is_paused

    async with httpx.AsyncClient(timeout=10) as client:
        await load_instruments(client)

    triangles = discover_triangles()
    if not triangles:
        await send_msg(bot, "❌ No triangles found — check API keys")
        return

    await send_msg(bot, f"🔍 Scanning `{len(triangles)}` triangles | `{SCAN_WORKERS}` workers | WebSocket active...")
    asyncio.create_task(ws_price_feed())

    log.info("Waiting 5s for WebSocket prices...")
    await asyncio.sleep(5)

    active_tasks = set()

    while True:
        try:
            now = time.time()

            # Check pause
            if is_paused:
                if now >= pause_until:
                    is_paused = False
                    await send_msg(bot, "▶️ *Bot resumed — scanning for opportunities...*")
                else:
                    await asyncio.sleep(5)
                    continue

            # Daily cap
            if session_stats["trades"] >= MAX_TRADES_DAY:
                await asyncio.sleep(60)
                continue

            # Hourly report
            if HOURLY_REPORT and now - last_hr_report >= 3600:
                await send_hourly_report(bot)

            # Find ALL opportunities this scan
            opps = await find_opportunities()

            if opps:
                # Execute top opportunity immediately
                pct, end, triangle, key = opps[0]
                trade_cooldown[key] = now

                # Clean up finished tasks
                active_tasks = {t for t in active_tasks if not t.done()}

                # Only run if not too many concurrent trades
                if len(active_tasks) < 3:
                    task = asyncio.create_task(execute_triangle(triangle, pct, bot))
                    active_tasks.add(task)
            else:
                if session_stats["scans"] % 1000 == 0:
                    priced = sum(1 for tri in triangles for s in tri if s["sym"] in orderbook)
                    log.info(f"Scans: {session_stats['scans']:,} | Priced symbols: {priced} | P&L: ${session_stats['profit_usdt']:+.5f}")

            await asyncio.sleep(0.05)  # 50ms — aggressive scanning

        except Exception as e:
            log.error(f"Loop error: {e}")
            await asyncio.sleep(1)

# ══════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════
async def main():
    bot = Bot(token=TELEGRAM_TOKEN)
    await send_msg(bot, f"""
⚡ *Bybit Arb Bot — AGGRESSIVE MODE*
━━━━━━━━━━━━━━━━━━━━━
🏦 Exchange: `Bybit`
💵 Capital: `${TRADE_AMOUNT_USDT} USDT`
🎯 Min profit: `{MIN_PROFIT_PCT}%`
💸 Fee per cycle: `{BYBIT_TAKER_FEE * 3}%`
⚡ Workers: `{SCAN_WORKERS}` parallel
🛡 Auto-pause: after `{MAX_CONSEC_LOSSES}` consecutive losses
⏱ Hourly reports: `{'ON' if HOURLY_REPORT else 'OFF'}`
🪙 Coins: `{len(FOCUS_COINS)}`
━━━━━━━━━━━━━━━━━━━━━
Loading Bybit instruments...
""")
    await trading_loop(bot)

if __name__ == "__main__":
    asyncio.run(main())
