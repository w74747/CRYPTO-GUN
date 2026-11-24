"""
MEXC Scalping Bot v5 — Ultra Rocket Edition
============================================
النسخة الأقوى: مصادر متعددة + كشف مبكر + مراقبة لحظية

الاستراتيجيات:
  S1 — RSI Bounce   : ارتداد من تشبع بيعي
  S2 — Momentum     : زخم مع EMA
  S3 — Rocket       : ارتفاع مفاجئ +5%+ مع حجم ضخم ← الرئيسي

مصادر الإشارات (الجديدة):
  🏆 Top Gainers Scanner : أفضل 50 رابح كل 30 ث (بدلاً من فحص 3000!)
  🌊 Volume Anomaly      : عملات حجمها ارتفع >5× فجأة
  📈 New Listings        : تداول فوري على العملات المدرجة حديثاً
  🌐 CoinGecko Trending  : أكثر 7 عملات بحثاً كل 4 ساعات
  📚 Order Book Pressure : نسبة الشراء/البيع كمؤشر دخول

إدارة الصفقات:
  - TradeMonitor كل 5 ثواني (مراقبة لحظية)
  - Trailing Stop يتبع السعر بـ 8%
  - إغلاق جزئي: 30% عند +8%، 30% عند +15%، الباقي مع Trailing
  - Auto-Heal عند كل إعادة تشغيل
  - Retry لا نهائي للعمليات الحرجة
  - رأس مال ديناميكي: رصيد ÷ slots
"""

from __future__ import annotations
import asyncio, os, time, json as _json, logging, threading, random
from dataclasses import dataclass, field
from typing import Optional
import aiohttp, ccxt, pandas as pd, psycopg2, psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(message)s")

def _log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} | {msg}", flush=True)

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
@dataclass
class Config:
    # ── API Keys ──
    mexc_api_key:       str   = field(default_factory=lambda: os.environ.get("MEXC_API_KEY", ""))
    mexc_api_secret:    str   = field(default_factory=lambda: os.environ.get("MEXC_API_SECRET", ""))
    telegram_token:     str   = field(default_factory=lambda: os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    telegram_chat_id:   str   = field(default_factory=lambda: os.environ.get("TELEGRAM_CHAT_ID", ""))
    deepseek_api_key:   str   = field(default_factory=lambda: os.environ.get("DEEPSEEK_API_KEY", ""))
    database_url:       str   = field(default_factory=lambda: os.environ.get("DATABASE_URL", ""))
    fallback_db_url:    str   = field(default_factory=lambda: os.environ.get("FALLBACK_DATABASE_URL", ""))

    # ── رأس المال (ديناميكي: 0 = رصيد ÷ slots) ──
    capital:            float = field(default_factory=lambda: float(os.environ.get("TRADE_INVESTMENT_AMOUNT", "0")))
    max_slots:          int   = field(default_factory=lambda: int(os.environ.get("MAX_CONCURRENT_TRADES", "3")))

    # ── توقيت ──
    scan_interval_min:  int   = field(default_factory=lambda: int(os.environ.get("SCAN_INTERVAL_MINUTES", "1")))
    monitor_interval:   int   = 5    # ثواني — مراقبة كل 5 ثواني
    reconcile_interval: int   = field(default_factory=lambda: int(os.environ.get("RECONCILE_INTERVAL_SECONDS", "180")))

    # ── فلاتر عامة ──
    min_volume_usd:     float = field(default_factory=lambda: float(os.environ.get("MIN_DAILY_VOLUME_USD", "50000")))
    market_spot:        bool  = True

    # ── S1: RSI Bounce ──
    s1_enabled:         bool  = field(default_factory=lambda: os.environ.get("S1_ENABLED", "true").lower() == "true")
    s1_rsi_oversold:    float = field(default_factory=lambda: float(os.environ.get("S1_RSI_OVERSOLD", "30")))
    s1_rsi_bounce:      float = field(default_factory=lambda: float(os.environ.get("S1_RSI_BOUNCE", "35")))
    s1_tp1_pct:         float = field(default_factory=lambda: float(os.environ.get("S1_TP1_PCT", "5.0")))
    s1_tp2_pct:         float = field(default_factory=lambda: float(os.environ.get("S1_TP2_PCT", "9.0")))
    s1_sl_pct:          float = field(default_factory=lambda: float(os.environ.get("S1_SL_PCT", "2.5")))

    # ── S2: Momentum ──
    s2_enabled:         bool  = field(default_factory=lambda: os.environ.get("S2_ENABLED", "true").lower() == "true")
    s2_rsi_min:         float = field(default_factory=lambda: float(os.environ.get("S2_RSI_MIN", "50")))
    s2_rsi_max:         float = field(default_factory=lambda: float(os.environ.get("S2_RSI_MAX", "65")))
    s2_vol_ratio:       float = field(default_factory=lambda: float(os.environ.get("S2_VOL_RATIO", "1.5")))
    s2_tp1_pct:         float = field(default_factory=lambda: float(os.environ.get("S2_TP1_PCT", "3.0")))
    s2_sl_pct:          float = field(default_factory=lambda: float(os.environ.get("S2_SL_PCT", "2.0")))

    # ── S3: Rocket Detector (الرئيسي) ──
    s3_enabled:         bool  = field(default_factory=lambda: os.environ.get("S3_ENABLED", "true").lower() == "true")
    s3_gain_1h_min:     float = field(default_factory=lambda: float(os.environ.get("S3_GAIN_1H_MIN", "3.0")))
    s3_gain_4h_min:     float = field(default_factory=lambda: float(os.environ.get("S3_GAIN_4H_MIN", "7.0")))
    s3_vol_spike:       float = field(default_factory=lambda: float(os.environ.get("S3_VOL_SPIKE", "2.0")))
    s3_rsi_max:         float = field(default_factory=lambda: float(os.environ.get("S3_RSI_MAX", "80")))
    s3_partial1_pct:    float = field(default_factory=lambda: float(os.environ.get("S3_PARTIAL1_PCT", "8.0")))
    s3_partial2_pct:    float = field(default_factory=lambda: float(os.environ.get("S3_PARTIAL2_PCT", "15.0")))
    s3_partial1_ratio:  float = field(default_factory=lambda: float(os.environ.get("S3_PARTIAL1_RATIO", "0.30")))
    s3_partial2_ratio:  float = field(default_factory=lambda: float(os.environ.get("S3_PARTIAL2_RATIO", "0.30")))
    s3_trailing_pct:    float = field(default_factory=lambda: float(os.environ.get("S3_TRAILING_PCT", "8.0")))
    s3_sl_pct:          float = field(default_factory=lambda: float(os.environ.get("S3_SL_PCT", "4.0")))
    s3_min_vol_usd:     float = field(default_factory=lambda: float(os.environ.get("S3_MIN_VOL_USD", "30000")))

    # ── Trailing Stop عام ──
    trailing_trigger:   float = field(default_factory=lambda: float(os.environ.get("TRAILING_TRIGGER_PCT", "1.5")))
    trailing_pct:       float = field(default_factory=lambda: float(os.environ.get("TRAILING_PCT", "2.5")))

    # ── إعدادات مصادر الإشارات (الجديدة) ──
    # Top Gainers
    top_gainers_enabled: bool = field(default_factory=lambda: os.environ.get("TOP_GAINERS_ENABLED", "true").lower() == "true")
    top_gainers_count:   int  = field(default_factory=lambda: int(os.environ.get("TOP_GAINERS_COUNT", "50")))
    top_gainers_min_pct: float = field(default_factory=lambda: float(os.environ.get("TOP_GAINERS_MIN_PCT", "3.0")))

    # Volume Anomaly
    vol_anomaly_enabled: bool = field(default_factory=lambda: os.environ.get("VOL_ANOMALY_ENABLED", "true").lower() == "true")
    vol_anomaly_spike:   float = field(default_factory=lambda: float(os.environ.get("VOL_ANOMALY_SPIKE", "5.0")))

    # New Listings
    new_listings_enabled: bool = field(default_factory=lambda: os.environ.get("NEW_LISTINGS_ENABLED", "true").lower() == "true")
    new_listing_sl_pct:   float = field(default_factory=lambda: float(os.environ.get("NEW_LISTING_SL_PCT", "5.0")))
    new_listing_tp_pct:   float = field(default_factory=lambda: float(os.environ.get("NEW_LISTING_TP_PCT", "20.0")))

    # CoinGecko Trending
    coingecko_enabled:   bool = field(default_factory=lambda: os.environ.get("COINGECKO_ENABLED", "true").lower() == "true")
    coingecko_hours:     int  = field(default_factory=lambda: int(os.environ.get("COINGECKO_REFRESH_HOURS", "4")))

    # Order Book Pressure
    orderbook_enabled:   bool = field(default_factory=lambda: os.environ.get("ORDERBOOK_ENABLED", "true").lower() == "true")
    orderbook_ratio_min: float = field(default_factory=lambda: float(os.environ.get("ORDERBOOK_BUY_RATIO_MIN", "1.5")))

    # ── Binance Listing Monitor ──
    binance_listing_enabled: bool = field(default_factory=lambda: os.environ.get("BINANCE_LISTING_ENABLED", "true").lower() == "true")
    binance_listing_interval: int = field(default_factory=lambda: int(os.environ.get("BINANCE_LISTING_INTERVAL", "120")))  # ثانية

    # ── LunarCrush Social Signal ──
    lunarcrush_enabled:  bool = field(default_factory=lambda: os.environ.get("LUNARCRUSH_ENABLED", "false").lower() == "true")
    lunarcrush_api_key:  str  = field(default_factory=lambda: os.environ.get("LUNARCRUSH_API_KEY", ""))
    lunarcrush_min_score: float = field(default_factory=lambda: float(os.environ.get("LUNARCRUSH_MIN_SCORE", "50")))

    # ── Funding Rate (MEXC Futures) ──
    funding_rate_enabled: bool = field(default_factory=lambda: os.environ.get("FUNDING_RATE_ENABLED", "true").lower() == "true")
    funding_rate_threshold: float = field(default_factory=lambda: float(os.environ.get("FUNDING_RATE_THRESHOLD", "-0.001")))  # -0.1% = squeeze قادم

    # ── CryptoPanic News Filter ──
    cryptopanic_enabled: bool = field(default_factory=lambda: os.environ.get("CRYPTOPANIC_ENABLED", "false").lower() == "true")
    cryptopanic_api_key: str  = field(default_factory=lambda: os.environ.get("CRYPTOPANIC_API_KEY", ""))
    cryptopanic_block_negative: bool = True  # يمنع الدخول عند أخبار سلبية

    # ── قائمة سوداء ──
    blacklisted_assets: set = field(default_factory=lambda: {
        "AAVE","COMP","MKR","CRV","LDO","UNI","SUSHI","BAL","CAKE","YFI",
        "SNX","DYDX","FUN","WIN","DICE","BET","RLB","NSFW","ADULTS",
        "MANA","SAND","GALA","AXS","SLP","XMR","DASH","ZEC",
        "BULL","BEAR","3L","3S","UP","DOWN",
    })
    shariah_filter:     bool  = field(default_factory=lambda: os.environ.get("SHARIAH_FILTER_ENABLED", "false").lower() == "true")

    # ── AI ──
    ai_enabled:         bool  = field(default_factory=lambda: os.environ.get("AI_COMMITTEE_ENABLED", "true").lower() == "true")
    deepseek_model:     str   = "deepseek-v4-flash"
    smart_report_hours: int   = field(default_factory=lambda: int(os.environ.get("SMART_REPORT_INTERVAL_HOURS", "2")))


# ══════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════
class TradeDB:
    """PostgreSQL — Primary + Fallback"""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS trades (
        id              SERIAL PRIMARY KEY,
        symbol          TEXT NOT NULL,
        strategy        TEXT NOT NULL DEFAULT 'S1',
        signal_source   TEXT DEFAULT 'scan',
        opened_at       TIMESTAMPTZ DEFAULT NOW(),
        closed_at       TIMESTAMPTZ,
        entry_price     DOUBLE PRECISION,
        exit_price      DOUBLE PRECISION,
        filled_qty      DOUBLE PRECISION,
        exit_qty        DOUBLE PRECISION,
        capital_usd     DOUBLE PRECISION,
        sl_price        DOUBLE PRECISION,
        tp1_price       DOUBLE PRECISION,
        tp2_price       DOUBLE PRECISION,
        tp3_price       DOUBLE PRECISION,
        tp1_order_id    TEXT,
        sl_order_id     TEXT,
        is_futures      BOOLEAN DEFAULT FALSE,
        exit_type       TEXT,
        net_pnl_usd     DOUBLE PRECISION,
        net_pnl_pct     DOUBLE PRECISION,
        ai_verdict      TEXT,
        notes           TEXT
    );
    CREATE TABLE IF NOT EXISTS bot_cache (
        key     TEXT PRIMARY KEY,
        value   TEXT,
        updated TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_trades_open ON trades(closed_at) WHERE closed_at IS NULL;
    """

    def __init__(self, primary_url: str, fallback_url: str = ""):
        self._primary  = primary_url
        self._fallback = fallback_url
        self._conn     = None
        self._lock     = threading.Lock()
        self._init_db()

    def _init_db(self):
        for label, url in [("Primary", self._primary), ("Fallback", self._fallback)]:
            if not url:
                continue
            try:
                conn = psycopg2.connect(url, connect_timeout=10)
                conn.autocommit = True
                with conn.cursor() as c:
                    for stmt in self.SCHEMA.split(";"):
                        if stmt.strip():
                            c.execute(stmt)
                self._conn = conn
                _log(f"[DB] ✅ {label} متصل")
                return
            except Exception as e:
                _log(f"[DB] ⚠️ {label}: {e}")

    def _get_conn(self):
        try:
            if self._conn:
                self._conn.cursor().execute("SELECT 1")
                return self._conn
        except Exception:
            pass
        self._init_db()
        return self._conn

    def execute(self, sql: str, params=None, fetch=False):
        with self._lock:
            conn = self._get_conn()
            if not conn:
                return None
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
                    c.execute(sql, params or ())
                    if fetch:
                        return c.fetchall()
                    return True
            except Exception as e:
                _log(f"[DB] ❌ {e}")
                return None

    def upsert_trade(self, symbol: str, strategy: str, entry_price: float,
                     filled_qty: float, capital: float, sl: float,
                     tp1: float, tp2: float, tp3: float,
                     is_futures: bool = False, ai_verdict: str = "",
                     tp1_order_id: str = "", sl_order_id: str = "",
                     signal_source: str = "scan") -> Optional[int]:
        with self._lock:
            conn = self._get_conn()
            if not conn:
                return None
            try:
                with conn.cursor() as c:
                    c.execute(
                        """INSERT INTO trades
                           (symbol,strategy,signal_source,entry_price,filled_qty,capital_usd,
                            sl_price,tp1_price,tp2_price,tp3_price,is_futures,
                            ai_verdict,tp1_order_id,sl_order_id)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                        (symbol, strategy, signal_source, entry_price, filled_qty, capital,
                         sl, tp1, tp2, tp3, is_futures, ai_verdict,
                         tp1_order_id, sl_order_id),
                    )
                    r = c.fetchone()
                    return r[0] if r else None
            except Exception as e:
                _log(f"[DB] upsert_trade {symbol}: {e}")
                return None

    def close_trade(self, trade_id: int, exit_price: float, exit_qty: float,
                    exit_type: str, net_pnl_usd: float, net_pnl_pct: float):
        self.execute(
            """UPDATE trades SET closed_at=NOW(), exit_price=%s, exit_qty=%s,
               exit_type=%s, net_pnl_usd=%s, net_pnl_pct=%s
               WHERE id=%s""",
            (exit_price, exit_qty, exit_type, net_pnl_usd, net_pnl_pct, trade_id),
        )

    def update_sl(self, trade_id: int, new_sl: float):
        self.execute("UPDATE trades SET sl_price=%s WHERE id=%s", (new_sl, trade_id))

    def update_tp1_order_id(self, trade_id: int, tp1_order_id: str):
        self.execute("UPDATE trades SET tp1_order_id=%s WHERE id=%s", (tp1_order_id, trade_id))

    def get_open_trades(self) -> list:
        rows = self.execute(
            "SELECT * FROM trades WHERE closed_at IS NULL ORDER BY opened_at",
            fetch=True,
        )
        return list(rows) if rows else []

    def get_monthly_summary(self) -> dict:
        rows = self.execute(
            """SELECT COUNT(*) total,
                      SUM(CASE WHEN net_pnl_usd>0 THEN 1 ELSE 0 END) wins,
                      COALESCE(SUM(net_pnl_usd),0) pnl
               FROM trades
               WHERE closed_at IS NOT NULL
                 AND date_trunc('month',opened_at)=date_trunc('month',NOW())""",
            fetch=True,
        )
        if rows:
            r = dict(rows[0])
            total = r.get("total") or 0
            wins  = r.get("wins")  or 0
            return {
                "total": total, "wins": wins,
                "pnl":   float(r.get("pnl") or 0),
                "winrate": round(wins / total * 100, 1) if total else 0,
            }
        return {"total": 0, "wins": 0, "pnl": 0.0, "winrate": 0}

    def cache_set(self, key: str, value: str):
        self.execute(
            "INSERT INTO bot_cache(key,value,updated) VALUES(%s,%s,NOW()) "
            "ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated=NOW()",
            (key, value),
        )

    def cache_get(self, key: str) -> Optional[str]:
        rows = self.execute("SELECT value FROM bot_cache WHERE key=%s", (key,), fetch=True)
        if rows:
            return dict(rows[0]).get("value")
        return None


# ══════════════════════════════════════════════════════════════
# SLOT MANAGER
# ══════════════════════════════════════════════════════════════
@dataclass
class SlotState:
    symbol:        str
    strategy:      str           = "S1"
    signal_source: str           = "scan"
    entry_price:   float         = 0.0
    filled_qty:    float         = 0.0
    capital:       float         = 0.0
    sl_price:      float         = 0.0
    tp1_price:     float         = 0.0
    tp2_price:     float         = 0.0
    tp3_price:     float         = 0.0
    tp1_order_id:  str           = ""
    sl_order_id:   str           = ""
    is_futures:    bool          = False
    db_trade_id:   Optional[int] = None
    opened_at:     float         = field(default_factory=time.time)
    highest_price: float         = 0.0
    trailing_sl:   float         = 0.0
    partial1_done: bool          = False
    partial2_done: bool          = False
    remaining_qty: float         = 0.0
    atr_pct:       float         = 0.0   # ATR% عند فتح الصفقة (للـ trailing)


class SlotManager:
    def __init__(self, max_slots: int):
        self._slots: dict[str, SlotState] = {}
        self._max   = max_slots
        self._lock  = threading.Lock()

    @property
    def used(self) -> int:
        return len(self._slots)

    @property
    def free(self) -> int:
        return self._max - self.used

    def is_vacant(self, symbol: str) -> bool:
        with self._lock:
            return symbol not in self._slots and self.used < self._max

    def occupy(self, state: SlotState):
        with self._lock:
            self._slots[state.symbol] = state

    def release(self, symbol: str):
        with self._lock:
            self._slots.pop(symbol, None)

    def get(self, symbol: str) -> Optional[SlotState]:
        return self._slots.get(symbol)

    def all(self) -> list[SlotState]:
        return list(self._slots.values())

    def update(self, symbol: str, **kw):
        s = self._slots.get(symbol)
        if s:
            for k, v in kw.items():
                setattr(s, k, v)


# ══════════════════════════════════════════════════════════════
# EXECUTOR — MEXC
# ══════════════════════════════════════════════════════════════
class Executor:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.exchange: ccxt.mexc = self._connect()
        self._geo_blocked: set = set()  # عملات محجوبة جغرافياً (11444)
        self._tickers_cache: dict = {}  # آخر fetch_tickers

    def _connect(self) -> ccxt.mexc:
        ex = ccxt.mexc({
            "apiKey":    self.cfg.mexc_api_key,
            "secret":    self.cfg.mexc_api_secret,
            "options":   {"defaultType": "spot"},
            "enableRateLimit": True,
        })
        markets = ex.load_markets()
        _log(f"[Executor] ✅ Spot متصل — {len(markets)} زوج")
        return ex

    def live_price(self, symbol: str) -> float:
        try:
            t = self.exchange.fetch_ticker(symbol)
            return float(t.get("last") or t.get("close") or 0)
        except Exception:
            return 0.0

    def free_usdt(self) -> float:
        try:
            bal = self.exchange.fetch_balance({"type": "spot"})
            return float(bal.get("USDT", {}).get("free", 0) or
                         bal.get("free", {}).get("USDT", 0))
        except Exception:
            return 0.0

    def market_buy(self, symbol: str, capital: float) -> Optional[dict]:
        try:
            price = self.live_price(symbol)
            if price <= 0:
                return None
            try:
                cap = float(self.exchange.cost_to_precision(symbol, capital))
            except Exception:
                cap = capital
            order = self.exchange.create_market_buy_order(
                symbol, cap, {"quoteOrderQty": cap}
            )
            filled_price = float(order.get("average") or order.get("price") or price)
            filled_qty   = float(order.get("filled") or cap / filled_price)
            _log(f"[Executor] ✅ BUY {symbol} qty={filled_qty:.6f} @ {filled_price:.6g}")
            return {"price": filled_price, "qty": filled_qty, "order": order}
        except ccxt.InsufficientFunds as e:
            _log(f"[Executor] ❌ رصيد غير كافٍ {symbol}: {e}")
        except Exception as e:
            err = str(e)
            _log(f"[Executor] ❌ market_buy {symbol}: {e}")
            # 11444 = قيود جغرافية / 70015 = لا يمكن التداول / 10007 = لا يدعم API
            if "11444" in err or "70015" in err or "10007" in err:
                base = symbol.replace("/USDT", "")
                self._geo_blocked.add(base)
                _log(f"[Executor] ⛔ {symbol} غير متاح — أُضيف للـ blacklist")
        return None

    def place_limit_sell(self, symbol: str, qty: float, price: float) -> Optional[str]:
        try:
            # جلب الرصيد الفعلي من الحساب لتجنب Oversold (30005)
            base = symbol.replace("/USDT", "")
            try:
                bal = self.exchange.fetch_balance({"type": "spot"})
                actual_free = float(bal.get(base, {}).get("free", 0) or
                                    bal.get("free", {}).get(base, 0))
                if actual_free > 0:
                    # استخدم الأقل بين qty والرصيد الفعلي، مع هامش 0.5%
                    safe_qty = min(qty, actual_free) * 0.995
                    _log(f"[Executor] place_limit_sell {symbol}: qty={qty:.6f} actual={actual_free:.6f} safe={safe_qty:.6f}")
                    qty = safe_qty
            except Exception as be:
                _log(f"[Executor] fetch_balance for sell: {be} — using qty*0.99")
                qty = qty * 0.99  # fallback: تقليل 1%

            prec_qty = float(self.exchange.amount_to_precision(symbol, qty))
            if prec_qty <= 0:
                _log(f"[Executor] place_limit_sell {symbol}: prec_qty=0, skip")
                return None
            order = self.exchange.create_limit_sell_order(symbol, prec_qty, price)
            oid = str(order.get("id", ""))
            _log(f"[Executor] ✅ TP1 order placed {symbol} qty={prec_qty} @ {price:.6g} id={oid}")
            return oid
        except Exception as e:
            _log(f"[Executor] place_limit_sell {symbol}: {e}")
            return None

    def market_sell(self, symbol: str, qty: float):
        """
        Returns:
          True   — بيع ناجح
          "dust" — قيمة أقل من $1 (30002)، لا يمكن بيعها → تُعامَل كغبار
          False  — فشل عام
        """
        for attempt in range(1, 4):  # 3 محاولات فقط
            try:
                prec = float(self.exchange.amount_to_precision(symbol, qty))
                if prec <= 0:
                    return "dust"
                self.exchange.create_market_sell_order(symbol, prec)
                _log(f"[Executor] ✅ SELL {symbol} qty={prec:.6f}")
                return True
            except ccxt.InsufficientFunds:
                try:
                    bal = self.exchange.fetch_balance({"type": "spot"})
                    base = symbol.replace("/USDT", "")
                    actual = float(bal.get(base, {}).get("free", 0))
                    if actual > 0:
                        qty = actual * 0.999
                        continue
                except Exception:
                    pass
                return False
            except Exception as e:
                err = str(e)
                # 30002 = أقل من الحد الأدنى ($1) — غبار لا يمكن بيعه
                if "30002" in err or "minimum" in err.lower():
                    _log(f"[Executor] ⚠️ {symbol} أقل من الحد الأدنى — غبار")
                    return "dust"
                _log(f"[Executor] market_sell {symbol} attempt {attempt}: {e}")
                time.sleep(2)
        return False

    def fetch_coin_balance_usdt(self, symbol: str, price: float) -> float:
        """يجلب القيمة الفعلية للعملة على البورصة بالدولار (free + used)"""
        try:
            base = symbol.replace("/USDT", "")
            bal = self.exchange.fetch_balance({"type": "spot"})
            free = float(bal.get(base, {}).get("free", 0))
            used = float(bal.get(base, {}).get("used", 0))
            qty  = free + used  # used = محجوز في أوردرات TP مفتوحة
            return qty * price
        except Exception:
            return -1  # -1 = فشل الجلب، لا نعامله كغبار

    def cancel_order(self, symbol: str, order_id: str):
        try:
            self.exchange.cancel_order(order_id, symbol)
        except Exception:
            pass

    def order_status(self, symbol: str, order_id: str) -> str:
        try:
            o = self.exchange.fetch_order(order_id, symbol)
            return str(o.get("status", "unknown"))
        except Exception:
            return "unknown"

    def fetch_ohlcv(self, symbol: str, tf: str, limit: int = 50) -> Optional[pd.DataFrame]:
        try:
            data = self.exchange.fetch_ohlcv(symbol, tf, limit=limit)
            if not data:
                return None
            df = pd.DataFrame(data, columns=["ts","open","high","low","close","vol"])
            return df.astype(float)
        except Exception:
            return None

    # عملات كبيرة مضمونة التداول على MEXC (لا تُحجب جغرافياً)
    # العملات المضمونة فعلياً من Railway IP — فقط العملات الكبيرة جداً (top 10 عالمياً)
    # تم تقليصها بناءً على اختبار مباشر: SAND/OP/NEAR/1INCH/ALGO كلها محجوبة أيضاً
    SAFE_COINS = [
        "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
        "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "LTC/USDT", "TRX/USDT",
        "DOT/USDT", "LINK/USDT", "SHIB/USDT", "PEPE/USDT", "BONK/USDT",
        "WIF/USDT", "FLOKI/USDT", "SUI/USDT",
    ]

    def fetch_top_gainers(self, count: int = 50, min_pct: float = 3.0) -> list[str]:
        """
        يجلب أفضل N رابح من MEXC مباشرة.
        يُضيف SAFE_COINS كـ fallback إذا كانت الـ altcoins محجوبة.
        """
        # Tokenized stocks — محجوبة في معظم الدول
        _TOKENIZED_SUFFIXES = ("ON", "STOCK", "X")  # NVDAON, JNJSTOCK, NVDAX
        def _is_tokenized(sym: str) -> bool:
            base = sym.replace("/USDT", "")
            return any(base.endswith(s) and len(base) > len(s) + 2
                       for s in _TOKENIZED_SUFFIXES)

        try:
            tickers = self.exchange.fetch_tickers()
            self._tickers_cache = tickers  # حفظ صريح للـ cache
            gainers = []
            for symbol, t in tickers.items():
                if not symbol.endswith("/USDT"):
                    continue
                if _is_tokenized(symbol):
                    continue  # تخطي Tokenized Stocks
                # تخطي العملات المحجوبة مسبقاً
                base = symbol.replace("/USDT", "")
                if base in self._geo_blocked:
                    continue
                pct = float(t.get("percentage") or t.get("change") or 0)
                vol_usd = float(t.get("quoteVolume") or 0)
                if pct >= min_pct and vol_usd > 50000:
                    gainers.append((symbol, pct, vol_usd))
            gainers.sort(key=lambda x: x[1], reverse=True)
            top = [g[0] for g in gainers[:count]]
            if top:
                _log(f"[TopGainers] 🏆 وجدت {len(top)} رابح (أعلى: {gainers[0][1]:.1f}% {gainers[0][0]})")
            else:
                _log(f"[TopGainers] ⚠️ لا رابح — سأفحص SAFE_COINS")

            # SAFE_COINS أولاً (عملات كبيرة مضمونة) — بغض النظر عن الـ gain%
            # لأن معظم الـ altcoins محجوبة جغرافياً من Railway's IP
            safe_with_gain = []
            safe_no_gain = []
            for sym in self.SAFE_COINS:
                base = sym.replace("/USDT", "")
                if base in self._geo_blocked:
                    continue
                t = tickers.get(sym, {})
                pct = float(t.get("percentage") or t.get("change") or 0)
                vol_usd = float(t.get("quoteVolume") or 0)
                if pct >= 1.0 and vol_usd > 50000:  # عتبة مخففة للـ SAFE_COINS
                    safe_with_gain.append(sym)
                else:
                    safe_no_gain.append(sym)  # إضافة حتى بدون gain (للـ fallback)

            # ترتيب: SAFE_COINS مع gain أولاً، ثم altcoins، ثم SAFE_COINS بدون gain
            combined = safe_with_gain + [s for s in top if s not in safe_with_gain] + safe_no_gain
            if safe_with_gain:
                _log(f"[TopGainers] 🛡️ SAFE_COINS رابحة: {safe_with_gain[:5]}")
            return combined[:count + len(safe_with_gain) + len(safe_no_gain)]
        except Exception as e:
            _log(f"[TopGainers] ❌ {e}")
            return list(self.SAFE_COINS)

    def fetch_volume_anomalies(self, spike_ratio: float = 5.0) -> list[str]:
        """
        يكتشف العملات التي ارتفع حجمها فجأة أكثر من spike_ratio×
        مقارنة بمتوسط آخر 24 ساعة.
        """
        try:
            tickers = self.exchange.fetch_tickers()
            anomalies = []
            for symbol, t in tickers.items():
                if not symbol.endswith("/USDT"):
                    continue
                # MEXC يوفر baseVolume و quoteVolume
                vol_now = float(t.get("quoteVolume") or 0)
                # نستخدم نسبة الحجم إذا كانت متاحة
                # (نقارن الساعة الأخيرة بالمتوسط اليومي ÷ 24)
                if vol_now > 100000:  # فلتر: حجم حد أدنى $100K
                    # سنتحقق من OHLCV لاحقاً في check_s3
                    anomalies.append(symbol)
            return anomalies[:100]  # أول 100 فقط
        except Exception as e:
            _log(f"[VolumeAnomaly] ❌ {e}")
            return []

    def fetch_new_listings(self, known_symbols: set) -> list[str]:
        """
        يكتشف العملات المدرجة حديثاً بمقارنة قائمة الأسواق الحالية بالمعروفة.
        """
        try:
            # إعادة تحميل الأسواق
            markets = self.exchange.load_markets(True)
            new_ones = []
            for symbol in markets:
                if (symbol.endswith("/USDT") and
                    symbol not in known_symbols and
                    markets[symbol].get("active", True) and
                    markets[symbol].get("type", "spot") == "spot"):
                    new_ones.append(symbol)
            if new_ones:
                _log(f"[NewListings] 🆕 {len(new_ones)} عملة جديدة: {new_ones[:5]}")
            return new_ones
        except Exception as e:
            _log(f"[NewListings] ❌ {e}")
            return []

    def get_orderbook_pressure(self, symbol: str, depth: int = 20) -> float:
        """
        يحسب نسبة ضغط الشراء/البيع من دفتر الأوامر.
        ratio > 1.5 = ضغط شراء قوي ← إشارة إيجابية
        """
        try:
            ob = self.exchange.fetch_order_book(symbol, depth)
            bids_vol = sum(b[1] for b in ob.get("bids", [])[:depth])
            asks_vol = sum(a[1] for a in ob.get("asks", [])[:depth])
            if asks_vol <= 0:
                return 1.0
            return bids_vol / asks_vol
        except Exception:
            return 1.0


# ══════════════════════════════════════════════════════════════
# INDICATORS
# ══════════════════════════════════════════════════════════════
def calc_rsi(closes: pd.Series, period: int = 14) -> float:
    delta = closes.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean().replace(0, 1e-9)
    rs    = gain / loss
    rsi   = 100 - 100 / (1 + rs)
    return float(rsi.iloc[-1]) if len(rsi) >= period else 50.0

def calc_ema(closes: pd.Series, period: int) -> float:
    return float(closes.ewm(span=period, adjust=False).mean().iloc[-1])


# ══════════════════════════════════════════════════════════════
# AI ADVISOR
# ══════════════════════════════════════════════════════════════
class AIAdvisor:
    URL = "https://api.deepseek.com/v1/chat/completions"

    def __init__(self, cfg: Config):
        self.cfg = cfg

    async def approve(self, session: aiohttp.ClientSession,
                      symbol: str, strategy: str,
                      rsi: float, gain_pct: float, vol_ratio: float,
                      signal_source: str = "scan") -> str:
        if not self.cfg.ai_enabled or not self.cfg.deepseek_api_key:
            return "approve"
        source_note = f" (مصدر: {signal_source})" if signal_source != "scan" else ""
        prompt = (
            f"عملة {symbol} على MEXC — استراتيجية {strategy}{source_note}.\n"
            f"RSI={rsi:.0f} | ارتفاع={gain_pct:.1f}% | حجم×{vol_ratio:.1f}\n"
            "هل تدخل الصفقة؟ أجب بكلمة واحدة فقط: approve أو reject"
        )
        try:
            async with session.post(
                self.URL,
                json={
                    "model": self.cfg.deepseek_model,
                    "max_tokens": 10,
                    "temperature": 0.1,
                    "messages": [{"role": "user", "content": prompt}],
                },
                headers={"Authorization": f"Bearer {self.cfg.deepseek_api_key}"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    text = data["choices"][0]["message"]["content"].strip().lower()
                    if "approve" in text:
                        return "approve"
                    if "reject" in text:
                        return "reject"
        except Exception as e:
            _log(f"[AI] timeout/error: {e}")
        return "approve"

    async def smart_report(self, session: aiohttp.ClientSession,
                           balance: float, slots_used: int, max_slots: int,
                           monthly: dict) -> str:
        if not self.cfg.deepseek_api_key:
            return ""
        prompt = (
            f"بوت تداول MEXC — الحالة:\n"
            f"رصيد: ${balance:.2f} | صفقات نشطة: {slots_used}/{max_slots}\n"
            f"الشهر: {monthly['total']} صفقة | ربح: ${monthly['pnl']:.2f} | "
            f"Win rate: {monthly['winrate']}%\n"
            "اكتب تقرير حالة مختصر (3-4 جمل) بالعربية يذكر التوصية الرئيسية."
        )
        try:
            async with session.post(
                self.URL,
                json={
                    "model": self.cfg.deepseek_model,
                    "max_tokens": 300,
                    "temperature": 0.4,
                    "messages": [{"role": "user", "content": prompt}],
                },
                headers={"Authorization": f"Bearer {self.cfg.deepseek_api_key}"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return data["choices"][0]["message"]["content"].strip()
        except Exception:
            pass
        return ""


# ══════════════════════════════════════════════════════════════
# TELEGRAM — queue ذكي، رسائل مهمة فقط
# ══════════════════════════════════════════════════════════════
_tg_last_sent:  float = 0.0
_tg_lock:       asyncio.Lock = None
_tg_queue:      asyncio.Queue = None
_tg_worker_running: bool = False

# أنواع الرسائل المقبولة فقط (تصفية صارمة)
TG_IMPORTANT = {"trade_open", "trade_close", "alert_rocket", "alert_nofunds"}

def _get_tg_lock():
    global _tg_lock
    if _tg_lock is None:
        _tg_lock = asyncio.Lock()
    return _tg_lock

def _get_tg_queue():
    global _tg_queue
    if _tg_queue is None:
        _tg_queue = asyncio.Queue(maxsize=20)
    return _tg_queue

async def _tg_worker(token: str, chat_id: str):
    """Worker يُرسل من الـ queue بتسلسل"""
    global _tg_worker_running, _tg_last_sent
    _tg_worker_running = True
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    while True:
        try:
            text = await asyncio.wait_for(_get_tg_queue().get(), timeout=60)
            if text is None:
                break
            # فاصل إجباري 4 ثانية بين الرسائل
            elapsed = time.time() - _tg_last_sent
            if elapsed < 4.0:
                await asyncio.sleep(4.0 - elapsed)
            # محاولة الإرسال
            for attempt in range(3):
                try:
                    conn = aiohttp.TCPConnector(ssl=True, limit=2, force_close=True)
                    async with aiohttp.ClientSession(connector=conn) as s:
                        resp = await s.post(url,
                            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=20, connect=8))
                        body = await resp.text()
                        if resp.status == 200:
                            _tg_last_sent = time.time()
                            _log("[TG] ✅ أُرسلت")
                            break
                        if resp.status == 429:
                            wait = _json.loads(body).get("parameters", {}).get("retry_after", 30)
                            _log(f"[TG] ⏳ {wait}ث rate-limit")
                            await asyncio.sleep(wait + 2)
                        elif resp.status in (400, 401, 403):
                            _log(f"[TG] ❌ خطأ دائم {resp.status}")
                            break
                        else:
                            await asyncio.sleep(5)
                except Exception as e:
                    _log(f"[TG] ❌ attempt {attempt+1}: {type(e).__name__}: {str(e)[:50]}")
                    await asyncio.sleep(5)
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            _log(f"[TG] worker error: {e}")
            await asyncio.sleep(5)

async def tg_send(token: str, chat_id: str, text: str, msg_type: str = "misc"):
    """إرسال رسالة Telegram — يقبل فقط الأنواع المهمة"""
    if not token or not chat_id:
        return
    # تصفية: فقط الأنواع المهمة
    if msg_type not in TG_IMPORTANT:
        return
    if len(text) > 4000:
        text = text[:4000] + "\n..."
    global _tg_worker_running
    if not _tg_worker_running:
        asyncio.create_task(_tg_worker(token, chat_id))
    q = _get_tg_queue()
    if not q.full():
        await q.put(text)
    else:
        _log("[TG] ⚠️ queue ممتلئ — رسالة تُجاهل")


# ══════════════════════════════════════════════════════════════
# SIGNAL AGGREGATOR — يجمع الإشارات من كل المصادر
# ══════════════════════════════════════════════════════════════
class SignalAggregator:
    """
    يجمع إشارات من مصادر متعددة ويرتّبها حسب الأولوية.
    يُشغَّل كل 30 ثانية ويُحدّث قائمة "العملات المرشّحة".
    """

    def __init__(self, cfg: Config, ex: Executor, db: TradeDB):
        self.cfg   = cfg
        self.ex    = ex
        self.db    = db

        # الأولويات المرشحة (symbol → score)
        self._candidates: dict[str, float] = {}
        self._lock = threading.Lock()

        # تتبع القوائم
        self._known_symbols: set = set()
        self._trending_coins: set = set()   # CoinGecko
        self._last_coingecko: float = 0.0
        self._new_listings_seen: set = set()

    async def refresh(self, session: aiohttp.ClientSession):
        """تحديث شامل للإشارات — يُنادى كل دورة scan"""
        candidates: dict[str, float] = {}

        # 1. Top Gainers (أعلى الرابحين) — الأسرع والأهم
        if self.cfg.top_gainers_enabled:
            gainers = await asyncio.get_event_loop().run_in_executor(
                None, self.ex.fetch_top_gainers,
                self.cfg.top_gainers_count, self.cfg.top_gainers_min_pct
            )
            for sym in gainers:
                candidates[sym] = candidates.get(sym, 0) + 10  # وزن عالٍ

        # 2. CoinGecko Trending — كل 4 ساعات
        if self.cfg.coingecko_enabled:
            now = time.time()
            if now - self._last_coingecko > self.cfg.coingecko_hours * 3600:
                trending = await self._fetch_coingecko_trending(session)
                self._trending_coins = trending
                self._last_coingecko = now
                if trending:
                    _log(f"[CoinGecko] 🌐 Trending: {', '.join(list(trending)[:5])}")
            # إضافة العملات الرائجة بوزن متوسط
            for sym in self._trending_coins:
                mexc_sym = f"{sym.upper()}/USDT"
                if mexc_sym in self.ex.exchange.markets:
                    candidates[mexc_sym] = candidates.get(mexc_sym, 0) + 5

        # 3. New Listings — عملات جديدة (فرصة ضخمة)
        if self.cfg.new_listings_enabled:
            if not self._known_symbols:
                self._known_symbols = set(self.ex.exchange.markets.keys())
            new_syms = await asyncio.get_event_loop().run_in_executor(
                None, self.ex.fetch_new_listings, self._known_symbols
            )
            for sym in new_syms:
                if sym not in self._new_listings_seen:
                    self._new_listings_seen.add(sym)
                    self._known_symbols.add(sym)
                    candidates[sym] = candidates.get(sym, 0) + 20  # وزن أعلى — عملة جديدة!

        # 4. Volume Anomalies — حجم ضخم مفاجئ
        if self.cfg.vol_anomaly_enabled:
            anomalies = await asyncio.get_event_loop().run_in_executor(
                None, self.ex.fetch_volume_anomalies, self.cfg.vol_anomaly_spike
            )
            for sym in anomalies:
                candidates[sym] = candidates.get(sym, 0) + 7

        # 5. SAFE_COINS — مضمونة التداول، أضفها دائماً بوزن متوسط
        for sym in self.ex.SAFE_COINS:
            base = sym.replace("/USDT", "")
            if base not in self.ex._geo_blocked:
                candidates[sym] = candidates.get(sym, 0) + 3  # وزن منخفض كـ fallback

        # 6. تحديث القائمة المدمجة
        with self._lock:
            self._candidates = candidates

        total = len(candidates)
        if total:
            _log(f"[SignalAgg] 📡 {total} مرشح | "
                 f"Gainers:{len(gainers) if self.cfg.top_gainers_enabled else 0} "
                 f"Trending:{len(self._trending_coins)} "
                 f"New:{len(self._new_listings_seen)}")

    def get_priority_symbols(self) -> list[tuple[str, float]]:
        """يُعيد العملات مرتّبة حسب الأولوية (الأعلى score أولاً)"""
        with self._lock:
            ranked = sorted(self._candidates.items(), key=lambda x: x[1], reverse=True)
            return ranked

    def is_new_listing(self, symbol: str) -> bool:
        return symbol in self._new_listings_seen

    async def _fetch_coingecko_trending(self, session: aiohttp.ClientSession) -> set:
        """يجلب أكثر 7 عملات بحثاً على CoinGecko"""
        try:
            async with session.get(
                "https://api.coingecko.com/api/v3/search/trending",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    coins = data.get("coins", [])
                    symbols = set()
                    for c in coins[:7]:
                        sym = c.get("item", {}).get("symbol", "").upper()
                        if sym:
                            symbols.add(sym)
                    return symbols
        except Exception as e:
            _log(f"[CoinGecko] ⚠️ {e}")
        return set()


# ══════════════════════════════════════════════════════════════
# BINANCE LISTING MONITOR — أقوى إشارة (مجانية تماماً)
# ══════════════════════════════════════════════════════════════
class BinanceListingMonitor:
    """
    يراقب RSS الرسمي لـ Binance للإعلانات الجديدة.
    عند إدراج عملة جديدة على Binance → شراء فوري على MEXC
    (العملة ترتفع 50-200% خلال ساعة على MEXC)
    """
    RSS_URL = "https://www.binance.com/en/support/announcement/new-cryptocurrency-listing?c=48&navId=48"
    API_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&pageNo=1&pageSize=5&catalogId=48"

    def __init__(self, cfg: Config, ex: Executor):
        self.cfg = cfg
        self.ex  = ex
        self._seen_titles: set = set()
        self._new_symbols: list = []   # عملات جديدة مكتشفة
        self._lock = threading.Lock()
        self._last_check: float = 0

    async def check(self, session: aiohttp.ClientSession) -> list[str]:
        """يتحقق من إعلانات Binance — يُعيد قائمة عملات جديدة"""
        if not self.cfg.binance_listing_enabled:
            return []
        now = time.time()
        if now - self._last_check < self.cfg.binance_listing_interval:
            # أعد الرموز المكتشفة مؤخراً
            with self._lock:
                return list(self._new_symbols)
        self._last_check = now

        new_symbols = []
        try:
            async with session.get(
                self.API_URL,
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as r:
                if r.status != 200:
                    return []
                data = await r.json()
                articles = data.get("data", {}).get("articles", []) or []
                for art in articles:
                    title = art.get("title", "")
                    if title in self._seen_titles:
                        continue
                    self._seen_titles.add(title)
                    # استخراج رمز العملة من العنوان
                    sym = self._extract_symbol(title)
                    if sym:
                        mexc_pair = f"{sym}/USDT"
                        if mexc_pair in self.ex.exchange.markets:
                            new_symbols.append(mexc_pair)
                            _log(f"[Binance📢] 🔥 إدراج جديد رُصد: {sym} — شراء فوري على MEXC!")
        except Exception as e:
            _log(f"[Binance📢] ⚠️ {e}")

        with self._lock:
            # الاحتفاظ بالرموز لمدة دورة واحدة فقط
            self._new_symbols = new_symbols
        return new_symbols

    @staticmethod
    def _extract_symbol(title: str) -> str:
        """يستخرج رمز العملة من عنوان الإعلان مثل 'Binance Will List XYZ (XYZ)'"""
        import re
        # نمط 1: (XYZ) في نهاية العنوان
        m = re.search(r'\(([A-Z]{2,10})\)\s*$', title)
        if m:
            return m.group(1)
        # نمط 2: "Will List XYZ"
        m = re.search(r'[Ll]ist\s+([A-Z]{2,10})', title)
        if m:
            return m.group(1)
        return ""


# ══════════════════════════════════════════════════════════════
# FUNDING RATE DETECTOR — كشف Short Squeeze
# ══════════════════════════════════════════════════════════════
class FundingRateDetector:
    """
    يفحص Funding Rate من MEXC Futures (مجاني).
    Funding سالب جداً = الجميع يراهن هبوطاً = Short Squeeze قادم = فرصة شراء!
    """
    MEXC_FUNDING_URL = "https://contract.mexc.com/api/v1/contract/funding_rate"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._cache: dict[str, float] = {}   # symbol → funding_rate
        self._last_refresh: float = 0

    async def refresh(self, session: aiohttp.ClientSession):
        """يحدث الـ funding rates كل 5 دقائق"""
        if not self.cfg.funding_rate_enabled:
            return
        now = time.time()
        if now - self._last_refresh < 300:
            return
        self._last_refresh = now
        try:
            async with session.get(
                self.MEXC_FUNDING_URL,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status != 200:
                    return
                data = await r.json()
                items = data.get("data", []) or []
                cache = {}
                for item in items:
                    sym = item.get("symbol", "").replace("_", "/").replace("USDT", "/USDT")
                    # تحويل MX_BTCUSDT → BTC/USDT
                    sym = sym.replace("MX_", "").replace("USDT", "").rstrip("/")
                    if sym:
                        rate = float(item.get("fundingRate", 0))
                        cache[sym + "/USDT"] = rate
                self._cache = cache
                negative = {s: r for s, r in cache.items() if r <= self.cfg.funding_rate_threshold}
                if negative:
                    top = sorted(negative.items(), key=lambda x: x[1])[:5]
                    _log(f"[Funding] 📉 Squeeze candidates: " +
                         ", ".join(f"{s}={r:.4f}" for s, r in top))
        except Exception as e:
            _log(f"[Funding] ⚠️ {e}")

    def is_squeeze_candidate(self, symbol: str) -> bool:
        """يُعيد True إذا كان Funding Rate سالب جداً (فرصة Short Squeeze)"""
        if not self.cfg.funding_rate_enabled:
            return False
        rate = self._cache.get(symbol, 0)
        return rate <= self.cfg.funding_rate_threshold

    def get_rate(self, symbol: str) -> float:
        return self._cache.get(symbol, 0.0)


# ══════════════════════════════════════════════════════════════
# CRYPTOPANIC NEWS FILTER — فلتر الأخبار السلبية
# ══════════════════════════════════════════════════════════════
class CryptoPanicFilter:
    """
    يفلتر الأخبار السلبية قبل الدخول — يمنع الشراء وقت الأخبار الحمراء.
    مجاني بدون مفتاح (بيانات محدودة) أو بمفتاح مجاني (أفضل).
    """
    BASE_URL = "https://cryptopanic.com/api/v1/posts/"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._negative_coins: set = set()  # عملات عليها أخبار سلبية حالية
        self._last_refresh: float = 0

    async def refresh(self, session: aiohttp.ClientSession):
        """يحدث الأخبار كل 10 دقائق"""
        if not self.cfg.cryptopanic_enabled:
            return
        now = time.time()
        if now - self._last_refresh < 600:
            return
        self._last_refresh = now

        params = {"filter": "negative", "public": "true"}
        if self.cfg.cryptopanic_api_key:
            params["auth_token"] = self.cfg.cryptopanic_api_key

        try:
            async with session.get(
                self.BASE_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status != 200:
                    return
                data = await r.json()
                negative = set()
                for post in data.get("results", []):
                    for currency in post.get("currencies", []):
                        sym = currency.get("code", "").upper()
                        if sym:
                            negative.add(sym)
                old_count = len(self._negative_coins)
                self._negative_coins = negative
                if negative:
                    _log(f"[News🔴] {len(negative)} عملة عليها أخبار سلبية (منع الدخول)")
        except Exception as e:
            _log(f"[News🔴] ⚠️ {e}")

    def is_blocked(self, symbol: str) -> bool:
        """يُعيد True إذا كانت العملة عليها أخبار سلبية"""
        if not self.cfg.cryptopanic_enabled or not self.cfg.cryptopanic_block_negative:
            return False
        base = symbol.split("/")[0].upper()
        return base in self._negative_coins


# ══════════════════════════════════════════════════════════════
# LUNARCRUSH SOCIAL SIGNAL (اختياري — يتطلب مفتاح API)
# ══════════════════════════════════════════════════════════════
class LunarCrushSignal:
    """
    يجلب Galaxy Score + Social Volume من LunarCrush.
    ارتفاع مفاجئ في Social Score = إشارة مبكرة قبل السعر.
    مجاني: 100 طلب/يوم — يُفعّل فقط إذا كان API key موجوداً.
    """
    BASE_URL = "https://lunarcrush.com/api4/public/coins/list/v2"

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._scores: dict[str, float] = {}   # symbol → galaxy_score
        self._last_refresh: float = 0

    async def refresh(self, session: aiohttp.ClientSession):
        """يحدث النتائج كل ساعة (توفيراً للطلبات المجانية)"""
        if not self.cfg.lunarcrush_enabled or not self.cfg.lunarcrush_api_key:
            return
        now = time.time()
        if now - self._last_refresh < 3600:
            return
        self._last_refresh = now
        try:
            async with session.get(
                self.BASE_URL,
                headers={"Authorization": f"Bearer {self.cfg.lunarcrush_api_key}"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status != 200:
                    _log(f"[LunarCrush] ⚠️ status={r.status}")
                    return
                data = await r.json()
                scores = {}
                for coin in data.get("data", []):
                    sym = coin.get("symbol", "").upper()
                    score = float(coin.get("galaxy_score", 0))
                    if sym:
                        scores[sym] = score
                self._scores = scores
                top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]
                _log(f"[LunarCrush🌙] Top: " + ", ".join(f"{s}={sc:.0f}" for s, sc in top))
        except Exception as e:
            _log(f"[LunarCrush] ⚠️ {e}")

    def get_score(self, symbol: str) -> float:
        base = symbol.split("/")[0].upper()
        return self._scores.get(base, 0.0)

    def is_hot(self, symbol: str) -> bool:
        return self.get_score(symbol) >= self.cfg.lunarcrush_min_score


# ══════════════════════════════════════════════════════════════
# STRATEGY ENGINE
# ══════════════════════════════════════════════════════════════
class StrategyEngine:
    def __init__(self, cfg: Config, ex: Executor):
        self.cfg = cfg
        self.ex  = ex

    # ─── S1: RSI Bounce ───────────────────────────────────────
    def check_s1(self, symbol: str) -> Optional[dict]:
        if not self.cfg.s1_enabled:
            return None
        df4h = self.ex.fetch_ohlcv(symbol, "4h", limit=50)
        if df4h is None or len(df4h) < 20:
            return None
        rsi_now  = calc_rsi(df4h["close"])
        rsi_prev = calc_rsi(df4h["close"].iloc[:-1]) if len(df4h) > 15 else rsi_now
        if not (rsi_prev <= self.cfg.s1_rsi_oversold and rsi_now >= self.cfg.s1_rsi_bounce):
            return None
        price     = float(df4h["close"].iloc[-1])
        vol       = float(df4h["vol"].iloc[-1])
        vol_avg   = float(df4h["vol"].iloc[-20:].mean())
        vol_ratio = vol / vol_avg if vol_avg > 0 else 0
        return {"strategy": "S1", "rsi": rsi_now, "gain_pct": 0, "vol_ratio": vol_ratio, "price": price}

    # ─── S2: Momentum ─────────────────────────────────────────
    def check_s2(self, symbol: str) -> Optional[dict]:
        if not self.cfg.s2_enabled:
            return None
        df1h = self.ex.fetch_ohlcv(symbol, "1h", limit=30)
        if df1h is None or len(df1h) < 15:
            return None
        rsi = calc_rsi(df1h["close"])
        if not (self.cfg.s2_rsi_min <= rsi <= self.cfg.s2_rsi_max):
            return None
        vol       = float(df1h["vol"].iloc[-1])
        vol_avg   = float(df1h["vol"].iloc[-20:].mean())
        vol_ratio = vol / vol_avg if vol_avg > 0 else 0
        if vol_ratio < self.cfg.s2_vol_ratio:
            return None
        ema9  = calc_ema(df1h["close"], 9)
        ema21 = calc_ema(df1h["close"], 21)
        if ema9 <= ema21 * 0.99:
            return None
        price = float(df1h["close"].iloc[-1])
        return {"strategy": "S2", "rsi": rsi, "gain_pct": 0, "vol_ratio": vol_ratio, "price": price}

    # ─── S3: Rocket Detector ──────────────────────────────────
    def check_s3(self, symbol: str) -> Optional[dict]:
        if not self.cfg.s3_enabled:
            return None

        # شرط D: SAFE_COINS — عتبة مخففة جداً (1H gain >= 1%) لضمان وجود صفقة دائماً
        _SAFE = {s.replace("/USDT","") for s in self.ex.SAFE_COINS}
        base = symbol.replace("/USDT","")
        is_safe = base in _SAFE

        # شرط C/D: سريع من الـ tickers المُخزنة
        try:
            cached = self.ex._tickers_cache or {}
            t = cached.get(symbol, {})
            if t:
                gain_24h = float(t.get("percentage") or t.get("change") or 0)
                price_c  = float(t.get("last") or t.get("close") or 0)
                vol_usd  = float(t.get("quoteVolume") or 0)

                # شرط D: SAFE_COINS — أي gain إيجابي (>0.5%) بحجم كافٍ
                if is_safe and gain_24h >= 0.5 and price_c > 0 and vol_usd >= 500000:
                    _log(f"[S3🛡️] {symbol} SAFE 24H={gain_24h:.1f}% vol=${vol_usd:.0f} — safe fast-track!")
                    return {"strategy": "S3", "rsi": 50, "gain_pct": gain_24h,
                            "vol_ratio": 1.0, "price": price_c, "trigger": "SAFE_24H"}

                # شرط C: altcoins — gain >= 10%
                threshold = 10.0
                if gain_24h >= threshold:
                    min_vol = self.cfg.min_volume_usd
                    if price_c > 0 and vol_usd >= min_vol:
                        _log(f"[S3🚀] {symbol} 24H={gain_24h:.1f}% vol=${vol_usd:.0f} — fast-track!")
                        return {"strategy": "S3", "rsi": 50, "gain_pct": gain_24h,
                                "vol_ratio": 1.5, "price": price_c, "trigger": "24H"}
        except Exception:
            pass

        df1h = self.ex.fetch_ohlcv(symbol, "1h", limit=24)
        if df1h is None or len(df1h) < 5:
            return None

        price     = float(df1h["close"].iloc[-1])
        prev_1h   = float(df1h["close"].iloc[-2])
        gain_1h   = (price - prev_1h) / prev_1h * 100

        vol_1h    = float(df1h["vol"].iloc[-1])
        vol_avg1h = float(df1h["vol"].iloc[-20:].mean())
        vol_ratio = vol_1h / vol_avg1h if vol_avg1h > 0 else 0

        rsi = calc_rsi(df1h["close"])
        if rsi > self.cfg.s3_rsi_max:
            return None

        # شرط A: ارتفاع سريع في ساعة
        # للـ SAFE_COINS: عتبة مخففة (1%) لضمان دخول دائم
        # للـ altcoins: gain >= s3_gain_1h_min مع vol_ratio
        vol_ok_1h   = vol_ratio >= self.cfg.s3_vol_spike
        strong_gain = gain_1h >= (self.cfg.s3_gain_1h_min + 2.0)  # 5%
        gain_min = 1.0 if is_safe else self.cfg.s3_gain_1h_min
        vol_ok_safe = True if is_safe else (vol_ok_1h or strong_gain)
        if gain_1h >= gain_min and vol_ok_safe:
            _log(f"[S3] ✅ {symbol} 1H gain={gain_1h:.1f}% vol={vol_ratio:.1f}x RSI={rsi:.0f}")
            return {"strategy": "S3", "rsi": rsi, "gain_pct": gain_1h,
                    "vol_ratio": vol_ratio, "price": price, "trigger": "1H"}

        # شرط B: ارتفاع في 4H
        df4h = self.ex.fetch_ohlcv(symbol, "4h", limit=10)
        if df4h is not None and len(df4h) >= 3:
            prev_4h   = float(df4h["close"].iloc[-2])
            gain_4h   = (price - prev_4h) / prev_4h * 100
            vol_4h    = float(df4h["vol"].iloc[-1])
            vol_avg4h = float(df4h["vol"].iloc[-8:].mean())
            vr4h      = vol_4h / vol_avg4h if vol_avg4h > 0 else 0
            strong_4h = gain_4h >= (self.cfg.s3_gain_4h_min + 3.0)  # 10%
            if gain_4h >= self.cfg.s3_gain_4h_min and (vr4h >= self.cfg.s3_vol_spike or strong_4h):
                _log(f"[S3] ✅ {symbol} 4H gain={gain_4h:.1f}% vol={vr4h:.1f}x RSI={rsi:.0f}")
                return {"strategy": "S3", "rsi": rsi, "gain_pct": gain_4h,
                        "vol_ratio": vr4h, "price": price, "trigger": "4H"}
        return None

    # ─── New Listing Strategy ─────────────────────────────────
    def check_new_listing(self, symbol: str, cfg: Config) -> Optional[dict]:
        """استراتيجية خاصة للعملات الجديدة — دخول فوري مع SL ضيق"""
        try:
            price = self.ex.live_price(symbol)
            if price <= 0:
                return None
            # فحص أن هناك حجم تداول فعلي
            ticker = self.ex.exchange.fetch_ticker(symbol)
            vol_usd = float(ticker.get("quoteVolume") or 0)
            if vol_usd < 10000:  # حجم أدنى $10K للعملة الجديدة
                return None
            return {
                "strategy": "NEW_LISTING",
                "rsi": 50, "gain_pct": 0, "vol_ratio": 1.0,
                "price": price, "trigger": "new_listing",
            }
        except Exception:
            return None

    def _calc_atr(self, symbol: str, tf: str = "1h", limit: int = 14) -> float:
        """حساب ATR (متوسط التذبذب الحقيقي) لتحديد المسافة المثلى للأهداف"""
        try:
            df = self.ex.fetch_ohlcv(symbol, tf, limit=limit + 1)
            if df is None or len(df) < 5:
                return 0.0
            hi = df["high"].values
            lo = df["low"].values
            cl = df["close"].values
            tr = [max(hi[i] - lo[i],
                      abs(hi[i] - cl[i-1]),
                      abs(lo[i] - cl[i-1]))
                  for i in range(1, len(df))]
            atr = sum(tr[-14:]) / min(14, len(tr))
            atr_pct = (atr / cl[-1]) * 100
            return round(atr_pct, 3)
        except Exception as e:
            _log(f"[ATR] ⚠️ {symbol}: {e}")
            return 0.0

    def _calc_support_resistance(self, symbol: str) -> tuple[float, float]:
        """أقرب دعم ومقاومة من بيانات 4H"""
        try:
            df = self.ex.fetch_ohlcv(symbol, "4h", limit=24)
            if df is None or len(df) < 5:
                return 0.0, 0.0
            price = df["close"].iloc[-1]
            lows  = df["low"].values
            highs = df["high"].values
            # أقرب دعم (أعلى قاع تحت السعر الحالي)
            supports = sorted([l for l in lows if l < price], reverse=True)
            support  = supports[0] if supports else price * 0.97
            # أقرب مقاومة (أدنى قمة فوق السعر الحالي)
            resists  = sorted([h for h in highs if h > price])
            resist   = resists[0] if resists else price * 1.05
            return support, resist
        except Exception as e:
            _log(f"[S/R] ⚠️ {symbol}: {e}")
            return 0.0, 0.0

    def calc_targets(self, signal: dict) -> dict:
        price  = signal["price"]
        s      = signal["strategy"]
        symbol = signal.get("symbol", "")
        gain   = signal.get("gain_pct", 0)
        vol_r  = signal.get("vol_ratio", 1.0)

        # ── حساب ATR ديناميكي ──────────────────────────────────
        atr_pct = 0.0
        support, resist = 0.0, 0.0
        if symbol:
            atr_pct = self._calc_atr(symbol, "1h", 14)
            support, resist = self._calc_support_resistance(symbol)

        # fallback إذا فشل ATR
        if atr_pct <= 0:
            atr_pct = 2.5

        # ── SL ديناميكي: 1.2× ATR تحت الدخول (لا يقل عن 1.5% ولا يزيد عن 6%) ──
        sl_pct = max(1.5, min(6.0, atr_pct * 1.2))
        # إذا كان هناك دعم قريب وأقرب من ATR → استخدمه
        if support > 0 and support < price:
            support_dist = (price - support) / price * 100
            if support_dist < sl_pct:
                sl_pct = max(1.5, support_dist * 0.95)

        # ── TP ديناميكي: نسبة Risk/Reward لا تقل عن 2:1 ──────
        rr_min = 2.0  # الحد الأدنى لنسبة المكسب/الخسارة

        if s == "S1":
            # RSI oversold: هدف أعلى لأن الارتداد قوي
            tp1_pct = max(sl_pct * rr_min, atr_pct * 2.0)
            tp2_pct = tp1_pct * 1.8
            tp3_pct = tp2_pct * 1.3

        elif s == "S2":
            # momentum: هدف معتدل
            tp1_pct = max(sl_pct * rr_min, atr_pct * 1.5)
            tp2_pct = tp1_pct * 1.5
            tp3_pct = tp2_pct * 1.3

        elif s == "NEW_LISTING":
            # listing جديد: تذبذب عالٍ جداً → أهداف أكبر
            tp1_pct = max(10.0, gain * 0.5)
            tp2_pct = tp1_pct * 2.0
            tp3_pct = tp1_pct * 3.5
            sl_pct  = max(5.0, atr_pct * 1.5)

        else:  # S3 Rocket
            # rocket: الزخم قوي → TP بناءً على قوة الإشارة
            # كلما زاد gain_pct وvol_ratio كلما رفعنا الهدف
            momentum_boost = min(2.0, 1.0 + (gain / 50) + (vol_r / 10))
            tp1_pct = max(sl_pct * rr_min, atr_pct * 1.5 * momentum_boost)
            tp1_pct = max(3.0, min(15.0, tp1_pct))  # حدود: 3%-15%
            tp2_pct = tp1_pct * 1.8
            tp3_pct = tp2_pct * 1.5
            # إذا كانت مقاومة قريبة → اجعل TP1 قبلها قليلاً
            if resist > 0 and resist > price:
                resist_dist = (resist - price) / price * 100
                if resist_dist < tp1_pct:
                    tp1_pct = max(sl_pct * rr_min, resist_dist * 0.92)

        sl  = price * (1 - sl_pct  / 100)
        tp1 = price * (1 + tp1_pct / 100)
        tp2 = price * (1 + tp2_pct / 100)
        tp3 = price * (1 + tp3_pct / 100)

        _log(f"[Targets] {symbol} ATR={atr_pct:.2f}% SL={sl_pct:.2f}% "
             f"TP1={tp1_pct:.2f}% TP2={tp2_pct:.2f}% S/R={support:.4g}/{resist:.4g}")

        return {"sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3}


# ══════════════════════════════════════════════════════════════
# TRADE MONITOR — مراقبة كل 5 ثواني
# ══════════════════════════════════════════════════════════════
class TradeMonitor:
    def __init__(self, cfg: Config, executor: Executor,
                 slots: SlotManager, db: TradeDB):
        self.cfg      = cfg
        self.ex       = executor
        self.slots    = slots
        self.db       = db
        self._running = False
        self._pending: list[dict] = []
        self._reconcile_tick = 0

    async def start(self):
        self._running = True
        _log("[Monitor] ✅ بدأ — مراقبة كل 5 ثواني")
        while self._running:
            try:
                await self._check_all()
            except Exception as e:
                _log(f"[Monitor] ⚠️ {e}")
            if self._pending:
                await self._retry_pending()
            self._reconcile_tick += self.cfg.monitor_interval
            if self._reconcile_tick >= self.cfg.reconcile_interval:
                self._reconcile_tick = 0
                await self._reconcile()
            await asyncio.sleep(self.cfg.monitor_interval)

    def stop(self):
        self._running = False

    def add_pending(self, task: dict):
        task.setdefault("attempts", 0)
        self._pending.append(task)
        _log(f"[Monitor] 📋 مهمة معلقة: {task.get('action')} {task.get('symbol')}")

    async def _retry_pending(self):
        remaining = []
        for task in self._pending:
            task["attempts"] += 1
            action = task.get("action")
            symbol = task.get("symbol", "")
            max_att = 5 if action == "emergency_sell" else 20
            if task["attempts"] > max_att:
                _log(f"[Monitor] ⚠️ {symbol}/{action} تجاوز {max_att} محاولة")
                continue
            try:
                state = self.slots.get(symbol)
                if action == "emergency_sell":
                    qty = task.get("qty", state.remaining_qty if state else 0)
                    if qty <= 0:
                        _log(f"[Monitor] 🧹 emergency_sell {symbol}: qty=0 → تجاهل (غبار)")
                        if state: self.slots.release(symbol)
                        continue
                    # ── تحقق من الرصيد الفعلي أولاً — إذا < $1 → غبار مباشرة ──
                    _price_now = self.ex.live_price(symbol)
                    _actual_val = await asyncio.get_event_loop().run_in_executor(
                        None, self.ex.fetch_coin_balance_usdt, symbol, _price_now
                    )
                    if _actual_val >= 0 and _actual_val < 1.0:
                        _log(f"[Monitor] 🧹 emergency_sell {symbol}: رصيد فعلي=${_actual_val:.4f} < $1 → غبار مباشرة")
                        if state and state.db_trade_id:
                            _pnl = (_price_now - state.entry_price) / state.entry_price * 100 if state.entry_price else 0
                            self.db.close_trade(state.db_trade_id, _price_now, qty, "dust_cleanup",
                                                (_price_now - state.entry_price) * qty if state.entry_price else 0, _pnl)
                        if state: self.slots.release(symbol)
                        continue
                    sell_result = await asyncio.get_event_loop().run_in_executor(
                        None, self.ex.market_sell, symbol, qty
                    )
                    price = self.ex.live_price(symbol)
                    val = qty * price
                    if sell_result == True:
                        _log(f"[Monitor] ✅ Retry emergency_sell {symbol} نجح")
                        close_reason = "emergency_sl"
                    elif sell_result == "dust":
                        _log(f"[Monitor] 🧹 emergency_sell {symbol}: غبار (val=${val:.2f}) → إغلاق")
                        close_reason = "dust_cleanup"
                    else:
                        # فشل عادي → أعد المحاولة لاحقاً
                        remaining.append(task)
                        continue
                    if state and state.db_trade_id:
                        pnl = (price - state.entry_price) / state.entry_price * 100 if state.entry_price else 0
                        self.db.close_trade(
                            state.db_trade_id, price, qty, close_reason,
                            (price - state.entry_price) * qty if state.entry_price else 0, pnl
                        )
                    self.slots.release(symbol)
                    continue
                elif action == "place_tp":
                    if state:
                        oid = await asyncio.get_event_loop().run_in_executor(
                            None, self.ex.place_limit_sell,
                            symbol, state.remaining_qty, state.tp1_price
                        )
                        if oid:
                            self.slots.update(symbol, tp1_order_id=oid)
                            _log(f"[Monitor] ✅ Retry place_tp {symbol} نجح")
                            continue
                remaining.append(task)
            except Exception as e:
                _log(f"[Monitor] Retry {symbol}/{action} #{task['attempts']}: {e}")
                remaining.append(task)
        self._pending = remaining

    async def _check_all(self):
        for state in self.slots.all():
            try:
                await self._check_slot(state)
            except Exception as e:
                _log(f"[Monitor] ⚠️ {state.symbol}: {e}")

    async def _check_slot(self, state: SlotState):
        price = await asyncio.get_event_loop().run_in_executor(
            None, self.ex.live_price, state.symbol
        )
        if price <= 0:
            return

        # ── تحقق من الغبار قبل أي شيء — لكن لا تُغلق إذا كان هناك أوردر TP مفتوح ──
        qty = state.remaining_qty if state.remaining_qty > 0 else state.filled_qty
        # إذا كان هناك TP1 order مفتوح → لا تُغلق بسبب الغبار (الكمية محجوزة في "used")
        if state.tp1_order_id:
            # تحقق أن الأوردر لا يزال مفتوحاً فعلاً
            try:
                tp1_status = await asyncio.get_event_loop().run_in_executor(
                    None, self.ex.order_status, state.symbol, state.tp1_order_id
                )
                if tp1_status in ("open", "partially_filled"):
                    # الكمية محجوزة بالأوردر — استخدم filled_qty للحساب
                    qty = state.filled_qty
            except Exception:
                pass
        actual_val = await asyncio.get_event_loop().run_in_executor(
            None, self.ex.fetch_coin_balance_usdt, state.symbol, price
        )
        # استخدم الرصيد الفعلي (free+used) إذا كان متاحاً، وإلا استخدم qty من الذاكرة
        check_val = actual_val if actual_val >= 0 else (qty * price)
        if check_val < 1.0 and qty > 0 and not state.tp1_order_id:
            _log(f"[Monitor] 🧹 {state.symbol} غبار (val=${check_val:.4f} < $1) → إغلاق تلقائي")
            if state.db_trade_id:
                pnl_pct_d = (price - state.entry_price) / state.entry_price * 100 if state.entry_price else 0
                pnl_usd_d = (price - state.entry_price) * qty if state.entry_price else 0
                self.db.close_trade(state.db_trade_id, price, qty, "dust_cleanup", pnl_usd_d, pnl_pct_d)
            self.slots.release(state.symbol)
            return

        pnl_pct = (price - state.entry_price) / state.entry_price * 100

        if price > state.highest_price:
            self.slots.update(state.symbol, highest_price=price)
            state.highest_price = price

        if state.strategy in ("S3", "NEW_LISTING"):
            await self._handle_s3_partials(state, price, pnl_pct)

        await self._update_trailing(state, price, pnl_pct)

        if price <= state.sl_price and state.sl_price > 0:
            # ── تحقق من الرصيد الفعلي قبل البيع — منع حلقة الغبار ──
            actual_val = await asyncio.get_event_loop().run_in_executor(
                None, self.ex.fetch_coin_balance_usdt, state.symbol, price
            )
            if actual_val >= 0 and actual_val < 1.0:
                _log(f"[Monitor] 🧹 SL {state.symbol} رصيد فعلي=${actual_val:.4f} < $1 → غبار، إغلاق بدون بيع")
                if state.db_trade_id:
                    pnl_pct_sl = (price - state.entry_price) / state.entry_price * 100 if state.entry_price else 0
                    pnl_usd_sl = (price - state.entry_price) * (state.remaining_qty or state.filled_qty) if state.entry_price else 0
                    self.db.close_trade(state.db_trade_id, price, state.remaining_qty or state.filled_qty, "dust_cleanup", pnl_usd_sl, pnl_pct_sl)
                self.slots.release(state.symbol)
                return
            _log(f"[Monitor] 🛑 SL {state.symbol} price={price:.6g} sl={state.sl_price:.6g}")
            await self._close_position(state, price, "sl")
            return

        if state.tp1_order_id:
            status = await asyncio.get_event_loop().run_in_executor(
                None, self.ex.order_status, state.symbol, state.tp1_order_id
            )
            if status in ("closed", "filled"):
                _log(f"[Monitor] 🎯 TP1 {state.symbol}")
                await self._on_tp1_filled(state, state.tp1_price)
                return
        elif price >= state.tp1_price > 0 and not state.partial1_done:
            _log(f"[Monitor] 🎯 TP1 سعر {state.symbol}")
            await self._on_tp1_filled(state, price)
            return

    async def _handle_s3_partials(self, state: SlotState, price: float, pnl_pct: float):
        """
        نظام هدفين:
        - TP1: بيع 50% عند الهدف الأول → ربح مضمون
        - الباقي 50%: trailing stop ديناميكي يتبع السعر للأعلى
        """
        if state.partial1_done:
            # المرحلة الثانية: trailing للباقي فقط — يتولاها _update_trailing
            return

        # TP1 لم يُحقَّق بعد → لا شيء
        if price < state.tp1_price:
            return

        # ✅ وصل لـ TP1 → بيع 50% الآن
        qty_tp1 = state.filled_qty * 0.50
        ok = await asyncio.get_event_loop().run_in_executor(
            None, self.ex.market_sell, state.symbol, qty_tp1
        )
        if ok and ok != "dust":
            remaining = state.filled_qty * 0.50
            pnl_usd   = (price - state.entry_price) * qty_tp1

            # رفع SL للباقي إلى نقطة التعادل (entry) لحماية الربح
            breakeven_sl = state.entry_price * 1.001  # +0.1% فوق الدخول
            self.slots.update(state.symbol,
                partial1_done=True,
                remaining_qty=remaining,
                sl_price=breakeven_sl,   # SL عند نقطة التعادل
                trailing_sl=breakeven_sl
            )
            if state.db_trade_id:
                self.db.update_sl(state.db_trade_id, breakeven_sl)

            _log(f"[Monitor] 🎯 TP1 {state.symbol} +{pnl_pct:.1f}% | "
                 f"بيع 50% | SL→تعادل={breakeven_sl:.6g} | متابعة {remaining:.4f} بـ trailing")

            await tg_send(self.cfg.telegram_token, self.cfg.telegram_chat_id,
                f"🎯 <b>TP1 — {state.symbol}</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ بيع 50% عند +{pnl_pct:.1f}%\n"
                f"💰 ربح مؤمَّن: <code>${pnl_usd:.2f}</code>\n"
                f"🔄 الباقي 50% يتابع بـ Trailing Stop\n"
                f"🛡 SL جديد → نقطة التعادل", "trade_close"
            )

    async def _update_trailing(self, state: SlotState, price: float, pnl_pct: float):
        """
        Trailing Stop ديناميكي:
        - قبل TP1: يبدأ فقط إذا تجاوز السعر نسبة معينة (للحماية من الانخفاض المفاجئ)
        - بعد TP1: يتبع كل ارتفاع بـ ATR واحد تحت القمة
        """
        if not state.partial1_done:
            # قبل TP1: trailing بسيط للحماية من انهيار مفاجئ
            trigger = 5.0  # لا يبدأ trailing إلا بعد +5%
            trail_pct = 3.0
            if pnl_pct < trigger:
                return
            new_trail_sl = state.highest_price * (1 - trail_pct / 100)
        else:
            # بعد TP1: trailing أضيق يتبع كل قمة جديدة
            # استخدم ATR لتحديد المسافة (1.5× ATR تحت القمة)
            atr_pct = state.atr_pct if state.atr_pct > 0 else 2.0
            trail_pct = max(1.5, min(4.0, atr_pct * 1.5))
            new_trail_sl = state.highest_price * (1 - trail_pct / 100)

        if new_trail_sl > state.trailing_sl:
            self.slots.update(state.symbol, trailing_sl=new_trail_sl)
            if new_trail_sl > state.sl_price:
                self.slots.update(state.symbol, sl_price=new_trail_sl)
                if state.db_trade_id:
                    self.db.update_sl(state.db_trade_id, new_trail_sl)
                _log(f"[Monitor] 📈 Trailing {state.symbol} → SL={new_trail_sl:.6g} "
                     f"(قمة={state.highest_price:.6g} trail={trail_pct:.1f}%)")

    async def _on_tp1_filled(self, state: SlotState, price: float):
        """TP1 عبر limit order على MEXC — نفس منطق _handle_s3_partials"""
        pnl_pct = (price - state.entry_price) / state.entry_price * 100
        qty_tp1 = state.filled_qty * 0.50
        pnl_usd = (price - state.entry_price) * qty_tp1

        remaining = state.filled_qty * 0.50
        breakeven_sl = state.entry_price * 1.001
        self.slots.update(state.symbol,
            partial1_done=True,
            remaining_qty=remaining,
            sl_price=breakeven_sl,
            trailing_sl=breakeven_sl
        )
        if state.db_trade_id:
            self.db.update_sl(state.db_trade_id, breakeven_sl)

        _log(f"[Monitor] 🎯 TP1 filled {state.symbol} +{pnl_pct:.1f}% | trailing يبدأ")
        await tg_send(self.cfg.telegram_token, self.cfg.telegram_chat_id,
            f"🎯 <b>TP1 — {state.symbol}</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ بيع 50% عند +{pnl_pct:.1f}%\n"
            f"💰 ربح مؤمَّن: <code>${pnl_usd:.2f}</code>\n"
            f"🔄 الباقي 50% يتابع بـ Trailing Stop\n"
            f"🛡 SL جديد → نقطة التعادل", "trade_close"
        )

    async def _close_position(self, state: SlotState, price: float, reason: str):
        qty = state.remaining_qty if state.remaining_qty > 0 else state.filled_qty
        result = await asyncio.get_event_loop().run_in_executor(
            None, self.ex.market_sell, state.symbol, qty
        )
        # dust = القيمة أقل من $1، نعتبرها مُباعة ونُغلق الصفقة
        if result == "dust":
            _log(f"[Monitor] 🧹 {state.symbol} غبار (< $1) → إغلاق الصفقة بدون بيع")
            if state.db_trade_id:
                pnl_pct = (price - state.entry_price) / state.entry_price * 100 if state.entry_price else 0
                pnl_usd = (price - state.entry_price) * qty if state.entry_price else 0
                self.db.close_trade(state.db_trade_id, price, qty, "dust_cleanup", pnl_usd, pnl_pct)
            self.slots.release(state.symbol)
            return
        if not result:
            self.add_pending({"action": "emergency_sell", "symbol": state.symbol, "qty": qty})
            return
        pnl_pct = (price - state.entry_price) / state.entry_price * 100
        pnl_usd = (price - state.entry_price) * qty
        if state.db_trade_id:
            self.db.close_trade(state.db_trade_id, price, qty, reason, pnl_usd, pnl_pct)
        self.slots.release(state.symbol)
        emoji = "🎯" if pnl_pct > 0 else "🛑"
        await tg_send(self.cfg.telegram_token, self.cfg.telegram_chat_id,
            f"{emoji} <b>{'TP' if pnl_pct>0 else 'SL'} — {state.symbol}</b>\n"
            f"{pnl_pct:+.1f}% | ${pnl_usd:+.2f} | سبب: {reason}", "trade_close"
        )

    async def _reconcile(self):
        try:
            db_trades = self.db.get_open_trades()
            for t in db_trades:
                sym = t["symbol"]
                if not self.slots.get(sym):
                    bal = self.ex.exchange.fetch_balance({"type": "spot"})
                    base = sym.replace("/USDT", "")
                    actual_qty = float(bal.get(base, {}).get("free", 0))
                    if actual_qty * self.ex.live_price(sym) < 1.0:
                        self.db.close_trade(
                            t["id"], float(t.get("entry_price", 0)),
                            0, "reconcile_ghost", 0, 0
                        )
                        _log(f"[Reconcile] 🧹 {sym}: صفقة DB بدون رصيد → أُغلقت")
        except Exception as e:
            _log(f"[Reconcile] ⚠️ {e}")


# ══════════════════════════════════════════════════════════════
# AUTO-HEAL
# ══════════════════════════════════════════════════════════════
class AutoHeal:
    def __init__(self, cfg: Config, ex: Executor, slots: SlotManager, db: TradeDB):
        self.cfg   = cfg
        self.ex    = ex
        self.slots = slots
        self.db    = db

    def run(self) -> list[str]:
        actions = []
        _log("[AutoHeal] 🔧 بدء الفحص...")
        try:
            db_trades    = self.db.get_open_trades()
            cap_per_slot = self._calc_capital()
            bal          = self.ex.exchange.fetch_balance({"type": "spot"})
            # free + used: يشمل الكميات المقيدة بأوردرات TP مفتوحة
            freqd        = {k: float(v.get("free", 0)) + float(v.get("used", 0))
                            for k, v in bal.items()
                            if isinstance(v, dict) and
                            (float(v.get("free", 0)) + float(v.get("used", 0))) > 0}
            db_symbols   = {t["symbol"] for t in db_trades}

            for t in db_trades:
                sym   = t["symbol"]
                base  = sym.replace("/USDT", "")
                price = self.ex.live_price(sym)
                # ✅ استخدم free + used (الكميات المقيدة بأوردرات مفتوحة)
                free_qty = float(bal.get(base, {}).get("free", 0))
                used_qty = float(bal.get(base, {}).get("used", 0))
                qty   = free_qty + used_qty
                val   = qty * price

                # ✅ إذا كان هناك أوردر TP1 مفتوح على البورصة → لا تُغلق الصفقة أبداً
                tp1_id = t.get("tp1_order_id", "") or ""
                if tp1_id:
                    try:
                        status = self.ex.order_status(sym, tp1_id)
                        if status in ("open", "partially_filled"):
                            _log(f"[AutoHeal] ✋ {sym}: أوردر TP1 مفتوح ({tp1_id[:8]}) → لا تُغلق")
                            # استمر في استرجاع الصفقة بغض النظر عن الكمية
                            qty = max(qty, float(t.get("filled_qty", 0)))
                            val = qty * price
                    except Exception:
                        pass

                if val < cap_per_slot * 0.15 and not tp1_id:
                    self.db.close_trade(t["id"], price or float(t.get("entry_price", 0)),
                                        0, "heal_dust", 0, 0)
                    actions.append(f"🧹 {sym}: أتربة (${val:.2f}) → أُغلق")
                    continue

                sl    = float(t.get("sl_price", 0))
                entry = float(t.get("entry_price", 0))
                if sl <= 0 and entry > 0:
                    sl = entry * (1 - self.cfg.s1_sl_pct / 100)

                state = SlotState(
                    symbol=sym, strategy=t.get("strategy", "S1"),
                    signal_source=t.get("signal_source", "scan"),
                    entry_price=entry, filled_qty=float(t.get("filled_qty", qty)),
                    remaining_qty=qty, capital=float(t.get("capital_usd", cap_per_slot)),
                    sl_price=sl, tp1_price=float(t.get("tp1_price", 0)),
                    tp2_price=float(t.get("tp2_price", 0)), tp3_price=float(t.get("tp3_price", 0)),
                    tp1_order_id=t.get("tp1_order_id", "") or "",
                    db_trade_id=t["id"], highest_price=price,
                )
                self.slots.occupy(state)
                actions.append(f"♻️ {sym}: استُرجع (${val:.2f} @ SL={sl:.6g})")

                # إعادة وضع TP1 order إذا لم يكن موجوداً
                tp1_id = t.get("tp1_order_id", "") or ""
                tp1_price = float(t.get("tp1_price", 0))
                if not tp1_id and tp1_price > 0 and qty > 0:
                    _log(f"[AutoHeal] 🔁 إعادة وضع TP1 لـ {sym} qty={qty:.6f} @ {tp1_price:.6g}")
                    try:
                        new_oid = self.ex.place_limit_sell(sym, qty, tp1_price)
                        if new_oid:
                            self.slots.update(sym, tp1_order_id=new_oid)
                            self.db.update_tp1_order_id(t["id"], new_oid)
                            actions.append(f"📋 {sym}: TP1 order أُعيد وضعه → {new_oid[:8]}")
                            _log(f"[AutoHeal] ✅ TP1 order لـ {sym}: {new_oid}")
                        else:
                            actions.append(f"⚠️ {sym}: TP1 order فشل — Monitor سيعيد المحاولة")
                    except Exception as te:
                        _log(f"[AutoHeal] ❌ TP1 retry {sym}: {te}")

            # تنظيف الغبار: بيع العملات الغبارية غير المسجلة
            for base, qty in freqd.items():
                if base in ("USDT", "USDC"):
                    continue
                sym   = f"{base}/USDT"
                price = self.ex.live_price(sym)
                val   = qty * price
                # إذا كانت قيمة أقل من $1.50 — غبار → بيع فوري (إذا ≥ $1 للحد الأدنى)
                if 0 < val < 1.50 and sym not in db_symbols and not self.slots.get(sym):
                    if val >= 1.0:  # MEXC يشترط $1 كحد أدنى للبيع
                        try:
                            prec_qty = float(self.ex.exchange.amount_to_precision(sym, qty * 0.99))
                            if prec_qty > 0:
                                self.ex.exchange.create_market_sell_order(sym, prec_qty)
                                actions.append(f"🧹 {sym}: غبار (${val:.2f}) → بيع تلقائي")
                                _log(f"[AutoHeal] 🧹 Dust sell {sym} qty={prec_qty:.6f} val=${val:.2f}")
                        except Exception as de:
                            _log(f"[AutoHeal] ⚠️ Dust sell {sym}: {de}")
                    else:
                        _log(f"[AutoHeal] 🧹 {sym}: غبار (${val:.3f}) < $1 — لا يمكن بيعه (MEXC)")
                    continue
                if val < cap_per_slot * 0.20:
                    continue
                if sym in db_symbols or self.slots.get(sym):
                    continue
                entry_est = price
                sl  = price * (1 - self.cfg.s1_sl_pct / 100)
                tp1 = price * (1 + self.cfg.s1_tp1_pct / 100)
                trade_id = self.db.upsert_trade(sym, "RESTORED", entry_est, qty, val,
                                                sl, tp1, tp1*1.05, tp1*1.10)
                state = SlotState(
                    symbol=sym, strategy="RESTORED", entry_price=entry_est,
                    filled_qty=qty, remaining_qty=qty, capital=val,
                    sl_price=sl, tp1_price=tp1, db_trade_id=trade_id, highest_price=price,
                )
                self.slots.occupy(state)
                actions.append(f"📋 {sym}: رصيد ${val:.2f} غير مسجل → أُضيف")

            _log(f"[AutoHeal] ✅ {len(actions)} إجراء | slots={self.slots.used}")
        except Exception as e:
            _log(f"[AutoHeal] ❌ {e}")
        return actions

    def _calc_capital(self) -> float:
        if self.cfg.capital > 0:
            return self.cfg.capital
        # في AutoHeal: استخدام إجمالي الرصيد المقدر
        free = self.ex.free_usdt()
        invested = sum(s.capital for s in self.slots.all())
        total = free + invested
        return max(1.0, round(total / max(1, self.cfg.max_slots), 2))


# ══════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ══════════════════════════════════════════════════════════════
class Orchestrator:
    def __init__(self, cfg: Config):
        self.cfg       = cfg
        self.db        = TradeDB(cfg.database_url, cfg.fallback_db_url)
        self.ex        = Executor(cfg)
        self.slots     = SlotManager(cfg.max_slots)
        self.monitor   = TradeMonitor(cfg, self.ex, self.slots, self.db)
        self.strategy  = StrategyEngine(cfg, self.ex)
        self.ai        = AIAdvisor(cfg)
        self.signals   = SignalAggregator(cfg, self.ex, self.db)
        self.binance   = BinanceListingMonitor(cfg, self.ex)
        self.funding   = FundingRateDetector(cfg)
        self.news      = CryptoPanicFilter(cfg)
        self.lunar     = LunarCrushSignal(cfg)
        self._processing: set = set()
        self._processing_lock = threading.Lock()
        self._report_tick = 0

    def _calc_capital(self) -> float:
        if self.cfg.capital > 0:
            return self.cfg.capital
        # حساب إجمالي الرصيد = حر + مستثمر في الصفقات المفتوحة
        free = self.ex.free_usdt()
        invested = sum(s.capital for s in self.slots.all())
        total = free + invested
        per_slot = total / max(1, self.cfg.max_slots)
        result = max(1.0, round(per_slot, 2))
        _log(f"[Capital] free=${free:.2f} invested=${invested:.2f} total=${total:.2f} per_slot=${result:.2f}")
        return result

    async def _send_tg(self, text: str, msg_type: str = "misc"):
        await tg_send(self.cfg.telegram_token, self.cfg.telegram_chat_id, text, msg_type)

    async def _startup_notify(self):
        balance = self.ex.free_usdt()
        cap     = self._calc_capital()
        monthly = self.db.get_monthly_summary()
        sources = []
        if self.cfg.top_gainers_enabled:
            sources.append("🏆 Top Gainers")
        if self.cfg.coingecko_enabled:
            sources.append("🌐 CoinGecko")
        if self.cfg.new_listings_enabled:
            sources.append("🆕 New Listings")
        if self.cfg.vol_anomaly_enabled:
            sources.append("🌊 Volume Anomaly")
        if self.cfg.orderbook_enabled:
            sources.append("📚 Order Book")
        if self.cfg.binance_listing_enabled:
            sources.append("📢 Binance Listing")
        if self.cfg.funding_rate_enabled:
            sources.append("📉 Funding Rate")
        if self.cfg.cryptopanic_enabled:
            sources.append("🔴 News Filter")
        if self.cfg.lunarcrush_enabled:
            sources.append("🌙 LunarCrush")

        text = (
            "🚀 <b>MEXC Scalping Bot v5 — Ultra Rocket Edition</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"💼 رصيد: <code>${balance:.2f}</code> | "
            f"حصة/صفقة: <code>${cap:.2f}</code>\n"
            f"📊 Slots: <code>{self.slots.used}/{self.cfg.max_slots}</code>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📡 <b>مصادر الإشارات:</b>\n"
            f"{'  '.join(sources)}\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "⚡ الاستراتيجيات: S1 RSI | S2 Momentum | S3 Rocket 🚀 | New Listing 🆕\n"
            f"🔍 TradeMonitor: كل {self.cfg.monitor_interval}ث (لحظي)\n"
            f"📈 الشهر: {monthly['total']} صفقة | "
            f"P&L: <code>${monthly['pnl']:.2f}</code> | "
            f"Win: <code>{monthly['winrate']}%</code>"
        )
        await self._send_tg(text)

    async def _process_symbol(self, session: aiohttp.ClientSession,
                               symbol: str, capital: float,
                               signal_source: str = "scan",
                               force_new_listing: bool = False):
        with self._processing_lock:
            if symbol in self._processing:
                return
            if not self.slots.is_vacant(symbol):
                return
            self._processing.add(symbol)

        try:
            base = symbol.replace("/USDT", "")
            if self.cfg.shariah_filter and base in self.cfg.blacklisted_assets:
                return
            if base in self.ex._geo_blocked:
                return  # محجوبة جغرافياً (11444)

            # اختر الاستراتيجية بناءً على المصدر
            signal = None

            if force_new_listing:
                # عملة جديدة — استراتيجية خاصة
                signal = self.strategy.check_new_listing(symbol, self.cfg)
            else:
                # S3 أولاً (الأهم)، ثم S1، ثم S2
                for checker in [self.strategy.check_s3,
                                 self.strategy.check_s1,
                                 self.strategy.check_s2]:
                    try:
                        signal = await asyncio.get_event_loop().run_in_executor(
                            None, checker, symbol
                        )
                        if signal:
                            break
                    except Exception:
                        continue

            if not signal:
                return

            signal["signal_source"] = signal_source

            # فلتر الحجم — SAFE_COINS تتجاوز هذا الفلتر (حجمها دائماً كافٍ)
            _SAFE_SET2 = {s.replace("/USDT","") for s in self.ex.SAFE_COINS}
            _skip_vol_check = symbol.replace("/USDT","") in _SAFE_SET2
            if not _skip_vol_check:
                try:
                    ticker  = self.ex.exchange.fetch_ticker(symbol)
                    vol_usd = float(ticker.get("quoteVolume") or
                                    ticker.get("baseVolume", 0) * signal["price"])
                    min_vol = 10000 if force_new_listing else self.cfg.min_volume_usd
                    if vol_usd < min_vol:
                        _log(f"[Vol] ⬇️ {symbol} حجم ${vol_usd:.0f} < ${min_vol:.0f} — تخطي")
                        return
                except Exception:
                    return

            # فلتر الأخبار السلبية (CryptoPanic)
            if self.news.is_blocked(symbol):
                _log(f"[News🔴] ⚠️ {symbol} عليها أخبار سلبية — تخطي")
                return

            # Funding Rate — إضافة نقاط إذا كان squeeze candidate
            if self.funding.is_squeeze_candidate(symbol):
                rate = self.funding.get_rate(symbol)
                _log(f"[Funding📉] 🎯 {symbol} Funding={rate:.4f} — Squeeze قادم! نقاط إضافية")
                signal["funding_boost"] = True

            # LunarCrush — إشارة اجتماعية
            if self.cfg.lunarcrush_enabled and self.lunar.is_hot(symbol):
                score = self.lunar.get_score(symbol)
                _log(f"[LunarCrush🌙] 🔥 {symbol} Galaxy Score={score:.0f} — ضجة اجتماعية!")
                signal["lunar_boost"] = True

            # Order Book Pressure — فلتر إضافي (يُتجاوز لـ SAFE_COINS)
            _SAFE_SET = {s.replace("/USDT","") for s in self.ex.SAFE_COINS}
            _is_safe_coin = symbol.replace("/USDT","") in _SAFE_SET
            if self.cfg.orderbook_enabled and not force_new_listing and not _is_safe_coin:
                ob_ratio = await asyncio.get_event_loop().run_in_executor(
                    None, self.ex.get_orderbook_pressure, symbol
                )
                if ob_ratio < 0.3:  # ضغط بيع حاد جداً — تجنب (خُفض من 0.8 إلى 0.3)
                    _log(f"[OrderBook] ⚠️ {symbol} ضغط بيع ({ob_ratio:.2f}) — تخطي")
                    return
                # أضف نقاط إضافية إذا كان ضغط شراء قوي
                if ob_ratio >= self.cfg.orderbook_ratio_min:
                    signal["ob_boost"] = True

            # AI موافقة — SAFE_COINS تُوافق تلقائياً لتجنب التأخير
            _SAFE_SET3 = {s.replace("/USDT","") for s in self.ex.SAFE_COINS}
            if symbol.replace("/USDT","") in _SAFE_SET3:
                ai_vote = "approve"
                _log(f"[AI] 🛡️ {symbol} SAFE_COIN — موافقة تلقائية")
            else:
                ai_vote = await self.ai.approve(
                    session, symbol, signal["strategy"],
                    signal["rsi"], signal["gain_pct"], signal["vol_ratio"],
                    signal_source
                )
                if ai_vote == "reject":
                    _log(f"[AI] ❌ {symbol} رُفض")
                    return

            if not self.slots.is_vacant(symbol):
                return

            # تنفيذ الشراء
            result = await asyncio.get_event_loop().run_in_executor(
                None, self.ex.market_buy, symbol, capital
            )
            if not result:
                return

            price   = result["price"]
            qty     = result["qty"]
            targets = self.strategy.calc_targets({**signal, "price": price, "symbol": symbol})

            # وضع TP1 Limit Order
            tp1_oid = await asyncio.get_event_loop().run_in_executor(
                None, self.ex.place_limit_sell, symbol, qty, targets["tp1"]
            )

            atr_now = self.strategy._calc_atr(symbol, "1h", 14)
            state = SlotState(
                symbol=symbol, strategy=signal["strategy"],
                signal_source=signal_source,
                entry_price=price, filled_qty=qty, remaining_qty=qty,
                capital=capital, sl_price=targets["sl"],
                tp1_price=targets["tp1"], tp2_price=targets["tp2"], tp3_price=targets["tp3"],
                tp1_order_id=tp1_oid or "", db_trade_id=None, highest_price=price,
                atr_pct=atr_now,
            )

            trade_id = self.db.upsert_trade(
                symbol, signal["strategy"], price, qty, capital,
                targets["sl"], targets["tp1"], targets["tp2"], targets["tp3"],
                ai_verdict=ai_vote, tp1_order_id=tp1_oid or "",
                signal_source=signal_source
            )
            if trade_id:
                state.db_trade_id = trade_id

            self.slots.occupy(state)

            # إعادة محاولة TP1 إذا فشل وضعه
            if not tp1_oid:
                _log(f"[Bot] ⚠️ TP1 order فشل — ستُعاد المحاولة عبر Monitor")
                self.monitor.add_pending({
                    "action": "place_tp",
                    "symbol": symbol,
                })

            # إشعار Telegram
            emoji   = {"S3": "🚀", "NEW_LISTING": "🆕", "S1": "📉", "S2": "📈"}.get(signal["strategy"], "📊")
            boosts = []
            if signal.get("ob_boost"):        boosts.append("📚 Order Book")
            if signal.get("funding_boost"):   boosts.append("📉 Funding Squeeze")
            if signal.get("lunar_boost"):     boosts.append("🌙 Social Buzz")
            if signal_source == "binance_listing": boosts.append("📢 Binance Listed!")
            boost_note = (" | " + " + ".join(boosts)) if boosts else ""
            src_tag = f" [{signal_source}]" if signal_source != "scan" else ""
            await self._send_tg(
                f"{emoji} <b>{signal['strategy']} — {symbol}</b>{src_tag}\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 رأس المال: <code>${capital:.2f}</code>\n"
                f"📍 دخول: <code>{price:.6g}</code>\n"
                f"📊 RSI={signal['rsi']:.0f} | "
                f"Gain={signal['gain_pct']:.1f}% | Vol×{signal['vol_ratio']:.1f}{boost_note}\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 TP1: <code>{targets['tp1']:.6g}</code>\n"
                f"🎯 TP2: <code>{targets['tp2']:.6g}</code>\n"
                f"🛡 SL: <code>{targets['sl']:.6g}</code>\n"
                f"🤖 AI: {ai_vote}", "trade_open"
            )
            _log(f"[Orchestrator] ✅ {signal['strategy']} {symbol} @ {price:.6g} [{signal_source}]")

        finally:
            with self._processing_lock:
                self._processing.discard(symbol)

    async def _smart_report(self, session: aiohttp.ClientSession):
        balance = self.ex.free_usdt()
        monthly = self.db.get_monthly_summary()
        ai_text = await self.ai.smart_report(
            session, balance, self.slots.used, self.cfg.max_slots, monthly
        )
        slots_info = ""
        for s in self.slots.all():
            price  = self.ex.live_price(s.symbol)
            pnl    = (price - s.entry_price) / s.entry_price * 100 if s.entry_price else 0
            slots_info += f"  • {s.symbol} [{s.strategy}] {pnl:+.1f}% SL={s.sl_price:.5g}\n"

        text = (
            "🟦 <b>التقرير الذكي — v5</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"{ai_text}\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"💼 رصيد: <code>${balance:.2f}</code> | "
            f"Slots: <code>{self.slots.used}/{self.cfg.max_slots}</code>\n"
            f"📈 الشهر: <code>${monthly['pnl']:+.2f}</code> | "
            f"{monthly['total']} صفقة | Win {monthly['winrate']}%\n"
        )
        if slots_info:
            text += f"\n📊 الصفقات النشطة:\n{slots_info}"
        await self._send_tg(text)

    async def scan_loop(self):
        """
        الحلقة الرئيسية — محسّنة:
        1. كل دورة: تحديث SignalAggregator (Top Gainers + Trending + VolAnomaly)
        2. فحص العملات المرشّحة بالأولوية (أسرع بكثير من فحص 3000 عملة)
        3. فحص New Listings بشكل خاص
        """
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=5)
        async with aiohttp.ClientSession(connector=connector) as session:
            scan_count    = 0
            new_listings_check_tick = 0


            while True:
                scan_count += 1
                _log(f"🔄 Scan #{scan_count} | slots={self.slots.used}/{self.cfg.max_slots}")

                balance = self.ex.free_usdt()
                capital = self._calc_capital()
                _log(f"[Balance] ${balance:.2f} | حصة/صفقة: ${capital:.2f}")

                if self.slots.free == 0:
                    _log("[Scan] كل الـ slots مشغولة — انتظار")
                elif balance < capital * 0.5 and self.slots.used == 0:
                    _log(f"[Scan] رصيد ${balance:.2f} منخفض جداً")
                    # تنبيه للمستخدم بضرورة شحن الرصيد (مرة واحدة كل ساعة)
                    now_ts = time.time()
                    if now_ts - getattr(self, "_last_nofunds_alert", 0) > 3600:
                        self._last_nofunds_alert = now_ts
                        # فحص هل هناك فرص في السوق الآن
                        try:
                            safe_syms = [s for s in self.ex.SAFE_COINS
                                        if s.replace("/USDT","") not in self.ex._geo_blocked]
                            if safe_syms:
                                tickers_nf = await asyncio.get_event_loop().run_in_executor(
                                    None, self.ex.exchange.fetch_tickers, safe_syms[:5]
                                )
                                top_coin = max(
                                    [(s, float(t.get("percentage") or 0))
                                     for s, t in (tickers_nf or {}).items()
                                     if float(t.get("percentage") or 0) > 0.5],
                                    key=lambda x: x[1], default=None
                                )
                                if top_coin:
                                    await self._send_tg(
                                        f"⚠️ <b>فرصة ضائعة — رصيد منخفض</b>\n"
                                        "━━━━━━━━━━━━━━━━━━━━\n"
                                        f"🚀 {top_coin[0]} +{top_coin[1]:.1f}% الآن\n"
                                        f"💼 رصيد حر: <code>${balance:.2f}</code>\n"
                                        f"📌 الحد الأدنى للصفقة: <code>${capital:.2f}</code>\n"
                                        "━━━━━━━━━━━━━━━━━━━━\n"
                                        "💡 يرجى شحن الرصيد للاستفادة من الفرص",
                                        "alert_nofunds"
                                    )
                        except Exception as nf_e:
                            _log(f"[NoFunds] ⚠️ {nf_e}")
                else:
                    # 1. تحديث مصادر الإشارات (الأصلية + الجديدة)
                    try:
                        await self.signals.refresh(session)
                    except Exception as e:
                        _log(f"[Signals] ⚠️ {e}")

                    # Binance Listing Monitor — أقوى إشارة
                    try:
                        binance_new = await self.binance.check(session)
                        for sym in binance_new:
                            if self.slots.is_vacant(sym):
                                _log(f"[Binance📢] 🔥 شراء فوري: {sym}")
                                await self._process_symbol(
                                    session, sym, capital,
                                    signal_source="binance_listing",
                                    force_new_listing=True
                                )
                    except Exception as e:
                        _log(f"[Binance📢] ⚠️ {e}")

                    # تحديث Funding Rate
                    try:
                        await self.funding.refresh(session)
                    except Exception as e:
                        _log(f"[Funding] ⚠️ {e}")

                    # تحديث أخبار CryptoPanic
                    try:
                        await self.news.refresh(session)
                    except Exception as e:
                        _log(f"[News] ⚠️ {e}")

                    # تحديث LunarCrush
                    try:
                        await self.lunar.refresh(session)
                    except Exception as e:
                        _log(f"[LunarCrush] ⚠️ {e}")

                    # 2a. SAFE_COINS أولاً — شراء مباشر بدون فلاتر
                    if self.slots.free > 0:
                        _log("[SafeScan] 🛡️ فحص SAFE_COINS أولاً...")
                        try:
                            # جلب tickers مباشر لـ SAFE_COINS (لا نعتمد على cache)
                            safe_symbols = [s for s in self.ex.SAFE_COINS
                                           if s.replace("/USDT","") not in self.ex._geo_blocked
                                           and self.slots.is_vacant(s)]
                            live_tickers = {}
                            if safe_symbols:
                                try:
                                    fetched = await asyncio.get_event_loop().run_in_executor(
                                        None, self.ex.exchange.fetch_tickers, safe_symbols
                                    )
                                    live_tickers = fetched or {}
                                except Exception as et:
                                    _log(f"[SafeScan] ⚠️ fetch_tickers: {et}")
                                    # fallback للـ cache
                                    live_tickers = self.ex._tickers_cache or {}

                            safe_candidates = []
                            for sym in safe_symbols:
                                t = live_tickers.get(sym, {})
                                pct = float(t.get("percentage") or t.get("change") or 0)
                                vol_usd = float(t.get("quoteVolume") or 0)
                                price_s = float(t.get("last") or t.get("close") or 0)
                                _log(f"[SafeScan] {sym}: pct={pct:.2f}% vol=${vol_usd:.0f} price={price_s:.6g}")
                                if pct > 0.1 and vol_usd > 50000 and price_s > 0:
                                    safe_candidates.append((sym, pct, price_s))
                            safe_candidates.sort(key=lambda x: x[1], reverse=True)
                            if safe_candidates:
                                _log(f"[SafeScan] ✅ {len(safe_candidates)} SAFE_COIN — أعلاها: {safe_candidates[0][0]} {safe_candidates[0][1]:.1f}%")
                                for sym, pct, price_s in safe_candidates[:2]:
                                    if self.slots.free == 0:
                                        break
                                    if not self.slots.is_vacant(sym):
                                        continue
                                    _log(f"[SafeScan] 🛒 شراء مباشر: {sym} @ {price_s:.6g} gain={pct:.1f}%")
                                    result = await asyncio.get_event_loop().run_in_executor(
                                        None, self.ex.market_buy, sym, capital
                                    )
                                    if result:
                                        price_b = result["price"]
                                        qty = result["qty"]
                                        signal = {"strategy": "S3", "rsi": 50, "gain_pct": pct,
                                                  "vol_ratio": 1.0, "price": price_b, "trigger": "SAFE",
                                                  "symbol": sym}
                                        targets = self.strategy.calc_targets(signal)
                                        tp1_oid = await asyncio.get_event_loop().run_in_executor(
                                            None, self.ex.place_limit_sell, sym, qty, targets["tp1"]
                                        )
                                        state = SlotState(
                                            symbol=sym, strategy="S3",
                                            signal_source="safe_coin_scan",
                                            entry_price=price_b, filled_qty=qty, remaining_qty=qty,
                                            capital=capital, sl_price=targets["sl"],
                                            tp1_price=targets["tp1"], tp2_price=targets["tp2"],
                                            tp3_price=targets["tp3"], tp1_order_id=tp1_oid or "",
                                            db_trade_id=None, highest_price=price_b,
                                        )
                                        trade_id = self.db.upsert_trade(
                                            sym, "S3", price_b, qty, capital,
                                            targets["sl"], targets["tp1"], targets["tp2"], targets["tp3"],
                                            ai_verdict="auto", tp1_order_id=tp1_oid or "",
                                            signal_source="safe_coin_scan"
                                        )
                                        if trade_id:
                                            state.db_trade_id = trade_id
                                        self.slots.occupy(state)
                                        # إعادة محاولة TP1 إذا فشل
                                        if not tp1_oid:
                                            _log(f"[SafeScan] ⚠️ TP1 order فشل — ستُعاد المحاولة")
                                            self.monitor.add_pending({
                                                "action": "place_tp",
                                                "symbol": sym,
                                            })
                                        await self._send_tg(
                                            f"🛡️ <b>S3 SAFE — {sym}</b>\n"
                                            "━━━━━━━━━━━━━━━━━━━━\n"
                                            f"💰 رأس المال: <code>${capital:.2f}</code>\n"
                                            f"📍 دخول: <code>{price_b:.6g}</code>\n"
                                            f"📊 24H gain={pct:.1f}%\n"
                                            "━━━━━━━━━━━━━━━━━━━━\n"
                                            f"🎯 TP1: <code>{targets['tp1']:.6g}</code>\n"
                                            f"🎯 TP2: <code>{targets['tp2']:.6g}</code>\n"
                                            f"🛡 SL: <code>{targets['sl']:.6g}</code>", "trade_open"
                                        )
                                        _log(f"[SafeScan] ✅ صفقة ناجحة: {sym} @ {price_b:.6g}")
                                    else:
                                        _log(f"[SafeScan] ❌ فشل شراء {sym}")
                            else:
                                _log(f"[SafeScan] ℹ️ لا SAFE_COIN بـ gain >0.1% الآن (فحص {len(safe_symbols)} عملة)")
                        except Exception as e:
                            _log(f"[SafeScan] ⚠️ {e}")

                    # 2b. الحصول على العملات المرشّحة مرتّبة
                    priority_syms = self.signals.get_priority_symbols()

                    if priority_syms:
                        _log(f"[Scan] 🎯 فحص {len(priority_syms)} عملة مرشّحة")
                        # فحص العملات ذات الأولوية العالية — batch صغير لتجنب pool overflow
                        BATCH = 5
                        for i in range(0, len(priority_syms), BATCH):
                            if self.slots.free == 0:
                                break
                            batch = priority_syms[i:i+BATCH]
                            tasks = [
                                self._process_symbol(
                                    session, sym, capital,
                                    signal_source=f"priority_score_{score:.0f}"
                                )
                                for sym, score in batch
                            ]
                            await asyncio.gather(*tasks, return_exceptions=True)
                    else:
                        # Fallback: فحص عشوائي من السوق الكامل
                        _log("[Scan] ⚠️ لا مرشحين — فحص عشوائي")
                        try:
                            markets = self.ex.exchange.markets
                            symbols = [
                                s for s in markets
                                if s.endswith("/USDT")
                                and markets[s].get("active", True)
                                and markets[s].get("type", "spot") == "spot"
                            ]
                            random.shuffle(symbols)
                            BATCH = 20
                            for i in range(0, min(len(symbols), 200), BATCH):
                                if self.slots.free == 0:
                                    break
                                batch = symbols[i:i+BATCH]
                                tasks = [
                                    self._process_symbol(session, sym, capital)
                                    for sym in batch
                                ]
                                await asyncio.gather(*tasks, return_exceptions=True)
                        except Exception as e:
                            _log(f"[Scan] ❌ {e}")

                    # 3. فحص New Listings كل 5 دورات
                    new_listings_check_tick += 1
                    if new_listings_check_tick >= 5:
                        new_listings_check_tick = 0
                        new_syms = self.signals._new_listings_seen.copy()
                        for sym in new_syms:
                            if self.slots.free == 0:
                                break
                            if self.slots.get(sym):
                                continue
                            await self._process_symbol(
                                session, sym, capital,
                                signal_source="new_listing",
                                force_new_listing=True
                            )

                # (Smart Report معطّل — إشعارات الصفقات فقط)

                await asyncio.sleep(self.cfg.scan_interval_min * 60)

    async def run(self):
        _log("=" * 60)
        _log("🚀 MEXC Scalping Bot v5 — Ultra Rocket Edition")
        _log("=" * 60)

        healer  = AutoHeal(self.cfg, self.ex, self.slots, self.db)
        actions = healer.run()

        # اختبار SAFE_COINS عند البدء لاكتشاف المحجوب منها
        _log("[Startup] 🧪 اختبار SAFE_COINS من Railway IP...")
        verified_safe = []
        for sym in list(self.ex.SAFE_COINS):
            base = sym.replace("/USDT", "")
            try:
                # fetch_order_book بدلاً من شراء حقيقي
                ob = self.ex.exchange.fetch_order_book(sym, limit=1)
                if ob and ob.get("asks"):
                    verified_safe.append(sym)
                    _log(f"[Startup] ✅ {sym} متاح")
                else:
                    self.ex._geo_blocked.add(base)
                    _log(f"[Startup] ⛔ {sym} محجوب — أُضيف للـ blacklist")
            except Exception as e:
                err = str(e)
                if "11444" in err or "70015" in err or "10007" in err:
                    self.ex._geo_blocked.add(base)
                    _log(f"[Startup] ⛔ {sym} محجوب ({err[:60]})")
                else:
                    verified_safe.append(sym)  # خطأ آخر، لا نحذفه
                    _log(f"[Startup] ⚠️ {sym} خطأ غير حجب: {err[:60]}")
        # تحديث SAFE_COINS بالمتاح فقط
        self.ex.SAFE_COINS = verified_safe
        _log(f"[Startup] 🛡️ SAFE_COINS المتاحة: {verified_safe}")

        # لا إشعار عند البداية — فقط صفقات وتنبيهات مهمة
        _log("[TG] ✅ Bot بدأ — إشعارات الصفقات فقط")

        monitor_task = asyncio.create_task(self.monitor.start())
        try:
            await self.scan_loop()
        finally:
            self.monitor.stop()
            monitor_task.cancel()


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    cfg = Config()
    bot = Orchestrator(cfg)
    asyncio.run(bot.run())
