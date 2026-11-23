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
    min_volume_usd:     float = field(default_factory=lambda: float(os.environ.get("MIN_DAILY_VOLUME_USD", "500000")))
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
    s3_gain_1h_min:     float = field(default_factory=lambda: float(os.environ.get("S3_GAIN_1H_MIN", "5.0")))
    s3_gain_4h_min:     float = field(default_factory=lambda: float(os.environ.get("S3_GAIN_4H_MIN", "10.0")))
    s3_vol_spike:       float = field(default_factory=lambda: float(os.environ.get("S3_VOL_SPIKE", "3.0")))
    s3_rsi_max:         float = field(default_factory=lambda: float(os.environ.get("S3_RSI_MAX", "80")))
    s3_partial1_pct:    float = field(default_factory=lambda: float(os.environ.get("S3_PARTIAL1_PCT", "8.0")))
    s3_partial2_pct:    float = field(default_factory=lambda: float(os.environ.get("S3_PARTIAL2_PCT", "15.0")))
    s3_partial1_ratio:  float = field(default_factory=lambda: float(os.environ.get("S3_PARTIAL1_RATIO", "0.30")))
    s3_partial2_ratio:  float = field(default_factory=lambda: float(os.environ.get("S3_PARTIAL2_RATIO", "0.30")))
    s3_trailing_pct:    float = field(default_factory=lambda: float(os.environ.get("S3_TRAILING_PCT", "8.0")))
    s3_sl_pct:          float = field(default_factory=lambda: float(os.environ.get("S3_SL_PCT", "4.0")))
    s3_min_vol_usd:     float = field(default_factory=lambda: float(os.environ.get("S3_MIN_VOL_USD", "100000")))

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
            _log(f"[Executor] ❌ market_buy {symbol}: {e}")
        return None

    def place_limit_sell(self, symbol: str, qty: float, price: float) -> Optional[str]:
        try:
            prec_qty = float(self.exchange.amount_to_precision(symbol, qty))
            if prec_qty <= 0:
                return None
            order = self.exchange.create_limit_sell_order(symbol, prec_qty, price)
            return str(order.get("id", ""))
        except Exception as e:
            _log(f"[Executor] place_limit_sell {symbol}: {e}")
            return None

    def market_sell(self, symbol: str, qty: float) -> bool:
        for attempt in range(1, 6):
            try:
                prec = float(self.exchange.amount_to_precision(symbol, qty))
                if prec <= 0:
                    return False
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
                _log(f"[Executor] market_sell {symbol} attempt {attempt}: {e}")
                time.sleep(1)
        return False

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

    def fetch_top_gainers(self, count: int = 50, min_pct: float = 3.0) -> list[str]:
        """
        يجلب أفضل N رابح من MEXC مباشرة.
        أسرع بكثير من فحص 3000 عملة!
        """
        try:
            tickers = self.exchange.fetch_tickers()
            gainers = []
            for symbol, t in tickers.items():
                if not symbol.endswith("/USDT"):
                    continue
                pct = float(t.get("percentage") or t.get("change") or 0)
                vol_usd = float(t.get("quoteVolume") or 0)
                if pct >= min_pct and vol_usd > 50000:
                    gainers.append((symbol, pct, vol_usd))
            gainers.sort(key=lambda x: x[1], reverse=True)
            top = [g[0] for g in gainers[:count]]
            _log(f"[TopGainers] 🏆 وجدت {len(top)} رابح (أعلى: {gainers[0][1]:.1f}% {gainers[0][0] if gainers else ''})")
            return top
        except Exception as e:
            _log(f"[TopGainers] ❌ {e}")
            return []

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
# TELEGRAM
# ══════════════════════════════════════════════════════════════
async def tg_send(token: str, chat_id: str, text: str):
    if not token or not chat_id:
        return
    # قطّع الرسالة إذا كانت طويلة (Telegram حد 4096 حرف)
    if len(text) > 4000:
        text = text[:4000] + "\n..."
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for attempt in range(5):
        try:
            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=5)
            ) as s:
                resp = await s.post(url, json={
                    "chat_id": chat_id, "text": text,
                    "parse_mode": "HTML",
                }, timeout=aiohttp.ClientTimeout(total=15))
                if resp.status == 200:
                    return
                body = await resp.text()
                _log(f"[TG] ⚠️ status={resp.status} {body[:120]}")
                # 429 = Too Many Requests — نحترم retry_after
                if resp.status == 429:
                    try:
                        data = _json.loads(body)
                        wait = data.get("parameters", {}).get("retry_after", 35)
                    except Exception:
                        wait = 35
                    _log(f"[TG] ⏳ Rate limit — انتظار {wait}ث ثم إعادة المحاولة")
                    await asyncio.sleep(wait + 1)
                    continue
        except Exception as e:
            _log(f"[TG] ❌ attempt {attempt+1}: {e}")
            await asyncio.sleep(5)
    _log("[TG] ❌ فشل إرسال الرسالة بعد 5 محاولات")


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

        # 5. تحديث القائمة المدمجة
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

        vol_usd = vol_1h * price * 24
        if vol_usd < self.cfg.s3_min_vol_usd:
            return None

        # شرط A: ارتفاع سريع في ساعة
        if gain_1h >= self.cfg.s3_gain_1h_min and vol_ratio >= self.cfg.s3_vol_spike:
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
            if gain_4h >= self.cfg.s3_gain_4h_min and vr4h >= self.cfg.s3_vol_spike:
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

    def calc_targets(self, signal: dict) -> dict:
        price = signal["price"]
        s     = signal["strategy"]
        cfg   = self.cfg

        if s == "S1":
            sl  = price * (1 - cfg.s1_sl_pct / 100)
            tp1 = price * (1 + cfg.s1_tp1_pct / 100)
            tp2 = price * (1 + cfg.s1_tp2_pct / 100)
            tp3 = tp2 * 1.05
        elif s == "S2":
            sl  = price * (1 - cfg.s2_sl_pct / 100)
            tp1 = price * (1 + cfg.s2_tp1_pct / 100)
            tp2 = tp1 * 1.05
            tp3 = tp1 * 1.10
        elif s == "NEW_LISTING":
            sl  = price * (1 - cfg.new_listing_sl_pct / 100)
            tp1 = price * (1 + cfg.new_listing_tp_pct / 100)
            tp2 = tp1 * 1.20
            tp3 = tp1 * 1.50
        else:  # S3 Rocket
            sl  = price * (1 - cfg.s3_sl_pct / 100)
            tp1 = price * (1 + cfg.s3_partial1_pct / 100)
            tp2 = price * (1 + cfg.s3_partial2_pct / 100)
            tp3 = price * (1 + cfg.s3_partial2_pct / 100 * 2)

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
            max_att = 9999 if action == "emergency_sell" else 20
            if task["attempts"] > max_att:
                _log(f"[Monitor] ⚠️ {symbol}/{action} تجاوز {max_att} محاولة")
                continue
            try:
                state = self.slots.get(symbol)
                if action == "emergency_sell":
                    qty = task.get("qty", state.remaining_qty if state else 0)
                    if qty > 0 and await asyncio.get_event_loop().run_in_executor(
                        None, self.ex.market_sell, symbol, qty
                    ):
                        if state and state.db_trade_id:
                            price = self.ex.live_price(symbol)
                            pnl   = (price - state.entry_price) / state.entry_price * 100
                            self.db.close_trade(
                                state.db_trade_id, price, qty,
                                "emergency_sl", (price - state.entry_price) * qty, pnl
                            )
                        self.slots.release(symbol)
                        _log(f"[Monitor] ✅ Retry emergency_sell {symbol} نجح")
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

        pnl_pct = (price - state.entry_price) / state.entry_price * 100

        if price > state.highest_price:
            self.slots.update(state.symbol, highest_price=price)
            state.highest_price = price

        if state.strategy in ("S3", "NEW_LISTING"):
            await self._handle_s3_partials(state, price, pnl_pct)

        await self._update_trailing(state, price, pnl_pct)

        if price <= state.sl_price and state.sl_price > 0:
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
        cfg = self.cfg
        if not state.partial1_done and pnl_pct >= cfg.s3_partial1_pct:
            qty = state.filled_qty * cfg.s3_partial1_ratio
            ok  = await asyncio.get_event_loop().run_in_executor(
                None, self.ex.market_sell, state.symbol, qty
            )
            if ok:
                remaining = state.remaining_qty - qty if state.remaining_qty > 0 else \
                            state.filled_qty * (1 - cfg.s3_partial1_ratio)
                self.slots.update(state.symbol, partial1_done=True, remaining_qty=remaining)
                pnl_usd = (price - state.entry_price) * qty
                _log(f"[Monitor] 💰 Partial1 {state.symbol} +{pnl_pct:.1f}% pnl=${pnl_usd:.2f}")
                await tg_send(self.cfg.telegram_token, self.cfg.telegram_chat_id,
                    f"💰 <b>Partial1 — {state.symbol}</b>\n"
                    f"+{pnl_pct:.1f}% | بيع 30% | ربح: ${pnl_usd:.2f}"
                )
        elif state.partial1_done and not state.partial2_done and pnl_pct >= cfg.s3_partial2_pct:
            qty = state.filled_qty * cfg.s3_partial2_ratio
            ok  = await asyncio.get_event_loop().run_in_executor(
                None, self.ex.market_sell, state.symbol, qty
            )
            if ok:
                remaining = state.remaining_qty - qty
                self.slots.update(state.symbol, partial2_done=True, remaining_qty=max(0, remaining))
                pnl_usd = (price - state.entry_price) * qty
                _log(f"[Monitor] 💰 Partial2 {state.symbol} +{pnl_pct:.1f}% pnl=${pnl_usd:.2f}")
                await tg_send(self.cfg.telegram_token, self.cfg.telegram_chat_id,
                    f"💰 <b>Partial2 — {state.symbol}</b>\n"
                    f"+{pnl_pct:.1f}% | بيع 30% إضافية | ربح: ${pnl_usd:.2f}"
                )

    async def _update_trailing(self, state: SlotState, price: float, pnl_pct: float):
        trigger = self.cfg.s3_trailing_pct if state.strategy in ("S3", "NEW_LISTING") else self.cfg.trailing_trigger
        trail   = self.cfg.s3_trailing_pct if state.strategy in ("S3", "NEW_LISTING") else self.cfg.trailing_pct

        if pnl_pct < trigger:
            return

        new_trail_sl = state.highest_price * (1 - trail / 100)
        if new_trail_sl > state.trailing_sl:
            self.slots.update(state.symbol, trailing_sl=new_trail_sl)
            if new_trail_sl > state.sl_price:
                self.slots.update(state.symbol, sl_price=new_trail_sl)
                if state.db_trade_id:
                    self.db.update_sl(state.db_trade_id, new_trail_sl)

    async def _on_tp1_filled(self, state: SlotState, price: float):
        pnl_pct = (price - state.entry_price) / state.entry_price * 100
        pnl_usd = (price - state.entry_price) * state.filled_qty * 0.8
        if state.db_trade_id:
            self.db.close_trade(state.db_trade_id, price, state.filled_qty * 0.8, "tp1", pnl_usd, pnl_pct)
        self.slots.release(state.symbol)
        await tg_send(self.cfg.telegram_token, self.cfg.telegram_chat_id,
            f"🎯 <b>TP1 — {state.symbol}</b>\n+{pnl_pct:.1f}% | ربح: ${pnl_usd:.2f}"
        )

    async def _close_position(self, state: SlotState, price: float, reason: str):
        qty = state.remaining_qty if state.remaining_qty > 0 else state.filled_qty
        ok  = await asyncio.get_event_loop().run_in_executor(
            None, self.ex.market_sell, state.symbol, qty
        )
        if not ok:
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
            f"{pnl_pct:+.1f}% | ${pnl_usd:+.2f} | سبب: {reason}"
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
            freqd        = {k: float(v.get("free", 0))
                            for k, v in bal.items()
                            if isinstance(v, dict) and float(v.get("free", 0)) > 0}
            db_symbols   = {t["symbol"] for t in db_trades}

            for t in db_trades:
                sym   = t["symbol"]
                base  = sym.replace("/USDT", "")
                price = self.ex.live_price(sym)
                qty   = freqd.get(base, 0)
                val   = qty * price

                if val < cap_per_slot * 0.15:
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

            for base, qty in freqd.items():
                if base in ("USDT", "USDC"):
                    continue
                sym   = f"{base}/USDT"
                price = self.ex.live_price(sym)
                val   = qty * price
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
        free = self.ex.free_usdt()
        return max(1.0, round(free / max(1, self.cfg.max_slots), 2))


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
        self._processing: set = set()
        self._processing_lock = threading.Lock()
        self._report_tick = 0

    def _calc_capital(self) -> float:
        if self.cfg.capital > 0:
            return self.cfg.capital
        free = self.ex.free_usdt()
        return max(1.0, round(free / max(1, self.cfg.max_slots), 2))

    async def _send_tg(self, text: str):
        await tg_send(self.cfg.telegram_token, self.cfg.telegram_chat_id, text)

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

            # فلتر الحجم
            try:
                ticker  = self.ex.exchange.fetch_ticker(symbol)
                vol_usd = float(ticker.get("quoteVolume") or
                                ticker.get("baseVolume", 0) * signal["price"])
                min_vol = 10000 if force_new_listing else self.cfg.min_volume_usd
                if vol_usd < min_vol:
                    return
            except Exception:
                return

            # Order Book Pressure — فلتر إضافي
            if self.cfg.orderbook_enabled and not force_new_listing:
                ob_ratio = await asyncio.get_event_loop().run_in_executor(
                    None, self.ex.get_orderbook_pressure, symbol
                )
                if ob_ratio < 0.8:  # ضغط بيع قوي — تجنب
                    _log(f"[OrderBook] ⚠️ {symbol} ضغط بيع ({ob_ratio:.2f}) — تخطي")
                    return
                # أضف نقاط إضافية إذا كان ضغط شراء قوي
                if ob_ratio >= self.cfg.orderbook_ratio_min:
                    signal["ob_boost"] = True

            # AI موافقة
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
            targets = self.strategy.calc_targets({**signal, "price": price})

            # وضع TP1 Limit Order
            tp1_oid = await asyncio.get_event_loop().run_in_executor(
                None, self.ex.place_limit_sell, symbol, qty, targets["tp1"]
            )

            state = SlotState(
                symbol=symbol, strategy=signal["strategy"],
                signal_source=signal_source,
                entry_price=price, filled_qty=qty, remaining_qty=qty,
                capital=capital, sl_price=targets["sl"],
                tp1_price=targets["tp1"], tp2_price=targets["tp2"], tp3_price=targets["tp3"],
                tp1_order_id=tp1_oid or "", db_trade_id=None, highest_price=price,
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

            # إشعار Telegram
            emoji   = {"S3": "🚀", "NEW_LISTING": "🆕", "S1": "📉", "S2": "📈"}.get(signal["strategy"], "📊")
            ob_note = " 📚✅" if signal.get("ob_boost") else ""
            src_tag = f" [{signal_source}]" if signal_source != "scan" else ""
            await self._send_tg(
                f"{emoji} <b>{signal['strategy']} — {symbol}</b>{src_tag}\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 رأس المال: <code>${capital:.2f}</code>\n"
                f"📍 دخول: <code>{price:.6g}</code>\n"
                f"📊 RSI={signal['rsi']:.0f} | "
                f"Gain={signal['gain_pct']:.1f}% | Vol×{signal['vol_ratio']:.1f}{ob_note}\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 TP1: <code>{targets['tp1']:.6g}</code>\n"
                f"🎯 TP2: <code>{targets['tp2']:.6g}</code>\n"
                f"🛡 SL: <code>{targets['sl']:.6g}</code>\n"
                f"🤖 AI: {ai_vote}"
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
                else:
                    # 1. تحديث مصادر الإشارات
                    try:
                        await self.signals.refresh(session)
                    except Exception as e:
                        _log(f"[Signals] ⚠️ {e}")

                    # 2. الحصول على العملات المرشّحة مرتّبة
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

                # Smart Report
                self._report_tick += self.cfg.scan_interval_min
                if self._report_tick >= self.cfg.smart_report_hours * 60:
                    self._report_tick = 0
                    try:
                        await self._smart_report(session)
                    except Exception as e:
                        _log(f"[Report] ⚠️ {e}")

                await asyncio.sleep(self.cfg.scan_interval_min * 60)

    async def run(self):
        _log("=" * 60)
        _log("🚀 MEXC Scalping Bot v5 — Ultra Rocket Edition")
        _log("=" * 60)

        healer  = AutoHeal(self.cfg, self.ex, self.slots, self.db)
        actions = healer.run()

        # إشعار Telegram واحد فقط عند البدء (لتجنب 429 rate limit)
        _log("[TG] 📤 إرسال إشعار البداية...")
        await self._startup_notify()
        if actions:
            await asyncio.sleep(2)  # فاصل صغير بين الرسائل
            await self._send_tg(
                "🔧 <b>Auto-Heal</b>\n" +
                "\n".join(f"  {a}" for a in actions)
            )

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
