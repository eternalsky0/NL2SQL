import os
import re
import json
import time
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from contextlib import asynccontextmanager

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

import yaml
import pandas as pd
import sqlglot
from sqlglot import exp
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
from openai import AsyncOpenAI

import reports_store
import notifications
from scheduler import ReportScheduler
from cron_utils import validate_cron, PRESETS

# ── Config ────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent

_db_env = os.getenv("DRIVEE_DB_PATH", "")
if _db_env and Path(_db_env).exists():
    DB_PATH = _db_env
else:
    # backend/data/drivee.db  (BASE_DIR = backend/)
    DB_PATH = str(BASE_DIR / "data" / "drivee.db")

SEMANTIC_PATH = os.getenv("SEMANTIC_PATH", str(BASE_DIR / "semantic_layer.yaml"))

# Startup log so you can see which DB is used
import logging
logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)
_log.info("DB_PATH = %s  (exists: %s)", DB_PATH, Path(DB_PATH).exists())

OPENROUTER_URL = os.getenv("OPENROUTER_URL", "https://openrouter.ai/api/v1")
OPENROUTER_KEY = os.getenv("OPENROUTER_KEY", "")
FAST_MODEL     = os.getenv("FAST_MODEL", "meta-llama/llama-3-8b-instruct")
STRONG_MODEL   = os.getenv("STRONG_MODEL", "deepseek/deepseek-v3.2")

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

HTTPS_PROXY = os.getenv("HTTPS_PROXY", "")

_timeout = httpx.Timeout(connect=30.0, read=120.0, write=30.0, pool=10.0)
_http_client = httpx.AsyncClient(proxy=HTTPS_PROXY, timeout=_timeout) if HTTPS_PROXY else httpx.AsyncClient(timeout=_timeout)

client = AsyncOpenAI(
    base_url=OPENROUTER_URL,
    api_key=OPENROUTER_KEY,
    max_retries=2,
    timeout=120.0,
    http_client=_http_client,
    default_headers={
        "HTTP-Referer": "http://localhost:8000",
        "X-Title": "Drivee NL2SQL",
    },
)

# ── Semantic layer ────────────────────────────────────────────────
def _load_semantic():
    try:
        with open(SEMANTIC_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

SEMANTIC = _load_semantic()

def get_semantic_context() -> str:
    metrics = SEMANTIC.get("metrics", {})
    if not metrics:
        return ""
    ctx = "ИСПОЛЬЗУЙ ЭТИ МЕТРИКИ:\n"
    for v in metrics.values():
        ctx += f"- {v['canonical']}: {v['sql']}\n"
    return ctx

# ── SQL system prompt ─────────────────────────────────────────────
SQL_SYSTEM = """Ты — Senior Data Analyst Drivee. Генерируй ТОЛЬКО SQLite-запросы.
ВАЖНО: «сегодня» = MAX(order_timestamp) в таблице incity. Никогда не используй реальную текущую дату.

СХЕМА БД — три таблицы:

── incity ── детальные заказы и тендеры (одна строка = один заказ + тендер):
  city_id              INTEGER   -- ID города
  order_id             TEXT      -- уникальный ID заказа
  tender_id            TEXT      -- ID тендера (NULL если без тендера)
  user_id              TEXT      -- ID пассажира
  driver_id            TEXT      -- ID водителя
  offset_hours         INTEGER   -- смещение часового пояса (UTC)
  status_order         TEXT      -- статус заказа: 'done', 'cancel', 'accept', 'delete'
  status_tender        TEXT      -- статус тендера: 'done', 'decline', 'cancel', 'accept', 'wait'
  order_timestamp      TEXT      -- время создания заказа (ISO)
  tender_timestamp     TEXT      -- время создания тендера (ISO)
  driveraccept_timestamp        TEXT
  driverarrived_timestamp       TEXT
  driverstarttheride_timestamp  TEXT
  driverdone_timestamp          TEXT
  clientcancel_timestamp        TEXT
  drivercancel_timestamp        TEXT
  order_modified_local          TEXT
  cancel_before_accept_local    TEXT
  distance_in_meters   INTEGER   -- дистанция в метрах
  duration_in_seconds  INTEGER   -- расчётная длительность в секундах
  price_order_local    REAL      -- итоговая цена заказа (₽)
  price_tender_local   REAL      -- цена на этапе тендера (₽)
  price_start_local    REAL      -- стартовая цена (₽)

── pass_detail ── дневные метрики пассажиров (одна строка = пассажир × город × день):
  city_id                  INTEGER
  user_id                  TEXT
  order_date_part          TEXT    -- локальная дата (YYYY-MM-DD)
  user_reg_date            TEXT    -- дата регистрации пассажира
  orders_count             INTEGER -- кол-во уникальных заказов за день
  orders_cnt_with_tenders  INTEGER -- заказов с тендерами
  orders_cnt_accepted      INTEGER -- заказов, принятых водителями
  rides_count              INTEGER -- завершённых поездок
  rides_time_sum_seconds   REAL    -- суммарное время поездок (сек)
  online_time_sum_seconds  REAL    -- суммарное время онлайн (сек)
  client_cancel_after_accept INTEGER -- отмен после принятия водителем

── driver_detail ── дневные метрики водителей (одна строка = водитель × город × день):
  city_id                  INTEGER
  driver_id                TEXT
  tender_date_part         TEXT    -- локальная дата (YYYY-MM-DD)
  driver_reg_date          TEXT    -- дата регистрации водителя
  orders                   INTEGER -- кол-во заказов за день
  orders_cnt_with_tenders  INTEGER -- заказов с тендерами
  orders_cnt_accepted      INTEGER -- принятых заказов
  rides_count              INTEGER -- завершённых поездок
  rides_time_sum_seconds   REAL    -- суммарное время поездок (сек)
  online_time_sum_seconds  REAL    -- суммарное время онлайн (сек)
  client_cancel_after_accept INTEGER -- отмен пассажиром после принятия

ВАЖНЫЕ ОСОБЕННОСТИ ДАННЫХ incity:
- tender_id IS NULL     → заказ отменён/удалён ДО создания тендера; у таких строк status_order НИКОГДА не бывает 'done'.
  Это не отдельный «канал» — это заказы, которые не дошли до стадии подбора водителя.
- tender_id IS NOT NULL → заказ прошёл через тендерную систему; только такие строки могут иметь status_order = 'done'.
- Один заказ НЕ может иметь одновременно строки с NULL и NOT NULL tender_id.
- Поэтому при анализе конверсии считай только заказы с тендерами (tender_id IS NOT NULL),
  а заказы без тендера — это «потерянный спрос» (отмены до назначения водителя).

СВЯЗИ:
  incity.user_id   → pass_detail.user_id   (+ совпадение DATE(order_timestamp) = order_date_part)
  incity.driver_id → driver_detail.driver_id (+ совпадение DATE(tender_timestamp) = tender_date_part)

КЛЮЧЕВЫЕ МЕТРИКИ (по таблице incity):
- Уникальные заказы:      COUNT(DISTINCT order_id)
- Завершённые поездки:    COUNT(DISTINCT CASE WHEN status_order = 'done' THEN order_id END)
- Отмены:                 COUNT(DISTINCT CASE WHEN status_order = 'cancel' THEN order_id END)
- Выручка:                SUM(CASE WHEN status_tender = 'done' THEN price_order_local ELSE 0 END)
- Средний чек:            AVG(CASE WHEN status_tender = 'done' THEN price_order_local END)
- Конверсия (%):          ROUND(COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) * 100.0 / COUNT(DISTINCT order_id), 2)
- Средняя дистанция (м):  ROUND(AVG(CASE WHEN status_tender='done' THEN distance_in_meters END), 0)
- Среднее время (мин):    ROUND(AVG(CASE WHEN status_tender='done' THEN duration_in_seconds END) / 60.0, 1)

ПРАВИЛА ДАТ:
- incity: группировка → DATE(order_timestamp); опорная дата → (SELECT MAX(order_timestamp) FROM incity)
- pass_detail: поле order_date_part уже DATE; опорная дата → (SELECT MAX(order_date_part) FROM pass_detail)
- driver_detail: поле tender_date_part уже DATE; опорная дата → (SELECT MAX(tender_date_part) FROM driver_detail)
- ВАЖНО: каждая таблица имеет свой диапазон дат — всегда используй MAX именно той таблицы, к которой применяешь фильтр
- "За последние N дней" по incity:        WHERE DATE(order_timestamp) >= DATE((SELECT MAX(order_timestamp) FROM incity), '-N days')
- "За последние N дней" по driver_detail: WHERE tender_date_part >= DATE((SELECT MAX(tender_date_part) FROM driver_detail), '-N days')
- "За последние N дней" по pass_detail:   WHERE order_date_part >= DATE((SELECT MAX(order_date_part) FROM pass_detail), '-N days')
- НИКОГДА не используй INTERVAL, NOW(), CURRENT_DATE, DATE('now')
- При сравнении двух периодов используй CTE: current_week и past_week
- Если используешь CTE для опорной даты — обращайся к столбцу напрямую (не через алиас таблицы):
  ПРАВИЛЬНО:   WITH d AS (SELECT MAX(order_date_part) AS max_dt FROM pass_detail)
               ... CROSS JOIN d WHERE order_date_part >= DATE(max_dt, '-30 days')
  НЕПРАВИЛЬНО: DATE(d.max_dt, ...) или DATE(max_dt.max_dt, ...)

ОБЯЗАТЕЛЬНЫЕ ПРАВИЛА:
1. Алиасы столбцов ВСЕГДА на русском в двойных кавычках: COUNT(*) AS "Количество".
2. Алиасы таблиц: СТРОГО следи за префиксами! Если используешь префикс (например, i.driver_id), убедись, что таблица объявлена как `incity i` в FROM или JOIN. Никогда не выдумывай несуществующие алиасы (например, d.driver_id, если таблицы d нет).
3. LIMIT: если пользователь указал число (топ-10, топ-5) — используй его. Иначе LIMIT 1000.
4. Только SELECT — никаких INSERT/UPDATE/DELETE/DROP
5. Отвечай СТРОГО JSON без markdown-блоков

ФОРМАТ ОТВЕТА:
{{
  "sql": "SELECT ...",
  "explanation": "Считаю ...",
  "chart_type": "bar | line | pie | table",
  "confidence": 0.95
}}
"""

# ── Ghost prompt ──────────────────────────────────────────────────
GHOST_SYSTEM = """You complete Russian analytics queries for a taxi service (Drivee).
Given partial user input, output ONLY the missing continuation — nothing else.
No explanations, no quotes, no punctuation at the end.
Use correct Russian grammar. Allowed phrases: по городам, по водителям, по часам, по дням, по статусам, за неделю, за месяц, за последние N дней.
NEVER use wrong prepositions like "за городам", "за часам" — always "по городам", "по часам".

Rules:
- If the input cuts off mid-word: complete that word first, then add context.
- If the input ends on a complete word or space: start your output with a space.
- Maximum 8 words total. Russian language only.

Input: отм
Output: ены по городам за неделю

Input: отмены
Output:  по городам за неделю

Input: отмены по
Output:  городам за последнюю неделю

Input: покажи выр
Output: учку по городам за месяц

Input: покажи выручку
Output:  по городам за месяц

Input: топ водит
Output: елей по поездкам за неделю

Input: топ 10
Output:  водителей по поездкам за неделю

Input: топ 5
Output:  городов по выручке за месяц

Input: динамик
Output: а выручки за последние 30 дней

Input: динамика выр
Output: учки по дням за неделю

Input: отмены по городам
Output:  за последнюю неделю

Input: средний чек
Output:  по городам за месяц

Input: средний чек по
Output:  статусам за месяц

Input: конверсия
Output:  по городам за неделю

Input: конверсия по
Output:  часам за последнюю неделю

Input: покажи
Output:  отмены по городам за неделю

Input: сколько
Output:  заказов за последнюю неделю"""

CHIPS_SYSTEM = """Ты — помощник аналитика сервиса такси Drivee. Предлагай 4 коротких вопроса на русском для анализа данных.
Доступные метрики: заказы, отмены, выручка, завершённые поездки, конверсия, средний чек.
Измерения: дата, час, статус, водитель.

Отвечай СТРОГО JSON: {"suggestions": ["вопрос1", "вопрос2", "вопрос3", "вопрос4"]}
Вопросы должны быть короткими (5-8 слов), конкретными и разными.
Если есть история запросов — предложи логичные follow-up вопросы."""

INSIGHT_SYSTEM = """Ты — аналитик сервиса такси Drivee. По данным SQL-отчёта напиши 2-3 предложения на русском: главный вывод, важный тренд или аномалию. Используй конкретные числа. Только текст — без заголовков, без списков.

Единицы измерения — СТРОГО:
- Деньги (price_order_local, price_tender_local, price_start_local): рубли (₽). Писать «рублей» или «₽», НИКОГДА не «долларов», не «тенге», не «USD».
- Расстояние: километры (км), если исходные данные в метрах — переводи.
- Время: минуты (мин), если исходные данные в секундах — переводи.
- Количество: заказы, поездки, водители, пассажиры — без валюты."""

# ── Guardrails ────────────────────────────────────────────────────
def validate_and_fix_sql(sql: str, role: str = "user") -> str:
    sql = sql.replace("```sql", "").replace("```", "").strip()
    # Fix quoted aliases in ORDER BY / GROUP BY
    sql = re.sub(r"ORDER\s+BY\s+'([^']+)'", r'ORDER BY "\1"', sql, flags=re.IGNORECASE)
    sql = re.sub(r"GROUP\s+BY\s+'([^']+)'", r'GROUP BY "\1"', sql, flags=re.IGNORECASE)

    if role != "admin":
        upper = sql.upper()
        for kw in ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE"]:
            if re.search(rf'\b{kw}\b', upper):
                raise ValueError(f"Разрешены только SELECT-запросы. Обнаружено: {kw}")

    allowed = {"incity", "pass_detail", "driver_detail"}

    def _regex_validate(s: str) -> str:
        """Fallback validation when sqlglot can't parse the SQL."""
        upper = s.upper()
        if not re.search(r'\bSELECT\b', upper):
            raise ValueError("Разрешены только SELECT-запросы")
        cte_names_fb = {m.group(1).lower() for m in re.finditer(r'\bWITH\s+(\w+)\s+AS\b', s, re.IGNORECASE)}
        cte_names_fb |= {m.group(1).lower() for m in re.finditer(r',\s*(\w+)\s+AS\s*\(', s, re.IGNORECASE)}
        from_tables = {m.group(1).lower() for m in re.finditer(r'\bFROM\s+(\w+)', s, re.IGNORECASE)}
        join_tables = {m.group(1).lower() for m in re.finditer(r'\bJOIN\s+(\w+)', s, re.IGNORECASE)}
        bad_fb = (from_tables | join_tables) - cte_names_fb - allowed
        if bad_fb:
            raise ValueError(f"Таблица не разрешена: {bad_fb}")
        if not re.search(r'\bLIMIT\b', upper):
            s = s.rstrip(";") + " LIMIT 1000"
        return s

    try:
        parsed = sqlglot.parse_one(sql, dialect="sqlite")
        # Reject actual DML/DDL statement types; allow Select and With (CTE+SELECT)
        if isinstance(parsed, (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Command)):
            raise ValueError("Разрешены только SELECT-запросы")
        cte_names = {cte.alias.lower() for cte in parsed.find_all(exp.CTE)}
        tables = {t.name.lower() for t in parsed.find_all(exp.Table)} - cte_names
        bad = tables - allowed
        if bad:
            raise ValueError(f"Таблица не разрешена: {bad}")
        # Try to regenerate SQL via sqlglot; fall back to original if it fails
        try:
            if isinstance(parsed, exp.Select) and not parsed.args.get("limit"):
                return parsed.limit(1000).sql(dialect="sqlite")
            return parsed.sql(dialect="sqlite")
        except Exception:
            return _regex_validate(sql)
    except ValueError:
        raise
    except Exception:
        # sqlglot can't parse some valid SQLite syntax — use regex fallback
        return _regex_validate(sql)


# ── DB helpers ────────────────────────────────────────────────────
def _get_conn():
    return sqlite3.connect(DB_PATH)

async def _run_sql(sql: str) -> dict:
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        df = pd.read_sql_query(sql, con)
        return {"columns": list(df.columns), "rows": df.fillna("").values.tolist()}
    finally:
        con.close()


def _init_app_tables():
    """Create user/chat/community tables if not present."""
    con = _get_conn()
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY, password TEXT, role TEXT
        );
        CREATE TABLE IF NOT EXISTS chat_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT, title TEXT, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER, role TEXT, content TEXT, data TEXT, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS community_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT, content TEXT, query_text TEXT, created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            report_id INTEGER,
            operator TEXT NOT NULL,
            threshold REAL NOT NULL,
            recipients TEXT DEFAULT '[]',
            enabled INTEGER DEFAULT 1,
            last_checked_at TEXT,
            last_triggered_at TEXT,
            last_value REAL,
            created_at TEXT
        );
    """)
    # migration: add category column if missing
    try:
        cur.execute("ALTER TABLE community_posts ADD COLUMN category TEXT DEFAULT 'Аналитика'")
        con.commit()
    except Exception:
        pass
    for row in [
        ("a.kobenko",  "admin123", "admin"),
        ("d.sezyomov", "admin456", "admin"),
        ("r.abramov",  "admin789", "admin"),
        ("guest",      "12345",    "user"),
    ]:
        cur.execute("INSERT OR IGNORE INTO users VALUES (?, ?, ?)", row)
    con.commit()
    con.close()


# ── Scheduler singleton ───────────────────────────────────────────
_scheduler: ReportScheduler | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    _init_app_tables()
    reports_store.init_db()
    _scheduler = ReportScheduler(execute_sql=_run_sql)
    _scheduler.start()
    _scheduler._scheduler.add_job(_check_alerts, "interval", minutes=15, id="alert_checker", replace_existing=True)
    yield
    if _scheduler:
        _scheduler.shutdown()


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── Pydantic models ───────────────────────────────────────────────
class LoginReq(BaseModel):
    username: str
    password: str

class MessageItem(BaseModel):
    role: str
    content: str

class QueryReq(BaseModel):
    chat_id: Optional[int] = None
    username: str = "guest"
    question: str
    history: List[MessageItem] = []

class PostReq(BaseModel):
    username: str
    content: str
    query_text: str
    category: str = "Аналитика"

class AlertReq(BaseModel):
    name: str
    report_id: int
    operator: str  # '<', '>', '<=', '>=', '='
    threshold: float
    recipients: List[str] = []
    enabled: bool = True

class AlertUpdate(BaseModel):
    name: Optional[str] = None
    operator: Optional[str] = None
    threshold: Optional[float] = None
    recipients: Optional[List[str]] = None
    enabled: Optional[bool] = None

class GhostReq(BaseModel):
    prefix: str

class ChipsReq(BaseModel):
    input: str = ""
    history: List[str] = []

class InsightReq(BaseModel):
    question: str
    columns: List[str]
    rows: List[list]

class ReportCreate(BaseModel):
    name: str
    question: str
    sql: str
    chart_type: Optional[str] = None
    schedule_cron: Optional[str] = None
    timezone: str = "Europe/Moscow"
    recipients: List[str] = []
    enabled: bool = True

class ReportUpdate(BaseModel):
    name: Optional[str] = None
    schedule_cron: Optional[str] = None
    timezone: Optional[str] = None
    recipients: Optional[List[str]] = None
    enabled: Optional[bool] = None
    chart_type: Optional[str] = None


# ── Auth ──────────────────────────────────────────────────────────
@app.post("/login")
async def login(req: LoginReq):
    con = _get_conn()
    row = con.execute(
        "SELECT username, role FROM users WHERE username=? AND password=?",
        (req.username, req.password)
    ).fetchone()
    con.close()
    if not row:
        raise HTTPException(401, "Доступ запрещён")
    return {"username": row[0], "role": row[1]}


# ── Chats ─────────────────────────────────────────────────────────
@app.get("/chats/{username}")
async def get_chats(username: str):
    con = _get_conn()
    rows = con.execute(
        "SELECT id, title, created_at FROM chat_sessions WHERE username=? ORDER BY created_at DESC",
        (username,)
    ).fetchall()
    con.close()
    return [{"id": r[0], "title": r[1], "created_at": r[2]} for r in rows]

@app.get("/chats/history/{chat_id}")
async def get_chat_history(chat_id: int):
    con = _get_conn()
    rows = con.execute(
        "SELECT role, content, data FROM chat_messages WHERE chat_id=? ORDER BY id ASC",
        (chat_id,)
    ).fetchall()
    con.close()
    return [{"role": r[0], "content": r[1], "data": json.loads(r[2]) if r[2] else None} for r in rows]

@app.delete("/chats/{chat_id}")
async def delete_chat(chat_id: int):
    con = _get_conn()
    con.execute("DELETE FROM chat_sessions WHERE id=?", (chat_id,))
    con.execute("DELETE FROM chat_messages WHERE chat_id=?", (chat_id,))
    con.commit()
    con.close()
    return {"status": "ok"}


# ── Query ─────────────────────────────────────────────────────────
@app.post("/query")
async def query(req: QueryReq):
    t_start = time.time()
    con = _get_conn()
    try:
        user_row = con.execute("SELECT role FROM users WHERE username=?", (req.username,)).fetchone()
        role = user_row[0] if user_row else "user"

        chat_id = req.chat_id
        if not chat_id:
            cur = con.execute(
                "INSERT INTO chat_sessions (username, title, created_at) VALUES (?, ?, ?)",
                (req.username, req.question[:50], datetime.now().isoformat())
            )
            chat_id = cur.lastrowid

        con.execute(
            "INSERT INTO chat_messages (chat_id, role, content, created_at) VALUES (?, 'user', ?, ?)",
            (chat_id, req.question, datetime.now().isoformat())
        )
        con.commit()

        messages = [{"role": "system", "content": SQL_SYSTEM}]
        for h in req.history[-5:]:
            messages.append({"role": h.role, "content": h.content})
        messages.append({"role": "user", "content": req.question})

        response = await client.chat.completions.create(
            model=STRONG_MODEL,
            messages=messages,
            temperature=0.1,
        )
        raw_content = response.choices[0].message.content or ""
        _log.info("LLM raw response: %s", raw_content[:500])
        m = re.search(r"\{.*\}", raw_content, re.DOTALL)
        if not m:
            raise ValueError(f"LLM не вернул JSON. Ответ: {raw_content[:300]}")
        llm_json = json.loads(m.group(0))
        sql_raw = llm_json.get("sql", "")

        # Fix LLM habit of writing alias.alias (e.g. max_dt.max_dt) instead of alias
        sql_raw = re.sub(r'\b(\w+)\.\1\b', r'\1', sql_raw)

        def _clean(msg: str) -> str:
            return _ANSI_RE.sub("", str(msg))

        sql_safe = None
        sql_error = None
        for attempt in range(2):
            try:
                sql_safe = validate_and_fix_sql(sql_raw, role)
                sql_error = None
                break
            except ValueError as e:
                sql_error = _clean(str(e))
                is_security = any(k in sql_error for k in ("не разрешена", "Разрешены только SELECT"))
                if is_security or attempt == 1:
                    break
                # SQL parse error — ask LLM to fix
                fix_messages = messages + [
                    {"role": "assistant", "content": raw_content},
                    {"role": "user", "content": f"Твой SQL содержит синтаксическую ошибку: {sql_error}\nИсправь SQL и верни тот же JSON-формат."},
                ]
                fix_response = await client.chat.completions.create(
                    model=STRONG_MODEL,
                    messages=fix_messages,
                    temperature=0,
                )
                raw_content = fix_response.choices[0].message.content or ""
                m2 = re.search(r"\{.*\}", raw_content, re.DOTALL)
                if m2:
                    llm_json = json.loads(m2.group(0))
                    sql_raw = llm_json.get("sql", "")

        if sql_safe is None:
            return {
                "chat_id": chat_id,
                "role": "assistant",
                "content": f"⚠️ {sql_error}",
                "error": True,
                "data": None,
            }

        df_data = await _run_sql(sql_safe)

        data_payload = {
            "sql": sql_safe,
            "explanation": llm_json.get("explanation"),
            "question": req.question,
            "chart_type": llm_json.get("chart_type", "bar"),
            "confidence": llm_json.get("confidence", 0.9),
            "data": df_data,
            "timings": {"total_ms": int((time.time() - t_start) * 1000)},
        }

        con.execute(
            "INSERT INTO chat_messages (chat_id, role, content, data, created_at) VALUES (?, 'assistant', ?, ?, ?)",
            (chat_id, llm_json.get("explanation"), json.dumps(data_payload), datetime.now().isoformat())
        )
        con.commit()

        return {
            "chat_id": chat_id,
            "role": "assistant",
            "content": llm_json.get("explanation"),
            "data": data_payload,
        }

    except Exception as e:
        _log.error("Query error: %s", e, exc_info=True)
        con.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        con.close()


# ── Ghost text ────────────────────────────────────────────────────
@app.post("/suggest/ghost")
async def suggest_ghost(req: GhostReq):
    if len(req.prefix.strip()) < 2:
        async def empty():
            yield "data: [DONE]\n\n"
        return StreamingResponse(empty(), media_type="text/event-stream")

    async def iter_tokens():
        # Последнее слово ввода как доп. стоп-токен — модель не будет его повторять
        last_word = req.prefix.strip().split()[-1] if req.prefix.strip() else ""
        stop_seqs = ["\n", "Input:", "Output:"]
        if last_word and len(last_word) > 2:
            stop_seqs.append(last_word)
        try:
            response = await client.chat.completions.create(
                model=FAST_MODEL,
                messages=[
                    {"role": "system", "content": GHOST_SYSTEM},
                    {"role": "user", "content": f"Input: {req.prefix}\nOutput:"},
                ],
                temperature=0.05,
                max_tokens=20,
                stream=True,
                stop=stop_seqs,
            )
            async for chunk in response:
                txt = chunk.choices[0].delta.content or ""
                if txt:
                    yield f"data: {json.dumps({'text': txt}, ensure_ascii=False)}\n\n"
        except Exception:
            pass
        yield "data: [DONE]\n\n"

    return StreamingResponse(iter_tokens(), media_type="text/event-stream")


# ── Chips ─────────────────────────────────────────────────────────
@app.post("/suggest/chips")
async def suggest_chips(req: ChipsReq):
    last_q = req.history[-1] if req.history else ""
    user_msg = f'Ввод: "{req.input}"\nПоследний вопрос: "{last_q}"'
    try:
        response = await client.chat.completions.create(
            model=FAST_MODEL,
            messages=[
                {"role": "system", "content": CHIPS_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.5,
            max_tokens=200,
        )
        raw = response.choices[0].message.content or ""
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            suggestions = json.loads(m.group(0)).get("suggestions", [])
            suggestions = [s.strip().strip('"') for s in suggestions if s.strip()][:4]
            if suggestions:
                return {"suggestions": suggestions}
    except Exception:
        pass
    return {"suggestions": [
        "Покажи отмены по дням за последний месяц",
        "Динамика выручки за последние 30 дней",
        "Топ 10 водителей по поездкам",
        "Конверсия в поездку по часам",
    ]}


# ── Insight ───────────────────────────────────────────────────────
@app.post("/insight")
async def generate_insight(req: InsightReq):
    preview = req.rows[:50]
    header = "\t".join(req.columns)
    body = "\n".join("\t".join(str(v) for v in row) for row in preview)
    suffix = f"\n(и ещё {len(req.rows)-50} строк)" if len(req.rows) > 50 else ""
    try:
        resp = await client.chat.completions.create(
            model=STRONG_MODEL,
            messages=[
                {"role": "system", "content": INSIGHT_SYSTEM},
                {"role": "user", "content": f"Вопрос: {req.question}\n\nДанные:\n{header}\n{body}{suffix}"},
            ],
            temperature=0.3,
            max_tokens=200,
        )
        return {"insight": (resp.choices[0].message.content or "").strip()}
    except Exception as e:
        return {"insight": None, "error": str(e)}


# ── Community ─────────────────────────────────────────────────────
@app.get("/community")
async def get_community():
    con = _get_conn()
    rows = con.execute(
        "SELECT id, username, content, query_text, created_at, COALESCE(category, 'Аналитика') FROM community_posts ORDER BY created_at DESC"
    ).fetchall()
    con.close()
    return [{"id": r[0], "username": r[1], "content": r[2], "query_text": r[3], "created_at": r[4], "category": r[5]} for r in rows]

@app.post("/community")
async def create_post(req: PostReq):
    con = _get_conn()
    con.execute(
        "INSERT INTO community_posts (username, content, query_text, created_at, category) VALUES (?, ?, ?, ?, ?)",
        (req.username, req.content, req.query_text, datetime.now().isoformat(), req.category)
    )
    con.commit()
    con.close()
    return {"status": "success"}


# ── Reports CRUD ──────────────────────────────────────────────────
@app.get("/reports")
async def list_reports():
    return [r.to_dict() for r in reports_store.list_reports()]

@app.post("/reports")
async def create_report(body: ReportCreate):
    validated_sql = validate_and_fix_sql(body.sql)
    r = reports_store.create_report(
        name=body.name, question=body.question, sql=validated_sql,
        chart_type=body.chart_type, schedule_cron=body.schedule_cron,
        timezone=body.timezone, recipients=body.recipients, enabled=body.enabled,
    )
    if r.enabled and r.schedule_cron and _scheduler:
        _scheduler.schedule(r.id, r.schedule_cron, r.timezone or "Europe/Moscow")
    return r.to_dict()

@app.get("/reports/{report_id}")
async def get_report(report_id: int):
    r = reports_store.get_report(report_id)
    if not r:
        raise HTTPException(404, "Report not found")
    return r.to_dict()

@app.patch("/reports/{report_id}")
async def update_report(report_id: int, body: ReportUpdate):
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    r = reports_store.update_report(report_id, **fields)
    if not r:
        raise HTTPException(404, "Report not found")
    if _scheduler:
        _scheduler.sync(report_id)
    return r.to_dict()

@app.delete("/reports/{report_id}")
async def delete_report(report_id: int):
    if _scheduler:
        _scheduler.unschedule(report_id)
    ok = reports_store.delete_report(report_id)
    if not ok:
        raise HTTPException(404, "Report not found")
    return {"ok": True}

@app.post("/reports/{report_id}/run")
async def run_report_now(report_id: int):
    if not _scheduler:
        raise HTTPException(503, "Scheduler not ready")
    return await _scheduler.run_and_dispatch(report_id, trigger="manual")

@app.get("/reports/{report_id}/runs")
async def get_runs(report_id: int):
    return reports_store.list_runs(report_id)

@app.get("/reports/{report_id}/runs/{run_id}")
async def get_run_detail(report_id: int, run_id: int):
    run = reports_store.get_run(run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    return run

@app.get("/reports/{report_id}/last_result")
async def get_last_result(report_id: int):
    """Get the last successful run result for a report (used by dashboard widgets)."""
    runs = reports_store.list_runs(report_id)
    for run in runs:
        if run.get("status") == "success":
            detail = reports_store.get_run(run["id"])
            snap = detail.get("data_snapshot") if detail else None
            if snap and snap.get("columns"):
                return {"ok": True, "data": snap, "run": run}
    return {"ok": False}


# ── Alerts ────────────────────────────────────────────────────────
def _alert_row(r):
    return {
        "id": r[0], "name": r[1], "report_id": r[2], "operator": r[3],
        "threshold": r[4], "recipients": json.loads(r[5] or "[]"),
        "enabled": bool(r[6]), "last_checked_at": r[7],
        "last_triggered_at": r[8], "last_value": r[9], "created_at": r[10],
    }

@app.get("/alerts")
async def list_alerts():
    con = _get_conn()
    rows = con.execute("SELECT id,name,report_id,operator,threshold,recipients,enabled,last_checked_at,last_triggered_at,last_value,created_at FROM alerts ORDER BY created_at DESC").fetchall()
    con.close()
    return [_alert_row(r) for r in rows]

@app.post("/alerts")
async def create_alert(req: AlertReq):
    con = _get_conn()
    cur = con.execute(
        "INSERT INTO alerts (name,report_id,operator,threshold,recipients,enabled,created_at) VALUES (?,?,?,?,?,?,?)",
        (req.name, req.report_id, req.operator, req.threshold, json.dumps(req.recipients), int(req.enabled), datetime.now().isoformat())
    )
    row = con.execute("SELECT id,name,report_id,operator,threshold,recipients,enabled,last_checked_at,last_triggered_at,last_value,created_at FROM alerts WHERE id=?", (cur.lastrowid,)).fetchone()
    con.commit(); con.close()
    return _alert_row(row)

@app.patch("/alerts/{alert_id}")
async def update_alert(alert_id: int, req: AlertUpdate):
    fields = {k: v for k, v in req.model_dump().items() if v is not None}
    if "recipients" in fields:
        fields["recipients"] = json.dumps(fields["recipients"])
    if "enabled" in fields:
        fields["enabled"] = int(fields["enabled"])
    if not fields:
        raise HTTPException(400, "No fields")
    set_clause = ", ".join(f"{k}=?" for k in fields)
    con = _get_conn()
    con.execute(f"UPDATE alerts SET {set_clause} WHERE id=?", (*fields.values(), alert_id))
    row = con.execute("SELECT id,name,report_id,operator,threshold,recipients,enabled,last_checked_at,last_triggered_at,last_value,created_at FROM alerts WHERE id=?", (alert_id,)).fetchone()
    con.commit(); con.close()
    if not row:
        raise HTTPException(404, "Alert not found")
    return _alert_row(row)

@app.delete("/alerts/{alert_id}")
async def delete_alert(alert_id: int):
    con = _get_conn()
    con.execute("DELETE FROM alerts WHERE id=?", (alert_id,))
    con.commit(); con.close()
    return {"ok": True}

async def _check_alerts():
    """Run by scheduler every 15 min — evaluate each enabled alert."""
    con = _get_conn()
    alerts = con.execute("SELECT id,name,report_id,operator,threshold,recipients FROM alerts WHERE enabled=1").fetchall()
    con.close()
    ops = {"<": lambda a,b: a<b, ">": lambda a,b: a>b, "<=": lambda a,b: a<=b, ">=": lambda a,b: a>=b, "=": lambda a,b: a==b}
    for alert_id, name, report_id, operator, threshold, recipients_json in alerts:
        try:
            report = reports_store.get_report(report_id)
            if not report:
                continue
            result = await _run_sql(report.sql)
            if not result.get("rows"):
                continue
            value = result["rows"][0][0]
            if value is None or not isinstance(value, (int, float)):
                continue
            value = float(value)
            now = datetime.now().isoformat()
            triggered = ops.get(operator, lambda a,b: False)(value, threshold)
            con = _get_conn()
            con.execute("UPDATE alerts SET last_checked_at=?, last_value=?, last_triggered_at=? WHERE id=?",
                        (now, value, now if triggered else None, alert_id))
            if triggered:
                msg = f"Алерт «{name}»: значение {value:g} {operator} {threshold:g}"
                reports_store.record_delivery(
                    report_id=report_id,
                    run_id=None,
                    channel="inapp",
                    target=None,
                    status="sent",
                    subject=f"⚠️ Алерт: {name}",
                    preview=msg,
                )
            con.commit(); con.close()
        except Exception:
            pass


# ── Cron ─────────────────────────────────────────────────────────
@app.get("/cron/presets")
async def cron_presets():
    return PRESETS

@app.post("/cron/validate")
async def cron_validate(body: dict):
    return validate_cron(body.get("expr", ""), body.get("tz", "Europe/Moscow"))


# ── Inbox ─────────────────────────────────────────────────────────
@app.get("/inbox")
async def inbox(unread_only: bool = False):
    return reports_store.list_deliveries(limit=50, unread_only=unread_only)

@app.get("/inbox/unread_count")
async def inbox_unread():
    return {"count": reports_store.count_unread_deliveries()}

@app.post("/inbox/{delivery_id}/read")
async def mark_read(delivery_id: int):
    reports_store.mark_delivery_read(delivery_id)
    return {"ok": True}


# ── Health ────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    try:
        await client.chat.completions.create(
            model=FAST_MODEL,
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=1,
        )
        llm_ok = True
    except Exception:
        llm_ok = False
    return {
        "llm": llm_ok,
        "fast_model": FAST_MODEL,
        "strong_model": STRONG_MODEL,
        "db": Path(DB_PATH).exists(),
        "reports_db": Path(reports_store.DB_PATH).exists(),
    }

@app.delete("/inbox/{delivery_id}")
async def delete_inbox_item(delivery_id: int):
    """Delete a single inbox delivery."""
    if not reports_store.delete_delivery(delivery_id):
        raise HTTPException(404, "Delivery not found")
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
