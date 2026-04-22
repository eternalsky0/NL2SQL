"""
Drivee NL2SQL — FastAPI backend.

Архитектура:
    Уровень 1 (ghost text):  Ollama + Qwen2.5-Coder 1.5B    ~100-300 мс
    Уровень 2 (chips):       Ollama + Qwen2.5-Coder 1.5B    ~300-600 мс
    Уровень 3 (full SQL):    Ollama (Qwen 7B) или OpenAI     ~1-3 сек

Эндпоинты:
    POST /suggest/ghost      — ghost-text по префиксу (стрим)
    POST /suggest/chips      — контекстные подсказки (3-5 шт)
    POST /query              — полный пайплайн NL -> SQL -> данные -> график
    POST /reports            — сохранить отчёт
    GET  /reports            — список сохранённых
    GET  /health             — проверка всех зависимостей
"""

import os
import re
import json
import time
import sqlite3
import logging
from datetime import datetime
from typing import Optional, Any
from pathlib import Path

import yaml
import httpx
import pandas as pd
import sqlglot
from sqlglot import exp
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# ==============================================================
# КОНФИГУРАЦИЯ
# ==============================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("drivee")

BASE_DIR = Path(__file__).parent

# --- LLM ---
# По умолчанию всё через Ollama. При желании уровень 3 можно переключить
# на OpenAI/Groq/GigaChat, задав переменные окружения.
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
FAST_MODEL = os.getenv("FAST_MODEL", "qwen2.5-coder:1.5b")
STRONG_MODEL = os.getenv("STRONG_MODEL", "qwen2.5-coder:3b")

# Альтернативный провайдер для уровня 3 (опционально)
STRONG_PROVIDER = os.getenv("STRONG_PROVIDER", "ollama")  # ollama | openai | groq
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_URL = os.getenv("OPENAI_URL", "https://api.openai.com/v1/chat/completions")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# --- БД ---
# Для MVP используем SQLite — не требует установки Postgres.
# Синтаксис SQL адаптирован в промптах.
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR.parent / "data" / "drivee.db"))
REPORTS_DB = str(BASE_DIR.parent / "data" / "reports.db")


# ==============================================================
# СЕМАНТИЧЕСКИЙ СЛОЙ
# ==============================================================

with open(BASE_DIR / "semantic_layer.yaml", "r", encoding="utf-8") as f:
    SEMANTIC = yaml.safe_load(f)


def semantic_brief() -> str:
    """Компактная сводка семантики для промпта. Кэшируется на старте."""
    parts = ["=== МЕТРИКИ ==="]
    for m in SEMANTIC["metrics"].values():
        syns = ", ".join(m.get("synonyms", [])[:3])
        parts.append(f"- {m['canonical']} ({syns})")

    parts.append("\n=== ИЗМЕРЕНИЯ ===")
    for d in SEMANTIC["dimensions"].values():
        parts.append(f"- {d['canonical']}")

    parts.append("\n=== ПЕРИОДЫ ===")
    for p in SEMANTIC["periods"].values():
        parts.append(f"- {p['canonical']}")

    parts.append("\n=== ГОРОДА ===")
    parts.append(", ".join(SEMANTIC["entities"]["cities"].keys()))

    return "\n".join(parts)


SEMANTIC_BRIEF = semantic_brief()


def schema_ddl() -> str:
    """DDL берём напрямую из реальной SQLite БД — это источник правды."""
    import sqlite3
    con = sqlite3.connect(DB_PATH)
    tables = [r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()]
    lines = []
    for t in tables:
        cols = con.execute(f"PRAGMA table_info({t})").fetchall()
        col_lines = [f"  {c[1]} {c[2]}" for c in cols]
        lines.append(f"TABLE {t} (\n" + ",\n".join(col_lines) + "\n);")
    con.close()
    return "\n\n".join(lines)


SCHEMA_DDL = schema_ddl()


# ==============================================================
# LLM-ВЫЗОВЫ
# ==============================================================

async def ollama_generate(
    model: str,
    system: str,
    user: str,
    temperature: float = 0.2,
    max_tokens: int = 80,
    stop: Optional[list[str]] = None,
    stream: bool = False,
):
    """Универсальный вызов Ollama /api/chat. Возвращает str или async-итератор."""
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": stream,
        "options": {
            "temperature": temperature,
            "num_predict": max_tokens,
            "stop": stop or [],
        },
    }

    if not stream:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(f"{OLLAMA_URL}/api/chat", json=payload)
            r.raise_for_status()
            return r.json()["message"]["content"].strip()

    # Стриминг: возвращаем async-генератор токенов
    async def iter_tokens():
        async with httpx.AsyncClient(timeout=30.0) as client:
            async with client.stream("POST", f"{OLLAMA_URL}/api/chat", json=payload) as r:
                async for line in r.aiter_lines():
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        chunk = obj.get("message", {}).get("content", "")
                        if chunk:
                            yield chunk
                        if obj.get("done"):
                            break
                    except json.JSONDecodeError:
                        continue

    return iter_tokens()


async def strong_llm_json(system: str, user: str) -> dict:
    """Вызов сильной модели для генерации SQL. Возвращает JSON-словарь."""
    if STRONG_PROVIDER == "openai":
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(
                OPENAI_URL,
                headers={"Authorization": f"Bearer {OPENAI_KEY}"},
                json={
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.1,
                },
            )
            r.raise_for_status()
            raw = r.json()["choices"][0]["message"]["content"]
    else:
        raw = await ollama_generate(STRONG_MODEL, system, user, temperature=0.1, max_tokens=500)

    # Достаём JSON из ответа (убираем ```json ... ``` обёртки)
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.MULTILINE)
    # На случай если модель добавила пояснения — ищем первый {...}
    m = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if m:
        cleaned = m.group(0)
    return json.loads(cleaned)


# ==============================================================
# УРОВЕНЬ 1 — GHOST TEXT
# ==============================================================

GHOST_CACHE: dict[str, str] = {}
GHOST_CACHE_MAX = 2000

GHOST_SYSTEM = f"""Ты — автодополнение для чата аналитики в сервисе такси Drivee.
Продолжи запрос пользователя кратко (до 10 слов), на русском, с тем же регистром.

Доступные метрики и измерения:
{SEMANTIC_BRIEF}

Правила:
- Не повторяй то, что пользователь уже ввёл
- Верни ТОЛЬКО продолжение, без кавычек и пояснений
- Если не уверен — продолжи стандартным шаблоном: "по городам за последнюю неделю"

Примеры:
Ввод: "покажи выручку"
Продолжение: по городам за последний месяц

Ввод: "сравни отмены"
Продолжение: в этом месяце с прошлым

Ввод: "топ водителей"
Продолжение: по количеству поездок за неделю

Ввод: "средний чек по"
Продолжение: классам машин за прошлую неделю

Ввод: "динамика"
Продолжение: выручки за последние 30 дней
"""


class GhostReq(BaseModel):
    prefix: str = Field(..., max_length=200)


async def ghost_stream(prefix: str):
    """SSE-стрим ghost-текста."""
    # Кэш — ключевая оптимизация: один и тот же префикс даёт один и тот же ответ
    if prefix in GHOST_CACHE:
        yield f"data: {json.dumps({'text': GHOST_CACHE[prefix], 'cached': True}, ensure_ascii=False)}\n\n"
        yield "data: [DONE]\n\n"
        return

    # Слишком короткие префиксы (1-2 символа) не стоит дополнять — будет мусор
    if len(prefix.strip()) < 3:
        yield "data: [DONE]\n\n"
        return

    user = f'Ввод: "{prefix}"\nПродолжение:'
    full = ""
    try:
        token_iter = await ollama_generate(
            FAST_MODEL, GHOST_SYSTEM, user,
            temperature=0.2, max_tokens=30, stop=["\n", "Ввод:", "."],
            stream=True,
        )
        async for chunk in token_iter:
            full += chunk
            yield f"data: {json.dumps({'text': chunk}, ensure_ascii=False)}\n\n"

        # Санитизация: убираем повтор префикса, если модель его дублирует
        full = full.strip()
        if full.lower().startswith(prefix.lower().strip()):
            full = full[len(prefix):].lstrip()

        # Кэшируем очищенную версию
        if len(GHOST_CACHE) < GHOST_CACHE_MAX:
            GHOST_CACHE[prefix] = full

    except Exception as e:
        log.warning(f"ghost_stream error: {e}")

    yield "data: [DONE]\n\n"


# ==============================================================
# УРОВЕНЬ 2 — CHIPS (контекстные подсказки)
# ==============================================================

CHIPS_SYSTEM = f"""Ты — генератор подсказок для чата аналитики.
На основе ввода пользователя предложи 4 коротких варианта завершённых вопросов.

Доступно:
{SEMANTIC_BRIEF}

Правила:
- Каждая подсказка — законченный осмысленный вопрос на русском
- До 10 слов на подсказку
- Разные грани: разные метрики, разные периоды, разные разрезы
- Если есть история — учитывай её, предлагай логичные продолжения (другой период, другой город, drill-down)
- Верни ТОЛЬКО JSON: {{"suggestions": ["...", "...", "...", "..."]}}

Примеры:

Ввод пользователя: "" (пустой)
История: нет
Ответ: {{"suggestions": ["покажи отмены по городам за прошлую неделю", "динамика выручки за последний месяц", "топ водителей по поездкам", "средний чек по классам машин"]}}

Ввод пользователя: "отмены"
История: нет
Ответ: {{"suggestions": ["отмены по городам за прошлую неделю", "отмены по часам за вчера", "доля отмен по классам машин", "динамика отмен за последний месяц"]}}

Ввод пользователя: "а теперь"
История: "покажи отмены по Москве за прошлую неделю"
Ответ: {{"suggestions": ["а теперь по Санкт-Петербургу", "а теперь за прошлый месяц", "а теперь с разбивкой по часам", "а теперь сравни с предыдущей неделей"]}}
"""


class ChipsReq(BaseModel):
    input: str = ""
    history: list[str] = []


async def gen_chips(input_text: str, history: list[str]) -> list[str]:
    last = history[-1] if history else "нет"
    user = f'Ввод пользователя: "{input_text}"\nИстория: "{last}"\nОтвет:'
    try:
        raw = await ollama_generate(
            FAST_MODEL, CHIPS_SYSTEM, user,
            temperature=0.5, max_tokens=250,
        )
        # Достаём JSON
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if not m:
            raise ValueError("no json in response")
        obj = json.loads(m.group(0))
        suggestions = obj.get("suggestions", [])
        # Санитизация: убираем дубли, пустые, обрезаем длинные
        seen, clean = set(), []
        for s in suggestions:
            s = str(s).strip().strip('"')
            if s and s.lower() not in seen and len(s) < 120:
                seen.add(s.lower())
                clean.append(s)
        return clean[:4]
    except Exception as e:
        log.warning(f"chips error: {e}, fallback to templates")
        # Fallback на шаблоны из семантического слоя
        return SEMANTIC.get("templates", [])[:4]


# ==============================================================
# УРОВЕНЬ 3 — ПОЛНЫЙ ПАЙПЛАЙН NL -> SQL -> DATA
# ==============================================================

SQL_SYSTEM = f"""Ты — SQL-аналитик сервиса такси Drivee. Генерируешь SQLite-запросы.

СХЕМА БД:
{SCHEMA_DDL}

БИЗНЕС-СЛОВАРЬ (канонические метрики и их SQL):
{chr(10).join(f"- {m['canonical']}: {m['sql']}" for m in SEMANTIC['metrics'].values())}

ИЗМЕРЕНИЯ:
{chr(10).join(f"- {d['canonical']}: {d.get('column', '')}" for d in SEMANTIC['dimensions'].values())}

ПЕРИОДЫ (фильтры по created_at):
{chr(10).join(f"- {p['canonical']}: {p['sql'].strip()}" for p in SEMANTIC['periods'].values())}

ПРАВИЛА:
1. ТОЛЬКО SELECT. Никаких INSERT/UPDATE/DELETE/DROP.
2. Обязательно LIMIT 1000.
3. Используй точные SQL-выражения из бизнес-словаря — не придумывай свои.
4. Если метрика неоднозначна — выбери самую вероятную, но снизь confidence.
5. Для SQLite используй date('now', '-7 days') вместо INTERVAL.
6. КРИТИЧНО: используй ТОЛЬКО колонки, явно перечисленные в схеме выше.
Если нужной колонки нет — переформулируй запрос, не выдумывай имена.
Помни: registered_at у drivers и clients, а created_at у orders. Не путай.

ФОРМАТ ОТВЕТА — строго JSON:
{{
  "sql": "SELECT ...",
  "explanation": "Краткое пояснение на русском, что я понял",
  "chart_type": "bar | line | pie | table",
  "confidence": 0.0-1.0,
  "clarification_needed": null или "уточняющий вопрос"
}}
"""


class QueryReq(BaseModel):
    question: str
    history: list[dict] = []  # [{"role": "user|assistant", "content": "..."}]


def adapt_sql_to_sqlite(sql: str) -> str:
    """Адаптирует Postgres-подобный SQL из промпта к диалекту SQLite.
    Минимальный набор замен для MVP.
    """
    replacements = [
        (r"CURRENT_DATE\b", "date('now')"),
        (r"INTERVAL\s+'(\d+)\s*days?'", r"'\1 days'"),
        (r"INTERVAL\s+'(\d+)\s*weeks?'", r"'\1 * 7 days'"),
        (r"INTERVAL\s+'(\d+)\s*months?'", r"'\1 months'"),
        (r"DATE_TRUNC\('week',\s*([^)]+)\)", r"date(\1, 'weekday 1', '-7 days')"),
        (r"DATE_TRUNC\('month',\s*([^)]+)\)", r"date(\1, 'start of month')"),
        (r"EXTRACT\(HOUR FROM\s*([^)]+)\)", r"CAST(strftime('%H', \1) AS INTEGER)"),
        (r"TO_CHAR\(([^,]+),\s*'Day'\)", r"strftime('%w', \1)"),
        (r"::float", ""),
        (r"::int", ""),
        (r"FILTER\s*\(WHERE\s+([^)]+)\)", r" * (CASE WHEN \1 THEN 1 ELSE 0 END)"),
        # Упрощение для SUM/COUNT FILTER — SQLite не поддерживает напрямую
    ]
    result = sql
    for pat, rep in replacements:
        result = re.sub(pat, rep, result, flags=re.IGNORECASE)
    return result


# ==============================================================
# SLOY C — GUARDRAILS
# ==============================================================

class SQLValidationError(Exception):
    pass


def validate_sql(sql: str) -> str:
    """Каскад проверок. Возвращает нормализованный SQL или бросает SQLValidationError."""
    try:
        parsed = sqlglot.parse_one(sql, dialect="sqlite")
    except Exception as e:
        raise SQLValidationError(f"Невалидный SQL: {e}")

    if parsed is None:
        raise SQLValidationError("Пустой SQL")

    # 1. Только SELECT
    if not isinstance(parsed, exp.Select):
        raise SQLValidationError("Разрешены только SELECT-запросы")

    # 2. Запрет DML/DDL (на случай если парсер пропустит вложенное)
    forbidden_types = (exp.Insert, exp.Update, exp.Delete, exp.Drop,
                       exp.Alter, exp.Create, exp.TruncateTable)
    for ft in forbidden_types:
        if parsed.find(ft):
            raise SQLValidationError(f"Запрещённая операция: {ft.__name__}")

    # 3. Проверка таблиц из whitelist
    allowed = set(SEMANTIC["rules"]["allowed_tables"])
    tables = {t.name.lower() for t in parsed.find_all(exp.Table)}
    not_allowed = tables - allowed
    if not_allowed:
        raise SQLValidationError(f"Недопустимые таблицы: {not_allowed}")

    # 4. Принудительный LIMIT
    limit_default = SEMANTIC["rules"]["default_limit"]
    if not parsed.args.get("limit"):
        parsed = parsed.limit(limit_default)

    # 5. Маскирование PII — просто запрещаем упоминание чувствительных колонок
    pii = {c.lower() for c in SEMANTIC["rules"]["pii_columns"]}
    for col in parsed.find_all(exp.Column):
        full = f"{col.table}.{col.name}".lower() if col.table else col.name.lower()
        if full in pii or col.name.lower() in {p.split(".")[-1] for p in pii}:
            raise SQLValidationError(f"Доступ к PII-колонке запрещён: {col.name}")

    return parsed.sql(dialect="sqlite")


# ==============================================================
# ВЫПОЛНЕНИЕ SQL
# ==============================================================

def execute_sql(sql: str, timeout_sec: int = 10) -> dict:
    """Выполняет SELECT в SQLite read-only режиме."""
    # read-only подключение: uri=True + mode=ro
    uri = f"file:{DB_PATH}?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=timeout_sec)
    try:
        con.set_progress_handler(None, 1_000_000)  # soft limit на операции
        df = pd.read_sql_query(sql, con)
    finally:
        con.close()

    # Ограничиваем размер возвращаемых данных
    MAX_ROWS = SEMANTIC["rules"]["default_limit"]
    truncated = False
    if len(df) > MAX_ROWS:
        df = df.head(MAX_ROWS)
        truncated = True

    return {
        "columns": list(df.columns),
        "rows": df.values.tolist(),
        "row_count": len(df),
        "truncated": truncated,
    }


# ==============================================================
# SLOY D — АВТОГРАФИК + ИНСАЙТЫ
# ==============================================================

def choose_chart(chart_hint: str, columns: list[str], rows: list) -> str:
    """Автовыбор типа графика. chart_hint от LLM приоритетнее."""
    if chart_hint in {"bar", "line", "pie", "table"}:
        return chart_hint
    if len(columns) < 2:
        return "table"
    if len(rows) > 30:
        return "table"
    first_col = columns[0].lower()
    if any(k in first_col for k in ["дата", "date", "день", "неделя", "месяц", "час"]):
        return "line"
    return "bar"


async def gen_insights(question: str, columns: list[str], rows: list) -> list[str]:
    """Краткие наблюдения по данным. Лёгкая модель, короткий ответ."""
    if not rows:
        return []
    # Усечённая таблица для промпта
    sample = rows[:10]
    data_str = f"Колонки: {columns}\nДанные (первые {len(sample)} строк): {sample}"

    system = "Ты бизнес-аналитик. На основе данных сформулируй 2-3 коротких наблюдения на русском. Верни JSON: {\"insights\": [\"...\", \"...\"]}. Каждое наблюдение — до 15 слов."
    user = f"Вопрос: {question}\n{data_str}"

    try:
        raw = await ollama_generate(FAST_MODEL, system, user, temperature=0.3, max_tokens=200)
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            return json.loads(m.group(0)).get("insights", [])[:3]
    except Exception as e:
        log.warning(f"insights error: {e}")
    return []


async def gen_followups(question: str, sql: str) -> list[str]:
    """Follow-up вопросы, которые логично задать после этого запроса."""
    system = (
        "Ты предлагаешь логичные follow-up вопросы. "
        "Верни JSON: {\"followups\": [\"...\", \"...\", \"...\"]}. "
        "По одной строке, каждый — короткий завершённый вопрос на русском."
    )
    user = f"Предыдущий вопрос: {question}\nSQL: {sql}\nПредложи 3 follow-up вопроса:"
    try:
        raw = await ollama_generate(FAST_MODEL, system, user, temperature=0.5, max_tokens=200)
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            return json.loads(m.group(0)).get("followups", [])[:3]
    except Exception as e:
        log.warning(f"followups error: {e}")
    return []


# ==============================================================
# ОТЧЁТЫ
# ==============================================================

def init_reports_db():
    con = sqlite3.connect(REPORTS_DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            question TEXT NOT NULL,
            sql TEXT NOT NULL,
            chart_type TEXT,
            schedule_cron TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.commit()
    con.close()


class ReportIn(BaseModel):
    name: str
    question: str
    sql: str
    chart_type: str = "bar"
    schedule_cron: Optional[str] = None


# ==============================================================
# FASTAPI APP
# ==============================================================

app = FastAPI(title="Drivee NL2SQL", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    init_reports_db()
    log.info(f"Загружено метрик: {len(SEMANTIC['metrics'])}, "
             f"измерений: {len(SEMANTIC['dimensions'])}, "
             f"периодов: {len(SEMANTIC['periods'])}")
    log.info(f"LLM: fast={FAST_MODEL}, strong={STRONG_MODEL} ({STRONG_PROVIDER})")


@app.get("/health")
async def health():
    """Проверяет доступность Ollama, БД и конфиг."""
    status = {"ollama": False, "db": False, "reports_db": False}
    try:
        async with httpx.AsyncClient(timeout=3.0) as c:
            r = await c.get(f"{OLLAMA_URL}/api/tags")
            status["ollama"] = r.status_code == 200
            if status["ollama"]:
                models = [m["name"] for m in r.json().get("models", [])]
                status["ollama_models"] = models
                status["fast_loaded"] = any(FAST_MODEL.split(":")[0] in m for m in models)
    except Exception as e:
        status["ollama_error"] = str(e)
    try:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        con.execute("SELECT 1")
        con.close()
        status["db"] = True
    except Exception as e:
        status["db_error"] = str(e)
    try:
        con = sqlite3.connect(REPORTS_DB)
        con.execute("SELECT 1 FROM reports LIMIT 1")
        con.close()
        status["reports_db"] = True
    except Exception:
        pass
    return status


@app.post("/suggest/ghost")
async def suggest_ghost(req: GhostReq):
    """SSE-стрим ghost-текста."""
    return StreamingResponse(ghost_stream(req.prefix), media_type="text/event-stream")


@app.post("/suggest/chips")
async def suggest_chips(req: ChipsReq):
    """Контекстные chips-подсказки."""
    t0 = time.time()
    suggestions = await gen_chips(req.input, req.history)
    return {
        "suggestions": suggestions,
        "latency_ms": int((time.time() - t0) * 1000),
    }


@app.post("/query")
async def query(req: QueryReq):
    """Полный пайплайн: NL -> понимание -> SQL -> выполнение -> график -> инсайты."""
    t_start = time.time()
    timings = {}

    # --- Слой B: Генерация SQL ---
    t0 = time.time()
    try:
        result = await strong_llm_json(SQL_SYSTEM, f"Вопрос: {req.question}")
    except Exception as e:
        raise HTTPException(500, f"Ошибка LLM: {e}")
    timings["llm_ms"] = int((time.time() - t0) * 1000)

    sql_raw = result.get("sql", "").strip()
    explanation = result.get("explanation", "")
    chart_hint = result.get("chart_type", "bar")
    confidence = float(result.get("confidence", 0.5))
    clarification = result.get("clarification_needed")

    # Если модель сама запрашивает уточнение — возвращаем его сразу
    if clarification and confidence < 0.6:
        return {
            "needs_clarification": True,
            "clarification": clarification,
            "explanation": explanation,
            "confidence": confidence,
        }

    # --- Слой C: адаптация к SQLite + валидация ---
    sql_adapted = adapt_sql_to_sqlite(sql_raw)
    try:
        sql_safe = validate_sql(sql_adapted)
    except SQLValidationError as e:
        return {
            "error": "sql_validation",
            "message": str(e),
            "sql_raw": sql_raw,
            "sql_adapted": sql_adapted,
            "explanation": explanation,
        }

    # --- Выполнение ---
    t0 = time.time()
    try:
        data = execute_sql(sql_safe)
    except Exception as e:
        return {
            "error": "execution",
            "message": str(e),
            "sql": sql_safe,
            "explanation": explanation,
        }
    timings["sql_ms"] = int((time.time() - t0) * 1000)

    # --- Слой D: график + инсайты + follow-ups ---
    chart = choose_chart(chart_hint, data["columns"], data["rows"])

    t0 = time.time()
    insights = await gen_insights(req.question, data["columns"], data["rows"])
    timings["insights_ms"] = int((time.time() - t0) * 1000)

    t0 = time.time()
    followups = await gen_followups(req.question, sql_safe)
    timings["followups_ms"] = int((time.time() - t0) * 1000)

    timings["total_ms"] = int((time.time() - t_start) * 1000)

    return {
        "question": req.question,
        "sql": sql_safe,
        "explanation": explanation,
        "chart_type": chart,
        "confidence": confidence,
        "data": data,
        "insights": insights,
        "followups": followups,
        "timings": timings,
    }


@app.post("/reports")
async def save_report(r: ReportIn):
    con = sqlite3.connect(REPORTS_DB)
    cur = con.execute(
        "INSERT INTO reports (name, question, sql, chart_type, schedule_cron) VALUES (?, ?, ?, ?, ?)",
        (r.name, r.question, r.sql, r.chart_type, r.schedule_cron),
    )
    report_id = cur.lastrowid
    con.commit()
    con.close()
    return {"id": report_id, "saved": True}


@app.get("/reports")
async def list_reports():
    con = sqlite3.connect(REPORTS_DB)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM reports ORDER BY created_at DESC").fetchall()
    con.close()
    return [dict(r) for r in rows]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
