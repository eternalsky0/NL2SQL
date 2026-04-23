import os
import re
import json
import time
import sqlite3
from pathlib import Path

import yaml
import pandas as pd
import sqlglot
from sqlglot import exp
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import AsyncOpenAI

from datetime import datetime


BASE_DIR = Path(__file__).parent
DB_PATH = '/Users/alinakobenko/Desktop/МПИТ/NL2SQL-main/data/drivee.db'

OLLAMA_URL = "http://localhost:11434/v1"
FAST_MODEL = "qwen2.5-coder:1.5b"
STRONG_MODEL = "qwen2.5-coder:7b"
client = AsyncOpenAI(base_url=OLLAMA_URL, api_key="local")

'''
OPENROUTER_URL = "https://openrouter.ai/api/v1"

# Укажи нужные модели из OpenRouter
FAST_MODEL = "qwen/qwen3.5-flash-02-23"   # Модель для быстрых подсказок (ghost)
STRONG_MODEL = "qwen/qwen3.5-flash-02-23" # Модель для генерации SQL (можешь поставить qwen/qwen-2.5-coder-32b-instruct)

client = AsyncOpenAI(
    base_url=OPENROUTER_URL,
    api_key="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", # Вставь сюда свой ключ
    default_headers={
        "HTTP-Referer": "http://localhost:8000", # OpenRouter рекомендует передавать эти заголовки
        "X-Title": "Drivee NL2SQL"
    }
)
'''
with open('/Users/alinakobenko/Desktop/МПИТ/NL2SQL-main/backend/semantic_layer.yaml', "r", encoding="utf-8") as f:
    SEMANTIC = yaml.safe_load(f)

def get_semantic_prompt():
    metrics = "\n".join([f"- {m['canonical']}: {m['sql']}" for m in SEMANTIC['metrics'].values()])
    return f"МЕТРИКИ:\n{metrics}"

SQL_SYSTEM = f"""Ты — Senior Data Analyst сервиса такси Drivee. 
Генерируешь SQLite-запросы. ОТВЕЧАЙ СТРОГО JSON ФОРМАТОМ.

СХЕМА:
TABLE orders (city_id int, order_id text, tender_id text, user_id text, driver_id text, status_order text, status_tender text, order_timestamp timestamp, distance_in_meters real, duration_in_seconds real, price_order_local real);

{get_semantic_prompt()}

КРИТИЧЕСКИЕ ПРАВИЛА ИМЕНОВАНИЯ:
1. ВСЕГДА называй результирующие колонки (алиасы) НА РУССКОМ ЯЗЫКЕ.
2. Используй понятные бизнес-термины: Выручка, Количество заказов, Количество отмен, Город, Средний чек, Дата.
3. ВАЖНО: Если алиас состоит из нескольких слов, ОН ОБЯЗАТЕЛЬНО ДОЛЖЕН БЫТЬ В ДВОЙНЫХ КАВЫЧКАХ ВЕЗДЕ, особенно в ORDER BY и GROUP BY.
   Правильно: ORDER BY "Количество отмен" DESC
   Неправильно: ORDER BY Количество отмен DESC

ПРАВИЛА ДЛЯ ДАТ:
- Период: с 2025-01-02 по 2026-04-20. Макс дата: '2026-04-20'.
- "Вчера" — это '2026-04-19'.

ОБЩИЕ ПРАВИЛА:
- Только SELECT. LIMIT 1000.
- Для денег/дистанции используй фильтр WHERE status_tender = 'done'.

ФОРМАТ ОТВЕТА:
{{
  "sql": "SELECT ... AS 'Название на русском'...",
  "explanation": "краткое объяснение",
  "chart_type": "bar | line | table",
  "confidence": 0.95
}}
"""
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

class QueryReq(BaseModel):
    question: str

class GhostReq(BaseModel):
    prefix: str

# === GUARDRAILS ===
def validate_sql(sql: str) -> str:
    try:
        parsed = sqlglot.parse_one(sql, dialect="sqlite")
    except Exception as e:
        raise ValueError(f"Синтаксическая ошибка: {e}")
    
    if not isinstance(parsed, exp.Select):
        raise ValueError("Разрешены только SELECT-запросы")
    
    allowed = set(SEMANTIC["rules"]["allowed_tables"])
    tables = {t.name.lower() for t in parsed.find_all(exp.Table)}
    if tables - allowed:
        raise ValueError(f"Доступ к запрещенным таблицам: {tables - allowed}")
    
    if not parsed.args.get("limit"):
        parsed = parsed.limit(SEMANTIC["rules"]["default_limit"])
        
    return parsed.sql(dialect="sqlite")

# === ЭНДПОИНТЫ ===
@app.post("/query")
async def query(req: QueryReq):
    t_start = time.time()
    
    try:
        response = await client.chat.completions.create(
            model=STRONG_MODEL,
            messages=[
                {"role": "system", "content": SQL_SYSTEM},
                {"role": "user", "content": f"Вопрос: {req.question}"}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        content = response.choices[0].message.content
        
        print("\n" + "="*40)
        print(f"ВОПРОС: {req.question}")
        print(f"ОТВЕТ LLM:\n{content}")
        print("="*40 + "\n")
        
        result = json.loads(content)
    except Exception as e:
        raise HTTPException(500, f"Ошибка LLM: {str(e)}")

    sql_raw = result.get("sql", "")
    
    try:
        sql_safe = validate_sql(sql_raw)
    except Exception as e:
        return {"error": "guardrails", "message": str(e), "sql": sql_raw, "explanation": result.get("explanation")}

    try:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        df = pd.read_sql_query(sql_safe, con)
        con.close()
    except Exception as e:
        return {"error": "db", "message": str(e), "sql": sql_safe}

    return {
        "question": req.question,
        "sql": sql_safe,
        "explanation": result.get("explanation"),
        "chart_type": result.get("chart_type", "table"),
        "confidence": result.get("confidence", 0.9),
        "data": {
            "columns": list(df.columns),
            "rows": df.fillna("").values.tolist(),
        },
        "timings": {"total_ms": int((time.time() - t_start) * 1000)}
    }

@app.post("/suggest/ghost")
async def suggest_ghost(req: GhostReq):
    async def iter_tokens():
        try:
            response = await client.chat.completions.create(
                model=FAST_MODEL,
                messages=[
                    {"role": "system", "content": "Продолжи запрос аналитики 2-5 словами. Не повторяй начало. Выведи только продолжение."},
                    {"role": "user", "content": f'"{req.prefix}"'}
                ],
                temperature=0.2,
                max_tokens=20,
                stream=True
            )
            async for chunk in response:
                txt = chunk.choices[0].delta.content or ""
                if txt:
                    yield f"data: {json.dumps({'text': txt}, ensure_ascii=False)}\n\n"
        except:
            pass
        yield "data: [DONE]\n\n"

    return StreamingResponse(iter_tokens(), media_type="text/event-stream")

@app.get("/kpi")
def kpi():
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT 
            COUNT(DISTINCT order_id) as orders,
            SUM(CASE WHEN status_order='done' THEN 1 ELSE 0 END) as done,
            SUM(CASE WHEN status_order='cancel' THEN 1 ELSE 0 END) as cancel
        FROM orders
    """, con)
    con.close()

    return df.to_dict(orient="records")[0]

def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saved_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            question TEXT,
            sql_query TEXT,
            explanation TEXT,
            created_at TIMESTAMP
        )
    """)
    con.commit()
    con.close()

init_db()

class SaveReportReq(BaseModel):
    name: str
    question: str
    sql: str
    explanation: str

# Эндпоинт для сохранения
@app.post("/reports")
async def save_report(req: SaveReportReq):
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO saved_reports (name, question, sql_query, explanation, created_at) VALUES (?, ?, ?, ?, ?)",
            (req.name, req.question, req.sql, req.explanation, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        con.commit()
        con.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(500, str(e))

# Эндпоинт для получения списка
@app.get("/reports")
async def get_reports():
    con = sqlite3.connect(DB_PATH)
    # Позволяем pandas прочитать список
    df = pd.read_sql_query("SELECT * FROM saved_reports ORDER BY created_at DESC", con)
    con.close()
    return df.to_dict(orient="records")

# Эндпоинт для удаления отчета
@app.delete("/reports/{report_id}")
async def delete_report(report_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM saved_reports WHERE id = ?", (report_id,))
    con.commit()
    con.close()
    return {"status": "deleted"}

class ExecuteReq(BaseModel):
    sql: str
    explanation: str
    question: str

@app.post("/execute_report")
async def execute_report(req: ExecuteReq):
    # Используем существующую валидацию для безопасности
    try:
        sql_safe = validate_sql(req.sql)
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        df = pd.read_sql_query(sql_safe, con)
        con.close()
        
        return {
            "question": req.question,
            "sql": sql_safe,
            "explanation": req.explanation,
            "data": {
                "columns": list(df.columns),
                "rows": df.fillna("").values.tolist(),
            }
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
