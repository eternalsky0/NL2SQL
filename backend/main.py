import sqlite3
import json
import time
import re
import yaml
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import AsyncOpenAI
import pandas as pd
import sqlglot
from sqlglot import exp

# --- НАСТРОЙКИ ---
DB_PATH = '/Users/alinakobenko/Desktop/МПИТ/NL2SQL-main/data/drivee.db'
SEMANTIC_PATH = '/Users/alinakobenko/Desktop/МПИТ/NL2SQL-main/backend/semantic_layer.yaml'

client = AsyncOpenAI(base_url="http://localhost:11434/v1", api_key="local")
STRONG_MODEL = "qwen2.5-coder:7b"
FAST_MODEL = "qwen2.5-coder:1.5b"

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- ЗАГРУЗКА СЕМАНТИКИ ---
with open(SEMANTIC_PATH, "r", encoding="utf-8") as f:
    SEMANTIC = yaml.safe_load(f)

def get_semantic_prompt():
    m = SEMANTIC['metrics']
    metrics_desc = "\n".join([f"- {k} ({v['canonical']}): {v['sql']}" for k, v in m.items()])
    return f"ИСПОЛЬЗУЙ ЭТИ МЕТРИКИ ДЛЯ РАСЧЕТОВ:\n{metrics_desc}"

# --- СИСТЕМНЫЙ ПРОМПТ ---
SQL_SYSTEM = f"""Ты — Senior Data Analyst Drivee. Твоя задача — генерировать SQLite запросы.
СЕГОДНЯ: Четверг, 23 апреля 2026 года. "Вчера" — это '2026-04-22'.

СХЕМА ТАБЛИЦЫ orders:
(city_id, order_id, tender_id, user_id, driver_id, status_order, status_tender, order_timestamp, distance_in_meters, duration_in_seconds, price_order_local);

{get_semantic_prompt()}

КРИТИЧЕСКИЕ ПРАВИЛА:
1. ОТМЕНЫ (cancelled_orders): Всегда используй COUNT(DISTINCT order_id) WHERE status_order = 'cancel'. Это учитывает отмены и от клиентов, и от водителей.
2. ВЫРУЧКА (revenue): SUM(price_order_local) WHERE status_tender = 'done'.
3. СИНТАКСИС: Только SQLite. Никаких INTERVAL. Для дат: date(order_timestamp).
4. АЛИАСЫ: Всегда на русском языке в двойных кавычках.
5. ЛИМИТ: Всегда LIMIT 1000.

ОТВЕТ В ФОРМАТЕ JSON:
{{"sql": "SELECT...", "explanation": "Краткое описание на русском"}}
"""

class LoginReq(BaseModel):
    username: str
    password: str

class MessageItem(BaseModel):
    role: str
    content: str

class QueryReq(BaseModel):
    chat_id: Optional[int] = None
    username: str
    question: str
    history: List[MessageItem] = []

# --- ЭНДПОИНТЫ ---

@app.post("/login")
async def login(req: LoginReq):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    user = cur.execute("SELECT username, role FROM users WHERE username=? AND password=?", (req.username, req.password)).fetchone()
    con.close()
    if not user: raise HTTPException(401, "Неверные данные")
    return {"username": user[0], "role": user[1]}

@app.get("/chats/{username}")
async def get_chats(username: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    chats = cur.execute("SELECT id, title, created_at FROM chat_sessions WHERE username=? ORDER BY created_at DESC", (username,)).fetchall()
    con.close()
    return [{"id": c[0], "title": c[1], "created_at": c[2]} for c in chats]

@app.delete("/chats/{chat_id}")
async def delete_chat(chat_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM chat_sessions WHERE id=?", (chat_id,))
    cur.execute("DELETE FROM chat_messages WHERE chat_id=?", (chat_id,))
    con.commit()
    con.close()
    return {"status": "deleted"}

@app.post("/query")
async def query(req: QueryReq):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    
    # Проверка роли
    user_role = cur.execute("SELECT role FROM users WHERE username=?", (req.username,)).fetchone()[0]
    
    try:
        if not req.chat_id:
            cur.execute("INSERT INTO chat_sessions (username, title, created_at) VALUES (?, ?, ?)", 
                        (req.username, req.question[:50], datetime.now()))
            chat_id = cur.lastrowid
        else:
            chat_id = req.chat_id

        messages = [{"role": "system", "content": SQL_SYSTEM}]
        for h in req.history[-5:]: messages.append({"role": h.role, "content": h.content})
        messages.append({"role": "user", "content": req.question})

        response = await client.chat.completions.create(
            model=STRONG_MODEL, messages=messages, response_format={"type": "json_object"}
        )
        result = json.loads(response.choices[0].message.content)
        sql = result.get("sql", "")

        # Guardrails: только SELECT для обычных юзеров
        if user_role != 'admin' and not sql.strip().upper().startswith("SELECT"):
            raise ValueError("У вас нет прав на изменение базы данных. Разрешены только SELECT-запросы.")

        # Выполнение
        con_query = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        df = pd.read_sql_query(sql, con_query)
        con_query.close()

        data_payload = {
            "sql": sql,
            "explanation": result.get("explanation"),
            "data": {"columns": list(df.columns), "rows": df.fillna("").values.tolist()}
        }
        
        cur.execute("INSERT INTO chat_messages (chat_id, role, content, data, created_at) VALUES (?, 'assistant', ?, ?, ?)",
                    (chat_id, result.get("explanation"), json.dumps(data_payload), datetime.now()))
        con.commit()
        return {"chat_id": chat_id, "role": "assistant", "content": result.get("explanation"), "data": data_payload}
    except Exception as e:
        raise HTTPException(400, detail=str(e))
    finally:
        con.close()

@app.post("/suggest/ghost")
async def suggest_ghost(req: BaseModel): # упростил модель для краткости
    pass # логика ghost text
