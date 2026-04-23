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

# --- КОНФИГУРАЦИЯ ---
DB_PATH = '/Users/alinakobenko/Desktop/МПИТ/NL2SQL-main/data/drivee.db'
SEMANTIC_PATH = '/Users/alinakobenko/Desktop/МПИТ/NL2SQL-main/backend/semantic_layer.yaml'

# Настройки LLM (Ollama)
client = AsyncOpenAI(base_url="http://localhost:11434/v1", api_key="local")
STRONG_MODEL = "qwen2.5-coder:7b"
FAST_MODEL = "qwen2.5-coder:1.5b"

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- ЗАГРУЗКА СЕМАНТИЧЕСКОГО СЛОЯ ---
def get_semantic_context():
    try:
        with open(SEMANTIC_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            metrics = data.get('metrics', {})
            context = "ИСПОЛЬЗУЙ ЭТИ МЕТРИКИ ДЛЯ SQL:\n"
            for k, v in metrics.items():
                context += f"- {v['canonical']}: {v['sql']}\n"
            return context
    except:
        return ""

# --- GUARDRAILS (ЗАЩИТА И КОРРЕКЦИЯ) ---
def validate_and_fix_sql(sql: str, role: str) -> str:
    # 1. Очистка от markdown
    sql = sql.replace("```sql", "").replace("```", "").strip()
    
    # 2. Исправление кавычек для SQLite (алиасы в ORDER BY)
    sql = re.sub(r"ORDER\s+BY\s+'([^']+)'", r'ORDER BY "\1"', sql, flags=re.IGNORECASE)
    sql = re.sub(r"GROUP\s+BY\s+'([^']+)'", r'GROUP BY "\1"', sql, flags=re.IGNORECASE)

    # 3. Проверка безопасности для не-админов
    if role != 'admin':
        upper_sql = sql.upper()
        forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE"]
        if any(x in upper_sql for x in forbidden):
            raise ValueError("Обычным пользователям разрешены только запросы SELECT.")

    try:
        parsed = sqlglot.parse_one(sql, dialect="sqlite")
        if not parsed.args.get("limit"):
            parsed = parsed.limit(1000)
        return parsed.sql(dialect="sqlite")
    except Exception as e:
        raise ValueError(f"Ошибка синтаксиса: {str(e)}")

# --- СИСТЕМНЫЙ ПРОМПТ ---
SQL_SYSTEM = f"""Ты — Senior Data Analyst Drivee. Твоя задача — генерировать SQLite запросы.
СЕГОДНЯ: Четверг, 23 апреля 2026 года. 
"Вчера" — это всегда '2026-04-22'.
"Сегодня" — это всегда '2026-04-23'.

СХЕМА ТАБЛИЦЫ orders:
TABLE orders (city_id int, order_id text, user_id text, driver_id text, status_order text, status_tender text, order_timestamp timestamp, price_order_local real);
Значения статусов: 'done', 'cancel'.

{get_semantic_context()}

КРИТИЧЕСКИЕ ПРАВИЛА:
1. ОТМЕНЫ: Используй метрику cancelled_orders (status_order = 'cancel'). Это учитывает всех.
2. ДАТЫ: Используй date(order_timestamp). НИКАКИХ INTERVAL.
3. АЛИАСЫ: Всегда на русском языке в двойных кавычках. Например: SELECT COUNT(*) AS "Всего".
4. ФОРМАТ: Отвечай ТОЛЬКО чистым JSON.

ФОРМАТ ОТВЕТА:
{{
  "sql": "SELECT ...",
  "explanation": "Понял вас, считаю выручку..."
}}
"""

# --- МОДЕЛИ ---
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

class PostReq(BaseModel):
    username: str
    content: str
    query_text: str

# --- ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ---
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    # Пользователи
    cur.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)")
    # Чаты
    cur.execute("CREATE TABLE IF NOT EXISTS chat_sessions (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, title TEXT, created_at TIMESTAMP)")
    # Сообщения
    cur.execute("CREATE TABLE IF NOT EXISTS chat_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, role TEXT, content TEXT, data TEXT, created_at TIMESTAMP)")
    # Сообщество
    cur.execute("CREATE TABLE IF NOT EXISTS community_posts (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, content TEXT, query_text TEXT, created_at TIMESTAMP)")
    
    # Сид данных (Админы)
    admins = [
        ('a.kobenko', 'admin123', 'admin'), 
        ('d.sezyomov', 'admin456', 'admin'), 
        ('r.abramov', 'admin789', 'admin'),
        ('guest', '12345', 'user')
    ]
    cur.executemany("INSERT OR IGNORE INTO users VALUES (?, ?, ?)", admins)
    con.commit()
    con.close()

init_db()

# --- ЭНДПОИНТЫ ---

@app.post("/login")
async def login(req: LoginReq):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    user = cur.execute("SELECT username, role FROM users WHERE username=? AND password=?", (req.username, req.password)).fetchone()
    con.close()
    if not user: raise HTTPException(401, "Доступ запрещен")
    return {"username": user[0], "role": user[1]}

@app.get("/chats/{username}")
async def get_chats(username: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    chats = cur.execute("SELECT id, title, created_at FROM chat_sessions WHERE username=? ORDER BY created_at DESC", (username,)).fetchall()
    con.close()
    return [{"id": c[0], "title": c[1], "created_at": c[2]} for c in chats]

@app.get("/chats/history/{chat_id}")
async def get_chat_history(chat_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    msgs = cur.execute("SELECT role, content, data FROM chat_messages WHERE chat_id=? ORDER BY id ASC", (chat_id,)).fetchall()
    con.close()
    return [{"role": m[0], "content": m[1], "data": json.loads(m[2]) if m[2] else None} for m in msgs]

@app.delete("/chats/{chat_id}")
async def delete_chat(chat_id: int):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM chat_sessions WHERE id=?", (chat_id,))
    cur.execute("DELETE FROM chat_messages WHERE chat_id=?", (chat_id,))
    con.commit()
    con.close()
    return {"status": "ok"}

@app.post("/query")
async def query(req: QueryReq):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    
    # Получаем роль пользователя
    user_data = cur.execute("SELECT role FROM users WHERE username=?", (req.username,)).fetchone()
    role = user_data[0] if user_data else 'user'
    
    try:
        chat_id = req.chat_id
        # Создаем сессию, если новый чат
        if not chat_id:
            cur.execute("INSERT INTO chat_sessions (username, title, created_at) VALUES (?, ?, ?)", 
                        (req.username, req.question[:50], datetime.now()))
            chat_id = cur.lastrowid
        
        # Сохраняем сообщение юзера
        cur.execute("INSERT INTO chat_messages (chat_id, role, content, created_at) VALUES (?, 'user', ?, ?)",
                    (chat_id, req.question, datetime.now()))
        
        # Контекст для LLM
        messages = [{"role": "system", "content": SQL_SYSTEM}]
        for h in req.history[-5:]:
            messages.append({"role": h.role, "content": h.content})
        messages.append({"role": "user", "content": req.question})

        # Запрос к LLM
        response = await client.chat.completions.create(
            model=STRONG_MODEL, messages=messages, response_format={"type": "json_object"}
        )
        llm_json = json.loads(response.choices[0].message.content)
        sql_raw = llm_json.get("sql", "")
        
        # Валидация
        sql_safe = validate_and_fix_sql(sql_raw, role)

        # Выполнение в БД
        con_query = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        df = pd.read_sql_query(sql_safe, con_query)
        con_query.close()

        data_payload = {
            "sql": sql_safe,
            "explanation": llm_json.get("explanation"),
            "question": req.question,
            "data": {"columns": list(df.columns), "rows": df.fillna("").values.tolist()}
        }
        
        # Сохраняем ответ ассистента
        cur.execute("INSERT INTO chat_messages (chat_id, role, content, data, created_at) VALUES (?, 'assistant', ?, ?, ?)",
                    (chat_id, llm_json.get("explanation"), json.dumps(data_payload), datetime.now()))
        con.commit()
        
        return {"chat_id": chat_id, "role": "assistant", "content": llm_json.get("explanation"), "data": data_payload}
    
    except Exception as e:
        con.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        con.close()

# --- ЭНДПОИНТЫ СООБЩЕСТВА ---

@app.get("/community")
async def get_community():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    posts = cur.execute("SELECT id, username, content, query_text, created_at FROM community_posts ORDER BY created_at DESC").fetchall()
    con.close()
    return [{"id": p[0], "username": p[1], "content": p[2], "query_text": p[3], "created_at": p[4]} for p in posts]

@app.post("/community")
async def create_post(req: PostReq):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("INSERT INTO community_posts (username, content, query_text, created_at) VALUES (?, ?, ?, ?)",
                (req.username, req.content, req.query_text, datetime.now()))
    con.commit()
    con.close()
    return {"status": "success"}

@app.post("/suggest/ghost")
async def suggest_ghost(req: BaseModel):
    # Тело запроса обрабатывается через Raw Request для скорости стриминга
    pass 

# Реальная реализация ghost для стриминга (FastAPI StreamingResponse)
@app.post("/suggest/ghost")
async def suggest_ghost_real(req: dict):
    prefix = req.get("prefix", "")
    async def iter_tokens():
        try:
            response = await client.chat.completions.create(
                model=FAST_MODEL,
                messages=[{"role": "user", "content": f"Продолжи запрос аналитика (2-3 слова): {prefix}"}],
                stream=True, max_tokens=10
            )
            async for chunk in response:
                txt = chunk.choices[0].delta.content or ""
                if txt: yield f"data: {json.dumps({'text': txt})}\n\n"
        except: pass
        yield "data: [DONE]\n\n"
    return StreamingResponse(iter_tokens(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
