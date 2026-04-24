# Drivee NL2SQL

Естественно-языковой интерфейс к аналитической базе данных такси-сервиса. Пользователь пишет вопрос на русском («покажи отмены по городам за прошлую неделю»), система генерирует SQL, выполняет его и строит график.

**Стек:** FastAPI · SQLite · OpenRouter / Ollama / Groq · Chart.js · React (без сборки)

---

## Реализация сценариев в рамках гранд финала МПИТ

Сценарий 1. Сравнительный анализ - https://drive.google.com/file/d/1a2iQ1PfuFKDBonfpvSrT0DA0FNDNIAHB/view?usp=drive_link

Сценарий 2. Сравнительный анализ -[https://drive.google.com/file/d/1l5Ul4j-vUirqDDzQoPrzGey1JQdp1S8e/view?usp=drive_link

Сценарий 3. Сравнительный анализ - https://drive.google.com/file/d/1-sFleJGR5urpJWIOd6E2BKWxYIR4146_/view?usp=drive_link

Сценарий 4. Сравнительный анализ - https://drive.google.com/file/d/1TWsszG9WtcNdKph-xoMMBoeucO_n7veu/view

---

## Требования

- Python 3.10+
- LLM-провайдер: [OpenRouter](https://openrouter.ai) (рекомендуется), Groq или Ollama (локально)

---

## Установка

```bash
# 1. Клонировать репозиторий
git clone <repo-url>
cd NL2SQL

# 2. Создать виртуальное окружение
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate

# 3. Установить зависимости
pip install -r backend/requirements.txt
```

---

## Конфигурация

Создайте файл `backend/.env` (в тг)

<summary>Ollama (локально, без интернета)</summary>

```bash
# Установить Ollama: https://ollama.com/download
ollama pull qwen2.5-coder:1.5b   # ~900 МБ, для ghost/chips
ollama pull qwen2.5-coder:7b     # ~4.5 ГБ, для SQL
```

```env
OPENROUTER_URL=http://localhost:11434/v1
OPENROUTER_KEY=ollama
FAST_MODEL=qwen2.5-coder:1.5b
STRONG_MODEL=qwen2.5-coder:7b
```
</details>

---

## Инициализация базы данных

Взять с ТГ

---

## Запуск

```bash
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
```

Сервер доступен на http://localhost:8000

**PyCharm:** main.py → Run

---

## Открыть интерфейс

Откройте в браузере двойным кликом:

| Файл | Назначение |
|---|---|
| `frontend/index.html` | Основной чат-интерфейс |
| `frontend/reports.html` | Управление расписаниями отчётов |

> CORS разрешён для всех origins — открывать можно прямо через `file://`.

---

## Проверка работоспособности

```
GET http://localhost:8000/health
```

Ожидаемый ответ:

```json
{
  "ollama": true,
  "db": true,
  "reports_db": true
}
```



## Тесты

```bash
# Тесты безопасности (guardrails, SQL-инъекции, prompt injection)
python backend/test/test_security.py --url http://localhost:8000

# Smoke-тесты планировщика отчётов (без LLM)
python backend/test_reports_smoke.py
```

---

## Демо-сценарий

Рекомендуемый порядок для демонстрации:

1. `покажи отмены по городам за прошлую неделю` — bar chart
2. `динамика выручки за последний месяц` — line chart
3. `топ 10 водителей по поездкам за неделю` — таблица
4. `доля отмен по классам машин` — pie chart
5. `а теперь только по Москве` — follow-up с памятью контекста
6. _(wow-эффект)_ напишите `DELETE FROM orders` — сработает guardrail

---

## Архитектура

```
frontend/index.html          ← React + Chart.js (один файл, без сборки)
        │
        │ HTTP / fetch
        ▼
backend/main.py              ← FastAPI: /query, /health, /reports/*
        │
        ├── semantic_layer.yaml   ← бизнес-словарь (метрики, синонимы)
        ├── scheduler.py          ← APScheduler (расписания отчётов)
        ├── reports_store.py      ← CRUD для отчётов (reports.db)
        ├── notifications.py      ← email / in-app доставка
        └── data/
            ├── drivee.db         ← аналитические данные (read-only)
            └── reports.db        ← сохранённые отчёты
```

**LLM-пайплайн (3 уровня):**

| Уровень | Модель | Назначение |
|---|---|---|
| 1 | FAST_MODEL | Ghost-text (автодополнение при вводе) |
| 2 | FAST_MODEL | Chips (4 подсказки-follow-up) |
| 3 | STRONG_MODEL | Финальная генерация SQL |

**Безопасность:** только `SELECT`, whitelist таблиц (`orders`), валидация AST через `sqlglot`, таймаут запросов.

---

## Частые проблемы

**`Connection refused` к LLM** — проверьте правильность `OPENROUTER_URL` и `OPENROUTER_KEY` в `backend/.env`.

**Ollama работает на CPU, а не GPU** — `ollama ps` покажет использование. Обновите драйверы NVIDIA.

**Кириллица в пути к проекту** (`C:\Users\Иван\...`) — перенесите проект в `C:\Dev\NL2SQL`.

**`ModuleNotFoundError`** — убедитесь, что виртуальное окружение активировано перед `uvicorn`.
