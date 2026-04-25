# Drivee NL2SQL

Естественно-языковой интерфейс к аналитической базе данных такси-сервиса. Пользователь пишет вопрос на русском («покажи отмены по городам за прошлую неделю»), система генерирует SQL, выполняет его и строит график.

**Стек:** FastAPI · SQLite · OpenRouter / Ollama / Groq · Chart.js · React (без сборки, jsx в html)

---

## Реализация сценариев в рамках гранд финала МПИТ

* **[Подробный разбор проекта под МПИТ](https://github.com/eternalsky0/NL2SQL/blob/main/MPIT.md)** — описание бизнес-сценариев, архитектуры безопасности, дашбордов и результатов стресс-тестирования. 
*(Рекомендуется смотреть в веб-версии GitHub для лучшего визуального представления и работы медиафайлов).*

---

🌐 **Продукт доступен по ссылке:** [http://158.160.227.151:81](http://158.160.227.151:81)

🔑 Данные для авторизации экспертов

| Статус аккаунта | Логин | Пароль |
| :--- | :--- | :--- |
| **Эскперт МПИТ** | `mpit` | `12345` |
| **Эскперт Drivee** | `drivee` | `12345` |
| *Запасной* | `a.kobenko` | `admin123` |
| *Запасной* | `d.sezyomov` | `admin456` |
| *Запасной* | `r.abramov` | `admin789` |

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

Создайте файл `backend/.env` и скопируйте содержимое с гугл диска - https://drive.google.com/drive/u/1/folders/1djuAqMHl44cm9fMqfFGtTWgOkGMkaeq1 

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

загрузите содержимое папки data, либо ее саму с https://drive.google.com/drive/u/1/folders/1djuAqMHl44cm9fMqfFGtTWgOkGMkaeq1, поместите папку data (или создайте) в папку backend

---

## Запуск

```bash
.venv\Scripts\activate     
```

```bash
cd .\backend\              

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

```

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
