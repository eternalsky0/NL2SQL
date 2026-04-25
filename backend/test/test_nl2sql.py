"""
Drivee NL2SQL — Quality Test Suite  (100 тест-кейсов)

Запуск:
    python test/test_nl2sql.py
    python test/test_nl2sql.py --id NL-05
    python test/test_nl2sql.py --category "Отмены"
    python test/test_nl2sql.py --verbose
    python test/test_nl2sql.py --save
"""
from __future__ import annotations
import argparse, json, re, time, urllib.request, urllib.error
from dataclasses import dataclass, field
from typing import Optional

DEFAULT_URL      = "http://localhost:8000"
DEFAULT_USERNAME = "r.abramov"
DEFAULT_PASSWORD = "admin789"
TIMEOUT          = 60

RED    = "\033[91m"; GREEN  = "\033[92m"; YELLOW = "\033[93m"
CYAN   = "\033[96m"; BOLD   = "\033[1m";  DIM    = "\033[2m"; RESET = "\033[0m"


# ── Scoring ───────────────────────────────────────────────────────
def score_sql(analyst: str, llm: str, must: list[str]) -> int:
    if not llm:
        return 50
    hits  = sum(1 for ch in must if ch.lower() in llm.lower())
    ratio = hits / len(must) if must else 1.0
    if ratio == 1.0:
        a_tok = set(re.sub(r"\s+", " ", analyst.lower()).split())
        l_tok = set(re.sub(r"\s+", " ", llm.lower()).split())
        ov    = len(a_tok & l_tok) / max(len(a_tok), 1)
        return 100 if ov >= 0.75 else 90 if ov >= 0.55 else 80
    if ratio >= 0.75: return 75
    if ratio >= 0.50: return 65
    return 55


# ── Data classes ──────────────────────────────────────────────────
@dataclass
class NLTestCase:
    id: str
    category: str
    name: str
    query_nl: str
    sql_analyst: str
    must_contain: list[str] = field(default_factory=list)
    note: str = ""

@dataclass
class NLTestResult:
    case: NLTestCase
    sql_llm: str
    explanation: str
    accuracy: int
    duration_ms: int
    error: Optional[str] = None


# ── 100 Test cases ────────────────────────────────────────────────
TESTS: list[NLTestCase] = [

    # ══════════════════════════════════════════════════════════════
    # 1. БАЗОВЫЕ МЕТРИКИ  (NL-01..10)
    # ══════════════════════════════════════════════════════════════
    NLTestCase(
        id="NL-01", category="Базовые метрики",
        name="Всего завершённых поездок",
        query_nl="Сколько завершённых поездок всего?",
        sql_analyst="""SELECT COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Поездок" FROM incity LIMIT 1000""",
        must_contain=["incity","status_order","done","order_id"],
    ),
    NLTestCase(
        id="NL-02", category="Базовые метрики",
        name="Общая выручка",
        query_nl="Какова общая выручка сервиса?",
        sql_analyst="""SELECT ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS "Выручка" FROM incity LIMIT 1000""",
        must_contain=["incity","price_order_local","status_tender","done"],
    ),
    NLTestCase(
        id="NL-03", category="Базовые метрики",
        name="Средний чек поездки",
        query_nl="Какой средний чек завершённой поездки?",
        sql_analyst="""SELECT ROUND(AVG(CASE WHEN status_tender='done' THEN price_order_local END),2) AS "Средний чек" FROM incity LIMIT 1000""",
        must_contain=["incity","avg","price_order_local","done"],
    ),
    NLTestCase(
        id="NL-04", category="Базовые метрики",
        name="Конверсия в поездку",
        query_nl="Какой процент конверсии из заказа в завершённую поездку?",
        sql_analyst="""SELECT ROUND(COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END)*100.0/COUNT(DISTINCT order_id),2) AS "Конверсия %" FROM incity WHERE tender_id IS NOT NULL LIMIT 1000""",
        must_contain=["incity","status_order","done","100.0","order_id"],
    ),
    NLTestCase(
        id="NL-05", category="Базовые метрики",
        name="Уникальные водители с поездками",
        query_nl="Сколько уникальных водителей завершили хотя бы одну поездку?",
        sql_analyst="""SELECT COUNT(DISTINCT driver_id) AS "Водителей" FROM incity WHERE status_order='done' LIMIT 1000""",
        must_contain=["incity","count","distinct","driver_id","done"],
    ),
    NLTestCase(
        id="NL-06", category="Базовые метрики",
        name="Уникальные пассажиры",
        query_nl="Сколько уникальных пассажиров делали заказы?",
        sql_analyst="""SELECT COUNT(DISTINCT user_id) AS "Пассажиров" FROM incity LIMIT 1000""",
        must_contain=["incity","count","distinct","user_id"],
    ),
    NLTestCase(
        id="NL-07", category="Базовые метрики",
        name="Среднее время поездки",
        query_nl="Каково среднее время завершённой поездки в минутах?",
        sql_analyst="""SELECT ROUND(AVG(CASE WHEN status_tender='done' THEN duration_in_seconds END)/60.0,1) AS "Среднее время (мин)" FROM incity LIMIT 1000""",
        must_contain=["incity","avg","duration_in_seconds","60","done"],
    ),
    NLTestCase(
        id="NL-08", category="Базовые метрики",
        name="Среднее расстояние поездки",
        query_nl="Какое среднее расстояние завершённой поездки в метрах?",
        sql_analyst="""SELECT ROUND(AVG(CASE WHEN status_tender='done' THEN distance_in_meters END),0) AS "Среднее расстояние (м)" FROM incity LIMIT 1000""",
        must_contain=["incity","avg","distance_in_meters","done"],
    ),
    NLTestCase(
        id="NL-09", category="Базовые метрики",
        name="Всего отменённых заказов",
        query_nl="Сколько заказов было отменено?",
        sql_analyst="""SELECT COUNT(DISTINCT CASE WHEN status_order='cancel' THEN order_id END) AS "Отменено" FROM incity LIMIT 1000""",
        must_contain=["incity","status_order","cancel","order_id"],
    ),
    NLTestCase(
        id="NL-10", category="Базовые метрики",
        name="Потерянный спрос — заказы без тендера",
        query_nl="Сколько заказов было отменено до создания тендера?",
        sql_analyst="""SELECT COUNT(DISTINCT order_id) AS "Без тендера" FROM incity WHERE tender_id IS NULL LIMIT 1000""",
        must_contain=["incity","tender_id is null","order_id"],
    ),

    # ══════════════════════════════════════════════════════════════
    # 2. ФИЛЬТРЫ ПО ВРЕМЕНИ  (NL-11..22)
    # ══════════════════════════════════════════════════════════════
    NLTestCase(
        id="NL-11", category="Фильтры по времени",
        name="Поездки за 7 дней",
        query_nl="Сколько завершённых поездок за последние 7 дней?",
        sql_analyst="""SELECT COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Поездок" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-7 days') LIMIT 1000""",
        must_contain=["incity","done","-7 days","order_timestamp"],
    ),
    NLTestCase(
        id="NL-12", category="Фильтры по времени",
        name="Выручка за 30 дней",
        query_nl="Какая выручка за последние 30 дней?",
        sql_analyst="""SELECT ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS "Выручка" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-30 days') LIMIT 1000""",
        must_contain=["incity","price_order_local","-30 days","order_timestamp"],
    ),
    NLTestCase(
        id="NL-13", category="Фильтры по времени",
        name="Динамика поездок по дням за 30 дней",
        query_nl="Покажи динамику поездок по дням за последние 30 дней",
        sql_analyst="""SELECT DATE(order_timestamp) AS "Дата", COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Поездок" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-30 days') GROUP BY DATE(order_timestamp) ORDER BY DATE(order_timestamp) LIMIT 1000""",
        must_contain=["incity","date(order_timestamp)","group by","done","-30 days"],
    ),
    NLTestCase(
        id="NL-14", category="Фильтры по времени",
        name="Поездки за 14 дней",
        query_nl="Сколько поездок и заказов за последние две недели?",
        sql_analyst="""SELECT COUNT(DISTINCT order_id) AS "Заказов", COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Поездок" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-14 days') LIMIT 1000""",
        must_contain=["incity","-14 days","done","order_id"],
    ),
    NLTestCase(
        id="NL-15", category="Фильтры по времени",
        name="Заказы за последние 3 дня",
        query_nl="Покажи количество заказов за каждый из последних 3 дней",
        sql_analyst="""SELECT DATE(order_timestamp) AS "Дата", COUNT(DISTINCT order_id) AS "Заказов" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-3 days') GROUP BY DATE(order_timestamp) ORDER BY DATE(order_timestamp) LIMIT 1000""",
        must_contain=["incity","-3 days","date(order_timestamp)","group by"],
    ),
    NLTestCase(
        id="NL-16", category="Фильтры по времени",
        name="Выручка за 7 дней",
        query_nl="Какая выручка за последнюю неделю?",
        sql_analyst="""SELECT ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS "Выручка" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-7 days') LIMIT 1000""",
        must_contain=["incity","price_order_local","-7 days"],
    ),
    NLTestCase(
        id="NL-17", category="Фильтры по времени",
        name="Динамика выручки по дням за 7 дней",
        query_nl="Покажи выручку по дням за последние 7 дней",
        sql_analyst="""SELECT DATE(order_timestamp) AS "Дата", ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS "Выручка" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-7 days') GROUP BY DATE(order_timestamp) ORDER BY DATE(order_timestamp) LIMIT 1000""",
        must_contain=["incity","price_order_local","-7 days","group by","date(order_timestamp)"],
    ),
    NLTestCase(
        id="NL-18", category="Фильтры по времени",
        name="Отмены за 7 дней",
        query_nl="Сколько заказов отменено за последние 7 дней?",
        sql_analyst="""SELECT COUNT(DISTINCT CASE WHEN status_order='cancel' THEN order_id END) AS "Отменено" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-7 days') LIMIT 1000""",
        must_contain=["incity","cancel","-7 days","order_timestamp"],
    ),
    NLTestCase(
        id="NL-19", category="Фильтры по времени",
        name="Конверсия за 30 дней",
        query_nl="Какова конверсия в поездку за последние 30 дней?",
        sql_analyst="""SELECT ROUND(COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END)*100.0/NULLIF(COUNT(DISTINCT order_id),0),2) AS "Конверсия %" FROM incity WHERE tender_id IS NOT NULL AND DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-30 days') LIMIT 1000""",
        must_contain=["incity","done","100.0","-30 days","tender_id is not null"],
    ),
    NLTestCase(
        id="NL-20", category="Фильтры по времени",
        name="Поездки по часам за 7 дней",
        query_nl="Как распределяются поездки по часам суток за последние 7 дней?",
        sql_analyst="""SELECT CAST(strftime('%H',order_timestamp) AS INTEGER) AS "Час", COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Поездок" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-7 days') GROUP BY "Час" ORDER BY "Час" LIMIT 1000""",
        must_contain=["incity","strftime","%h","-7 days","group by"],
    ),
    NLTestCase(
        id="NL-21", category="Фильтры по времени",
        name="Средний чек за 30 дней",
        query_nl="Какой средний чек за последние 30 дней?",
        sql_analyst="""SELECT ROUND(AVG(CASE WHEN status_tender='done' THEN price_order_local END),2) AS "Средний чек" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-30 days') LIMIT 1000""",
        must_contain=["incity","avg","price_order_local","-30 days"],
    ),
    NLTestCase(
        id="NL-22", category="Фильтры по времени",
        name="Новые пассажиры за 30 дней",
        query_nl="Сколько новых пассажиров зарегистрировалось за последние 30 дней?",
        sql_analyst="""SELECT COUNT(DISTINCT user_id) AS "Новых пассажиров" FROM pass_detail WHERE user_reg_date>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-30 days') LIMIT 1000""",
        must_contain=["pass_detail","user_reg_date","user_id","-30 days"],
    ),

    # ══════════════════════════════════════════════════════════════
    # 3. ГРУППИРОВКИ  (NL-23..34)
    # ══════════════════════════════════════════════════════════════
    NLTestCase(
        id="NL-23", category="Группировки",
        name="Поездки по городам",
        query_nl="Сколько завершённых поездок в каждом городе?",
        sql_analyst="""SELECT city_id AS "Город", COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Поездок" FROM incity GROUP BY city_id ORDER BY "Поездок" DESC LIMIT 1000""",
        must_contain=["incity","city_id","group by","done"],
    ),
    NLTestCase(
        id="NL-24", category="Группировки",
        name="Поездки по часам суток",
        query_nl="Как распределяются завершённые поездки по часам суток?",
        sql_analyst="""SELECT CAST(strftime('%H',order_timestamp) AS INTEGER) AS "Час", COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Поездок" FROM incity GROUP BY "Час" ORDER BY "Час" LIMIT 1000""",
        must_contain=["incity","strftime","%h","group by","done"],
    ),
    NLTestCase(
        id="NL-25", category="Группировки",
        name="Отмены по статусу тендера",
        query_nl="Сколько отменённых заказов по каждому статусу тендера?",
        sql_analyst="""SELECT status_tender AS "Статус тендера", COUNT(DISTINCT order_id) AS "Отменено" FROM incity WHERE status_order='cancel' GROUP BY status_tender ORDER BY "Отменено" DESC LIMIT 1000""",
        must_contain=["incity","status_tender","cancel","group by"],
    ),
    NLTestCase(
        id="NL-26", category="Группировки",
        name="Выручка по городам",
        query_nl="Покажи выручку по каждому городу",
        sql_analyst="""SELECT city_id AS "Город", ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS "Выручка" FROM incity GROUP BY city_id ORDER BY "Выручка" DESC LIMIT 1000""",
        must_contain=["incity","city_id","price_order_local","group by"],
    ),
    NLTestCase(
        id="NL-27", category="Группировки",
        name="Конверсия по городам",
        query_nl="Какова конверсия в поездку по каждому городу?",
        sql_analyst="""SELECT city_id AS "Город", ROUND(COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END)*100.0/NULLIF(COUNT(DISTINCT order_id),0),2) AS "Конверсия %" FROM incity WHERE tender_id IS NOT NULL GROUP BY city_id ORDER BY "Конверсия %" DESC LIMIT 1000""",
        must_contain=["incity","city_id","done","100.0","group by"],
    ),
    NLTestCase(
        id="NL-28", category="Группировки",
        name="Среднее расстояние по городам",
        query_nl="Каково среднее расстояние поездки по городам?",
        sql_analyst="""SELECT city_id AS "Город", ROUND(AVG(CASE WHEN status_tender='done' THEN distance_in_meters END),0) AS "Ср. расстояние (м)" FROM incity GROUP BY city_id ORDER BY "Ср. расстояние (м)" DESC LIMIT 1000""",
        must_contain=["incity","city_id","distance_in_meters","avg","group by"],
    ),
    NLTestCase(
        id="NL-29", category="Группировки",
        name="Отмены по городам за 7 дней",
        query_nl="Сколько отмен в каждом городе за последние 7 дней?",
        sql_analyst="""SELECT city_id AS "Город", COUNT(DISTINCT CASE WHEN status_order='cancel' THEN order_id END) AS "Отменено" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-7 days') GROUP BY city_id ORDER BY "Отменено" DESC LIMIT 1000""",
        must_contain=["incity","city_id","cancel","-7 days","group by"],
    ),
    NLTestCase(
        id="NL-30", category="Группировки",
        name="Поездки по дням недели",
        query_nl="Как распределяются поездки по дням недели?",
        sql_analyst="""SELECT strftime('%w',order_timestamp) AS "День недели", COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Поездок" FROM incity GROUP BY "День недели" ORDER BY "День недели" LIMIT 1000""",
        must_contain=["incity","strftime","%w","group by","done"],
    ),
    NLTestCase(
        id="NL-31", category="Группировки",
        name="Средний чек по городам",
        query_nl="Какой средний чек поездки в каждом городе?",
        sql_analyst="""SELECT city_id AS "Город", ROUND(AVG(CASE WHEN status_tender='done' THEN price_order_local END),2) AS "Средний чек" FROM incity GROUP BY city_id ORDER BY "Средний чек" DESC LIMIT 1000""",
        must_contain=["incity","city_id","avg","price_order_local","group by"],
    ),
    NLTestCase(
        id="NL-32", category="Группировки",
        name="Выручка по дням за месяц",
        query_nl="Покажи выручку по каждому дню за последние 30 дней",
        sql_analyst="""SELECT DATE(order_timestamp) AS "Дата", ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS "Выручка" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-30 days') GROUP BY DATE(order_timestamp) ORDER BY DATE(order_timestamp) LIMIT 1000""",
        must_contain=["incity","date(order_timestamp)","price_order_local","-30 days","group by"],
    ),
    NLTestCase(
        id="NL-33", category="Группировки",
        name="Отмены по часам суток",
        query_nl="В какие часы суток чаще всего отменяют заказы?",
        sql_analyst="""SELECT CAST(strftime('%H',order_timestamp) AS INTEGER) AS "Час", COUNT(DISTINCT CASE WHEN status_order='cancel' THEN order_id END) AS "Отменено" FROM incity GROUP BY "Час" ORDER BY "Отменено" DESC LIMIT 1000""",
        must_contain=["incity","strftime","%h","cancel","group by"],
    ),
    NLTestCase(
        id="NL-34", category="Группировки",
        name="Заказы и поездки по статусам",
        query_nl="Покажи количество заказов по каждому статусу",
        sql_analyst="""SELECT status_order AS "Статус", COUNT(DISTINCT order_id) AS "Заказов" FROM incity GROUP BY status_order ORDER BY "Заказов" DESC LIMIT 1000""",
        must_contain=["incity","status_order","group by","order_id"],
    ),

    # ══════════════════════════════════════════════════════════════
    # 4. ТОП-N  (NL-35..44)
    # ══════════════════════════════════════════════════════════════
    NLTestCase(
        id="NL-35", category="Топ-N",
        name="Топ-10 водителей по поездкам",
        query_nl="Топ 10 водителей по количеству завершённых поездок",
        sql_analyst="""SELECT driver_id AS "Водитель", COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Поездок" FROM incity WHERE driver_id IS NOT NULL GROUP BY driver_id ORDER BY "Поездок" DESC LIMIT 10""",
        must_contain=["incity","driver_id","done","order by","limit 10"],
    ),
    NLTestCase(
        id="NL-36", category="Топ-N",
        name="Топ-5 городов по выручке за 30 дней",
        query_nl="Топ 5 городов по выручке за последние 30 дней",
        sql_analyst="""SELECT city_id AS "Город", ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS "Выручка" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-30 days') GROUP BY city_id ORDER BY "Выручка" DESC LIMIT 5""",
        must_contain=["incity","city_id","price_order_local","limit 5","-30 days"],
    ),
    NLTestCase(
        id="NL-37", category="Топ-N",
        name="Топ-10 пассажиров по числу поездок",
        query_nl="Топ 10 пассажиров по количеству завершённых поездок",
        sql_analyst="""SELECT user_id AS "Пассажир", COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Поездок" FROM incity WHERE user_id IS NOT NULL GROUP BY user_id ORDER BY "Поездок" DESC LIMIT 10""",
        must_contain=["incity","user_id","done","order by","limit 10"],
    ),
    NLTestCase(
        id="NL-38", category="Топ-N",
        name="Топ-5 водителей по выручке",
        query_nl="Топ 5 водителей по сумме выручки",
        sql_analyst="""SELECT driver_id AS "Водитель", ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS "Выручка" FROM incity WHERE driver_id IS NOT NULL GROUP BY driver_id ORDER BY "Выручка" DESC LIMIT 5""",
        must_contain=["incity","driver_id","price_order_local","limit 5"],
    ),
    NLTestCase(
        id="NL-39", category="Топ-N",
        name="Топ-10 водителей с отменами пассажиром",
        query_nl="Топ 10 водителей у которых больше всего отмен пассажирами после принятия",
        sql_analyst="""SELECT driver_id AS "Водитель", SUM(client_cancel_after_accept) AS "Отмен пассажиром" FROM driver_detail WHERE driver_id IS NOT NULL GROUP BY driver_id ORDER BY "Отмен пассажиром" DESC LIMIT 10""",
        must_contain=["driver_detail","driver_id","client_cancel_after_accept","limit 10"],
    ),
    NLTestCase(
        id="NL-40", category="Топ-N",
        name="Топ-3 города по конверсии",
        query_nl="Топ 3 города по конверсии в поездку",
        sql_analyst="""SELECT city_id AS "Город", ROUND(COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END)*100.0/NULLIF(COUNT(DISTINCT order_id),0),2) AS "Конверсия %" FROM incity WHERE tender_id IS NOT NULL GROUP BY city_id ORDER BY "Конверсия %" DESC LIMIT 3""",
        must_contain=["incity","city_id","done","100.0","limit 3"],
    ),
    NLTestCase(
        id="NL-41", category="Топ-N",
        name="Топ-10 водителей по онлайн-времени за месяц",
        query_nl="Топ 10 водителей по суммарному времени онлайн за последние 30 дней",
        sql_analyst="""SELECT driver_id AS "Водитель", ROUND(SUM(online_time_sum_seconds)/3600.0,1) AS "Онлайн часов" FROM driver_detail WHERE tender_date_part>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-30 days') GROUP BY driver_id ORDER BY "Онлайн часов" DESC LIMIT 10""",
        must_contain=["driver_detail","online_time_sum_seconds","driver_id","limit 10","-30 days"],
    ),
    NLTestCase(
        id="NL-42", category="Топ-N",
        name="Топ-5 водителей по среднему числу поездок в день",
        query_nl="Топ 5 водителей по среднему числу поездок в день за последний месяц",
        sql_analyst="""SELECT driver_id AS "Водитель", ROUND(AVG(rides_count),2) AS "Среднее поездок в день" FROM driver_detail WHERE tender_date_part>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-30 days') AND rides_count>0 GROUP BY driver_id ORDER BY "Среднее поездок в день" DESC LIMIT 5""",
        must_contain=["driver_detail","driver_id","rides_count","avg","limit 5"],
    ),
    NLTestCase(
        id="NL-43", category="Топ-N",
        name="Топ-10 пассажиров по онлайн-времени",
        query_nl="Топ 10 пассажиров по суммарному времени онлайн за последние 30 дней",
        sql_analyst="""SELECT user_id AS "Пассажир", ROUND(SUM(online_time_sum_seconds)/3600.0,1) AS "Онлайн часов" FROM pass_detail WHERE order_date_part>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-30 days') GROUP BY user_id ORDER BY "Онлайн часов" DESC LIMIT 10""",
        must_contain=["pass_detail","user_id","online_time_sum_seconds","limit 10","-30 days"],
    ),
    NLTestCase(
        id="NL-44", category="Топ-N",
        name="Топ-5 дней по выручке",
        query_nl="Какие 5 дней принесли наибольшую выручку?",
        sql_analyst="""SELECT DATE(order_timestamp) AS "Дата", ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS "Выручка" FROM incity GROUP BY DATE(order_timestamp) ORDER BY "Выручка" DESC LIMIT 5""",
        must_contain=["incity","date(order_timestamp)","price_order_local","order by","limit 5"],
    ),

    # ══════════════════════════════════════════════════════════════
    # 5. СРАВНЕНИЕ ПЕРИОДОВ  (NL-45..54)
    # ══════════════════════════════════════════════════════════════
    NLTestCase(
        id="NL-45", category="Сравнение периодов",
        name="Поездки текущая vs прошлая неделя",
        query_nl="Сравни количество завершённых поездок за текущую и прошлую неделю",
        sql_analyst="""WITH b AS (SELECT MAX(order_timestamp) AS mx FROM incity), cw AS (SELECT COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS cnt FROM incity CROSS JOIN b WHERE DATE(order_timestamp)>=DATE(mx,'-7 days')), pw AS (SELECT COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS cnt FROM incity CROSS JOIN b WHERE DATE(order_timestamp) BETWEEN DATE(mx,'-14 days') AND DATE(mx,'-8 days')) SELECT cw.cnt AS "Текущая неделя", pw.cnt AS "Прошлая неделя" FROM cw,pw LIMIT 1000""",
        must_contain=["incity","current_week","past_week","-7 days","-14 days","done"],
        note="CTE с двумя временными окнами",
    ),
    NLTestCase(
        id="NL-46", category="Сравнение периодов",
        name="Выручка текущая vs прошлая неделя",
        query_nl="Сравни выручку за текущую и прошлую неделю",
        sql_analyst="""WITH b AS (SELECT MAX(order_timestamp) AS mx FROM incity), cw AS (SELECT ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS rev FROM incity CROSS JOIN b WHERE DATE(order_timestamp)>=DATE(mx,'-7 days')), pw AS (SELECT ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS rev FROM incity CROSS JOIN b WHERE DATE(order_timestamp) BETWEEN DATE(mx,'-14 days') AND DATE(mx,'-8 days')) SELECT cw.rev AS "Текущая неделя", pw.rev AS "Прошлая неделя" FROM cw,pw LIMIT 1000""",
        must_contain=["incity","current_week","past_week","price_order_local","-7 days"],
    ),
    NLTestCase(
        id="NL-47", category="Сравнение периодов",
        name="Отмены текущая vs прошлая неделя",
        query_nl="Как изменилось число отмен по сравнению с прошлой неделей?",
        sql_analyst="""WITH b AS (SELECT MAX(order_timestamp) AS mx FROM incity), cw AS (SELECT COUNT(DISTINCT CASE WHEN status_order='cancel' THEN order_id END) AS cnt FROM incity CROSS JOIN b WHERE DATE(order_timestamp)>=DATE(mx,'-7 days')), pw AS (SELECT COUNT(DISTINCT CASE WHEN status_order='cancel' THEN order_id END) AS cnt FROM incity CROSS JOIN b WHERE DATE(order_timestamp) BETWEEN DATE(mx,'-14 days') AND DATE(mx,'-8 days')) SELECT cw.cnt AS "Текущая неделя", pw.cnt AS "Прошлая неделя", cw.cnt-pw.cnt AS "Изменение" FROM cw,pw LIMIT 1000""",
        must_contain=["incity","cancel","-7 days","-14 days"],
    ),
    NLTestCase(
        id="NL-48", category="Сравнение периодов",
        name="Конверсия текущая vs прошлая неделя",
        query_nl="Как изменилась конверсия за текущую неделю по сравнению с прошлой?",
        sql_analyst="""WITH b AS (SELECT MAX(order_timestamp) AS mx FROM incity), cw AS (SELECT ROUND(COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END)*100.0/NULLIF(COUNT(DISTINCT order_id),0),2) AS cv FROM incity CROSS JOIN b WHERE tender_id IS NOT NULL AND DATE(order_timestamp)>=DATE(mx,'-7 days')), pw AS (SELECT ROUND(COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END)*100.0/NULLIF(COUNT(DISTINCT order_id),0),2) AS cv FROM incity CROSS JOIN b WHERE tender_id IS NOT NULL AND DATE(order_timestamp) BETWEEN DATE(mx,'-14 days') AND DATE(mx,'-8 days')) SELECT cw.cv AS "Текущая нед. %", pw.cv AS "Прошлая нед. %", ROUND(cw.cv-pw.cv,2) AS "Изменение п.п." FROM cw,pw LIMIT 1000""",
        must_contain=["incity","done","100.0","-7 days","-14 days"],
    ),
    NLTestCase(
        id="NL-49", category="Сравнение периодов",
        name="Средний чек текущая vs прошлая неделя",
        query_nl="Как изменился средний чек за текущую неделю по сравнению с прошлой?",
        sql_analyst="""WITH b AS (SELECT MAX(order_timestamp) AS mx FROM incity), cw AS (SELECT ROUND(AVG(CASE WHEN status_tender='done' THEN price_order_local END),2) AS avg_price FROM incity CROSS JOIN b WHERE DATE(order_timestamp)>=DATE(mx,'-7 days')), pw AS (SELECT ROUND(AVG(CASE WHEN status_tender='done' THEN price_order_local END),2) AS avg_price FROM incity CROSS JOIN b WHERE DATE(order_timestamp) BETWEEN DATE(mx,'-14 days') AND DATE(mx,'-8 days')) SELECT cw.avg_price AS "Текущая неделя", pw.avg_price AS "Прошлая неделя" FROM cw,pw LIMIT 1000""",
        must_contain=["incity","avg","price_order_local","-7 days","-14 days"],
    ),
    NLTestCase(
        id="NL-50", category="Сравнение периодов",
        name="Активные водители текущая vs прошлая неделя",
        query_nl="Сравни количество активных водителей за текущую и прошлую неделю",
        sql_analyst="""WITH b AS (SELECT MAX(tender_date_part) AS mx FROM driver_detail), cw AS (SELECT COUNT(DISTINCT driver_id) AS cnt FROM driver_detail CROSS JOIN b WHERE tender_date_part>=DATE(mx,'-7 days') AND rides_count>0), pw AS (SELECT COUNT(DISTINCT driver_id) AS cnt FROM driver_detail CROSS JOIN b WHERE tender_date_part BETWEEN DATE(mx,'-14 days') AND DATE(mx,'-8 days') AND rides_count>0) SELECT cw.cnt AS "Текущая неделя", pw.cnt AS "Прошлая неделя" FROM cw,pw LIMIT 1000""",
        must_contain=["driver_detail","driver_id","rides_count","-7 days","-14 days"],
    ),
    NLTestCase(
        id="NL-51", category="Сравнение периодов",
        name="Поездки текущий vs прошлый месяц",
        query_nl="Сравни число поездок за текущий и прошлый месяц",
        sql_analyst="""WITH b AS (SELECT MAX(order_timestamp) AS mx FROM incity), cw AS (SELECT COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS cnt FROM incity CROSS JOIN b WHERE DATE(order_timestamp)>=DATE(mx,'-30 days')), pw AS (SELECT COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS cnt FROM incity CROSS JOIN b WHERE DATE(order_timestamp) BETWEEN DATE(mx,'-60 days') AND DATE(mx,'-31 days')) SELECT cw.cnt AS "Текущий месяц", pw.cnt AS "Прошлый месяц" FROM cw,pw LIMIT 1000""",
        must_contain=["incity","done","-30 days","-60 days"],
    ),
    NLTestCase(
        id="NL-52", category="Сравнение периодов",
        name="Выручка по городам текущая vs прошлая неделя",
        query_nl="Сравни выручку по городам за текущую и прошлую неделю",
        sql_analyst="""WITH b AS (SELECT MAX(order_timestamp) AS mx FROM incity), cw AS (SELECT city_id, ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS rev FROM incity CROSS JOIN b WHERE DATE(order_timestamp)>=DATE(mx,'-7 days') GROUP BY city_id), pw AS (SELECT city_id, ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS rev FROM incity CROSS JOIN b WHERE DATE(order_timestamp) BETWEEN DATE(mx,'-14 days') AND DATE(mx,'-8 days') GROUP BY city_id) SELECT COALESCE(cw.city_id,pw.city_id) AS "Город", COALESCE(cw.rev,0) AS "Текущая нед.", COALESCE(pw.rev,0) AS "Прошлая нед." FROM cw LEFT JOIN pw ON cw.city_id=pw.city_id ORDER BY "Текущая нед." DESC LIMIT 1000""",
        must_contain=["incity","city_id","price_order_local","-7 days","-14 days"],
    ),
    NLTestCase(
        id="NL-53", category="Сравнение периодов",
        name="Новые пассажиры текущая vs прошлая неделя",
        query_nl="Сравни количество новых пассажиров за текущую и прошлую неделю",
        sql_analyst="""WITH b AS (SELECT MAX(order_date_part) AS mx FROM pass_detail), cw AS (SELECT COUNT(DISTINCT user_id) AS cnt FROM pass_detail CROSS JOIN b WHERE user_reg_date>=DATE(mx,'-7 days')), pw AS (SELECT COUNT(DISTINCT user_id) AS cnt FROM pass_detail CROSS JOIN b WHERE user_reg_date BETWEEN DATE(mx,'-14 days') AND DATE(mx,'-8 days')) SELECT cw.cnt AS "Текущая неделя", pw.cnt AS "Прошлая неделя" FROM cw,pw LIMIT 1000""",
        must_contain=["pass_detail","user_reg_date","user_id","-7 days","-14 days"],
    ),
    NLTestCase(
        id="NL-54", category="Сравнение периодов",
        name="Конверсия по городам текущая vs прошлая неделя",
        query_nl="Как изменилась конверсия по городам за текущую неделю по сравнению с прошлой?",
        sql_analyst="""WITH b AS (SELECT MAX(order_timestamp) AS mx FROM incity), cw AS (SELECT city_id, ROUND(COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END)*100.0/NULLIF(COUNT(DISTINCT order_id),0),2) AS cv FROM incity CROSS JOIN b WHERE tender_id IS NOT NULL AND DATE(order_timestamp)>=DATE(mx,'-7 days') GROUP BY city_id), pw AS (SELECT city_id, ROUND(COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END)*100.0/NULLIF(COUNT(DISTINCT order_id),0),2) AS cv FROM incity CROSS JOIN b WHERE tender_id IS NOT NULL AND DATE(order_timestamp) BETWEEN DATE(mx,'-14 days') AND DATE(mx,'-8 days') GROUP BY city_id) SELECT COALESCE(cw.city_id,pw.city_id) AS "Город", COALESCE(cw.cv,0) AS "Текущая нед. %", COALESCE(pw.cv,0) AS "Прошлая нед. %", ROUND(COALESCE(cw.cv,0)-COALESCE(pw.cv,0),2) AS "Изм. п.п." FROM cw LEFT JOIN pw ON cw.city_id=pw.city_id ORDER BY "Изм. п.п." DESC LIMIT 1000""",
        must_contain=["incity","city_id","done","100.0","-7 days","-14 days"],
    ),

    # ══════════════════════════════════════════════════════════════
    # 6. МЕТРИКИ ПАССАЖИРОВ  (NL-55..64)
    # ══════════════════════════════════════════════════════════════
    NLTestCase(
        id="NL-55", category="Метрики пассажиров",
        name="Активные пассажиры за 7 дней",
        query_nl="Сколько уникальных пассажиров совершили поездку за последние 7 дней?",
        sql_analyst="""SELECT COUNT(DISTINCT user_id) AS "Активных пассажиров" FROM pass_detail WHERE order_date_part>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-7 days') AND rides_count>0 LIMIT 1000""",
        must_contain=["pass_detail","user_id","rides_count","-7 days"],
    ),
    NLTestCase(
        id="NL-56", category="Метрики пассажиров",
        name="Среднее поездок на пассажира за 30 дней",
        query_nl="Какое среднее количество поездок на одного пассажира за последние 30 дней?",
        sql_analyst="""SELECT ROUND(AVG(total),2) AS "Среднее поездок на пассажира" FROM (SELECT user_id, SUM(rides_count) AS total FROM pass_detail WHERE order_date_part>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-30 days') GROUP BY user_id) LIMIT 1000""",
        must_contain=["pass_detail","rides_count","avg","-30 days","user_id"],
    ),
    NLTestCase(
        id="NL-57", category="Метрики пассажиров",
        name="Пассажиры с 5+ поездками за месяц",
        query_nl="Сколько пассажиров совершили 5 и более поездок за последние 30 дней?",
        sql_analyst="""SELECT COUNT(*) AS "Пассажиров" FROM (SELECT user_id, SUM(rides_count) AS total FROM pass_detail WHERE order_date_part>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-30 days') GROUP BY user_id HAVING total>=5) LIMIT 1000""",
        must_contain=["pass_detail","rides_count","having","user_id","-30 days"],
    ),
    NLTestCase(
        id="NL-58", category="Метрики пассажиров",
        name="Пассажиры с отменами после принятия",
        query_nl="Сколько пассажиров отменяли заказ после того как водитель его принял?",
        sql_analyst="""SELECT COUNT(DISTINCT user_id) AS "Пассажиров с отменами" FROM pass_detail WHERE client_cancel_after_accept>0 LIMIT 1000""",
        must_contain=["pass_detail","client_cancel_after_accept","user_id"],
    ),
    NLTestCase(
        id="NL-59", category="Метрики пассажиров",
        name="Среднее онлайн-время пассажира в день",
        query_nl="Каково среднее время онлайн пассажира в день за последние 7 дней?",
        sql_analyst="""SELECT ROUND(AVG(online_time_sum_seconds)/60.0,1) AS "Среднее онлайн (мин)" FROM pass_detail WHERE order_date_part>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-7 days') LIMIT 1000""",
        must_contain=["pass_detail","online_time_sum_seconds","avg","-7 days"],
    ),
    NLTestCase(
        id="NL-60", category="Метрики пассажиров",
        name="Новые пассажиры за 7 дней",
        query_nl="Сколько новых пассажиров зарегистрировалось за последние 7 дней?",
        sql_analyst="""SELECT COUNT(DISTINCT user_id) AS "Новых пассажиров" FROM pass_detail WHERE user_reg_date>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-7 days') LIMIT 1000""",
        must_contain=["pass_detail","user_reg_date","user_id","-7 days"],
    ),
    NLTestCase(
        id="NL-61", category="Метрики пассажиров",
        name="Активные пассажиры по городам",
        query_nl="Сколько активных пассажиров в каждом городе за последние 30 дней?",
        sql_analyst="""SELECT city_id AS "Город", COUNT(DISTINCT user_id) AS "Пассажиров" FROM pass_detail WHERE order_date_part>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-30 days') AND rides_count>0 GROUP BY city_id ORDER BY "Пассажиров" DESC LIMIT 1000""",
        must_contain=["pass_detail","city_id","user_id","rides_count","group by"],
    ),
    NLTestCase(
        id="NL-62", category="Метрики пассажиров",
        name="Суммарное время поездок пассажиров за неделю",
        query_nl="Каково суммарное время поездок всех пассажиров за последние 7 дней в часах?",
        sql_analyst="""SELECT ROUND(SUM(rides_time_sum_seconds)/3600.0,1) AS "Суммарное время (ч)" FROM pass_detail WHERE order_date_part>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-7 days') LIMIT 1000""",
        must_contain=["pass_detail","rides_time_sum_seconds","-7 days","3600"],
    ),
    NLTestCase(
        id="NL-63", category="Метрики пассажиров",
        name="Пассажиры с заказами но без поездок",
        query_nl="Сколько пассажиров делали заказы но ни разу не совершили поездку за последние 30 дней?",
        sql_analyst="""SELECT COUNT(DISTINCT user_id) AS "Пассажиров" FROM pass_detail WHERE order_date_part>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-30 days') AND orders_count>0 AND rides_count=0 LIMIT 1000""",
        must_contain=["pass_detail","orders_count","rides_count","user_id"],
    ),
    NLTestCase(
        id="NL-64", category="Метрики пассажиров",
        name="Конверсия заказов пассажиров по городам",
        query_nl="Какова конверсия заказов в поездки у пассажиров по городам за последние 30 дней?",
        sql_analyst="""SELECT city_id AS "Город", ROUND(SUM(rides_count)*100.0/NULLIF(SUM(orders_count),0),2) AS "Конверсия %" FROM pass_detail WHERE order_date_part>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-30 days') GROUP BY city_id ORDER BY "Конверсия %" DESC LIMIT 1000""",
        must_contain=["pass_detail","city_id","rides_count","orders_count","100.0","group by"],
    ),

    # ══════════════════════════════════════════════════════════════
    # 7. МЕТРИКИ ВОДИТЕЛЕЙ  (NL-65..74)
    # ══════════════════════════════════════════════════════════════
    NLTestCase(
        id="NL-65", category="Метрики водителей",
        name="Активные водители по городам за 7 дней",
        query_nl="Сколько активных водителей в каждом городе за последние 7 дней?",
        sql_analyst="""SELECT city_id AS "Город", COUNT(DISTINCT driver_id) AS "Водителей" FROM driver_detail WHERE tender_date_part>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-7 days') AND rides_count>0 GROUP BY city_id ORDER BY "Водителей" DESC LIMIT 1000""",
        must_contain=["driver_detail","city_id","driver_id","rides_count","-7 days"],
    ),
    NLTestCase(
        id="NL-66", category="Метрики водителей",
        name="Водители с наибольшим числом принятых заказов за неделю",
        query_nl="Топ 10 водителей по числу принятых заказов за последние 7 дней",
        sql_analyst="""SELECT driver_id AS "Водитель", SUM(orders_cnt_accepted) AS "Принято заказов" FROM driver_detail WHERE tender_date_part>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-7 days') GROUP BY driver_id ORDER BY "Принято заказов" DESC LIMIT 10""",
        must_contain=["driver_detail","driver_id","orders_cnt_accepted","-7 days","limit 10"],
    ),
    NLTestCase(
        id="NL-67", category="Метрики водителей",
        name="Среднее поездок водителя в день за 30 дней",
        query_nl="Каково среднее число завершённых поездок водителя в день за последние 30 дней?",
        sql_analyst="""SELECT ROUND(AVG(rides_count),2) AS "Ср. поездок в день" FROM driver_detail WHERE tender_date_part>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-30 days') AND rides_count>0 LIMIT 1000""",
        must_contain=["driver_detail","rides_count","avg","-30 days"],
    ),
    NLTestCase(
        id="NL-68", category="Метрики водителей",
        name="Водители без поездок, но онлайн",
        query_nl="Сколько водителей выходили онлайн но не совершили ни одной поездки за последние 7 дней?",
        sql_analyst="""SELECT COUNT(DISTINCT driver_id) AS "Водителей" FROM driver_detail WHERE tender_date_part>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-7 days') AND online_time_sum_seconds>0 AND rides_count=0 LIMIT 1000""",
        must_contain=["driver_detail","driver_id","online_time_sum_seconds","rides_count","-7 days"],
    ),
    NLTestCase(
        id="NL-69", category="Метрики водителей",
        name="Новые водители за 30 дней",
        query_nl="Сколько новых водителей зарегистрировалось за последние 30 дней?",
        sql_analyst="""SELECT COUNT(DISTINCT driver_id) AS "Новых водителей" FROM driver_detail WHERE driver_reg_date>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-30 days') LIMIT 1000""",
        must_contain=["driver_detail","driver_reg_date","driver_id","-30 days"],
    ),
    NLTestCase(
        id="NL-70", category="Метрики водителей",
        name="Суммарное онлайн-время водителей по городам за 7 дней",
        query_nl="Каково суммарное онлайн-время водителей по городам за последние 7 дней?",
        sql_analyst="""SELECT city_id AS "Город", ROUND(SUM(online_time_sum_seconds)/3600.0,1) AS "Онлайн часов" FROM driver_detail WHERE tender_date_part>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-7 days') GROUP BY city_id ORDER BY "Онлайн часов" DESC LIMIT 1000""",
        must_contain=["driver_detail","city_id","online_time_sum_seconds","group by","-7 days"],
    ),
    NLTestCase(
        id="NL-71", category="Метрики водителей",
        name="Эффективность водителей (поездки на час онлайн)",
        query_nl="Топ 10 водителей по количеству поездок на час онлайн за последние 30 дней",
        sql_analyst="""SELECT driver_id AS "Водитель", ROUND(SUM(rides_count)*1.0/NULLIF(SUM(online_time_sum_seconds)/3600.0,0),2) AS "Поездок на час" FROM driver_detail WHERE tender_date_part>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-30 days') AND online_time_sum_seconds>0 GROUP BY driver_id ORDER BY "Поездок на час" DESC LIMIT 10""",
        must_contain=["driver_detail","driver_id","rides_count","online_time_sum_seconds","limit 10"],
    ),
    NLTestCase(
        id="NL-72", category="Метрики водителей",
        name="Отмены пассажирами по водителям",
        query_nl="Топ 10 водителей с наибольшим числом отмен пассажирами после принятия за последние 30 дней",
        sql_analyst="""SELECT driver_id AS "Водитель", SUM(client_cancel_after_accept) AS "Отмен" FROM driver_detail WHERE tender_date_part>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-30 days') GROUP BY driver_id ORDER BY "Отмен" DESC LIMIT 10""",
        must_contain=["driver_detail","driver_id","client_cancel_after_accept","-30 days","limit 10"],
    ),
    NLTestCase(
        id="NL-73", category="Метрики водителей",
        name="Динамика активных водителей по дням",
        query_nl="Покажи динамику числа активных водителей по дням за последние 14 дней",
        sql_analyst="""SELECT tender_date_part AS "Дата", COUNT(DISTINCT driver_id) AS "Активных водителей" FROM driver_detail WHERE tender_date_part>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-14 days') AND rides_count>0 GROUP BY tender_date_part ORDER BY tender_date_part LIMIT 1000""",
        must_contain=["driver_detail","tender_date_part","driver_id","group by","-14 days"],
    ),
    NLTestCase(
        id="NL-74", category="Метрики водителей",
        name="Топ водитель по поездкам в каждом городе",
        query_nl="Покажи водителя с наибольшим числом поездок в каждом городе за все время",
        sql_analyst="""WITH ranked AS (SELECT city_id, driver_id, SUM(rides_count) AS total, RANK() OVER (PARTITION BY city_id ORDER BY SUM(rides_count) DESC) AS rnk FROM driver_detail GROUP BY city_id, driver_id) SELECT city_id AS "Город", driver_id AS "Водитель", total AS "Поездок" FROM ranked WHERE rnk=1 ORDER BY city_id LIMIT 1000""",
        must_contain=["driver_detail","city_id","driver_id","rides_count","partition by"],
        note="Требует оконную функцию или CTE с RANK/MAX",
    ),

    # ══════════════════════════════════════════════════════════════
    # 8. ОТМЕНЫ И ОТКАЗЫ  (NL-75..82)
    # ══════════════════════════════════════════════════════════════
    NLTestCase(
        id="NL-75", category="Отмены",
        name="Всего отмен и доля от заказов",
        query_nl="Сколько всего отмен и какой их процент от всех заказов?",
        sql_analyst="""SELECT COUNT(DISTINCT CASE WHEN status_order='cancel' THEN order_id END) AS "Отменено", COUNT(DISTINCT order_id) AS "Всего заказов", ROUND(COUNT(DISTINCT CASE WHEN status_order='cancel' THEN order_id END)*100.0/COUNT(DISTINCT order_id),2) AS "Доля отмен %" FROM incity LIMIT 1000""",
        must_contain=["incity","cancel","order_id","100.0"],
    ),
    NLTestCase(
        id="NL-76", category="Отмены",
        name="Отмены: пассажир vs водитель",
        query_nl="Сколько заказов отменил пассажир и сколько водитель?",
        sql_analyst="""SELECT COUNT(DISTINCT CASE WHEN clientcancel_timestamp IS NOT NULL THEN order_id END) AS "Отмены пассажиром", COUNT(DISTINCT CASE WHEN drivercancel_timestamp IS NOT NULL THEN order_id END) AS "Отмены водителем" FROM incity LIMIT 1000""",
        must_contain=["incity","clientcancel_timestamp","drivercancel_timestamp"],
    ),
    NLTestCase(
        id="NL-77", category="Отмены",
        name="Доля declined-тендеров",
        query_nl="Какой процент тендеров был отклонён водителями?",
        sql_analyst="""SELECT COUNT(DISTINCT CASE WHEN status_tender='decline' THEN tender_id END) AS "Отклонено", COUNT(DISTINCT tender_id) AS "Всего тендеров", ROUND(COUNT(DISTINCT CASE WHEN status_tender='decline' THEN tender_id END)*100.0/NULLIF(COUNT(DISTINCT tender_id),0),2) AS "Доля %" FROM incity WHERE tender_id IS NOT NULL LIMIT 1000""",
        must_contain=["incity","status_tender","decline","tender_id","100.0"],
    ),
    NLTestCase(
        id="NL-78", category="Отмены",
        name="Отмены по городам за 7 дней с долей",
        query_nl="Покажи число и долю отмен по городам за последние 7 дней",
        sql_analyst="""SELECT city_id AS "Город", COUNT(DISTINCT CASE WHEN status_order='cancel' THEN order_id END) AS "Отменено", COUNT(DISTINCT order_id) AS "Всего", ROUND(COUNT(DISTINCT CASE WHEN status_order='cancel' THEN order_id END)*100.0/NULLIF(COUNT(DISTINCT order_id),0),2) AS "Доля %" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-7 days') GROUP BY city_id ORDER BY "Доля %" DESC LIMIT 1000""",
        must_contain=["incity","city_id","cancel","100.0","-7 days","group by"],
    ),
    NLTestCase(
        id="NL-79", category="Отмены",
        name="Отмены до принятия тендера",
        query_nl="Сколько заказов было отменено до того как водитель принял тендер?",
        sql_analyst="""SELECT COUNT(DISTINCT order_id) AS "Отмены до принятия" FROM incity WHERE cancel_before_accept_local IS NOT NULL LIMIT 1000""",
        must_contain=["incity","cancel_before_accept_local","order_id"],
    ),
    NLTestCase(
        id="NL-80", category="Отмены",
        name="Тендеры в статусе wait",
        query_nl="Сколько тендеров сейчас находятся в статусе ожидания?",
        sql_analyst="""SELECT COUNT(DISTINCT tender_id) AS "Тендеров в ожидании" FROM incity WHERE status_tender='wait' AND tender_id IS NOT NULL LIMIT 1000""",
        must_contain=["incity","status_tender","wait","tender_id"],
    ),
    NLTestCase(
        id="NL-81", category="Отмены",
        name="Динамика отмен по дням за 14 дней",
        query_nl="Покажи динамику отмен по дням за последние 14 дней",
        sql_analyst="""SELECT DATE(order_timestamp) AS "Дата", COUNT(DISTINCT CASE WHEN status_order='cancel' THEN order_id END) AS "Отменено" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-14 days') GROUP BY DATE(order_timestamp) ORDER BY DATE(order_timestamp) LIMIT 1000""",
        must_contain=["incity","cancel","date(order_timestamp)","-14 days","group by"],
    ),
    NLTestCase(
        id="NL-82", category="Отмены",
        name="Отмены водителями по городам",
        query_nl="В каком городе водители чаще всего отменяют заказы?",
        sql_analyst="""SELECT city_id AS "Город", COUNT(DISTINCT CASE WHEN drivercancel_timestamp IS NOT NULL THEN order_id END) AS "Отмен водителем" FROM incity GROUP BY city_id ORDER BY "Отмен водителем" DESC LIMIT 1000""",
        must_contain=["incity","city_id","drivercancel_timestamp","group by"],
    ),

    # ══════════════════════════════════════════════════════════════
    # 9. ЦЕНОВАЯ АНАЛИТИКА  (NL-83..90)
    # ══════════════════════════════════════════════════════════════
    NLTestCase(
        id="NL-83", category="Ценовая аналитика",
        name="Стартовая цена vs итоговая цена",
        query_nl="Сравни среднюю стартовую цену и среднюю итоговую цену завершённых заказов",
        sql_analyst="""SELECT ROUND(AVG(CASE WHEN status_tender='done' THEN price_start_local END),2) AS "Средняя стартовая", ROUND(AVG(CASE WHEN status_tender='done' THEN price_order_local END),2) AS "Средняя итоговая" FROM incity LIMIT 1000""",
        must_contain=["incity","price_start_local","price_order_local","avg","done"],
    ),
    NLTestCase(
        id="NL-84", category="Ценовая аналитика",
        name="Поездки дороже 500 рублей",
        query_nl="Сколько завершённых поездок стоило дороже 500 рублей?",
        sql_analyst="""SELECT COUNT(DISTINCT order_id) AS "Поездок дороже 500" FROM incity WHERE status_tender='done' AND price_order_local>500 LIMIT 1000""",
        must_contain=["incity","price_order_local","500","done"],
    ),
    NLTestCase(
        id="NL-85", category="Ценовая аналитика",
        name="Распределение по ценовым диапазонам",
        query_nl="Покажи распределение завершённых поездок по ценовым диапазонам",
        sql_analyst="""SELECT CASE WHEN price_order_local<100 THEN 'до 100' WHEN price_order_local<300 THEN '100-300' WHEN price_order_local<500 THEN '300-500' ELSE '500+' END AS "Диапазон", COUNT(DISTINCT order_id) AS "Поездок" FROM incity WHERE status_tender='done' GROUP BY "Диапазон" ORDER BY MIN(price_order_local) LIMIT 1000""",
        must_contain=["incity","price_order_local","case when","group by","done"],
    ),
    NLTestCase(
        id="NL-86", category="Ценовая аналитика",
        name="Средний чек по городам за 30 дней",
        query_nl="Какой средний чек поездки по каждому городу за последние 30 дней?",
        sql_analyst="""SELECT city_id AS "Город", ROUND(AVG(CASE WHEN status_tender='done' THEN price_order_local END),2) AS "Средний чек" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-30 days') GROUP BY city_id ORDER BY "Средний чек" DESC LIMIT 1000""",
        must_contain=["incity","city_id","avg","price_order_local","-30 days","group by"],
    ),
    NLTestCase(
        id="NL-87", category="Ценовая аналитика",
        name="Максимальная и минимальная цена поездки",
        query_nl="Какие максимальная и минимальная стоимость завершённой поездки?",
        sql_analyst="""SELECT MAX(CASE WHEN status_tender='done' THEN price_order_local END) AS "Максимум", MIN(CASE WHEN status_tender='done' AND price_order_local>0 THEN price_order_local END) AS "Минимум" FROM incity LIMIT 1000""",
        must_contain=["incity","max","min","price_order_local","done"],
    ),
    NLTestCase(
        id="NL-88", category="Ценовая аналитика",
        name="Выручка по дням недели",
        query_nl="Как распределяется выручка по дням недели?",
        sql_analyst="""SELECT strftime('%w',order_timestamp) AS "День недели", ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS "Выручка" FROM incity GROUP BY "День недели" ORDER BY "День недели" LIMIT 1000""",
        must_contain=["incity","strftime","%w","price_order_local","group by"],
    ),
    NLTestCase(
        id="NL-89", category="Ценовая аналитика",
        name="Средняя цена тендера vs итоговая цена",
        query_nl="Насколько итоговая цена отличается от цены тендера в среднем?",
        sql_analyst="""SELECT ROUND(AVG(CASE WHEN status_tender='done' THEN price_tender_local END),2) AS "Ср. цена тендера", ROUND(AVG(CASE WHEN status_tender='done' THEN price_order_local END),2) AS "Ср. итоговая цена", ROUND(AVG(CASE WHEN status_tender='done' THEN price_order_local-price_tender_local END),2) AS "Разница" FROM incity LIMIT 1000""",
        must_contain=["incity","price_tender_local","price_order_local","avg","done"],
    ),
    NLTestCase(
        id="NL-90", category="Ценовая аналитика",
        name="Динамика среднего чека по дням за 30 дней",
        query_nl="Покажи динамику среднего чека по дням за последние 30 дней",
        sql_analyst="""SELECT DATE(order_timestamp) AS "Дата", ROUND(AVG(CASE WHEN status_tender='done' THEN price_order_local END),2) AS "Средний чек" FROM incity WHERE DATE(order_timestamp)>=DATE((SELECT MAX(order_timestamp) FROM incity),'-30 days') GROUP BY DATE(order_timestamp) ORDER BY DATE(order_timestamp) LIMIT 1000""",
        must_contain=["incity","avg","price_order_local","date(order_timestamp)","-30 days","group by"],
    ),

    # ══════════════════════════════════════════════════════════════
    # 10. СЛОЖНЫЕ ЗАПРОСЫ  (NL-91..100)
    # ══════════════════════════════════════════════════════════════
    NLTestCase(
        id="NL-91", category="Сложные запросы",
        name="Потерянный спрос по городам",
        query_nl="Какой процент заказов отменяется до создания тендера по каждому городу?",
        sql_analyst="""SELECT city_id AS "Город", COUNT(DISTINCT CASE WHEN tender_id IS NULL THEN order_id END) AS "Без тендера", COUNT(DISTINCT order_id) AS "Всего", ROUND(COUNT(DISTINCT CASE WHEN tender_id IS NULL THEN order_id END)*100.0/COUNT(DISTINCT order_id),2) AS "Потерянный спрос %" FROM incity GROUP BY city_id ORDER BY "Потерянный спрос %" DESC LIMIT 1000""",
        must_contain=["incity","city_id","tender_id is null","100.0","group by"],
    ),
    NLTestCase(
        id="NL-92", category="Сложные запросы",
        name="Воронка заказа по городам",
        query_nl="Покажи воронку: заказы созданы — получили тендер — принято водителем — завершено, по каждому городу",
        sql_analyst="""SELECT city_id AS "Город", COUNT(DISTINCT order_id) AS "Создано", COUNT(DISTINCT CASE WHEN tender_id IS NOT NULL THEN order_id END) AS "Тендер", COUNT(DISTINCT CASE WHEN driveraccept_timestamp IS NOT NULL THEN order_id END) AS "Принято", COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Завершено" FROM incity GROUP BY city_id ORDER BY city_id LIMIT 1000""",
        must_contain=["incity","city_id","tender_id is not null","driveraccept_timestamp","done","group by"],
    ),
    NLTestCase(
        id="NL-93", category="Сложные запросы",
        name="Водители выше среднего по поездкам за месяц",
        query_nl="Сколько водителей совершили больше среднего числа поездок за последние 30 дней?",
        sql_analyst="""WITH agg AS (SELECT driver_id, SUM(rides_count) AS total FROM driver_detail WHERE tender_date_part>=DATE((SELECT MAX(tender_date_part) FROM driver_detail),'-30 days') GROUP BY driver_id), avg_val AS (SELECT AVG(total) AS avg_rides FROM agg) SELECT COUNT(*) AS "Водителей выше среднего" FROM agg CROSS JOIN avg_val WHERE agg.total>avg_val.avg_rides LIMIT 1000""",
        must_contain=["driver_detail","rides_count","avg","-30 days","driver_id"],
    ),
    NLTestCase(
        id="NL-94", category="Сложные запросы",
        name="Часы пик по заказам в каждом городе",
        query_nl="Определи часы пик (час с максимальным числом заказов) для каждого города",
        sql_analyst="""WITH hourly AS (SELECT city_id, CAST(strftime('%H',order_timestamp) AS INTEGER) AS hr, COUNT(DISTINCT order_id) AS cnt FROM incity GROUP BY city_id, hr), ranked AS (SELECT city_id, hr, cnt, RANK() OVER (PARTITION BY city_id ORDER BY cnt DESC) AS rnk FROM hourly) SELECT city_id AS "Город", hr AS "Час пик", cnt AS "Заказов" FROM ranked WHERE rnk=1 ORDER BY city_id LIMIT 1000""",
        must_contain=["incity","city_id","strftime","%h","partition by","order_id"],
    ),
    NLTestCase(
        id="NL-95", category="Сложные запросы",
        name="Среднее время принятия тендера водителем",
        query_nl="Какое среднее время между созданием тендера и его принятием водителем в минутах?",
        sql_analyst="""SELECT ROUND(AVG((strftime('%s',driveraccept_timestamp)-strftime('%s',tender_timestamp))/60.0),1) AS "Среднее время принятия (мин)" FROM incity WHERE driveraccept_timestamp IS NOT NULL AND tender_timestamp IS NOT NULL AND status_tender='done' LIMIT 1000""",
        must_contain=["incity","driveraccept_timestamp","tender_timestamp","strftime","avg"],
    ),
    NLTestCase(
        id="NL-96", category="Сложные запросы",
        name="Доля завершённых из принятых тендеров",
        query_nl="Какой процент принятых водителями тендеров завершается поездкой?",
        sql_analyst="""SELECT COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Завершено", COUNT(DISTINCT CASE WHEN driveraccept_timestamp IS NOT NULL THEN order_id END) AS "Принято водителем", ROUND(COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END)*100.0/NULLIF(COUNT(DISTINCT CASE WHEN driveraccept_timestamp IS NOT NULL THEN order_id END),0),2) AS "Доля %" FROM incity LIMIT 1000""",
        must_contain=["incity","driveraccept_timestamp","done","100.0","order_id"],
    ),
    NLTestCase(
        id="NL-97", category="Сложные запросы",
        name="Сравнение активности водителей по дням недели",
        query_nl="В какие дни недели водители совершают больше всего поездок?",
        sql_analyst="""SELECT strftime('%w',tender_date_part) AS "День недели", SUM(rides_count) AS "Поездок", COUNT(DISTINCT driver_id) AS "Водителей" FROM driver_detail GROUP BY "День недели" ORDER BY "Поездок" DESC LIMIT 1000""",
        must_contain=["driver_detail","strftime","%w","rides_count","group by"],
    ),
    NLTestCase(
        id="NL-98", category="Сложные запросы",
        name="Рейтинг городов по трём метрикам",
        query_nl="Покажи города с числом поездок, выручкой и конверсией одновременно",
        sql_analyst="""SELECT city_id AS "Город", COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END) AS "Поездок", ROUND(SUM(CASE WHEN status_tender='done' THEN price_order_local ELSE 0 END),2) AS "Выручка", ROUND(COUNT(DISTINCT CASE WHEN status_order='done' THEN order_id END)*100.0/NULLIF(COUNT(DISTINCT CASE WHEN tender_id IS NOT NULL THEN order_id END),0),2) AS "Конверсия %" FROM incity GROUP BY city_id ORDER BY "Поездок" DESC LIMIT 1000""",
        must_contain=["incity","city_id","done","price_order_local","100.0","group by"],
    ),
    NLTestCase(
        id="NL-99", category="Сложные запросы",
        name="Пассажиры не ездившие 14+ дней",
        query_nl="Сколько пассажиров не совершали поездок 14 и более дней подряд?",
        sql_analyst="""SELECT COUNT(DISTINCT user_id) AS "Пассажиров" FROM pass_detail WHERE rides_count>0 AND user_id NOT IN (SELECT DISTINCT user_id FROM pass_detail WHERE order_date_part>=DATE((SELECT MAX(order_date_part) FROM pass_detail),'-14 days') AND rides_count>0) LIMIT 1000""",
        must_contain=["pass_detail","user_id","rides_count","-14 days"],
    ),
    NLTestCase(
        id="NL-100", category="Сложные запросы",
        name="Соотношение онлайн-времени к поездкам у водителей",
        query_nl="Каков средний KPI водителей: доля времени в поездках от общего онлайн-времени по городам?",
        sql_analyst="""SELECT city_id AS "Город", ROUND(SUM(rides_time_sum_seconds)*100.0/NULLIF(SUM(online_time_sum_seconds),0),1) AS "КПД % (время в поездках / онлайн)" FROM driver_detail WHERE online_time_sum_seconds>0 GROUP BY city_id ORDER BY "КПД % (время в поездках / онлайн)" DESC LIMIT 1000""",
        must_contain=["driver_detail","city_id","rides_time_sum_seconds","online_time_sum_seconds","100.0","group by"],
    ),
]


# ── HTTP helpers ──────────────────────────────────────────────────
def http_post(url: str, data: dict) -> tuple[int, str]:
    body = json.dumps(data).encode()
    req  = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return 0, str(e)


def login(base_url: str, username: str, password: str) -> bool:
    code, body = http_post(f"{base_url}/login", {"username": username, "password": password})
    if code == 200:
        d = json.loads(body)
        print(f"{GREEN}Авторизован как {d['username']} (role: {d['role']}){RESET}\n")
        return True
    print(f"{RED}Авторизация не удалась: {code}{RESET}\n")
    return False


# ── Runner ────────────────────────────────────────────────────────
def run_test(base_url: str, username: str, case: NLTestCase) -> NLTestResult:
    t0 = time.time()
    code, body = http_post(f"{base_url}/query",
                           {"username": username, "question": case.query_nl, "history": []})
    ms = int((time.time() - t0) * 1000)
    if code == 0:
        return NLTestResult(case=case, sql_llm="", explanation="", accuracy=50, duration_ms=ms, error=body)
    try:
        data = json.loads(body)
        if data.get("error") or not data.get("data"):
            return NLTestResult(case=case, sql_llm="", explanation=data.get("content",""),
                                accuracy=50, duration_ms=ms, error=data.get("content",""))
        sql_llm = data["data"].get("sql", "")
        return NLTestResult(case=case, sql_llm=sql_llm,
                            explanation=data["data"].get("explanation",""),
                            accuracy=score_sql(case.sql_analyst, sql_llm, case.must_contain),
                            duration_ms=ms)
    except Exception as e:
        return NLTestResult(case=case, sql_llm="", explanation="", accuracy=50, duration_ms=ms, error=str(e))


# ── Printer ───────────────────────────────────────────────────────
def acc_color(a: int) -> str:
    if a >= 90: return GREEN
    if a >= 75: return CYAN
    if a >= 60: return YELLOW
    return RED


def print_result(r: NLTestResult, verbose: bool = False):
    ac  = acc_color(r.accuracy)
    bar = "█" * (r.accuracy // 10) + "░" * (10 - r.accuracy // 10)
    err = f" {RED}[ERR]{RESET}" if r.error else ""
    print(f"  {r.case.id:<8} {ac}{bar} {r.accuracy:>3}%{RESET}{err}  {DIM}{r.case.name}  ({r.duration_ms} мс){RESET}")
    if verbose or r.error:
        print(f"           {DIM}Q: {r.case.query_nl}{RESET}")
        if r.sql_llm:
            print(f"           {DIM}SQL: {r.sql_llm.replace(chr(10),' ')[:150]}{RESET}")
        missing = [ch for ch in r.case.must_contain if ch.lower() not in r.sql_llm.lower()]
        if missing:
            print(f"           {YELLOW}Нет: {', '.join(missing)}{RESET}")
        if r.error:
            print(f"           {RED}Ошибка: {r.error[:100]}{RESET}")
    print()


def print_summary(results: list[NLTestResult], output: list[str] | None = None):
    def w(line=""):
        print(line)
        if output is not None:
            output.append(re.sub(r"\033\[[0-9;]*m", "", line))

    cats: dict[str, list[NLTestResult]] = {}
    for r in results:
        cats.setdefault(r.case.category, []).append(r)

    avg_all = sum(r.accuracy for r in results) / len(results) if results else 0
    w("─" * 72)
    w(f"{BOLD}  Тестов: {len(results)}   Средняя точность: {avg_all:.1f}%{RESET}")
    w()
    for cat, rs in cats.items():
        avg = sum(r.accuracy for r in rs) / len(rs)
        col = acc_color(int(avg))
        w(f"  {col}{cat:<30}{RESET}  avg {avg:.0f}%")
        for r in rs:
            w(f"    {r.case.id:<8} {acc_color(r.accuracy)}{r.accuracy:>3}%{RESET}  {r.case.name}")
    w()

    if output is not None:
        output.append("\n" + "=" * 72)
        output.append("ПОДРОБНЫЙ ОТЧЁТ")
        output.append("=" * 72)
        for r in results:
            output.append(f"\n[{r.case.id}] {r.case.name}  |  {r.accuracy}%  ({r.duration_ms} мс)")
            output.append(f"Категория : {r.case.category}")
            output.append(f"Вопрос    : {r.case.query_nl}")
            output.append(f"SQL аналитика:\n{r.case.sql_analyst.strip()}")
            output.append(f"SQL LLM:\n{r.sql_llm.strip() if r.sql_llm else '(нет)'}")
            missing = [ch for ch in r.case.must_contain if ch.lower() not in r.sql_llm.lower()]
            if missing:
                output.append(f"Отсутствует: {', '.join(missing)}")
            if r.error:
                output.append(f"Ошибка: {r.error}")


# ── Main ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url",      default=DEFAULT_URL)
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--verbose",  action="store_true")
    parser.add_argument("--save",     action="store_true")
    parser.add_argument("--category", default=None)
    parser.add_argument("--id",       default=None)
    args = parser.parse_args()

    print(f"\n{BOLD}{'═'*72}{RESET}")
    print(f"{BOLD}  Drivee NL2SQL — Quality Test Suite  ({len(TESTS)} тестов){RESET}")
    print(f"{BOLD}{'═'*72}{RESET}")
    print(f"  URL: {args.url}   User: {args.username}")
    print()

    try:
        with urllib.request.urlopen(urllib.request.Request(f"{args.url}/health"), timeout=5) as resp:
            h = json.loads(resp.read())
            print(f"  DB: {'ok' if h.get('db') else 'FAIL'}  "
                  f"LLM: {'ok' if h.get('llm') else 'FAIL'}  "
                  f"Model: {h.get('strong_model','?')}")
            print()
    except Exception as e:
        print(f"{RED}  Сервер недоступен: {e}{RESET}\n"); return

    if not login(args.url, args.username, args.password):
        return

    tests = TESTS
    if args.id:
        tests = [t for t in tests if t.id == args.id]
    elif args.category:
        tests = [t for t in tests if t.category == args.category]

    cats: dict[str, list[NLTestCase]] = {}
    for t in tests:
        cats.setdefault(t.category, []).append(t)

    results: list[NLTestResult] = []
    for cat, cases in cats.items():
        print(f"{BOLD}  {cat}{RESET}")
        print(f"  {'─'*60}")
        for case in cases:
            r = run_test(args.url, args.username, case)
            results.append(r)
            print_result(r, verbose=args.verbose)
        print()

    output_lines: list[str] | None = [] if args.save else None
    print_summary(results, output=output_lines)

    if args.save and output_lines:
        path = "test/result_nl2sql.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(output_lines))
        print(f"  Отчёт сохранён: {path}")


if __name__ == "__main__":
    main()
