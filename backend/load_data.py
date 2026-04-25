import sqlite3
import pandas as pd
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_FILE  = os.path.join(DATA_DIR, "drivee.db")

TABLES = {
    "incity":        "incity.csv",
    "pass_detail":   "pass_detail.csv",
    "driver_detail": "driver_detail.csv",
}

INDEXES = {
    "incity": [
        "CREATE INDEX idx_incity_order_id      ON incity(order_id);",
        "CREATE INDEX idx_incity_status        ON incity(status_order, status_tender);",
        "CREATE INDEX idx_incity_order_ts      ON incity(order_timestamp);",
        "CREATE INDEX idx_incity_user_id       ON incity(user_id);",
        "CREATE INDEX idx_incity_driver_id     ON incity(driver_id);",
    ],
    "pass_detail": [
        "CREATE INDEX idx_pass_user_date ON pass_detail(user_id, order_date_part);",
        "CREATE INDEX idx_pass_city      ON pass_detail(city_id);",
    ],
    "driver_detail": [
        "CREATE INDEX idx_drv_driver_date ON driver_detail(driver_id, tender_date_part);",
        "CREATE INDEX idx_drv_city        ON driver_detail(city_id);",
    ],
}


def init_real_db():
    missing = [name for name, f in TABLES.items() if not os.path.exists(os.path.join(DATA_DIR, f))]
    if missing:
        print(f"Отсутствуют файлы для таблиц: {missing}")
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)

    for table, csv_name in TABLES.items():
        csv_path = os.path.join(DATA_DIR, csv_name)
        print(f"Loading {csv_name} -> {table} ...")
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        for chunk in pd.read_csv(csv_path, chunksize=100_000):
            chunk.to_sql(table, conn, if_exists="append", index=False)
        for idx_sql in INDEXES[table]:
            conn.execute(idx_sql)
        print(f"  done: {table}")

    conn.commit()
    conn.close()
    print("База данных готова:", DB_FILE)


if __name__ == "__main__":
    init_real_db()
