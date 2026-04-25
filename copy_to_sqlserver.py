# C:\BigData\InstacartProject\copy_to_sqlserver.py

import pandas as pd
from sqlalchemy import create_engine, text, event

# =========================
# Source: Postgres (Docker)
# =========================
PG_USER = "instacart"
PG_PASS = "instacart"
PG_HOST = "localhost"
PG_PORT = "5433"          # published port -> warehouse:5432
PG_DB   = "instacart"

pg_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
pg_engine = create_engine(pg_url)

# =========================
# Target: SQL Server (Windows)
# =========================
# Using Windows Authentication (Trusted_Connection=yes)
MSSQL_SERVER = "localhost,1433"
MSSQL_DB = "InstacartDW"
ODBC_DRIVER = "ODBC Driver 17 for SQL Server"

mssql_url = (
    f"mssql+pyodbc://@{MSSQL_SERVER}/{MSSQL_DB}"
    f"?driver={ODBC_DRIVER.replace(' ', '+')}"
    f"&Trusted_Connection=yes"
    f"&TrustServerCertificate=yes"
)

mssql_engine = create_engine(mssql_url, fast_executemany=True, future=True)

@event.listens_for(mssql_engine, "before_cursor_execute")
def _enable_fast_executemany(conn, cursor, statement, parameters, context, executemany):
    # Speeds up executemany inserts with pyodbc
    if executemany:
        try:
            cursor.fast_executemany = True
        except Exception:
            pass

TABLES = [
    "customer_features",
    "customer_segments",
    "peak_order_times",
    "product_performance",
]

def safe_to_sql(df: pd.DataFrame, table_name: str) -> None:
    """
    SQL Server has a 2100-parameter limit per statement.
    Avoid pandas/SQLAlchemy generating giant multi-row INSERT statements.

    We compute a safe chunksize based on column count and insert in batches.
    """
    ncols = max(1, len(df.columns))
    # keep a buffer under 2100; 2000/ncols is a safe rule of thumb
    batch_rows = max(1, 2000 // ncols)

    print(f"  -> Writing {table_name}: rows={len(df)}, cols={ncols}, chunksize={batch_rows}")

    # IMPORTANT: method=None (default) prevents huge multi-row INSERT with too many parameters
    df.to_sql(
        name=table_name,
        con=mssql_engine,
        if_exists="replace",
        index=False,
        chunksize=batch_rows,
        method=None,
    )

def main() -> None:
    print("Connecting to Postgres (source)...")
    with pg_engine.connect() as pg_conn:
        for t in TABLES:
            print(f"Loading {t} from Postgres...")
            df = pd.read_sql(text(f'SELECT * FROM "{t}"'), pg_conn)

            # Optional: make column names SQL-Server-friendly if needed
            # df.columns = [c.strip().replace(" ", "_") for c in df.columns]

            print(f"Writing {t} to SQL Server database '{MSSQL_DB}'...")
            safe_to_sql(df, t)

    print("\n✅ Done. Tables copied to SQL Server (InstacartDW).")

if __name__ == "__main__":
    main()