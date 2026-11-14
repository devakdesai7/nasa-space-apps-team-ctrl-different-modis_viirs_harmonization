
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = ROOT / "NASA_DATA_FROM_2023_TO_2026"

con = duckdb.connect()
con.execute("SET threads = 4")

files = sorted(DATA_ROOT.rglob("*.csv"))

for file in files:
    # Identify zero brightness or negative FRP records.
    rows = con.execute("""
        SELECT *
        FROM read_csv(?, header = true, all_varchar = true)
        WHERE
            try_cast(brightness AS DOUBLE) = 0
            OR try_cast(frp AS DOUBLE) < 0
    """, [str(file)]).fetchall()

    if rows:
        print(f"\n{'=' * 80}")
        print(f"FILE: {file.name}")
        print(f"ANOMALOUS RECORDS: {len(rows)}")

        columns = [
            item[0] for item in con.description
        ]

        for row in rows:
            print(dict(zip(columns, row)))

con.close()
