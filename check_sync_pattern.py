import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    port=os.getenv("DB_PORT") or 5432
)

cursor = conn.cursor()

print("=" * 70)
print(f"API Sync Pattern Analysis - {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
print("=" * 70)

# Check sync times for last 7 days
print("\n1. Data creation times by day (last 7 days):")
print("-" * 70)
cursor.execute("""
    SELECT
        created_at::date as sync_date,
        EXTRACT(HOUR FROM created_at) as hour,
        COUNT(*) as records
    FROM cn_data
    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY created_at::date, EXTRACT(HOUR FROM created_at)
    ORDER BY sync_date DESC, hour
""")
results = cursor.fetchall()

current_date = None
for row in results:
    if current_date != row[0]:
        current_date = row[0]
        print(f"\n   {row[0]}:")
    print(f"      {int(row[1]):02d}:00 - {row[2]} records")

# Check if there are updates (not just creates) at different times
print("\n\n2. Data UPDATE times by day (last 7 days):")
print("-" * 70)
cursor.execute("""
    SELECT
        updated_at::date as update_date,
        EXTRACT(HOUR FROM updated_at) as hour,
        COUNT(*) as records
    FROM cn_data
    WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days'
    AND updated_at != created_at
    GROUP BY updated_at::date, EXTRACT(HOUR FROM updated_at)
    ORDER BY update_date DESC, hour
""")
results = cursor.fetchall()

if results:
    current_date = None
    for row in results:
        if current_date != row[0]:
            current_date = row[0]
            print(f"\n   {row[0]}:")
        print(f"      {int(row[1]):02d}:00 - {row[2]} records updated")
else:
    print("   No records where updated_at differs from created_at")

# Check distinct sync times
print("\n\n3. Distinct sync timestamps (last 7 days):")
print("-" * 70)
cursor.execute("""
    SELECT DISTINCT created_at
    FROM cn_data
    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
    ORDER BY created_at DESC
    LIMIT 20
""")
results = cursor.fetchall()
for row in results:
    print(f"   {row[0]}")

conn.close()
