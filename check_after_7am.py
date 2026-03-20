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
print(f"Records Created After 7 AM Today - {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
print("=" * 70)

# Check records created after 7 AM today
cursor.execute("""
    SELECT cn_no, cn_date, bill_no, bill_date, created_at, updated_at, is_active
    FROM cn_data
    WHERE created_at > CURRENT_DATE + INTERVAL '7 hours'
    ORDER BY created_at DESC
    LIMIT 20
""")
results = cursor.fetchall()

print(f"\nLatest 20 records created after 7 AM:")
print("-" * 70)
for row in results:
    print(f"CN: {row[0]}")
    print(f"   CN Date: {row[1]}, Bill No: {row[2]}, Bill Date: {row[3]}")
    print(f"   Created: {row[4]}, Updated: {row[5]}, Active: {row[6]}")
    print()

# Check by hour
print("\n" + "=" * 70)
print("Records created by hour today:")
print("=" * 70)
cursor.execute("""
    SELECT
        EXTRACT(HOUR FROM created_at) as hour,
        COUNT(*) as count,
        COUNT(*) FILTER (WHERE is_active = true OR is_active::text = 'Yes') as active_count
    FROM cn_data
    WHERE created_at::date = CURRENT_DATE
    GROUP BY EXTRACT(HOUR FROM created_at)
    ORDER BY hour
""")
results = cursor.fetchall()
for row in results:
    print(f"   {int(row[0]):02d}:00 - {row[1]} total, {row[2]} active")

# Check if there's a pattern with is_active
print("\n" + "=" * 70)
print("Records after 7 AM by is_active status:")
cursor.execute("""
    SELECT is_active, COUNT(*)
    FROM cn_data
    WHERE created_at > CURRENT_DATE + INTERVAL '7 hours'
    GROUP BY is_active
""")
results = cursor.fetchall()
for row in results:
    print(f"   is_active = {row[0]}: {row[1]} records")

conn.close()
