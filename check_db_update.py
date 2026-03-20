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

print("=" * 60)
print(f"Database Check - {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
print("=" * 60)

# Check latest updated_at
cursor.execute("SELECT MAX(updated_at) FROM cn_data WHERE (is_active = true OR is_active::text = 'Yes')")
result = cursor.fetchone()
print(f"\n1. Latest updated_at: {result[0]}")

# Check records updated today
cursor.execute("""
    SELECT COUNT(*) FROM cn_data
    WHERE (is_active = true OR is_active::text = 'Yes')
    AND updated_at::date = CURRENT_DATE
""")
result = cursor.fetchone()
print(f"2. Records updated today: {result[0]}")

# Check records by update hour today
cursor.execute("""
    SELECT
        EXTRACT(HOUR FROM updated_at) as hour,
        COUNT(*) as count
    FROM cn_data
    WHERE (is_active = true OR is_active::text = 'Yes')
    AND updated_at::date = CURRENT_DATE
    GROUP BY EXTRACT(HOUR FROM updated_at)
    ORDER BY hour
""")
results = cursor.fetchall()
print(f"\n3. Records by hour today:")
for row in results:
    print(f"   {int(row[0]):02d}:00 - {row[1]} records")

# Check latest CN entries
cursor.execute("""
    SELECT cn_no, cn_date, bill_date, updated_at
    FROM cn_data
    WHERE (is_active = true OR is_active::text = 'Yes')
    ORDER BY updated_at DESC
    LIMIT 5
""")
results = cursor.fetchall()
print(f"\n4. Latest 5 records by updated_at:")
for row in results:
    print(f"   CN: {row[0]}, CN Date: {row[1]}, Bill Date: {row[2]}, Updated: {row[3]}")

# Check if there are any records with created_at column
cursor.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'cn_data'
    AND column_name IN ('created_at', 'updated_at', 'synced_at')
""")
results = cursor.fetchall()
print(f"\n5. Timestamp columns in cn_data: {[r[0] for r in results]}")

conn.close()
print("\n" + "=" * 60)
