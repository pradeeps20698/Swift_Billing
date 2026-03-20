import psycopg2
import os
from datetime import datetime, timedelta
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
print(f"Database cn_data Check - {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
print("=" * 70)

# 1. Check latest cn_date in database
cursor.execute("""
    SELECT MAX(cn_date) FROM cn_data WHERE (is_active = true OR is_active::text = 'Yes')
""")
result = cursor.fetchone()
print(f"\n1. Latest CN Date in database: {result[0]}")

# 2. Check CNs by cn_date for last 5 days
print(f"\n2. CN count by cn_date (last 5 days):")
cursor.execute("""
    SELECT cn_date::date, COUNT(*)
    FROM cn_data
    WHERE (is_active = true OR is_active::text = 'Yes')
    AND cn_date >= CURRENT_DATE - INTERVAL '5 days'
    GROUP BY cn_date::date
    ORDER BY cn_date::date DESC
""")
results = cursor.fetchall()
for row in results:
    print(f"   {row[0]}: {row[1]} CNs")

# 3. Check today's CNs
cursor.execute("""
    SELECT COUNT(*) FROM cn_data
    WHERE (is_active = true OR is_active::text = 'Yes')
    AND cn_date::date = CURRENT_DATE
""")
result = cursor.fetchone()
print(f"\n3. CNs with today's date (18-03-2026): {result[0]}")

# 4. Check latest bill_date
cursor.execute("""
    SELECT MAX(bill_date) FROM cn_data WHERE (is_active = true OR is_active::text = 'Yes') AND bill_date IS NOT NULL
""")
result = cursor.fetchone()
print(f"\n4. Latest Bill Date in database: {result[0]}")

# 5. Check bills by bill_date for last 5 days
print(f"\n5. Bills by bill_date (last 5 days):")
cursor.execute("""
    SELECT bill_date::date, COUNT(DISTINCT bill_no) as bills, COUNT(*) as cns
    FROM cn_data
    WHERE (is_active = true OR is_active::text = 'Yes')
    AND bill_date >= CURRENT_DATE - INTERVAL '5 days'
    GROUP BY bill_date::date
    ORDER BY bill_date::date DESC
""")
results = cursor.fetchall()
for row in results:
    print(f"   {row[0]}: {row[1]} bills, {row[2]} CNs")

# 6. Check updated_at vs created_at pattern
print(f"\n6. Latest records (checking updated_at vs created_at):")
cursor.execute("""
    SELECT cn_no, cn_date, created_at, updated_at
    FROM cn_data
    WHERE (is_active = true OR is_active::text = 'Yes')
    ORDER BY created_at DESC
    LIMIT 5
""")
results = cursor.fetchall()
for row in results:
    print(f"   CN: {row[0]}, CN Date: {row[1]}")
    print(f"      Created: {row[2]}, Updated: {row[3]}")

# 7. Check if there's data created after 7 AM today
cursor.execute("""
    SELECT COUNT(*) FROM cn_data
    WHERE created_at > CURRENT_DATE + INTERVAL '7 hours'
""")
result = cursor.fetchone()
print(f"\n7. Records created after 7 AM today: {result[0]}")

# 8. Check total records
cursor.execute("SELECT COUNT(*) FROM cn_data WHERE (is_active = true OR is_active::text = 'Yes')")
result = cursor.fetchone()
print(f"\n8. Total active records in cn_data: {result[0]}")

conn.close()
print("\n" + "=" * 70)
