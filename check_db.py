import sqlite3

conn = sqlite3.connect('warehouse.db')
cursor = conn.cursor()

cursor.execute("SELECT * FROM sensor_logs ORDER BY id DESC LIMIT 10")
rows = cursor.fetchall()

print("--- LAST 10 ENTRIES IN WAREHOUSE ---")
for r in rows:
    print(r)

conn.close()