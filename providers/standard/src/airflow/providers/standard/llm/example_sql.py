import sqlite3

# Connect to SQLite (creates the file if it doesn't exist)
conn = sqlite3.connect("/tmp/sample.db")
cursor = conn.cursor()

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    age INTEGER,
    salary REAL,
    department TEXT,
    join_date TEXT,
    active INTEGER DEFAULT 1
);
""")

print(cursor.execute("""
PRAGMA table_info('employees')
""").fetchall())

# Commit and close
conn.commit()
conn.close()

print("Table 'employees' created successfully!")
