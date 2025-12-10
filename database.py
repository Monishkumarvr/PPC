import sqlite3
import hashlib
import json
from datetime import datetime
import pandas as pd

DB_FILE = "foundry_planner.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Users Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL
        )
    ''')
    
    # History Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            timestamp TEXT,
            melt_capacity REAL,
            total_orders INTEGER,
            fulfillment_pct REAL,
            total_melt_tons REAL,
            run_status TEXT,
            FOREIGN KEY (username) REFERENCES users (username)
        )
    ''')
    
    # Seed default admin user if not exists
    # Default: admin / admin123
    default_user = "admin"
    default_pass = "admin123"
    pass_hash = hashlib.sha256(default_pass.encode()).hexdigest()
    
    try:
        c.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (default_user, pass_hash))
    except sqlite3.IntegrityError:
        pass # Already exists
        
    conn.commit()
    conn.close()

def verify_user(username, password):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    pass_hash = hashlib.sha256(password.encode()).hexdigest()
    
    c.execute("SELECT * FROM users WHERE username = ? AND password_hash = ?", (username, pass_hash))
    user = c.fetchone()
    conn.close()
    return user is not None

def log_run(username, melt_capacity, total_orders, fulfillment_pct, total_melt_tons, run_status="Success"):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    c.execute('''
        INSERT INTO history (username, timestamp, melt_capacity, total_orders, fulfillment_pct, total_melt_tons, run_status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (username, timestamp, melt_capacity, total_orders, fulfillment_pct, total_melt_tons, run_status))
    
    conn.commit()
    conn.close()

def get_history(username):
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM history WHERE username = ? ORDER BY id DESC", conn, params=(username,))
    conn.close()
    return df
