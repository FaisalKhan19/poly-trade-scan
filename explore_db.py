import sqlite3
import pandas as pd

conn = sqlite3.connect("activity_tracker.db")

df = pd.read_sql_query("SELECT * FROM events", conn)

df.to_csv("events.csv", index=False)