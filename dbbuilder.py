#!/usr/bin/python3.6

import sqlite3
conn = sqlite3.connect("rinspect.sqlite")
cursor = conn.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS fdinsp (
librow INTEGER,
county TEXT,
licnum TEXT,
sitename TEXT,
streetaddy TEXT,
cityaddy TEXT,
zip TEXT,
inspnum TEXT,
insptype TEXT,
inspdispos TEXT,
inspdate DATETIME,
totalvio INTEGER,
highvio INTEGER,
licid TEXT,
visitid INTEGER UNIQUE,
time_now DATETIME,
time_posted DATETIME DEFAULT (STRFTIME('%Y-%m-%d %H:%M', 'NOW', 'localtime')))
""")

cursor.execute("""CREATE TABLE IF NOT EXISTS violations (
id INTEGER PRIMARY KEY,
visitid INTEGER,
violation TEXT,
details_id INTEGER,
obs TEXT)
""")

conn.commit()
conn.close()
