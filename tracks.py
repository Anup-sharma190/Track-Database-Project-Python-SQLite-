"""
Track Database Normalization Project
------------------------------------
This project demonstrates how to build a normalized relational database
for music tracks using Python, SQLite, and CSV.

Author: Anup Sharma
Skills: Python, SQLite, Database Normalization, CSV Handling
"""

import sqlite3
import csv

# -------------------------------------------------------
# STEP 1: Connect to SQLite database (or create if missing)
# -------------------------------------------------------
conn = sqlite3.connect('trackdb.sqlite')
cur = conn.cursor()

# -------------------------------------------------------
# STEP 2: Create normalized tables: Artist, Genre, Album, Track
# -------------------------------------------------------
cur.executescript('''
DROP TABLE IF EXISTS Artist;
DROP TABLE IF EXISTS Genre;
DROP TABLE IF EXISTS Album;
DROP TABLE IF EXISTS Track;

CREATE TABLE Artist (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name   TEXT UNIQUE
);

CREATE TABLE Genre (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name   TEXT UNIQUE
);

CREATE TABLE Album (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    title  TEXT UNIQUE,
    artist_id INTEGER
);

CREATE TABLE Track (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    title  TEXT UNIQUE,
    album_id  INTEGER,
    genre_id  INTEGER,
    len    INTEGER, 
    rating INTEGER, 
    count  INTEGER
);
''')

# -------------------------------------------------------
# STEP 3: Open the CSV file containing track data
# -------------------------------------------------------
fname = 'tracks.csv'   # CSV file must exist in the same folder
with open(fname) as f:
    reader = csv.reader(f)
    next(reader)  # Skip header row

    # ---------------------------------------------------
    # STEP 4: Process each row and insert into normalized tables
    # ---------------------------------------------------
    for row in reader:
        if len(row) < 8:
            continue  # skip incomplete rows

        # Extract fields from CSV row
        name, artist, album, count, rating, length, genre = (
            row[0], row[1], row[2], row[3], row[4], row[5], row[7]
        )

        # Insert artist (ignore if already exists)
        cur.execute('INSERT OR IGNORE INTO Artist (name) VALUES (?)', (artist,))
        cur.execute('SELECT id FROM Artist WHERE name = ?', (artist,))
        artist_id = cur.fetchone()[0]

        # Insert genre (ignore if already exists)
        cur.execute('INSERT OR IGNORE INTO Genre (name) VALUES (?)', (genre,))
        cur.execute('SELECT id FROM Genre WHERE name = ?', (genre,))
        genre_id = cur.fetchone()[0]

        # Insert album (ignore if already exists)
        cur.execute('INSERT OR IGNORE INTO Album (title, artist_id) VALUES (?, ?)', (album, artist_id))
        cur.execute('SELECT id FROM Album WHERE title = ?', (album,))
        album_id = cur.fetchone()[0]

        # Insert track (replace if already exists)
        cur.execute('''
            INSERT OR REPLACE INTO Track
            (title, album_id, genre_id, len, rating, count)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (name, album_id, genre_id, length, rating, count))

# -------------------------------------------------------
# STEP 5: Commit changes and close connection
# -------------------------------------------------------
conn.commit()
conn.close()

print("✅ Track database created successfully: trackdb.sqlite")
