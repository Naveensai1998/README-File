from flask import Flask, jsonify
import requests
import sqlite3

app = Flask(__name__)

# Fetch jokes from JokeAPI
def fetch_jokes():
    url = "https://v2.jokeapi.dev/joke/Any?amount=100"
    response = requests.get(url)
    jokes = response.json().get('jokes', [])
    return jokes

# Store jokes in database
def store_jokes_in_db(jokes):
    conn = sqlite3.connect("jokes.db")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jokes (
            id INTEGER PRIMARY KEY,
            category TEXT,
            type TEXT,
            joke TEXT,
            setup TEXT,
            delivery TEXT,
            nsfw INTEGER,
            political INTEGER,
            sexist INTEGER,
            safe INTEGER,
            lang TEXT
        )
    """)
    for joke in jokes:
        values = (
            joke['category'], joke['type'], 
            joke.get('joke', None), joke.get('setup', None),
            joke.get('delivery', None), joke['flags']['nsfw'], 
            joke['flags']['political'], joke['flags']['sexist'], 
            joke['safe'], joke['lang']
        )
        conn.execute("""
            INSERT INTO jokes (category, type, joke, setup, delivery, nsfw, political, sexist, safe, lang)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, values)
    conn.commit()
    conn.close()

@app.route('/fetch-jokes', methods=['GET'])
def fetch_and_store_jokes():
    jokes = fetch_jokes()
    store_jokes_in_db(jokes)
    return jsonify({"message": "Jokes fetched and stored successfully!"})

if __name__ == "__main__":
    app.run(debug=True)
