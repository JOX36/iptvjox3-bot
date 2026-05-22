from flask import Flask
from threading import Thread
import bot

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot activo ✅"

def run_flask():
    app.run(host='0.0.0.0', port=8000)

def run_bot():
    bot.main()

if __name__ == '__main__':
    t1 = Thread(target=run_flask)
    t2 = Thread(target=run_bot)
    t1.daemon = True
    t1.start()
    t2.start()
    t2.join()
