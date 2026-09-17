# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  webserver.py  ·  RiRuRu Music  ·  Render Keep-Alive
#  Developer : @iam_eshh
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
from flask import Flask
from threading import Thread
import logging

logging.getLogger("werkzeug").setLevel(logging.ERROR)
app = Flask(__name__)

@app.route("/")
def home():
    return (
        "<html><body style='margin:0;background:#09090f;"
        "display:flex;align-items:center;justify-content:center;"
        "height:100vh;font-family:Segoe UI,sans-serif'>"
        "<div style='text-align:center'>"
        "<h1 style='font-size:2.6rem;background:linear-gradient(135deg,#c084fc,#f472b6);"
        "-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin:0 0 .5rem'>"
        "&#127925; RiRuRu Music</h1>"
        "<p style='color:#a78bfa;margin:0;font-size:1rem'>Bot is alive · 24/7</p>"
        "<p style='color:#374151;margin:.4rem 0 0;font-size:.8rem'>"
        "pyrofork · py-tgcalls · yt-dlp</p>"
        "</div></body></html>"
    )

@app.route("/health")
def health():
    return {"status": "ok", "bot": "RiRuRu Music"}, 200

def _run():
    app.run(host="0.0.0.0", port=8080)

def keep_alive():
    Thread(target=_run, daemon=True).start()
