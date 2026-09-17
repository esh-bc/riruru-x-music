# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  config.py  ·  RiRuRu Music
#  Developer  : @iam_eshh
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
import re
from os import getenv
from dotenv import load_dotenv
from pyrogram import filters

load_dotenv()

# ── Telegram API ─────────────────────────────────────────────
API_ID   = int(getenv("API_ID",  30439917))
API_HASH = getenv("API_HASH",    "4f408081dbb976a9943ada5b551288b7")
BOT_TOKEN = getenv("BOT_TOKEN",  "8719204270:AAHT9f7YlaQOBvjHYfeDUZ3AAhluBz_tFSg")

# ── MongoDB ──────────────────────────────────────────────────
MONGO_DB_URI = getenv("MONGO_DB_URI", "")

# ── Owner & Admins ───────────────────────────────────────────
OWNER_ID  = int(getenv("OWNER_ID",  8189708860))
LOGGER_ID = int(getenv("LOGGER_ID", 0))
# Comma-separated extra admin IDs e.g. "123,456"
_raw = getenv("ADMIN_IDS", "")
ADMIN_IDS: list = [int(x) for x in _raw.split(",") if x.strip().isdigit()]
if OWNER_ID and OWNER_ID not in ADMIN_IDS:
    ADMIN_IDS.insert(0, OWNER_ID)

# ── Bot branding ─────────────────────────────────────────────
BOT_NAME      = "𝐑𝐢𝐑𝐮𝐑𝐮 𝐌𝐮𝐬𝐢𝐜"
DEVELOPER     = "@iam_eshh"
DEVELOPER_URL = "https://t.me/iam_eshh"

# ── Support links ────────────────────────────────────────────
SUPPORT_CHANNEL = getenv("SUPPORT_CHANNEL", "")
SUPPORT_CHAT    = getenv("SUPPORT_CHAT",    "")
UPSTREAM_REPO   = getenv("UPSTREAM_REPO",  "")
UPSTREAM_BRANCH = getenv("UPSTREAM_BRANCH", "master")

# ── Assistant sessions ───────────────────────────────────────
STRING1 = getenv("STRING_SESSION",  None)
STRING2 = getenv("STRING_SESSION2", None)
STRING3 = getenv("STRING_SESSION3", None)
STRING4 = getenv("STRING_SESSION4", None)
STRING5 = getenv("STRING_SESSION5", None)

# ── Spotify ──────────────────────────────────────────────────
SPOTIFY_CLIENT_ID     = getenv("SPOTIFY_CLIENT_ID",     None)
SPOTIFY_CLIENT_SECRET = getenv("SPOTIFY_CLIENT_SECRET", None)

# ── OpenAI ───────────────────────────────────────────────────
OPENAI_API_KEY = getenv("OPENAI_API_KEY", None)

# ── Start media ──────────────────────────────────────────────
# Paste the file_id of a video from any PUBLIC Telegram channel.
# The bot will send it as a video note on /start.
# Leave empty to fall back to a photo.
START_VIDEO_FILE_ID = getenv("START_VIDEO_FILE_ID", None)
START_IMG_URL       = getenv("START_IMG_URL", "https://files.catbox.moe/vf873g.jpg")
PING_IMG_URL        = getenv("PING_IMG_URL",  "https://files.catbox.moe/t1l756.jpg")

# ── Force-subscribe ──────────────────────────────────────────
# Set channel username (no @) or negative int chat_id.
# Bot must be admin with "Invite via Link" right in that channel.
FORCE_SUB_CHANNEL = getenv("FORCE_SUB_CHANNEL", None)

# ── Limits ───────────────────────────────────────────────────
DURATION_LIMIT_MIN      = int(getenv("DURATION_LIMIT",          18000))
PLAYLIST_FETCH_LIMIT    = int(getenv("PLAYLIST_FETCH_LIMIT",    25))
TG_AUDIO_FILESIZE_LIMIT = int(getenv("TG_AUDIO_FILESIZE_LIMIT", 104857600))   # 100 MB
TG_VIDEO_FILESIZE_LIMIT = int(getenv("TG_VIDEO_FILESIZE_LIMIT", 1073741824))  # 1  GB

# ── Feature flags ────────────────────────────────────────────
AUTO_LEAVING_ASSISTANT = bool(getenv("AUTO_LEAVING_ASSISTANT", True))
ADS_MODE               = getenv("ADS_MODE", None)

# ── Heroku ───────────────────────────────────────────────────
HEROKU_APP_NAME = getenv("HEROKU_APP_NAME", None)
HEROKU_API_KEY  = getenv("HEROKU_API_KEY",  None)
GIT_TOKEN       = getenv("GIT_TOKEN",       None)

# ── Runtime shared state ─────────────────────────────────────
BANNED_USERS = filters.user()
adminlist    = {}
lyrical      = {}
votemode     = {}
autoclean    = []
confirmer    = {}

# ── Helpers ──────────────────────────────────────────────────
def time_to_seconds(t: str) -> int:
    return sum(int(x) * 60 ** i for i, x in enumerate(reversed(str(t).split(":"))))

DURATION_LIMIT = int(time_to_seconds(f"{DURATION_LIMIT_MIN}:00"))

for _url, _name in ((SUPPORT_CHANNEL, "SUPPORT_CHANNEL"), (SUPPORT_CHAT, "SUPPORT_CHAT")):
    if _url and not re.match(r"https?://", _url):
        raise SystemExit(f"[ERROR] {_name} must start with https://")
