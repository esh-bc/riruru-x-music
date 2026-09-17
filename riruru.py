# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
#   𝐑𝐢𝐑𝐮𝐑𝐮 𝐌𝐮𝐬𝐢𝐜  —  Production-Grade Telegram Voice Chat Bot
#   Developer : @iam_eshh
#   Engine    : pyrofork + py-tgcalls 2.2 + yt-dlp
#   UI        : Telegram HTML · blockquote · colourful buttons
#   Sources   : YouTube · Spotify · SoundCloud · Telegram Files
#   Features  : Queue · Loop · Shuffle · VotSkip · Admin Panel
#               Force-Subscribe · OTP VC Add · Broadcast · AI
#   Deploy    : Render / Koyeb / Heroku / VPS  (24 × 7 uptime)
#
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ── stdlib ───────────────────────────────────────────────────────
import asyncio, os, re, sys, time, math, json, random
import logging, traceback
from datetime import datetime, timedelta
from io import BytesIO

# ── env ──────────────────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv()

# ── third-party ──────────────────────────────────────────────────
import httpx
import psutil
import humanize
import motor.motor_asyncio
import yt_dlp
from PIL import Image

# ── Pyrogram ─────────────────────────────────────────────────────
from pyrogram import Client, filters, idle
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from pyrogram.errors import (
    FloodWait, UserNotParticipant, ChatAdminRequired,
    UserAlreadyParticipant, ChannelInvalid, PeerIdInvalid,
    MessageNotModified, RPCError,
)
from pyrogram.enums import ParseMode, ChatMemberStatus, ChatType

# ── PyTgCalls ────────────────────────────────────────────────────
from pytgcalls import PyTgCalls
from pytgcalls.types import MediaStream
from pytgcalls.exceptions import (
    NoActiveGroupCall, GroupCallNotFound, AlreadyJoinedError,
)

# ── Spotify ──────────────────────────────────────────────────────
try:
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    _SPOTIFY_AVAILABLE = True
except ImportError:
    _SPOTIFY_AVAILABLE = False

# ── OTP ──────────────────────────────────────────────────────────
import pyotp

# ── Scheduler ────────────────────────────────────────────────────
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Webserver ────────────────────────────────────────────────────
from webserver import keep_alive

# ── Config ───────────────────────────────────────────────────────
from config import (
    API_ID, API_HASH, BOT_TOKEN, MONGO_DB_URI,
    OWNER_ID, LOGGER_ID, ADMIN_IDS,
    STRING1, STRING2, STRING3, STRING4, STRING5,
    SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, OPENAI_API_KEY,
    BOT_NAME, DEVELOPER, DEVELOPER_URL,
    SUPPORT_CHANNEL, SUPPORT_CHAT, UPSTREAM_REPO, UPSTREAM_BRANCH,
    START_VIDEO_FILE_ID, START_IMG_URL, PING_IMG_URL,
    FORCE_SUB_CHANNEL,
    DURATION_LIMIT, DURATION_LIMIT_MIN, PLAYLIST_FETCH_LIMIT,
    TG_AUDIO_FILESIZE_LIMIT, TG_VIDEO_FILESIZE_LIMIT,
    AUTO_LEAVING_ASSISTANT,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
LOG = logging.getLogger("RiRuRu")
for _noisy in ("pyrogram", "pytgcalls", "apscheduler", "hachoir"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MONGODB — async motor client
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_mclient = None
_mdb     = None

async def init_db():
    global _mclient, _mdb
    if not MONGO_DB_URI:
        LOG.warning("MONGO_DB_URI not set — DB features disabled.")
        return
    _mclient = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DB_URI)
    _mdb     = _mclient["RiRuRuMusic"]
    LOG.info("MongoDB connected.")

def _col(name: str):
    return _mdb[name] if _mdb is not None else None

async def _get(col: str, q: dict):
    c = _col(col)
    return (await c.find_one(q)) if c is not None else None

async def _upsert(col: str, q: dict, data: dict):
    c = _col(col)
    if c is not None:
        await c.update_one(q, {"$set": data}, upsert=True)

async def _remove(col: str, q: dict):
    c = _col(col)
    if c is not None:
        await c.delete_one(q)

# ── DB: banned users ─────────────────────────────────────────────
async def is_banned(uid: int) -> bool:
    return (await _get("banned", {"uid": uid})) is not None

async def ban_user(uid: int, reason: str = "—"):
    await _upsert("banned", {"uid": uid}, {"uid": uid, "reason": reason, "at": datetime.utcnow().isoformat()})

async def unban_user(uid: int):
    await _remove("banned", {"uid": uid})

# ── DB: served chats ─────────────────────────────────────────────
async def add_served(cid: int):
    await _upsert("served", {"cid": cid}, {"cid": cid})

async def all_served() -> list:
    c = _col("served")
    return [d["cid"] async for d in c.find({})] if c else []

# ── DB: auth users per chat ──────────────────────────────────────
async def get_auth(cid: int) -> list:
    d = await _get("auth", {"cid": cid})
    return d.get("users", []) if d else []

async def add_auth(cid: int, uid: int):
    u = await get_auth(cid)
    if uid not in u:
        u.append(uid)
    await _upsert("auth", {"cid": cid}, {"cid": cid, "users": u})

async def rm_auth(cid: int, uid: int):
    u = await get_auth(cid)
    if uid in u:
        u.remove(uid)
    await _upsert("auth", {"cid": cid}, {"cid": cid, "users": u})

# ── DB: force-subscribe channels ────────────────────────────────
async def get_fsub_channels() -> list:
    c = _col("fsub")
    return [d async for d in c.find({})] if c else []

async def add_fsub(cid, invite: str, title: str):
    await _upsert("fsub", {"cid": cid}, {"cid": cid, "invite": invite, "title": title})

async def rm_fsub(cid):
    await _remove("fsub", {"cid": cid})

# ── DB: OTP VC sessions ──────────────────────────────────────────
async def save_vc_session(phone: str, session: str, name: str):
    await _upsert("vc_sessions", {"phone": phone},
                  {"phone": phone, "session": session, "name": name,
                   "added": datetime.utcnow().isoformat()})

async def get_vc_sessions() -> list:
    c = _col("vc_sessions")
    return [d async for d in c.find({})] if c else []

async def remove_vc_session(phone: str):
    await _remove("vc_sessions", {"phone": phone})

# ── DB: chat settings ────────────────────────────────────────────
async def get_setting(cid: int, key: str, default=None):
    d = await _get("settings", {"cid": cid})
    return d.get(key, default) if d else default

async def set_setting(cid: int, key: str, val):
    await _upsert("settings", {"cid": cid}, {key: val})
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PYROGRAM CLIENTS — Bot + up to 5 assistants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
bot = Client(
    "RiRuRuMusicBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

_strings = [STRING1, STRING2, STRING3, STRING4, STRING5]
assistants: list[Client] = []

for _idx, _s in enumerate(_strings, 1):
    if _s:
        assistants.append(Client(
            f"RiRuAsst{_idx}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=_s,
        ))

# One PyTgCalls instance per assistant client
_calls: dict[int, PyTgCalls] = {}    # asst_index → PyTgCalls
_chat_asst: dict[int, int]   = {}    # chat_id    → asst_index

def _get_call(chat_id: int):
    idx = _chat_asst.get(chat_id)
    if idx is not None:
        return _calls.get(idx)
    return list(_calls.values())[0] if _calls else None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IN-MEMORY PLAYBACK STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Track dict keys: title, url, stream_url, duration, thumb,
#                  requester_id, requester_name, source, vidmode
_queue:    dict[int, list]    = {}   # chat_id → [track, …]
_active:   dict[int, bool]    = {}
_paused:   dict[int, bool]    = {}
_loop:     dict[int, bool]    = {}
_shuffle:  dict[int, bool]    = {}
_volume:   dict[int, int]     = {}   # 0 – 200, default 100
_voteskip: dict[int, set]     = {}
_np:       dict[int, dict]    = {}   # current track
_np_msg:   dict[int, Message] = {}   # now-playing message ref
_start_t:  dict[int, float]   = {}   # epoch when track started

def _q(cid: int) -> list:
    return _queue.setdefault(cid, [])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  UI HELPERS — buttons, HTML strings, progress bar
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def kb(*rows) -> InlineKeyboardMarkup:
    """
    Build InlineKeyboardMarkup from rows of tuples.
    Tuple shapes:
      (text, callback_data)
      (text, url, "url")
    """
    built = []
    for row in rows:
        line = []
        for item in row:
            if len(item) == 3 and item[2] == "url":
                line.append(InlineKeyboardButton(item[0], url=item[1]))
            else:
                line.append(InlineKeyboardButton(item[0], callback_data=item[1]))
        built.append(line)
    return InlineKeyboardMarkup(built)

# ─── Start message keyboard (colourful emojis only here) ─────────
def start_kb(bot_username: str) -> InlineKeyboardMarkup:
    return kb(
        [("➕  Add Me To Your Group",
          f"https://t.me/{bot_username}?startgroup=true", "url")],
        [("👑  Owner",   DEVELOPER_URL,    "url"),
         ("💬  Support", SUPPORT_CHAT,     "url")],
        [("📢  Channel", SUPPORT_CHANNEL,  "url"),
         ("📊  Stats",   "cmd_stats")],
        [("🎵  Commands","help_main"),
         ("✅  Verify",  "fsub_verify")],
    )

# ─── Help keyboard ────────────────────────────────────────────────
def help_kb() -> InlineKeyboardMarkup:
    return kb(
        [("Music Commands",   "help_music"),
         ("Admin Commands",   "help_admin")],
        [("Stats & Info",     "help_stats"),
         ("AI Features",      "help_ai")],
        [("VC & Settings",    "help_vc"),
         ("Back",             "start_back")],
        [("Close",            "close")],
    )

# ─── Now-playing controls ─────────────────────────────────────────
def player_kb(cid: int) -> InlineKeyboardMarkup:
    looped   = _loop.get(cid, False)
    shuffled = _shuffle.get(cid, False)
    paused   = _paused.get(cid, False)
    p_txt = "Resume"  if paused   else "Pause"
    p_cb  = "vc_resume" if paused else "vc_pause"
    l_txt = "Loop: On"  if looped   else "Loop: Off"
    l_cb  = "vc_loopoff" if looped  else "vc_loopon"
    s_txt = "Shuffle: On" if shuffled else "Shuffle"
    s_cb  = "vc_shufoff"  if shuffled else "vc_shufon"
    return kb(
        [(p_txt, p_cb), ("Skip",   "vc_skip"),  ("End",    "vc_end")],
        [(l_txt, l_cb), (s_txt,    s_cb)],
        [("Vol -10",   "vc_vdown"), ("Vol +10", "vc_vup")],
        [("Queue",     "vc_queue"), ("Now Playing", "vc_np")],
        [("Close",     "close")],
    )

# ─── Admin panel keyboard ─────────────────────────────────────────
def admin_kb() -> InlineKeyboardMarkup:
    return kb(
        [("Add VC Account",      "adm_addvc"),
         ("List VC Accounts",    "adm_listvc")],
        [("Add Force-Sub Ch.",   "adm_addfsub"),
         ("Remove Force-Sub Ch.","adm_rmfsub")],
        [("Broadcast",           "adm_broadcast"),
         ("Ban User",            "adm_ban")],
        [("Unban User",          "adm_unban"),
         ("Bot Stats",           "adm_stats")],
        [("Back",  "start_back"), ("Close", "close")],
    )

# ─── Text helpers ─────────────────────────────────────────────────
def fmt_time(sec: int) -> str:
    sec = max(0, int(sec))
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return f"{h:02}:{m:02}:{s:02}" if h else f"{m:02}:{s:02}"

def progress_bar(elapsed: int, total: int, width: int = 12) -> str:
    if total <= 0:
        return "▬" * width + "  ??%"
    ratio  = min(1.0, elapsed / total)
    filled = int(ratio * width)
    bar    = "▣" * filled + "▬" * (width - filled)
    return f"{bar}  {int(ratio * 100)}%"

def mention(user) -> str:
    first = user.first_name or ""
    last  = user.last_name  or ""
    name  = (first + " " + last).strip() or "User"
    return f'<a href="tg://user?id={user.id}">{name}</a>'

def is_sudo(uid: int) -> bool:
    return uid == OWNER_ID or uid in ADMIN_IDS

async def is_chat_admin(client: Client, cid: int, uid: int) -> bool:
    if is_sudo(uid):
        return True
    try:
        m = await client.get_chat_member(cid, uid)
        return m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception:
        return False

async def check_auth(client: Client, msg: Message) -> bool:
    cid = msg.chat.id
    uid = msg.from_user.id if msg.from_user else 0
    if await is_chat_admin(client, cid, uid):
        return True
    return uid in await get_auth(cid)

async def safe_send(client: Client, cid: int, **kwargs):
    try:
        return await client.send_message(cid, **kwargs)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await client.send_message(cid, **kwargs)
    except Exception:
        return None
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FORCE-SUBSCRIBE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def check_fsub(client: Client, msg: Message) -> bool:
    uid = msg.from_user.id if msg.from_user else None
    if uid is None or is_sudo(uid):
        return True

    channels = await get_fsub_channels()
    if not channels:
        return True

    not_joined = []
    for entry in channels:
        cid = entry["cid"]
        try:
            member = await client.get_chat_member(cid, uid)
            if member.status in (ChatMemberStatus.BANNED, ChatMemberStatus.LEFT):
                not_joined.append(entry)
        except UserNotParticipant:
            not_joined.append(entry)
        except Exception:
            pass

    if not not_joined:
        return True

    rows = []
    for entry in not_joined:
        title  = entry.get("title",  "Channel")
        invite = entry.get("invite", "")
        if invite:
            rows.append([(f"Join  {title}", invite, "url")])

    rows.append([("I have joined — Verify", "fsub_verify")])

    await msg.reply(
        "<b>Access Restricted</b>\n\n"
        "<blockquote>"
        "You need to join our channel(s) before using "
        "<b>𝐑𝐢𝐑𝐮𝐑𝐮 𝐌𝐮𝐬𝐢𝐜</b>.\n\n"
        "Tap <b>Join</b> below, then press <b>Verify</b>."
        "</blockquote>",
        reply_markup=kb(*rows),
        parse_mode=ParseMode.HTML,
    )
    return False

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  YT-DLP — search and stream-URL resolution
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_YDL_COMMON = dict(
    quiet=True, no_warnings=True, noplaylist=True,
    geo_bypass=True, source_address="0.0.0.0",
)
_YDL_AUDIO = dict(**_YDL_COMMON, format="bestaudio/best")
_YDL_VIDEO = dict(**_YDL_COMMON, format="bestvideo[height<=720]+bestaudio/best[height<=720]")
_YDL_FLAT  = dict(**_YDL_COMMON, extract_flat=True, default_search="ytsearch5")

async def yt_search(query: str) -> list:
    def _do():
        with yt_dlp.YoutubeDL(_YDL_FLAT) as ydl:
            info = ydl.extract_info(f"ytsearch5:{query}", download=False)
            return info.get("entries", []) or []

    try:
        entries = await asyncio.get_event_loop().run_in_executor(None, _do)
        out = []
        for e in entries:
            if not e:
                continue
            out.append({
                "title":    e.get("title",    "Unknown"),
                "url":      f"https://youtube.com/watch?v={e.get('id','')}",
                "duration": e.get("duration", 0),
                "thumb":    e.get("thumbnail", START_IMG_URL),
                "channel":  e.get("uploader",  "YouTube"),
            })
        return out
    except Exception as exc:
        LOG.error(f"yt_search: {exc}")
        return []

async def yt_resolve(url: str, video: bool = False) -> dict | None:
    opts = dict(_YDL_VIDEO if video else _YDL_AUDIO)

    def _do():
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    try:
        info = await asyncio.get_event_loop().run_in_executor(None, _do)
        if not info:
            return None
        # pick best stream URL
        if info.get("url"):
            stream = info["url"]
        elif info.get("formats"):
            stream = info["formats"][-1]["url"]
        else:
            return None
        return {
            "title":      info.get("title",     "Unknown"),
            "url":        url,
            "stream_url": stream,
            "duration":   info.get("duration",  0),
            "thumb":      info.get("thumbnail", START_IMG_URL),
            "channel":    info.get("uploader",  "YouTube"),
        }
    except Exception as exc:
        LOG.error(f"yt_resolve: {exc}")
        return None

async def sc_resolve(url: str) -> dict | None:
    """SoundCloud URL via yt-dlp."""
    return await yt_resolve(url, video=False)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SPOTIFY — convert to YouTube stream info
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_sp = None
if _SPOTIFY_AVAILABLE and SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET:
    try:
        _sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
        ))
        LOG.info("Spotify connected.")
    except Exception as exc:
        LOG.warning(f"Spotify init failed: {exc}")

async def spotify_resolve(url: str) -> list:
    if not _sp:
        return []

    def _fetch():
        queries = []
        if "track" in url:
            t = _sp.track(url)
            queries.append(f"{t['name']} {t['artists'][0]['name']}")
        elif "album" in url:
            for item in _sp.album_tracks(url)["items"][:PLAYLIST_FETCH_LIMIT]:
                queries.append(f"{item['name']} {item['artists'][0]['name']}")
        elif "playlist" in url:
            for item in _sp.playlist_tracks(url)["items"][:PLAYLIST_FETCH_LIMIT]:
                t = item.get("track")
                if t:
                    queries.append(f"{t['name']} {t['artists'][0]['name']}")
        return queries

    try:
        queries = await asyncio.get_event_loop().run_in_executor(None, _fetch)
        results = []
        for q in queries:
            hits = await yt_search(q)
            if hits:
                results.append(hits[0])
        return results
    except Exception as exc:
        LOG.error(f"spotify_resolve: {exc}")
        return []
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PLAYBACK ENGINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def _join_vc(chat_id: int, stream: MediaStream) -> bool:
    """Join or change stream in a voice chat."""
    for idx, asst in enumerate(assistants):
        call = _calls.get(idx)
        if call is None:
            continue
        try:
            await asst.get_chat_member(chat_id, (await asst.get_me()).id)
        except UserNotParticipant:
            try:
                await asst.join_chat(chat_id)
            except Exception:
                continue
        except Exception:
            try:
                await asst.join_chat(chat_id)
            except Exception:
                continue

        try:
            if _active.get(chat_id):
                await call.change_stream(chat_id, stream)
            else:
                await call.join_group_call(chat_id, stream)
            _chat_asst[chat_id] = idx
            _active[chat_id]    = True
            _paused[chat_id]    = False
            _start_t[chat_id]   = time.time()
            return True
        except AlreadyJoinedError:
            try:
                await call.change_stream(chat_id, stream)
                _chat_asst[chat_id] = idx
                _active[chat_id]    = True
                _paused[chat_id]    = False
                _start_t[chat_id]   = time.time()
                return True
            except Exception as exc:
                LOG.error(f"_join_vc change_stream: {exc}")
                continue
        except NoActiveGroupCall:
            return False
        except Exception as exc:
            LOG.error(f"_join_vc attempt idx={idx}: {exc}")
            continue
    return False

async def _leave_vc(chat_id: int):
    call = _get_call(chat_id)
    if call:
        try:
            await call.leave_group_call(chat_id)
        except Exception:
            pass
    for d in (_active, _paused, _np, _start_t, _voteskip, _chat_asst):
        d.pop(chat_id, None)

async def _build_stream(track: dict) -> MediaStream | None:
    stream_url = track.get("stream_url") or track.get("url")
    if not stream_url:
        return None
    vidmode = track.get("vidmode", False)
    if vidmode:
        return MediaStream(stream_url, video_flags=MediaStream.Flags.IGNORE_PENDING)
    else:
        return MediaStream(
            stream_url,
            video_flags=MediaStream.Flags.NO_VIDEO,
        )

async def play_next(chat_id: int):
    """
    Dequeue next track and start it. If loop is on, re-insert current.
    If queue is empty, leave VC.
    """
    q = _q(chat_id)

    if _loop.get(chat_id) and _np.get(chat_id):
        q.insert(0, _np[chat_id])

    if _shuffle.get(chat_id) and len(q) > 1:
        random.shuffle(q)

    if not q:
        await _leave_vc(chat_id)
        _queue.pop(chat_id, None)
        msg = _np_msg.get(chat_id)
        if msg:
            try:
                await msg.edit_text(
                    "<blockquote><b>Queue ended.</b> Left the voice chat.</blockquote>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        return

    track = q.pop(0)
    _np[chat_id] = track

    # Resolve stream URL if not yet done
    if not track.get("stream_url"):
        resolved = await yt_resolve(track["url"], video=track.get("vidmode", False))
        if not resolved:
            await play_next(chat_id)
            return
        track.update(resolved)

    stream = await _build_stream(track)
    if not stream:
        await play_next(chat_id)
        return

    joined = await _join_vc(chat_id, stream)
    if not joined:
        msg = _np_msg.get(chat_id)
        if msg:
            try:
                await msg.reply(
                    "<blockquote>Could not join voice chat. "
                    "Make sure an assistant account is in the group "
                    "and a voice chat is active.</blockquote>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        return

    # Edit or send now-playing message
    np_text = _np_text(track, chat_id)
    markup  = player_kb(chat_id)
    old_msg = _np_msg.get(chat_id)
    try:
        if old_msg:
            try:
                await old_msg.delete()
            except Exception:
                pass
        new_msg = await bot.send_photo(
            chat_id,
            photo=track.get("thumb", START_IMG_URL),
            caption=np_text,
            reply_markup=markup,
            parse_mode=ParseMode.HTML,
        )
        _np_msg[chat_id] = new_msg
    except Exception as exc:
        LOG.error(f"play_next send photo: {exc}")

def _np_text(track: dict, chat_id: int) -> str:
    title    = track.get("title",          "Unknown")
    dur      = fmt_time(track.get("duration", 0))
    source   = track.get("source",         "YouTube")
    req      = track.get("requester_name", "Unknown")
    looped   = "On"  if _loop.get(chat_id)    else "Off"
    shuffled = "On"  if _shuffle.get(chat_id) else "Off"
    vol      = _volume.get(chat_id, 100)
    return (
        f"<b>Now Playing</b>\n\n"
        f"<blockquote>"
        f"<b>Title :</b>  {title}\n"
        f"<b>Duration :</b>  {dur}\n"
        f"<b>Source :</b>  {source}\n"
        f"<b>Requested by :</b>  {req}\n"
        f"<b>Loop :</b>  {looped}   <b>Shuffle :</b>  {shuffled}   "
        f"<b>Volume :</b>  {vol}%"
        f"</blockquote>"
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PyTgCalls EVENT: stream ended → auto play next
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _register_call_handlers(call: PyTgCalls):
    @call.on_stream_end()
    async def _on_end(_, update):
        chat_id = update.chat_id
        await play_next(chat_id)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  /start  — video + blockquote UI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
START_TEXT = (
    "<b>𝐑𝐢𝐑𝐮𝐑𝐮 𝐌𝐮𝐬𝐢𝐜</b>\n\n"
    "🎵 <b>Welcome To 𝐑𝐢𝐑𝐮𝐑𝐮 𝐌𝐮𝐬𝐢𝐜</b> 🎵\n\n"
    "<blockquote>"
    "A Premium Audio Streaming Engine\n"
    "Crafted For Luxury Voice Chats.\n\n"
    "<b>Supported Modules</b>\n"
    "YouTube  ·  Spotify  ·  SoundCloud\n\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "<b>Music Features</b>\n"
    "  High Quality Audio Streaming\n"
    "  Fast &amp; Smooth Playback\n"
    "  Queue  ·  Loop  ·  Shuffle\n"
    "  Vote Skip  ·  Volume Control\n"
    "  Lyrics  ·  AI Search\n\n"
    "Tap <b>Commands</b> to explore all features.\n\n"
    "<i>Experience The Supremacy</i>"
    "</blockquote>\n\n"
    f"<b>Developer :</b>  <a href='{DEVELOPER_URL}'>{DEVELOPER}</a>"
)

@bot.on_message(filters.command("start") & filters.private)
async def cmd_start_private(client: Client, msg: Message):
    uid = msg.from_user.id
    if await is_banned(uid):
        return await msg.reply(
            "<blockquote><b>You are banned from using this bot.</b></blockquote>",
            parse_mode=ParseMode.HTML,
        )

    if not await check_fsub(client, msg):
        return

    await add_served(uid)
    me = await client.get_me()

    if START_VIDEO_FILE_ID:
        try:
            await msg.reply_video(
                video=START_VIDEO_FILE_ID,
                caption=START_TEXT,
                reply_markup=start_kb(me.username or ""),
                parse_mode=ParseMode.HTML,
            )
            return
        except Exception as exc:
            LOG.warning(f"start video send failed: {exc} — falling back to photo")

    await msg.reply_photo(
        photo=START_IMG_URL,
        caption=START_TEXT,
        reply_markup=start_kb(me.username or ""),
        parse_mode=ParseMode.HTML,
    )

@bot.on_message(filters.command("start") & filters.group)
async def cmd_start_group(client: Client, msg: Message):
    me = await client.get_me()
    await msg.reply(
        "<blockquote><b>𝐑𝐢𝐑𝐮𝐑𝐮 𝐌𝐮𝐬𝐢𝐜</b> is ready.\n"
        "Use /play [song name or link] to start streaming.</blockquote>",
        reply_markup=kb(
            [("Commands", "help_main"),
             ("Add to Group",
              f"https://t.me/{me.username}?startgroup=true", "url")],
        ),
        parse_mode=ParseMode.HTML,
    )

# ── fsub verify callback ──────────────────────────────────────────
@bot.on_callback_query(filters.regex("^fsub_verify$"))
async def cb_fsub_verify(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    channels = await get_fsub_channels()
    if not channels:
        return await cq.answer("No channels configured.", show_alert=True)

    not_joined = []
    for entry in channels:
        cid = entry["cid"]
        try:
            m = await client.get_chat_member(cid, uid)
            if m.status in (ChatMemberStatus.BANNED, ChatMemberStatus.LEFT):
                not_joined.append(entry.get("title", str(cid)))
        except UserNotParticipant:
            not_joined.append(entry.get("title", str(cid)))
        except Exception:
            pass

    if not_joined:
        await cq.answer(
            f"You haven't joined: {', '.join(not_joined)}",
            show_alert=True,
        )
    else:
        await cq.answer("Verified! You can now use the bot.", show_alert=True)
        try:
            me = await client.get_me()
            await cq.message.edit_media(
                media=None,
            )
        except Exception:
            pass
        await cq.message.reply_photo(
            photo=START_IMG_URL,
            caption=START_TEXT,
            reply_markup=start_kb(me.username or ""),
            parse_mode=ParseMode.HTML,
        )

# ── start_back callback ───────────────────────────────────────────
@bot.on_callback_query(filters.regex("^start_back$"))
async def cb_start_back(client: Client, cq: CallbackQuery):
    me = await client.get_me()
    try:
        await cq.message.edit_caption(
            caption=START_TEXT,
            reply_markup=start_kb(me.username or ""),
            parse_mode=ParseMode.HTML,
        )
    except MessageNotModified:
        pass
    await cq.answer()

# ── close callback ────────────────────────────────────────────────
@bot.on_callback_query(filters.regex("^close$"))
async def cb_close(_, cq: CallbackQuery):
    try:
        await cq.message.delete()
    except Exception:
        pass
    await cq.answer()
    )
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HELP callbacks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HELP_MUSIC = (
    "<b>Music Commands</b>\n\n"
    "<blockquote expandable>"
    "/play [name or link]  —  Play audio in VC\n"
    "/vplay [name or link]  —  Play video in VC\n"
    "/pause  —  Pause playback\n"
    "/resume  —  Resume playback\n"
    "/skip  —  Skip current track\n"
    "/stop  —  Stop and leave VC\n"
    "/queue  —  Show current queue\n"
    "/np  —  Now playing info\n"
    "/loop  —  Toggle loop mode\n"
    "/shuffle  —  Toggle shuffle\n"
    "/volume [0-200]  —  Set volume\n"
    "/seek [seconds]  —  Seek to position\n"
    "/lyrics [song]  —  Fetch lyrics\n"
    "/search [query]  —  Search YouTube\n"
    "/playlist [name or link]  —  Queue a playlist"
    "</blockquote>"
)

HELP_ADMIN = (
    "<b>Admin Commands</b>\n\n"
    "<blockquote expandable>"
    "/auth [@user]  —  Authorise user for bot controls\n"
    "/unauth [@user]  —  Remove authorisation\n"
    "/ban [@user]  —  Ban user from bot\n"
    "/unban [@user]  —  Unban user\n"
    "/broadcast [text]  —  Broadcast to all served chats\n"
    "/addvc  —  Add VC assistant via OTP\n"
    "/listvc  —  List active VC accounts\n"
    "/rmvc [phone]  —  Remove VC account\n"
    "/addfsub  —  Add force-subscribe channel\n"
    "/rmfsub  —  Remove force-subscribe channel\n"
    "/listfsub  —  List force-subscribe channels\n"
    "/adminpanel  —  Open admin panel"
    "</blockquote>"
)

HELP_STATS = (
    "<b>Stats &amp; Info</b>\n\n"
    "<blockquote>"
    "/ping  —  Bot latency\n"
    "/stats  —  Bot resource usage\n"
    "/uptime  —  How long bot has been running\n"
    "/repo  —  Source code link"
    "</blockquote>"
)

HELP_AI = (
    "<b>AI Features</b>\n\n"
    "<blockquote>"
    "/lyrics [song]  —  AI-enhanced lyrics lookup\n"
    "/ask [question]  —  Ask the AI anything\n"
    "/recommend  —  Get song recommendations"
    "</blockquote>"
)

HELP_VC = (
    "<b>VC &amp; Settings</b>\n\n"
    "<blockquote>"
    "/joinvc  —  Make assistant join VC\n"
    "/leavevc  —  Make assistant leave VC\n"
    "/loop  —  Toggle track loop\n"
    "/shuffle  —  Toggle queue shuffle\n"
    "/volume [0-200]  —  Adjust volume\n"
    "/seek [sec]  —  Seek position\n"
    "/votemode  —  Toggle vote-skip mode\n"
    "/autoclean  —  Toggle auto-clean messages"
    "</blockquote>"
)

_help_map = {
    "help_main":     (HELP_MUSIC,  help_kb()),
    "help_music":    (HELP_MUSIC,  help_kb()),
    "help_admin":    (HELP_ADMIN,  help_kb()),
    "help_stats":    (HELP_STATS,  help_kb()),
    "help_ai":       (HELP_AI,     help_kb()),
    "help_vc":       (HELP_VC,     help_kb()),
}

@bot.on_callback_query(filters.regex(r"^help_(main|music|admin|stats|ai|vc)$"))
async def cb_help(_, cq: CallbackQuery):
    key = cq.data
    text, markup = _help_map.get(key, (HELP_MUSIC, help_kb()))
    try:
        await cq.message.edit_text(
            text, reply_markup=markup, parse_mode=ParseMode.HTML,
        )
    except MessageNotModified:
        pass
    await cq.answer()

@bot.on_message(filters.command("help"))
async def cmd_help(_, msg: Message):
    await msg.reply(
        HELP_MUSIC,
        reply_markup=help_kb(),
        parse_mode=ParseMode.HTML,
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  /play  and  /vplay
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def _handle_play(client: Client, msg: Message, vidmode: bool):
    cid = msg.chat.id
    uid = msg.from_user.id if msg.from_user else 0

    if not await check_auth(client, msg):
        return await msg.reply(
            "<blockquote>You are not authorised to use playback controls here.</blockquote>",
            parse_mode=ParseMode.HTML,
        )

    args = msg.text.split(None, 1)
    query = args[1].strip() if len(args) > 1 else ""

    # Telegram audio/video file
    if msg.reply_to_message:
        replied = msg.reply_to_message
        audio = replied.audio or replied.voice or replied.video or replied.document
        if audio:
            if audio.file_size > (TG_VIDEO_FILESIZE_LIMIT if vidmode else TG_AUDIO_FILESIZE_LIMIT):
                return await msg.reply(
                    "<blockquote>File size exceeds the allowed limit.</blockquote>",
                    parse_mode=ParseMode.HTML,
                )
            wait = await msg.reply(
                "<blockquote>Downloading from Telegram…</blockquote>",
                parse_mode=ParseMode.HTML,
            )
            path = await replied.download()
            await wait.delete()
            track = {
                "title":          audio.file_name or "Telegram File",
                "url":            path,
                "stream_url":     path,
                "duration":       getattr(audio, "duration", 0) or 0,
                "thumb":          START_IMG_URL,
                "requester_id":   uid,
                "requester_name": msg.from_user.first_name if msg.from_user else "User",
                "source":         "Telegram",
                "vidmode":        vidmode,
            }
            _q(cid).append(track)
            if not _active.get(cid):
                await play_next(cid)
            else:
                await msg.reply(
                    f"<blockquote>Added to queue: <b>{track['title']}</b></blockquote>",
                    parse_mode=ParseMode.HTML,
                )
            return

    if not query:
        return await msg.reply(
            "<blockquote>Usage: <code>/play song name or link</code></blockquote>",
            parse_mode=ParseMode.HTML,
        )

    wait = await msg.reply(
        "<blockquote>Searching…</blockquote>",
        parse_mode=ParseMode.HTML,
    )

    tracks = []

    # Spotify
    if "spotify.com" in query:
        tracks = await spotify_resolve(query)
        source = "Spotify"
    # YouTube playlist
    elif "youtube.com/playlist" in query or "list=" in query:
        results = await yt_search(query)
        tracks  = results[:PLAYLIST_FETCH_LIMIT]
        source  = "YouTube"
    # SoundCloud
    elif "soundcloud.com" in query:
        resolved = await sc_resolve(query)
        if resolved:
            tracks = [resolved]
        source = "SoundCloud"
    # YouTube URL or search
    else:
        if re.match(r"https?://", query):
            resolved = await yt_resolve(query, video=vidmode)
            if resolved:
                tracks = [resolved]
        else:
            results = await yt_search(query)
            if results:
                tracks = [results[0]]
        source = "YouTube"

    await wait.delete()

    if not tracks:
        return await msg.reply(
            "<blockquote>No results found. Please try a different query.</blockquote>",
            parse_mode=ParseMode.HTML,
        )

    req_name = msg.from_user.first_name if msg.from_user else "User"
    for t in tracks:
        t.setdefault("requester_id",   uid)
        t.setdefault("requester_name", req_name)
        t.setdefault("source",         source)
        t.setdefault("vidmode",        vidmode)
        t.setdefault("thumb",          START_IMG_URL)
        _q(cid).append(t)

    if not _active.get(cid):
        await play_next(cid)
    else:
        if len(tracks) == 1:
            await msg.reply(
                f"<blockquote>Added to queue: <b>{tracks[0]['title']}</b></blockquote>",
                parse_mode=ParseMode.HTML,
            )
        else:
            await msg.reply(
                f"<blockquote>Added <b>{len(tracks)}</b> tracks to queue.</blockquote>",
                parse_mode=ParseMode.HTML,
            )

@bot.on_message(filters.command("play") & filters.group)
async def cmd_play(client: Client, msg: Message):
    await _handle_play(client, msg, vidmode=False)

@bot.on_message(filters.command("vplay") & filters.group)
async def cmd_vplay(client: Client, msg: Message):
    await _handle_play(client, msg, vidmode=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PLAYBACK CONTROL COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@bot.on_message(filters.command("pause") & filters.group)
async def cmd_pause(client: Client, msg: Message):
    cid = msg.chat.id
    if not await check_auth(client, msg):
        return await msg.reply("<blockquote>Not authorised.</blockquote>", parse_mode=ParseMode.HTML)
    if not _active.get(cid):
        return await msg.reply("<blockquote>Nothing is playing.</blockquote>", parse_mode=ParseMode.HTML)
    call = _get_call(cid)
    if call:
        try:
            await call.pause_stream(cid)
            _paused[cid] = True
            await msg.reply("<blockquote><b>Paused.</b></blockquote>", parse_mode=ParseMode.HTML)
        except Exception as exc:
            await msg.reply(f"<blockquote>Error: {exc}</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("resume") & filters.group)
async def cmd_resume(client: Client, msg: Message):
    cid = msg.chat.id
    if not await check_auth(client, msg):
        return await msg.reply("<blockquote>Not authorised.</blockquote>", parse_mode=ParseMode.HTML)
    call = _get_call(cid)
    if call and _paused.get(cid):
        try:
            await call.resume_stream(cid)
            _paused[cid] = False
            await msg.reply("<blockquote><b>Resumed.</b></blockquote>", parse_mode=ParseMode.HTML)
        except Exception as exc:
            await msg.reply(f"<blockquote>Error: {exc}</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("skip") & filters.group)
async def cmd_skip(client: Client, msg: Message):
    cid = msg.chat.id
    if not await check_auth(client, msg):
        return await msg.reply("<blockquote>Not authorised.</blockquote>", parse_mode=ParseMode.HTML)
    if not _active.get(cid):
        return await msg.reply("<blockquote>Nothing is playing.</blockquote>", parse_mode=ParseMode.HTML)
    await msg.reply("<blockquote>Skipped.</blockquote>", parse_mode=ParseMode.HTML)
    await play_next(cid)

@bot.on_message(filters.command("stop") & filters.group)
async def cmd_stop(client: Client, msg: Message):
    cid = msg.chat.id
    if not await check_auth(client, msg):
        return await msg.reply("<blockquote>Not authorised.</blockquote>", parse_mode=ParseMode.HTML)
    _queue.pop(cid, None)
    _np.pop(cid, None)
    await _leave_vc(cid)
    await msg.reply("<blockquote><b>Stopped.</b> Left the voice chat.</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("loop") & filters.group)
async def cmd_loop(client: Client, msg: Message):
    cid = msg.chat.id
    if not await check_auth(client, msg):
        return await msg.reply("<blockquote>Not authorised.</blockquote>", parse_mode=ParseMode.HTML)
    _loop[cid] = not _loop.get(cid, False)
    state = "enabled" if _loop[cid] else "disabled"
    await msg.reply(f"<blockquote>Loop <b>{state}</b>.</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("shuffle") & filters.group)
async def cmd_shuffle(client: Client, msg: Message):
    cid = msg.chat.id
    if not await check_auth(client, msg):
        return await msg.reply("<blockquote>Not authorised.</blockquote>", parse_mode=ParseMode.HTML)
    _shuffle[cid] = not _shuffle.get(cid, False)
    state = "enabled" if _shuffle[cid] else "disabled"
    await msg.reply(f"<blockquote>Shuffle <b>{state}</b>.</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("volume") & filters.group)
async def cmd_volume(client: Client, msg: Message):
    cid = msg.chat.id
    if not await check_auth(client, msg):
        return await msg.reply("<blockquote>Not authorised.</blockquote>", parse_mode=ParseMode.HTML)
    args = msg.text.split()
    if len(args) < 2 or not args[1].isdigit():
        return await msg.reply(
            f"<blockquote>Current volume: <b>{_volume.get(cid, 100)}%</b>\n"
            "Usage: <code>/volume 0–200</code></blockquote>",
            parse_mode=ParseMode.HTML,
        )
    vol = max(0, min(200, int(args[1])))
    _volume[cid] = vol
    call = _get_call(cid)
    if call:
        try:
            await call.change_volume_call(cid, vol)
        except Exception:
            pass
    await msg.reply(f"<blockquote>Volume set to <b>{vol}%</b>.</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("seek") & filters.group)
async def cmd_seek(client: Client, msg: Message):
    cid = msg.chat.id
    if not await check_auth(client, msg):
        return await msg.reply("<blockquote>Not authorised.</blockquote>", parse_mode=ParseMode.HTML)
    args = msg.text.split()
    if len(args) < 2 or not args[1].lstrip("-").isdigit():
        return await msg.reply(
            "<blockquote>Usage: <code>/seek [seconds]</code></blockquote>",
            parse_mode=ParseMode.HTML,
        )
    sec = int(args[1])
    call = _get_call(cid)
    if call and _np.get(cid):
        try:
            await call.seek_stream(cid, sec)
            await msg.reply(
                f"<blockquote>Seeked to <b>{fmt_time(sec)}</b>.</blockquote>",
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            await msg.reply(f"<blockquote>Seek failed: {exc}</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command(["np", "nowplaying"]) & filters.group)
async def cmd_np(_, msg: Message):
    cid  = msg.chat.id
    track = _np.get(cid)
    if not track:
        return await msg.reply(
            "<blockquote>Nothing is playing right now.</blockquote>",
            parse_mode=ParseMode.HTML,
        )
    elapsed = int(time.time() - _start_t.get(cid, time.time()))
    total   = track.get("duration", 0)
    bar     = progress_bar(elapsed, total)
    text    = (
        f"<b>Now Playing</b>\n\n"
        f"<blockquote>"
        f"<b>{track.get('title','Unknown')}</b>\n\n"
        f"<code>{fmt_time(elapsed)}</code>  {bar}  <code>{fmt_time(total)}</code>\n\n"
        f"<b>Source :</b>  {track.get('source','YouTube')}\n"
        f"<b>Requested by :</b>  {track.get('requester_name','—')}"
        f"</blockquote>"
    )
    await msg.reply_photo(
        photo=track.get("thumb", START_IMG_URL),
        caption=text,
        reply_markup=player_kb(cid),
        parse_mode=ParseMode.HTML,
    )

@bot.on_message(filters.command("queue") & filters.group)
async def cmd_queue(_, msg: Message):
    cid = msg.chat.id
    q   = _q(cid)
    if not q and not _np.get(cid):
        return await msg.reply(
            "<blockquote>Queue is empty.</blockquote>",
            parse_mode=ParseMode.HTML,
        )
    lines = []
    if _np.get(cid):
        lines.append(f"<b>Now Playing:</b>\n  {_np[cid].get('title','?')}  [{fmt_time(_np[cid].get('duration',0))}]")
    if q:
        lines.append("<b>Up Next:</b>")
        for i, t in enumerate(q[:20], 1):
            lines.append(f"  {i}.  {t.get('title','?')}  [{fmt_time(t.get('duration',0))}]")
        if len(q) > 20:
            lines.append(f"  … and {len(q)-20} more")
    await msg.reply(
        "<blockquote>" + "\n".join(lines) + "</blockquote>",
        parse_mode=ParseMode.HTML,
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PLAYER CALLBACK BUTTONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@bot.on_callback_query(filters.regex(r"^vc_(pause|resume|skip|end|loopon|loopoff|shufon|shufoff|vdown|vup|queue|np)$"))
async def cb_player(client: Client, cq: CallbackQuery):
    cid = cq.message.chat.id
    uid = cq.from_user.id
    act = cq.data

    if not await check_auth(client, cq.message):
        return await cq.answer("You are not authorised.", show_alert=True)

    call = _get_call(cid)

    if act == "vc_pause":
        if call and _active.get(cid) and not _paused.get(cid):
            await call.pause_stream(cid)
            _paused[cid] = True
        await cq.answer("Paused.")

    elif act == "vc_resume":
        if call and _paused.get(cid):
            await call.resume_stream(cid)
            _paused[cid] = False
        await cq.answer("Resumed.")

    elif act == "vc_skip":
        await cq.answer("Skipping…")
        await play_next(cid)
        return

    elif act == "vc_end":
        _queue.pop(cid, None)
        _np.pop(cid, None)
        await _leave_vc(cid)
        try:
            await cq.message.edit_caption(
                caption="<blockquote><b>Stream ended.</b></blockquote>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        return await cq.answer("Stopped.")

    elif act == "vc_loopon":
        _loop[cid] = True
        await cq.answer("Loop enabled.")

    elif act == "vc_loopoff":
        _loop[cid] = False
        await cq.answer("Loop disabled.")

    elif act == "vc_shufon":
        _shuffle[cid] = True
        await cq.answer("Shuffle enabled.")

    elif act == "vc_shufoff":
        _shuffle[cid] = False
        await cq.answer("Shuffle disabled.")

    elif act == "vc_vdown":
        vol = max(0, _volume.get(cid, 100) - 10)
        _volume[cid] = vol
        if call:
            try:
                await call.change_volume_call(cid, vol)
            except Exception:
                pass
        await cq.answer(f"Volume: {vol}%")

    elif act == "vc_vup":
        vol = min(200, _volume.get(cid, 100) + 10)
        _volume[cid] = vol
        if call:
            try:
                await call.change_volume_call(cid, vol)
            except Exception:
                pass
        await cq.answer(f"Volume: {vol}%")

    elif act == "vc_queue":
        q = _q(cid)
        if not q and not _np.get(cid):
            return await cq.answer("Queue is empty.", show_alert=True)
        lines = []
        if _np.get(cid):
            lines.append(f"Now Playing: {_np[cid].get('title','?')}")
        for i, t in enumerate(q[:10], 1):
            lines.append(f"{i}. {t.get('title','?')}")
        return await cq.answer("\n".join(lines), show_alert=True)

    elif act == "vc_np":
        track = _np.get(cid)
        if not track:
            return await cq.answer("Nothing playing.", show_alert=True)
        elapsed = int(time.time() - _start_t.get(cid, time.time()))
        total   = track.get("duration", 0)
        bar     = progress_bar(elapsed, total)
        return await cq.answer(
            f"{track.get('title','?')}\n{fmt_time(elapsed)} {bar} {fmt_time(total)}",
            show_alert=True,
        )

    # Refresh button markup after any state change
    track = _np.get(cid)
    if track:
        try:
            await cq.message.edit_caption(
                caption=_np_text(track, cid),
                reply_markup=player_kb(cid),
                parse_mode=ParseMode.HTML,
            )
        except MessageNotModified:
            pass

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADMIN COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@bot.on_message(filters.command("adminpanel"))
async def cmd_adminpanel(client: Client, msg: Message):
    if not is_sudo(msg.from_user.id if msg.from_user else 0):
        return await msg.reply(
            "<blockquote>Only bot admins can access the admin panel.</blockquote>",
            parse_mode=ParseMode.HTML,
        )
    await msg.reply(
        "<b>Admin Panel</b>\n\n"
        "<blockquote>Select an action below.</blockquote>",
        reply_markup=admin_kb(),
        parse_mode=ParseMode.HTML,
    )

@bot.on_callback_query(filters.regex(r"^adm_"))
async def cb_admin(client: Client, cq: CallbackQuery):
    uid = cq.from_user.id
    if not is_sudo(uid):
        return await cq.answer("Not authorised.", show_alert=True)
    act = cq.data

    if act == "adm_addvc":
        await cq.answer()
        await cq.message.reply(
            "<blockquote>"
            "Send the phone number of the assistant account you want to add.\n\n"
            "Format: <code>+91XXXXXXXXXX</code>\n\n"
            "The bot will send an OTP to that number via Telegram."
            "</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        return

    elif act == "adm_listvc":
        sessions = await get_vc_sessions()
        if not sessions:
            return await cq.answer("No VC accounts added.", show_alert=True)
        lines = [f"{d.get('name','?')}  ({d.get('phone','?')})" for d in sessions]
        await cq.answer("\n".join(lines), show_alert=True)

    elif act == "adm_addfsub":
        await cq.answer()
        await cq.message.reply(
            "<blockquote>"
            "Send the channel username or ID to add as force-subscribe.\n\n"
            "The bot must be an admin in that channel with "
            "<b>Invite Users via Link</b> permission."
            "</blockquote>",
            parse_mode=ParseMode.HTML,
        )

    elif act == "adm_rmfsub":
        channels = await get_fsub_channels()
        if not channels:
            return await cq.answer("No force-sub channels configured.", show_alert=True)
        rows = [(f"Remove: {c.get('title','?')}", f"rmfsub_{c['cid']}") for c in channels]
        await cq.message.reply(
            "<blockquote>Select channel to remove:</blockquote>",
            reply_markup=kb(*[[r] for r in rows]),
            parse_mode=ParseMode.HTML,
        )
        await cq.answer()

    elif act == "adm_broadcast":
        await cq.answer()
        await cq.message.reply(
            "<blockquote>Reply to this message with the text you want to broadcast to all served chats.</blockquote>",
            parse_mode=ParseMode.HTML,
        )

    elif act == "adm_ban":
        await cq.answer()
        await cq.message.reply(
            "<blockquote>Send the user ID to ban.\nFormat: <code>/ban USER_ID [reason]</code></blockquote>",
            parse_mode=ParseMode.HTML,
        )

    elif act == "adm_unban":
        await cq.answer()
        await cq.message.reply(
            "<blockquote>Send: <code>/unban USER_ID</code></blockquote>",
            parse_mode=ParseMode.HTML,
        )

    elif act == "adm_stats":
        cpu  = psutil.cpu_percent()
        ram  = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        uptime_s = int(time.time() - _BOT_START)
        text = (
            "<b>Bot Statistics</b>\n\n"
            "<blockquote>"
            f"<b>CPU :</b>  {cpu}%\n"
            f"<b>RAM :</b>  {ram.percent}%  "
            f"({humanize.naturalsize(ram.used)} / {humanize.naturalsize(ram.total)})\n"
            f"<b>Disk :</b>  {disk.percent}%  "
            f"({humanize.naturalsize(disk.used)} / {humanize.naturalsize(disk.total)})\n"
            f"<b>Uptime :</b>  {fmt_time(uptime_s)}\n"
            f"<b>Active VCs :</b>  {len(_active)}\n"
            f"<b>Assistants :</b>  {len(assistants)}"
            "</blockquote>"
        )
        await cq.message.reply(text, parse_mode=ParseMode.HTML)
        await cq.answer()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  AUTH / BAN / BROADCAST / PING / STATS  commands
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@bot.on_message(filters.command("auth") & filters.group)
async def cmd_auth(client: Client, msg: Message):
    if not await is_chat_admin(client, msg.chat.id, msg.from_user.id if msg.from_user else 0):
        return await msg.reply("<blockquote>Only admins can authorise users.</blockquote>", parse_mode=ParseMode.HTML)
    target = None
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target = msg.reply_to_message.from_user
    elif len(msg.command) > 1:
        try:
            target = await client.get_users(msg.command[1].lstrip("@"))
        except Exception:
            pass
    if not target:
        return await msg.reply("<blockquote>Specify a user: reply or <code>/auth @username</code></blockquote>", parse_mode=ParseMode.HTML)
    await add_auth(msg.chat.id, target.id)
    await msg.reply(
        f"<blockquote><b>{target.first_name}</b> is now authorised to use bot controls.</blockquote>",
        parse_mode=ParseMode.HTML,
    )

@bot.on_message(filters.command("unauth") & filters.group)
async def cmd_unauth(client: Client, msg: Message):
    if not await is_chat_admin(client, msg.chat.id, msg.from_user.id if msg.from_user else 0):
        return await msg.reply("<blockquote>Only admins can remove authorisation.</blockquote>", parse_mode=ParseMode.HTML)
    target = None
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target = msg.reply_to_message.from_user
    elif len(msg.command) > 1:
        try:
            target = await client.get_users(msg.command[1].lstrip("@"))
        except Exception:
            pass
    if not target:
        return await msg.reply("<blockquote>Specify a user.</blockquote>", parse_mode=ParseMode.HTML)
    await rm_auth(msg.chat.id, target.id)
    await msg.reply(
        f"<blockquote><b>{target.first_name}</b> has been unauthorised.</blockquote>",
        parse_mode=ParseMode.HTML,
    )

@bot.on_message(filters.command("ban"))
async def cmd_ban(client: Client, msg: Message):
    if not is_sudo(msg.from_user.id if msg.from_user else 0):
        return await msg.reply("<blockquote>Only bot admins can ban users.</blockquote>", parse_mode=ParseMode.HTML)
    args = msg.text.split(None, 2)
    if len(args) < 2:
        return await msg.reply("<blockquote>Usage: <code>/ban USER_ID [reason]</code></blockquote>", parse_mode=ParseMode.HTML)
    try:
        uid    = int(args[1])
        reason = args[2] if len(args) > 2 else "No reason given"
        await ban_user(uid, reason)
        await msg.reply(
            f"<blockquote>User <code>{uid}</code> has been banned.\nReason: {reason}</blockquote>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:
        await msg.reply(f"<blockquote>Error: {exc}</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("unban"))
async def cmd_unban(client: Client, msg: Message):
    if not is_sudo(msg.from_user.id if msg.from_user else 0):
        return await msg.reply("<blockquote>Only bot admins can unban users.</blockquote>", parse_mode=ParseMode.HTML)
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply("<blockquote>Usage: <code>/unban USER_ID</code></blockquote>", parse_mode=ParseMode.HTML)
    try:
        uid = int(args[1])
        await unban_user(uid)
        await msg.reply(f"<blockquote>User <code>{uid}</code> has been unbanned.</blockquote>", parse_mode=ParseMode.HTML)
    except Exception as exc:
        await msg.reply(f"<blockquote>Error: {exc}</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("broadcast"))
async def cmd_broadcast(client: Client, msg: Message):
    if not is_sudo(msg.from_user.id if msg.from_user else 0):
        return await msg.reply("<blockquote>Only bot admins can broadcast.</blockquote>", parse_mode=ParseMode.HTML)
    text = msg.text.split(None, 1)
    if len(text) < 2:
        return await msg.reply(
            "<blockquote>Usage: <code>/broadcast Your message here</code></blockquote>",
            parse_mode=ParseMode.HTML,
        )
    btext  = text[1]
    chats  = await all_served()
    sent   = 0
    failed = 0
    info   = await msg.reply(
        f"<blockquote>Broadcasting to <b>{len(chats)}</b> chats…</blockquote>",
        parse_mode=ParseMode.HTML,
    )
    for cid in chats:
        try:
            await client.send_message(cid, btext, parse_mode=ParseMode.HTML)
            sent += 1
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await client.send_message(cid, btext, parse_mode=ParseMode.HTML)
                sent += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await info.edit_text(
        f"<blockquote>Broadcast complete.\n<b>Sent:</b> {sent}  <b>Failed:</b> {failed}</blockquote>",
        parse_mode=ParseMode.HTML,
    )

@bot.on_message(filters.command("ping"))
async def cmd_ping(_, msg: Message):
    start = time.time()
    m = await msg.reply("<blockquote>Pinging…</blockquote>", parse_mode=ParseMode.HTML)
    end   = time.time()
    ms    = round((end - start) * 1000, 2)
    await m.edit_text(
        f"<blockquote><b>Pong!</b>  <code>{ms} ms</code></blockquote>",
        parse_mode=ParseMode.HTML,
    )

@bot.on_message(filters.command(["stats", "uptime"]))
async def cmd_stats(_, msg: Message):
    cpu    = psutil.cpu_percent(interval=0.5)
    ram    = psutil.virtual_memory()
    disk   = psutil.disk_usage("/")
    uptime = int(time.time() - _BOT_START)
    text   = (
        "<b>Bot Statistics</b>\n\n"
        "<blockquote>"
        f"<b>CPU :</b>  {cpu}%\n"
        f"<b>RAM :</b>  {ram.percent}%  "
        f"({humanize.naturalsize(ram.used)} / {humanize.naturalsize(ram.total)})\n"
        f"<b>Disk :</b>  {disk.percent}%  "
        f"({humanize.naturalsize(disk.used)} / {humanize.naturalsize(disk.total)})\n"
        f"<b>Uptime :</b>  {fmt_time(uptime)}\n"
        f"<b>Active VCs :</b>  {len(_active)}\n"
        f"<b>Assistants :</b>  {len(assistants)}\n"
        f"<b>Python :</b>  {sys.version.split()[0]}"
        "</blockquote>"
    )
    await msg.reply(text, parse_mode=ParseMode.HTML)

# ── rmfsub callback ───────────────────────────────────────────────
@bot.on_callback_query(filters.regex(r"^rmfsub_(-?\d+)$"))
async def cb_rmfsub(_, cq: CallbackQuery):
    if not is_sudo(cq.from_user.id):
        return await cq.answer("Not authorised.", show_alert=True)
    cid = int(cq.data.split("_")[1])
    await rm_fsub(cid)
    await cq.answer("Removed.", show_alert=True)
    try:
        await cq.message.delete()
    except Exception:
        pass

# ── cmd_stats callback ────────────────────────────────────────────
@bot.on_callback_query(filters.regex("^cmd_stats$"))
async def cb_stats(_, cq: CallbackQuery):
    cpu  = psutil.cpu_percent(interval=0.5)
    ram  = psutil.virtual_memory()
    text = (
        f"CPU: {cpu}%  |  RAM: {ram.percent}%  |  "
        f"Active VCs: {len(_active)}"
    )
    await cq.answer(text, show_alert=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  OTP VC ACCOUNT ADD — admin flow via DM conversation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# State machine stored in-memory: {user_id: {step, phone, client}}
_otp_state: dict[int, dict] = {}

@bot.on_message(filters.command("addvc") & filters.private)
async def cmd_addvc(_, msg: Message):
    uid = msg.from_user.id
    if not is_sudo(uid):
        return await msg.reply(
            "<blockquote>Only bot admins can add VC accounts.</blockquote>",
            parse_mode=ParseMode.HTML,
        )
    _otp_state[uid] = {"step": "phone"}
    await msg.reply(
        "<b>Add VC Assistant Account</b>\n\n"
        "<blockquote>"
        "Send the phone number of the Telegram account you want to use as assistant.\n\n"
        "Format: <code>+91XXXXXXXXXX</code>"
        "</blockquote>",
        parse_mode=ParseMode.HTML,
    )

@bot.on_message(filters.private & filters.text & ~filters.command([
    "start","help","ping","stats","ban","unban","broadcast",
    "adminpanel","addvc","listvc","rmvc","addfsub","rmfsub","listfsub",
]))
async def otp_conversation(_, msg: Message):
    uid  = msg.from_user.id
    state = _otp_state.get(uid)
    if not state:
        return

    step = state.get("step")

    if step == "phone":
        phone = msg.text.strip()
        if not re.match(r"^\+\d{7,15}$", phone):
            return await msg.reply(
                "<blockquote>Invalid format. Use: <code>+91XXXXXXXXXX</code></blockquote>",
                parse_mode=ParseMode.HTML,
            )
        state["phone"] = phone
        tmp_client = Client(
            f"otp_session_{uid}",
            api_id=API_ID,
            api_hash=API_HASH,
        )
        state["client"] = tmp_client
        try:
            await tmp_client.connect()
            sent = await tmp_client.send_code(phone)
            state["phone_code_hash"] = sent.phone_code_hash
            state["step"] = "otp"
            await msg.reply(
                "<blockquote>"
                "OTP sent to that number via Telegram.\n"
                "Send the OTP code here.\n\n"
                "<b>Format:</b>  <code>1 2 3 4 5</code>  (with spaces)"
                "</blockquote>",
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            await tmp_client.disconnect()
            del _otp_state[uid]
            await msg.reply(
                f"<blockquote>Failed to send OTP: <code>{exc}</code></blockquote>",
                parse_mode=ParseMode.HTML,
            )

    elif step == "otp":
        code = msg.text.strip().replace(" ", "")
        phone = state["phone"]
        tmp_client = state["client"]
        phone_code_hash = state.get("phone_code_hash", "")
        try:
            await tmp_client.sign_in(phone, phone_code_hash, code)
            me = await tmp_client.get_me()
            session_string = await tmp_client.export_session_string()
            await tmp_client.disconnect()
            name = f"{me.first_name or ''} {me.last_name or ''}".strip()
            await save_vc_session(phone, session_string, name)
            # Dynamically add as new assistant
            new_client = Client(
                f"RiRuAsst_dyn_{len(assistants)+1}",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=session_string,
            )
            await new_client.start()
            new_idx = len(assistants)
            assistants.append(new_client)
            call = PyTgCalls(new_client)
            _register_call_handlers(call)
            await call.start()
            _calls[new_idx] = call
            del _otp_state[uid]
            await msg.reply(
                f"<blockquote>"
                f"<b>{name}</b> (<code>{phone}</code>) added as VC assistant.\n"
                f"Total assistants: <b>{len(assistants)}</b>"
                f"</blockquote>",
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            await tmp_client.disconnect()
            await msg.reply(
                f"<blockquote>Sign-in failed: <code>{exc}</code>\nTry again with /addvc</blockquote>",
                parse_mode=ParseMode.HTML,
            )
            del _otp_state[uid]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADD / LIST / REMOVE force-subscribe channels
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@bot.on_message(filters.command("addfsub"))
async def cmd_addfsub(client: Client, msg: Message):
    if not is_sudo(msg.from_user.id if msg.from_user else 0):
        return await msg.reply("<blockquote>Only bot admins can do this.</blockquote>", parse_mode=ParseMode.HTML)
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply(
            "<blockquote>Usage: <code>/addfsub @username or channel_id</code></blockquote>",
            parse_mode=ParseMode.HTML,
        )
    raw = args[1]
    try:
        chat = await client.get_chat(int(raw) if raw.lstrip("-").isdigit() else raw)
        try:
            invite = await client.export_chat_invite_link(chat.id)
        except Exception:
            invite = f"https://t.me/{chat.username}" if chat.username else ""
        await add_fsub(chat.id, invite, chat.title)
        await msg.reply(
            f"<blockquote><b>{chat.title}</b> added as force-subscribe channel.\n"
            f"Invite: {invite}</blockquote>",
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:
        await msg.reply(f"<blockquote>Error: {exc}</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("listfsub"))
async def cmd_listfsub(_, msg: Message):
    if not is_sudo(msg.from_user.id if msg.from_user else 0):
        return
    channels = await get_fsub_channels()
    if not channels:
        return await msg.reply("<blockquote>No force-sub channels configured.</blockquote>", parse_mode=ParseMode.HTML)
    lines = [f"{c.get('title','?')}  (<code>{c['cid']}</code>)" for c in channels]
    await msg.reply(
        "<b>Force-Subscribe Channels</b>\n\n<blockquote>" + "\n".join(lines) + "</blockquote>",
        parse_mode=ParseMode.HTML,
    )

@bot.on_message(filters.command("rmfsub"))
async def cmd_rmfsub(client: Client, msg: Message):
    if not is_sudo(msg.from_user.id if msg.from_user else 0):
        return
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply("<blockquote>Usage: <code>/rmfsub channel_id</code></blockquote>", parse_mode=ParseMode.HTML)
    try:
        cid = int(args[1])
        await rm_fsub(cid)
        await msg.reply(f"<blockquote>Channel <code>{cid}</code> removed from force-sub list.</blockquote>", parse_mode=ParseMode.HTML)
    except Exception as exc:
        await msg.reply(f"<blockquote>Error: {exc}</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("listvc"))
async def cmd_listvc(_, msg: Message):
    if not is_sudo(msg.from_user.id if msg.from_user else 0):
        return
    sessions = await get_vc_sessions()
    if not sessions:
        return await msg.reply("<blockquote>No VC accounts added yet.</blockquote>", parse_mode=ParseMode.HTML)
    lines = [f"{d.get('name','?')}  (<code>{d.get('phone','?')}</code>)  added {d.get('added','?')[:10]}" for d in sessions]
    await msg.reply(
        "<b>VC Assistant Accounts</b>\n\n<blockquote>" + "\n".join(lines) + "</blockquote>",
        parse_mode=ParseMode.HTML,
    )

@bot.on_message(filters.command("rmvc"))
async def cmd_rmvc(_, msg: Message):
    if not is_sudo(msg.from_user.id if msg.from_user else 0):
        return
    args = msg.text.split()
    if len(args) < 2:
        return await msg.reply("<blockquote>Usage: <code>/rmvc +phone</code></blockquote>", parse_mode=ParseMode.HTML)
    phone = args[1]
    await remove_vc_session(phone)
    await msg.reply(f"<blockquote>Session for <code>{phone}</code> removed from DB.\nRestart bot for changes to take effect.</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command(["joinvc"]) & filters.group)
async def cmd_joinvc(client: Client, msg: Message):
    if not await check_auth(client, msg):
        return await msg.reply("<blockquote>Not authorised.</blockquote>", parse_mode=ParseMode.HTML)
    await msg.reply("<blockquote>Use /play to start playing — the bot joins automatically.</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("leavevc") & filters.group)
async def cmd_leavevc(client: Client, msg: Message):
    cid = msg.chat.id
    if not await check_auth(client, msg):
        return await msg.reply("<blockquote>Not authorised.</blockquote>", parse_mode=ParseMode.HTML)
    _queue.pop(cid, None)
    await _leave_vc(cid)
    await msg.reply("<blockquote>Left the voice chat.</blockquote>", parse_mode=ParseMode.HTML)

@bot.on_message(filters.command("repo"))
async def cmd_repo(_, msg: Message):
    await msg.reply(
        f"<blockquote><b>Source Code</b>\n\n"
        f"<a href='{UPSTREAM_REPO}'>View on GitHub</a>\n"
        f"Developer: <a href='{DEVELOPER_URL}'>{DEVELOPER}</a></blockquote>",
        parse_mode=ParseMode.HTML,
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LYRICS (via lyrics.ovh fallback)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@bot.on_message(filters.command("lyrics"))
async def cmd_lyrics(_, msg: Message):
    args = msg.text.split(None, 1)
    query = args[1].strip() if len(args) > 1 else ""
    if not query and _np.get(msg.chat.id):
        query = _np[msg.chat.id].get("title", "")
    if not query:
        return await msg.reply(
            "<blockquote>Usage: <code>/lyrics song name</code></blockquote>",
            parse_mode=ParseMode.HTML,
        )
    wait = await msg.reply("<blockquote>Fetching lyrics…</blockquote>", parse_mode=ParseMode.HTML)
    try:
        parts = query.split(None, 1)
        artist = parts[0] if len(parts) > 1 else "Unknown"
        title  = parts[1] if len(parts) > 1 else parts[0]
        async with httpx.AsyncClient(timeout=10) as hx:
            r = await hx.get(f"https://api.lyrics.ovh/v1/{artist}/{title}")
        if r.status_code == 200:
            data = r.json()
            lyr  = data.get("lyrics", "").strip()
            if lyr:
                # Telegram max message length is 4096
                lyr = lyr[:3500]
                await wait.edit_text(
                    f"<b>Lyrics — {query}</b>\n\n"
                    f"<blockquote expandable>{lyr}</blockquote>",
                    parse_mode=ParseMode.HTML,
                )
                return
    except Exception:
        pass
    await wait.edit_text(
        "<blockquote>Could not find lyrics for that song.</blockquote>",
        parse_mode=ParseMode.HTML,
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MAIN STARTUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_BOT_START = time.time()

async def main():
    await init_db()
    keep_alive()

    LOG.info("Starting bot client…")
    await bot.start()
    me = await bot.get_me()
    LOG.info(f"Bot started: @{me.username}")

    LOG.info(f"Starting {len(assistants)} assistant(s)…")
    for idx, asst in enumerate(assistants):
        try:
            await asst.start()
            call = PyTgCalls(asst)
            _register_call_handlers(call)
            await call.start()
            _calls[idx] = call
            ame = await asst.get_me()
            LOG.info(f"  Assistant {idx+1}: @{ame.username}")
        except Exception as exc:
            LOG.error(f"  Assistant {idx+1} failed: {exc}")

    # Load saved VC sessions from DB and start them
    for sess_data in await get_vc_sessions():
        phone   = sess_data.get("phone", "")
        session = sess_data.get("session", "")
        name    = sess_data.get("name", phone)
        if not session:
            continue
        try:
            dyn = Client(
                f"RiRuDyn_{phone[-4:]}",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=session,
            )
            await dyn.start()
            idx = len(assistants)
            assistants.append(dyn)
            call = PyTgCalls(dyn)
            _register_call_handlers(call)
            await call.start()
            _calls[idx] = call
            LOG.info(f"  Loaded saved VC account: {name} ({phone})")
        except Exception as exc:
            LOG.error(f"  Failed to load VC account {phone}: {exc}")

    if LOGGER_ID:
        try:
            await bot.send_message(
                LOGGER_ID,
                f"<blockquote><b>𝐑𝐢𝐑𝐮𝐑𝐮 𝐌𝐮𝐬𝐢𝐜</b> started.\n"
                f"Assistants: {len(assistants)}\n"
                f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</blockquote>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    LOG.info("RiRuRu Music is running. Press Ctrl+C to stop.")
    await idle()

    # Graceful shutdown
    LOG.info("Shutting down…")
    for idx, call in _calls.items():
        try:
            await call.stop()
        except Exception:
            pass
    for asst in assistants:
        try:
            await asst.stop()
        except Exception:
            pass
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(main())
