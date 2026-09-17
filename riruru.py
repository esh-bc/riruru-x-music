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
