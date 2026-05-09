"""
Radio Boomerang - HTTP API (FastAPI)
"""
from fastapi import FastAPI, APIRouter, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, FileResponse, PlainTextResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os, logging, uuid, asyncio, mimetypes
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
from datetime import datetime, timezone
import aiofiles

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = os.environ['MONGO_URL']
mongo_client = AsyncIOMotorClient(mongo_url)
db = mongo_client[os.environ['DB_NAME']]

AUDIO_BASE_DIR = ROOT_DIR / "audio_files"
MUSIC_DIR = AUDIO_BASE_DIR / "music"
JINGLES_DIR = AUDIO_BASE_DIR / "jingles"
ADS_DIR = AUDIO_BASE_DIR / "ads"
WARNINGS_DIR = AUDIO_BASE_DIR / "warnings"
for d in [AUDIO_BASE_DIR, MUSIC_DIR, JINGLES_DIR, ADS_DIR, WARNINGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

MAX_FILES_PER_CATEGORY = 200000
SOCKET_PATH = os.environ.get('RADIO_SOCKET_PATH', '/tmp/radio_stream.sock')

app = FastAPI()
api_router = APIRouter(prefix="/api")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AudioFile(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    filename: str
    original_filename: str
    type: str
    file_path: str
    file_size: int
    upload_date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


async def _read_state() -> dict:
    doc = await db.radio_state.find_one({"_id": "main"}) or {}
    return {
        "on_air": bool(doc.get("on_air", False)),
        "shuffle_enabled": bool(doc.get("shuffle_enabled", False)),
        "current_music_index": int(doc.get("current_music_index", 0)),
    }


async def _write_state(**fields):
    await db.radio_state.update_one({"_id": "main"}, {"$set": fields}, upsert=True)


_active_listeners = 0
_listeners_lock = asyncio.Lock()



@api_router.post("/upload", response_model=AudioFile)
async def upload_audio(file: UploadFile = File(...), file_type: str = "music"):
    if file_type not in ["music", "jingle", "ad", "warning"]:
        raise HTTPException(status_code=400, detail="Invalid file type")
    storage_dir = {"music": MUSIC_DIR, "jingle": JINGLES_DIR,
                   "ad": ADS_DIR, "warning": WARNINGS_DIR}[file_type]
    extension = Path(file.filename).suffix or ".mp3"
    unique = f"{uuid.uuid4()}{extension}"
    file_path = storage_dir / unique
    async with aiofiles.open(file_path, 'wb') as out:
        content = await file.read()
        await out.write(content)
    audio = AudioFile(filename=unique, original_filename=file.filename,
                     type=file_type, file_path=str(file_path), file_size=len(content))
    doc = audio.model_dump()
    doc['upload_date'] = doc['upload_date'].isoformat()
    await db.audio_files.insert_one(doc)
    return audio


@api_router.get("/files", response_model=List[AudioFile])
async def list_audio(file_type: Optional[str] = None):
    query = {"type": file_type} if file_type else {}
    files = await db.audio_files.find(query, {"_id": 0}).to_list(MAX_FILES_PER_CATEGORY)
    for f in files:
        if isinstance(f.get('upload_date'), str):
            f['upload_date'] = datetime.fromisoformat(f['upload_date'])
    return files


@api_router.delete("/files/{file_id}")
async def delete_audio(file_id: str):
    file_doc = await db.audio_files.find_one({"id": file_id}, {"_id": 0})
    if not file_doc:
        raise HTTPException(status_code=404, detail="File not found")
    file_path = Path(file_doc["file_path"])
    if file_path.exists():
        file_path.unlink()
    await db.audio_files.delete_one({"id": file_id})
    return {"message": "File deleted successfully", "file_id": file_id}


@api_router.get("/stream/{file_id}")
async def stream_single_file(file_id: str):
    file_doc = await db.audio_files.find_one({"id": file_id}, {"_id": 0})
    if not file_doc:
        raise HTTPException(status_code=404, detail="File not found")
    file_path = Path(file_doc["file_path"])
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found on disk")
    content_type, _ = mimetypes.guess_type(str(file_path))
    return FileResponse(file_path, media_type=content_type or "audio/mpeg",
                        filename=file_doc["original_filename"])


@api_router.post("/playlist/shuffle")
async def toggle_shuffle(enabled: bool):
    await _write_state(shuffle_enabled=enabled)
    return {"shuffle_enabled": enabled}


@api_router.get("/playlist/state")
async def get_playlist_state():
    state = await _read_state()
    return {**state, "listeners": _active_listeners}


@api_router.post("/broadcast/toggle")
async def toggle_broadcast(on_air: bool):
    await _write_state(on_air=on_air)
    return {"on_air": on_air,
            "status": "ON AIR - LIVE" if on_air else "OFF AIR",
            "message": f"Broadcasting is now {'ON AIR - LIVE' if on_air else 'OFF AIR'}"}


@api_router.get("/broadcast/status")
async def broadcast_status():
    state = await _read_state()
    return {"on_air": state["on_air"],
            "status": "ON AIR - LIVE" if state["on_air"] else "OFF AIR",
            "listeners": _active_listeners}


def _public_url() -> str:
    return os.environ.get('BACKEND_URL', 'http://localhost:8001').rstrip('/')


@api_router.get("/stream-url")
async def stream_url():
    base = _public_url()
    return {"stream_url": f"{base}/api/radio/stream.m3u",
            "direct_stream": f"{base}/api/radio/stream",
            "description": "Use the .m3u URL for AIMP/VLC or direct for browsers."}


PLAYLIST_REPEAT_ENTRIES = 1000
NO_CACHE_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate, max-age=0",
    "Pragma": "no-cache","Expires": "0",
    "X-Accel-Buffering": "no",
}


def _build_m3u(base: str) -> str:
    stream = f"{base}/api/radio/stream"
    out = ["#EXTM3U"]
    for _ in range(PLAYLIST_REPEAT_ENTRIES):
        out.append("#EXTINF:-1,Radio Boomerang România")
        out.append(stream)
    return "\n".join(out) + "\n"


def _build_pls(base: str) -> str:
    stream = f"{base}/api/radio/stream"
    out = ["[playlist]", f"NumberOfEntries={PLAYLIST_REPEAT_ENTRIES}"]
    for i in range(1, PLAYLIST_REPEAT_ENTRIES + 1):
        out.append(f"File{i}={stream}")
        out.append(f"Title{i}=Radio Boomerang România")
        out.append(f"Length{i}=-1")
    out.append("Version=2")
    return "\n".join(out) + "\n"


@api_router.get("/radio/stream.m3u")
async def get_m3u():
    return PlainTextResponse(_build_m3u(_public_url()), media_type="audio/x-mpegurl",
                             headers={**NO_CACHE_HEADERS,
                                      "Content-Disposition": "inline; filename=radio_boomerang.m3u"})


@api_router.get("/radio/stream.m3u8")
async def get_m3u8():
    return PlainTextResponse(_build_m3u(_public_url()), media_type="audio/x-mpegurl",
                             headers={**NO_CACHE_HEADERS,
                                      "Content-Disposition": "inline; filename=radio_boomerang.m3u"})


@api_router.get("/radio/stream.pls")
async def get_pls():
    return PlainTextResponse(_build_pls(_public_url()), media_type="audio/x-scpls",
                             headers={**NO_CACHE_HEADERS,
                                      "Content-Disposition": "inline; filename=radio_boomerang.pls"})


async def _open_broadcaster():
    try:
        reader, writer = await asyncio.open_unix_connection(SOCKET_PATH)
        return reader, writer
    except (FileNotFoundError, ConnectionRefusedError, OSError) as e:
        logger.warning(f"broadcaster socket unavailable ({e})")
        return None


@api_router.get("/radio/stream")
async def continuous_stream():
    global _active_listeners
    conn = await _open_broadcaster()
    if conn is None:
        raise HTTPException(status_code=503, detail="Broadcaster offline.")
    reader, writer = conn

    async with _listeners_lock:
        _active_listeners += 1

    async def relay():
        try:
            while True:
                chunk = await reader.read(8192)
                if not chunk:
                    break
                yield chunk
        except (asyncio.CancelledError, GeneratorExit):
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            global _active_listeners
            async with _listeners_lock:
                _active_listeners = max(0, _active_listeners - 1)

    return StreamingResponse(relay(), media_type="audio/mpeg",
        headers={**NO_CACHE_HEADERS, "Accept-Ranges": "none",
                 "Connection": "keep-alive",
                 "icy-name": "Radio Boomerang Romania",
                 "icy-genre": "Various", "icy-br": "128", "icy-pub": "1",
                 "icy-description": "Radio Boomerang Romania - Live 24/7"})
  app.include_router(api_router)
app.add_middleware(CORSMiddleware, allow_credentials=True,
                   allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
                   allow_methods=["*"], allow_headers=["*"],
                   expose_headers=["icy-name", "icy-genre", "icy-br", "icy-pub", "icy-description"])


@app.on_event("startup")
async def startup_event():
    if not await db.radio_state.find_one({"_id": "main"}):
        await db.radio_state.insert_one({"_id": "main", "on_air": False,
                                         "shuffle_enabled": False, "current_music_index": 0})


@app.on_event("shutdown")
async def shutdown_event():
    mongo_client.close()
