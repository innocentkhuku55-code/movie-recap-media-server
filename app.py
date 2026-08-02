"""
Combined media server for the Movie Recap / Facebook Reel n8n pipelines.

Endpoints:
  POST /ingest   {url}                         -> {ok, source_id, title, description, transcript, has_transcript, thumbnail_url}
  POST /tts      {text, voice, format}          -> audio file (mp3)
  POST /compose  multipart: audio, source_id, title  -> composed video file (mp4)

Deploy on Render.com / Railway.app (needs ffmpeg installed - see Dockerfile).
"""
import os
import re
import uuid
import shutil
import asyncio
import subprocess
import tempfile
from pathlib import Path

from fastapi import FastAPI, HTTPException, Form, UploadFile, File
from fastapi.responses import FileResponse, JSONResponse
import yt_dlp
import edge_tts

app = FastAPI(title="movie-recap-media-server")

STORAGE_DIR = Path(os.environ.get("STORAGE_DIR", "/tmp/media-server"))
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

# How long a source_id's downloaded video is kept before cleanup (seconds).
MAX_AGE_SECONDS = 60 * 60 * 2  # 2 hours


def _source_dir(source_id: str) -> Path:
    d = STORAGE_DIR / source_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _cleanup_old_sources():
    import time
    now = time.time()
    if not STORAGE_DIR.exists():
        return
    for entry in STORAGE_DIR.iterdir():
        try:
            if entry.is_dir() and (now - entry.stat().st_mtime) > MAX_AGE_SECONDS:
                shutil.rmtree(entry, ignore_errors=True)
        except OSError:
            pass


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/ingest")
def ingest(payload: dict):
    """Download a YouTube (or other yt-dlp supported) video and pull metadata + subtitles."""
    _cleanup_old_sources()

    url = (payload or {}).get("url", "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="Missing 'url'")

    source_id = uuid.uuid4().hex[:12]
    out_dir = _source_dir(source_id)
    video_path = out_dir / "source.mp4"

    ydl_opts = {
        "outtmpl": str(video_path.with_suffix("")) + ".%(ext)s",
        "format": "bv*[height<=1080]+ba/b[height<=1080]/best",
        "merge_output_format": "mp4",
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitleslangs": ["en", "my"],
        "subtitlesformat": "vtt",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"yt-dlp failed: {e}")

    # yt-dlp may write the final file with a different extension resolved by merge_output_format
    downloaded = list(out_dir.glob("source.*"))
    mp4_candidates = [p for p in downloaded if p.suffix == ".mp4"]
    if not mp4_candidates:
        raise HTTPException(status_code=502, detail="No video file produced by yt-dlp")
    final_video = mp4_candidates[0]
    if final_video != video_path:
        final_video.rename(video_path)

    # Try to read subtitles/auto-captions as a rough transcript
    transcript = ""
    has_transcript = False
    for sub_file in out_dir.glob("source*.vtt"):
        try:
            raw = sub_file.read_text(errors="ignore")
            lines = [
                l.strip() for l in raw.splitlines()
                if l.strip() and "-->" not in l and not l.strip().isdigit()
                and not l.startswith("WEBVTT") and not l.startswith("Kind:") and not l.startswith("Language:")
            ]
            text = " ".join(dict.fromkeys(lines))  # de-dupe repeated caption lines, keep order
            if len(text) > len(transcript):
                transcript = text
                has_transcript = True
        except OSError:
            continue

    thumbnail_url = info.get("thumbnail", "") if isinstance(info, dict) else ""

    return {
        "ok": True,
        "source_id": source_id,
        "title": info.get("title", "") if isinstance(info, dict) else "",
        "description": (info.get("description", "") or "")[:4000] if isinstance(info, dict) else "",
        "transcript": transcript[:8000],
        "has_transcript": has_transcript,
        "thumbnail_url": thumbnail_url,
        "duration": info.get("duration") if isinstance(info, dict) else None,
    }


@app.post("/tts")
async def tts(payload: dict):
    text = (payload or {}).get("text", "").strip()
    voice = (payload or {}).get("voice", "my-MM-NilarNeural")
    if not text:
        raise HTTPException(status_code=400, detail="Missing 'text'")

    tmp_id = uuid.uuid4().hex[:12]
    out_path = STORAGE_DIR / f"tts-{tmp_id}.mp3"

    try:
        communicate = edge_tts.Communicate(text, voice)
        await communicate.save(str(out_path))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"edge-tts failed: {e}")

    if not out_path.exists() or out_path.stat().st_size == 0:
        raise HTTPException(status_code=502, detail="TTS produced no audio")

    return FileResponse(str(out_path), media_type="audio/mpeg", filename="voice.mp3")


@app.post("/compose")
def compose(source_id: str = Form(...), title: str = Form(""), audio: UploadFile = File(...)):
    src_dir = _source_dir(source_id)
    video_path = src_dir / "source.mp4"
    if not video_path.exists():
        raise HTTPException(status_code=404, detail="source_id not found or expired — re-run /ingest")

    audio_path = src_dir / "narration.mp3"
    with open(audio_path, "wb") as f:
        shutil.copyfileobj(audio.file, f)

    out_path = src_dir / "final.mp4"

    # Replace original audio track with the Burmese narration, trim video to narration length,
    # scale/crop to 9:16 vertical for Reels, burn in a simple title card via drawtext.
    safe_title = re.sub(r"[\"':]", "", title)[:120]

    filter_complex = (
        "[0:v]scale=1080:1920:force_original_aspect_ratio=increase,"
        "crop=1080:1920,"
        f"drawtext=text='{safe_title}':fontcolor=white:fontsize=48:"
        "box=1:boxcolor=black@0.5:boxborderw=12:x=(w-text_w)/2:y=80:"
        "enable='between(t,0,4)'[v]"
    )

    cmd = [
        "ffmpeg", "-y",
        "-i", str(video_path),
        "-i", str(audio_path),
        "-filter_complex", filter_complex,
        "-map", "[v]", "-map", "1:a",
        "-shortest",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
        "-c:a", "aac", "-b:a", "192k",
        str(out_path),
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=540)
        if result.returncode != 0:
            raise HTTPException(status_code=502, detail=f"ffmpeg failed: {result.stderr[-1500:]}")
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="ffmpeg compose timed out")

    if not out_path.exists() or out_path.stat().st_size == 0:
        raise HTTPException(status_code=502, detail="ffmpeg produced no output")

    # Return a path (not the raw file) so a pull-based uploader (e.g. Facebook's
    # video_reels file_url upload) can fetch it via GET /files/{source_id}/final.mp4.
    return JSONResponse({
        "ok": True,
        "source_id": source_id,
        "video_path": f"/files/{source_id}/final.mp4",
        "size_bytes": out_path.stat().st_size,
    })


@app.get("/files/{source_id}/{filename}")
def serve_file(source_id: str, filename: str):
    safe_name = os.path.basename(filename)
    file_path = _source_dir(source_id) / safe_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found or expired")
    media_type = "video/mp4" if safe_name.endswith(".mp4") else "application/octet-stream"
    return FileResponse(str(file_path), media_type=media_type, filename=safe_name)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
