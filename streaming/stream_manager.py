from __future__ import annotations

import os
import subprocess
import threading
from pathlib import Path
from typing import Optional


def _log_stderr(camera_id: str, stream) -> None:
    """Read and log FFmpeg stderr asynchronously."""
    try:
        for line in iter(stream.readline, ""):
            if line:
                print(f"[FFMPEG:{camera_id[:8]}] {line.rstrip()}")
    except Exception:
        pass

HLS_DIR = Path(__file__).parent / "hls"
HLS_DIR.mkdir(exist_ok=True)

FFMPEG_PATH: Optional[str] = None

# Try to locate ffmpeg
_ffmpeg_candidates = [
    "ffmpeg",
    r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
    str(Path(os.environ.get("LOCALAPPDATA", ""))
        / "Microsoft" / "WinGet" / "Packages"
        / "Gyan.FFmpeg.Essentials_Microsoft.Winget.Source_8wekyb3d8bbwe"
        / "ffmpeg-8.1.1-essentials_build" / "bin" / "ffmpeg.exe"),
]
for c in _ffmpeg_candidates:
    try:
        subprocess.run([c, "-version"], capture_output=True, timeout=5)
        FFMPEG_PATH = c
        break
    except (FileNotFoundError, subprocess.TimeoutExpired):
        continue


_processes: dict[str, subprocess.Popen] = {}


def _normalize_rtsp(url: str) -> str:
    if url.startswith("rstp://"):
        corrected = "rtsp://" + url[7:]
        print(f"[INFO] URL RTSP corrigée : {url} → {corrected}")
        return corrected
    return url


def start_stream(camera_id: str, rtsp_url: str) -> bool:
    """Start FFmpeg HLS transcoding for a camera."""
    if FFMPEG_PATH is None:
        print("[ERROR] FFmpeg introuvable. Vérifie l'installation ou le chemin.")
        return False

    rtsp_url = _normalize_rtsp(rtsp_url)

    stop_stream(camera_id)

    output_dir = HLS_DIR / camera_id
    output_dir.mkdir(exist_ok=True)
    playlist = output_dir / "index.m3u8"

    cmd = [
        FFMPEG_PATH,
        "-rtsp_transport", "tcp",
        "-timeout", "6000000",
        "-i", rtsp_url,
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-tune", "zerolatency",
        "-pix_fmt", "yuv420p",
        "-profile:v", "high",
        "-level", "4.0",
        "-g", "30",
        "-b:v", "1500k",
        "-maxrate", "2000k",
        "-bufsize", "4000k",
        "-an",
        "-f", "hls",
        "-hls_time", "2",
        "-hls_list_size", "5",
        "-hls_flags", "delete_segments",
        "-hls_segment_filename", str(output_dir / "seg_%03d.ts"),
        "-progress", "pipe:1",
        "-loglevel", "warning",
        "-y",
        str(playlist),
    ]

    print(f"[INFO] Démarrage du streaming HLS pour {camera_id}")

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0,
        )
        _processes[camera_id] = proc
        # Log FFmpeg errors asynchronously
        t = threading.Thread(target=_log_stderr, args=(camera_id, proc.stderr), daemon=True)
        t.start()
        return True
    except Exception as e:
        print(f"[ERROR] FFmpeg n'a pas pu démarrer pour {rtsp_url} : {e}")
        return False


def stop_stream(camera_id: str) -> None:
    """Stop FFmpeg transcoding for a camera."""
    proc = _processes.pop(camera_id, None)
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    # Clean up HLS files
    output_dir = HLS_DIR / camera_id
    if output_dir.exists():
        for f in output_dir.iterdir():
            f.unlink(missing_ok=True)
        output_dir.rmdir()


def stop_all() -> None:
    """Stop all FFmpeg processes."""
    for cam_id in list(_processes.keys()):
        stop_stream(cam_id)


def get_active_streams() -> list[str]:
    """Return list of camera IDs with active streams."""
    return [cid for cid, proc in _processes.items() if proc and proc.poll() is None]
