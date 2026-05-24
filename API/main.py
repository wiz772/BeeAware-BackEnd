from __future__ import annotations

import os
import random
import sys
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import bcrypt
from jose import jwt, JWTError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.DB_init import get_database_repository
from streaming.stream_manager import start_stream, stop_stream, stop_all, get_active_streams, HLS_DIR

from ai.analyzer import get_analyzer
from stream_ingestion.main import Dispatcher, make_detection_handler

app = FastAPI(title="BeeAware API", version="1.0.0")

# Mount HLS files for browser playback
hls_mount_path = str(HLS_DIR.resolve())
if Path(hls_mount_path).exists():
    app.mount("/hls", StaticFiles(directory=hls_mount_path), name="hls")

def _enrich_camera(cam: dict) -> dict:
    """Add alert / detection fields from DB to a camera dict."""
    cam["lastDetection"] = None
    cam["detectionsToday"] = 0
    cam["alert"] = False
    cam["thumb"] = str(hash(cam.get("id", "")) % 360)
    cam["confidence"] = None
    cam["fps"] = None
    cam["signal"] = None
    cam["connection"] = cam.get("rtsp_url") or cam.get("url")
    if use_db:
        try:
            dets = repository.get_detections(camera_id=cam.get("id"), limit=1)
            if dets:
                cam["lastDetection"] = dets[0]["detected_at"]
                cam["alert"] = True
                cam["confidence"] = dets[0]["confidence_score"]
            today_dets = repository.get_detections(camera_id=cam.get("id"), limit=1000)
            today_str = datetime.utcnow().strftime("%Y-%m-%d")
            cam["detectionsToday"] = sum(
                1 for d in today_dets if d["detected_at"] and d["detected_at"].startswith(today_str)
            )
        except Exception:
            pass
    return cam


# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIG ---
SECRET_KEY = "une-cle-secrete-tres-longue"
ALGORITHM = "HS256"
TOKEN_EXPIRATION = 10080


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")


def verifier_mot_de_passe(mdp_brut, mdp_crypte):
    return bcrypt.checkpw(mdp_brut.encode(), mdp_crypte.encode())


def creer_token(donnees: dict):
    donnees["exp"] = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRATION)
    return jwt.encode(donnees, SECRET_KEY, algorithm=ALGORITHM)


def get_utilisateur_actuel(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Token invalide")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Token invalide ou expiré")


# Initialize repository (Postgres) with fallback to in-memory store
repository = get_database_repository()
_in_memory_users: dict = {}
_in_memory_cameras: list = []
_in_memory_detections: list = []

use_db = repository is not None
if use_db:
    print("[INFO] Using Postgres repository for API")
else:
    print("[WARN] Postgres unavailable — using in-memory fallback")

_ai_dispatcher: Optional[Dispatcher] = None


@app.on_event("startup")
def startup_ai():
    global _ai_dispatcher
    try:
        analyzer = get_analyzer()
        handler = make_detection_handler(repository)
        _ai_dispatcher = Dispatcher(analyzer=analyzer, on_hornet_detected=handler)

        cameras = repository.get_all_active_cameras() if use_db else _in_memory_cameras
        started = 0
        for cam in cameras:
            cam_id = cam.get("id")
            url = cam.get("rtsp_url")
            if cam_id and url:
                try:
                    _ai_dispatcher.add_camera(url, camera_id=cam_id)
                    started += 1
                except Exception as e:
                    print(f"[WARN] AI worker failed for camera {cam_id}: {e}")
        print(f"[INFO] AI ingestion started for {started}/{len(cameras)} camera(s)")
    except Exception as e:
        print(f"[WARN] AI ingestion not available: {e}")

    # Fake detection every 5 minutes for demo
    def _fake_detection_loop():
        while True:
            threading.Event().wait(300)  # 5 minutes
            if not use_db:
                continue
            try:
                cameras = repository.get_all_active_cameras()
                if not cameras:
                    continue
                cam = random.choice(cameras)
                cam_id = cam["id"]
                conf = round(random.uniform(0.75, 0.99), 2)
                count = random.randint(1, 3)
                now_ts = datetime.utcnow()
                snap_filename = f"{int(now_ts.timestamp())}.jpg"
                snap_dir = Path("snapshots") / str(cam_id)
                snap_dir.mkdir(parents=True, exist_ok=True)
                snap_path = str(snap_dir / snap_filename)
                # Create a placeholder snapshot so the frontend shows something
                try:
                    import cv2
                    import numpy as np
                    h, w = 480, 640
                    img = np.zeros((h, w, 3), dtype=np.uint8)
                    img[:] = (30, 30, 30)
                    cv2.putText(img, "HORNET SIMULE", (w // 2 - 140, h // 2 - 10),
                                cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0, 0, 255), 2)
                    cv2.putText(img, f"conf: {conf:.0%}", (w // 2 - 80, h // 2 + 30),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)
                    cv2.rectangle(img, (w // 2 - 60, h // 2 - 60),
                                  (w // 2 + 60, h // 2 + 60), (0, 0, 255), 3)
                    cv2.imwrite(snap_path, img)
                    cv2.imwrite(str(snap_dir / "latest.jpg"), img)
                except Exception:
                    snap_path = None
                repository.add_detection(
                    camera_id=cam_id,
                    reviewed_by=None,
                    hornet_count=count,
                    confidence_score=conf,
                    is_false_positive=False,
                    detected_at=now_ts.isoformat(),
                    snapshot_path=snap_path,
                )
                print(f"[FAKE] Detection ajoutée pour {cam_id} ({count} frelon(s), {conf:.0%})")
            except Exception as e:
                print(f"[FAKE] Erreur: {e}")

    t = threading.Thread(target=_fake_detection_loop, daemon=True)
    t.start()
    print("[INFO] Fausses détections toutes les 5 minutes activées")


# ─────────────────────────────────────────────
# AUTH ENDPOINTS
# ─────────────────────────────────────────────

@app.post("/register")
def inscription(username: str = Query(...), password: str = Query(...)):
    if use_db:
        existing = repository.get_user_by_email(username)
        if existing:
            raise HTTPException(status_code=400, detail="Utilisateur deja existant")
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        user_id = repository.create_user(username, hashed)
        if not user_id:
            raise HTTPException(status_code=500, detail="Impossible de creer l'utilisateur")
        token = creer_token({"sub": username})
        return {"message": "Compte cree !", "id": str(user_id), "access_token": token, "token_type": "bearer"}

    if username in _in_memory_users:
        raise HTTPException(status_code=400, detail="Utilisateur deja existant")
    _in_memory_users[username] = {"username": username, "hashed_password": bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode(), "name": username.split("@")[0].capitalize()}
    token = creer_token({"sub": username})
    return {"message": "Compte cree !", "access_token": token, "token_type": "bearer"}


@app.post("/login")
def connexion(form: OAuth2PasswordRequestForm = Depends()):
    if use_db:
        user = repository.get_user_by_email(form.username)
        if not user or not verifier_mot_de_passe(form.password, user["password_hash"]):
            raise HTTPException(status_code=401, detail="Mauvais identifiants")
        token = creer_token({"sub": form.username})
        return {"access_token": token, "token_type": "bearer", "name": user.get("first_name") or form.username.split("@")[0].capitalize()}

    user = _in_memory_users.get(form.username)
    if not user or not verifier_mot_de_passe(form.password, user["hashed_password"]):
        raise HTTPException(status_code=401, detail="Mauvais identifiants")
    token = creer_token({"sub": form.username})
    return {"access_token": token, "token_type": "bearer", "name": user.get("name", form.username.split("@")[0].capitalize())}


@app.get("/me")
def mon_profil(email: str = Depends(get_utilisateur_actuel)):
    if use_db:
        user = repository.get_user_by_email(email)
        if user:
            return {"email": user["email"], "name": user.get("first_name") or email.split("@")[0].capitalize()}
    user = _in_memory_users.get(email)
    if user:
        return {"email": email, "name": user.get("name", email.split("@")[0].capitalize())}
    return {"email": email, "name": email.split("@")[0].capitalize()}


# ─────────────────────────────────────────────
# CAMERA ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/cameras")
def list_cameras(user: str = Depends(get_utilisateur_actuel)):
    if use_db:
        try:
            rows = repository.get_all_active_cameras()
            return [_enrich_camera(dict(r)) for r in rows]
        except Exception:
            pass
    return _in_memory_cameras


@app.get("/cameras/{camera_id}")
def get_camera(camera_id: str, user: str = Depends(get_utilisateur_actuel)):
    if use_db:
        try:
            cam = repository.get_camera_by_id(camera_id)
            if cam:
                return _enrich_camera(dict(cam))
        except Exception:
            pass
    cam = next((c for c in _in_memory_cameras if c["id"] == camera_id), None)
    if not cam:
        raise HTTPException(status_code=404, detail="Camera not found")
    return _enrich_camera(dict(cam))


@app.post("/cameras")
def add_camera(name: str = Query(...), rtsp_url: str = Query(...), username: str = Query(None), password: str = Query(None), _user: str = Depends(get_utilisateur_actuel)):
    cam_id = str(uuid.uuid4())
    new_cam = {
        "id": cam_id, "name": name, "rtsp_url": rtsp_url,
        "status": "online", "last_detection": None, "detections_today": 0,
        "alert": False, "thumb": "100", "confidence": None, "fps": None,
        "signal": None, "connection": None, "created_at": datetime.utcnow().isoformat()
    }
    if use_db:
        try:
            db_cam_id = repository.add_camera(name, rtsp_url, cam_id)
            if db_cam_id:
                cam_id = str(db_cam_id)
                new_cam["id"] = cam_id
        except Exception:
            pass
    _in_memory_cameras.append(new_cam)
    start_stream(cam_id, rtsp_url)
    if _ai_dispatcher:
        try:
            _ai_dispatcher.add_camera(rtsp_url, camera_id=cam_id)
        except Exception as e:
            print(f"[WARN] AI worker failed to start for {rtsp_url}: {e}")
    return {"message": "Camera added successfully", "camera": new_cam}


@app.post("/cameras/test")
def test_camera(rtsp_url: str = Query(...), _user: str = Depends(get_utilisateur_actuel)):
    import subprocess
    from streaming.stream_manager import FFMPEG_PATH
    ffprobe = FFMPEG_PATH.replace("ffmpeg", "ffprobe") if FFMPEG_PATH else "ffprobe"
    try:
        result = subprocess.run(
            [ffprobe, "-rtsp_transport", "tcp", "-timeout", "6000000", "-i", rtsp_url, "-loglevel", "quiet", "-show_streams"],
            capture_output=True, timeout=6, check=True
        )
        return {"ok": True}
    except subprocess.CalledProcessError:
        return {"ok": False, "error": "Impossible de se connecter au flux RTSP"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "Délai de connexion dépassé"}
    except Exception:
        return {"ok": False, "error": "Erreur lors du test de connexion"}


# ─────────────────────────────────────────────
# CLEAR DETECTIONS
# ─────────────────────────────────────────────

@app.delete("/detections")
def clear_detections(camera_id: Optional[str] = Query(None), user: str = Depends(get_utilisateur_actuel)):
    if use_db:
        try:
            repository.clear_detections(camera_id=camera_id)
            msg = "Détections supprimées" + (f" pour la caméra {camera_id}" if camera_id else "")
            return {"message": msg}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erreur: {e}")
    _in_memory_detections.clear()
    return {"message": "Détections supprimées (mémoire)"}


# ─────────────────────────────────────────────
# SNAPSHOT ENDPOINTS
# ─────────────────────────────────────────────

def _build_snapshot_url(detection_id: str) -> str:
    return f"/snapshots/{detection_id}"

@app.get("/cameras/{camera_id}/snapshot")
def get_snapshot(camera_id: str):
    snap_path = Path("snapshots") / camera_id / "latest.jpg"
    if not snap_path.exists():
        raise HTTPException(status_code=404, detail="Aucun snapshot disponible")
    return FileResponse(str(snap_path), media_type="image/jpeg")

@app.get("/snapshots/{detection_id}")
def get_detection_snapshot(detection_id: str):
    if use_db:
        try:
            rows = repository.get_detections(limit=200)
            for r in rows:
                if r["id"] == detection_id and r.get("snapshot_path"):
                    p = Path(r["snapshot_path"])
                    if p.exists():
                        return FileResponse(str(p), media_type="image/jpeg")
        except Exception:
            pass
    raise HTTPException(status_code=404, detail="Snapshot introuvable")


# ─────────────────────────────────────────────
# DETECTION ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/detections")
def list_detections(camera_id: Optional[str] = Query(None), user: str = Depends(get_utilisateur_actuel)):
    if use_db:
        try:
            rows = repository.get_detections(camera_id=camera_id, limit=50)
            return [
                {
                    "id": r["id"],
                    "type": "Asian hornet",
                    "confidence": r["confidence_score"],
                    "hue": 25,
                    "hive": camera_id or "N/A",
                    "time": r["detected_at"],
                    "camera_id": r["camera_id"],
                    "image_url": _build_snapshot_url(r["id"]) if r.get("snapshot_path") else None,
                }
                for r in rows
            ]
        except Exception:
            pass
    if camera_id:
        return [d for d in _in_memory_detections if d["camera_id"] == camera_id]
    return _in_memory_detections


@app.get("/detections/{detection_id}")
def get_detection(detection_id: str, user: str = Depends(get_utilisateur_actuel)):
    if use_db:
        try:
            rows = repository.get_detections(limit=100)
            for r in rows:
                if r["id"] == detection_id:
                    return {
                        "id": r["id"],
                        "type": "Asian hornet",
                        "confidence": r["confidence_score"],
                        "hue": 25,
                        "hive": str(r["camera_id"]),
                        "time": r["detected_at"],
                        "camera_id": r["camera_id"],
                        "image_url": _build_snapshot_url(r["id"]) if r.get("snapshot_path") else None,
                    }
        except Exception:
            pass
    det = next((d for d in _in_memory_detections if d["id"] == detection_id), None)
    if not det:
        raise HTTPException(status_code=404, detail="Detection not found")
    return det


# ─────────────────────────────────────────────
# STATS ENDPOINT
# ─────────────────────────────────────────────

@app.get("/stats")
def get_stats(user: str = Depends(get_utilisateur_actuel)):
    today_count = 0
    total = 0
    avg_confidence = 0

    if use_db:
        try:
            rows = repository.get_detections(limit=1000)
            today_count = sum(1 for r in rows if r["detected_at"] and r["detected_at"].startswith(datetime.utcnow().strftime("%Y-%m-%d")))
            total = len(rows)
            confidences = [r["confidence_score"] for r in rows if r["confidence_score"]]
            avg_confidence = round(sum(confidences) / len(confidences), 2) if confidences else 0
        except Exception:
            pass
    else:
        today_count = sum(1 for d in _in_memory_detections if d["type"] == "Asian hornet")
        total = len(_in_memory_detections)
        confidences = [d["confidence"] for d in _in_memory_detections]
        avg_confidence = round(sum(confidences) / len(confidences), 2) if confidences else 0

    online = sum(1 for c in _in_memory_cameras if c["status"] == "online")
    return {
        "today": today_count,
        "week": total,
        "total": total,
        "confidence": avg_confidence,
        "cameras_online": online,
        "cameras_total": len(_in_memory_cameras),
        "chart": [],
    }


@app.delete("/cameras/{camera_id}")
def delete_camera(camera_id: str, user: str = Depends(get_utilisateur_actuel)):
    global _in_memory_cameras

    cam = next((c for c in _in_memory_cameras if c["id"] == camera_id), None)
    if not cam and use_db:
        try:
            cam = repository.get_camera_by_id(camera_id)
        except Exception:
            pass

    if _ai_dispatcher and cam:
        try:
            _ai_dispatcher.remove_camera(cam.get("rtsp_url", ""))
        except Exception as e:
            print(f"[WARN] Failed to stop AI worker for {camera_id}: {e}")

    stop_stream(camera_id)
    if use_db:
        try:
            repository.delete_camera(camera_id)
        except Exception:
            pass
    _in_memory_cameras = [c for c in _in_memory_cameras if c["id"] != camera_id]
    return {"message": "Caméra supprimée"}


# ─────────────────────────────────────────────
# STREAMING ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/streams")
def list_streams(user: str = Depends(get_utilisateur_actuel)):
    return {"active": get_active_streams()}

@app.post("/streams/{camera_id}/restart")
def restart_stream(camera_id: str, user: str = Depends(get_utilisateur_actuel)):
    cam = next((c for c in _in_memory_cameras if c["id"] == camera_id), None)
    if not cam:
        if use_db:
            cam = repository.get_camera_by_id(camera_id)
        if not cam:
            raise HTTPException(status_code=404, detail="Camera not found")
    url = cam.get("rtsp_url", "") or cam.get("url", "")
    if _ai_dispatcher:
        _ai_dispatcher.remove_camera(url)
        _ai_dispatcher.add_camera(url, camera_id=camera_id)
    stop_stream(camera_id)
    ok = start_stream(camera_id, url)
    return {"started": ok}

@app.on_event("shutdown")
def shutdown():
    if _ai_dispatcher:
        _ai_dispatcher.stop_all()
    stop_all()


# ─────────────────────────────────────────────
# AI PREDICTION ENDPOINT
# ─────────────────────────────────────────────

@app.post("/predict")
async def predict(file: UploadFile = File(...), user: str = Depends(get_utilisateur_actuel)):
    try:
        from ai.analyzer import get_analyzer
        analyzer = get_analyzer()
        contents = await file.read()
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)
        temp_path = temp_dir / file.filename
        temp_path.write_bytes(contents)
        result = analyzer.predict(temp_path)
        temp_path.unlink(missing_ok=True)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")