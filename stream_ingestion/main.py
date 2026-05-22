import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dispatcher import Dispatcher
from ai.analyzer import get_analyzer

DEFAULT_URLS = ["rtsp://localhost:554/live"]


def retrieve_cameras_from_db(repository):
    rows = repository.get_all_active_cameras()
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        return rows
    if isinstance(rows, list):
        return [{"id": None, "rtsp_url": url} for url in rows]
    return []


def start_workers_from_db(dispatcher, repository):
    cameras = retrieve_cameras_from_db(repository)
    if not cameras:
        print("[WARN] Aucune caméra active trouvée dans la base; utilisation des URLs par défaut.")
        for url in DEFAULT_URLS:
            dispatcher.add_camera(url, camera_id=None)
        return

    for cam in cameras:
        dispatcher.add_camera(cam["rtsp_url"], camera_id=cam.get("id"))


def bootstrap(dispatcher, repository):
    start_workers_from_db(dispatcher, repository)


def register_camera(repository, dispatcher, url, user_id=None):
    repository.add_camera(url, user_id=user_id)
    dispatcher.add_camera(url)
    print(f"[INFO] Caméra enregistrée et démarrée : {url}")


class FakeCameraRepository:
    def __init__(self):
        self._cameras = [
            {"id": str(uuid.uuid4()), "rtsp_url": "rtsp://localhost:554/live"},
        ]

    def get_all_active_cameras(self):
        return self._cameras

    def add_camera(self, url, user_id=None):
        self._cameras.append({"id": str(uuid.uuid4()), "rtsp_url": url})


def get_database_repository():
    try:
        from pathlib import Path
        repo_root = Path(__file__).resolve().parents[1]
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))

        from DB.DB_init import get_database_repository as pg_get_db

        repo = pg_get_db()
        if repo is not None:
            return repo
    except Exception as exc:
        print(f"[WARN] Unable to use Postgres repository: {exc}")

    return FakeCameraRepository()


def make_detection_handler(repository):
    def on_hornet_detected(camera_id, detection):
        hornet_dets = [d for d in detection["detections"] if d["class"] == 1]
        max_conf = max(d["confidence"] for d in hornet_dets) if hornet_dets else 0
        print(f"  -> Saving detection: {len(hornet_dets)} hornet(s), confidence={max_conf:.2f}")

        if camera_id and hasattr(repository, "add_detection"):
            try:
                repository.add_detection(
                    camera_id=camera_id,
                    reviewed_by=None,
                    hornet_count=len(hornet_dets),
                    confidence_score=max_conf,
                    is_false_positive=False,
                    detected_at=datetime.now(timezone.utc).isoformat(),
                )
            except Exception as e:
                print(f"  [ERROR] Failed to save detection: {e}")

    return on_hornet_detected


def start_ingestion():
    repository = get_database_repository()
    if repository is None:
        print("[ERROR] Repository de base de données non implémenté.")
        sys.exit(1)

    analyzer = get_analyzer()
    print(f"[INFO] Analyzer ready (device: {analyzer.model.device})")

    on_hornet = make_detection_handler(repository)

    dispatcher = Dispatcher(analyzer=analyzer, on_hornet_detected=on_hornet)

    bootstrap(dispatcher, repository)

    try:
        dispatcher.join_all()
    except KeyboardInterrupt:
        print("\n[INFO] Arrêt demandé par l'utilisateur.")

if __name__ == "__main__":
    start_ingestion()