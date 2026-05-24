import cv2
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


class CameraWorker:
    def __init__(self, url, fps=1, max_retry=5, retry_delay=2, camera_id=None, analyzer=None, on_hornet_detected=None):
        self.url = url
        self.fps = fps
        self.max_retry = max_retry
        self.retry_delay = retry_delay
        self.camera_id = camera_id
        self.analyzer = analyzer
        self.on_hornet_detected = on_hornet_detected

        self.cap = None
        self.retry_count = 0
        self.frame_count = 0
        self.fake = self.url.startswith("fake://")
        self.connected = False

        self.interval = 1 / fps
        self.last_time = 0
        self._stop = threading.Event()
        self.is_dead = False
        self._last_hornet_alert = 0.0
        self._alert_cooldown = 300  # 5 minutes

    def connect(self):
        if self.fake:
            print(f"[INFO] Connecting to fake stream {self.url}...")
            self.connected = True
            return

        print(f"[INFO] Connecting to {self.url}...")
        self.cap = cv2.VideoCapture(self.url, cv2.CAP_FFMPEG)
        time.sleep(1)

    def is_valid(self):
        if self.fake:
            return self.connected
        return self.cap is not None and self.cap.isOpened()

    def reconnect(self):
        print(f"[WARN] Reconnecting stream {self.url}...")

        try:
            if self.cap:
                self.cap.release()
        except Exception:
            pass

        time.sleep(self.retry_delay)
        self.connect()
        self.retry_count += 1

    def should_skip(self):
        now = time.time()
        if now - self.last_time < self.interval:
            return True
        self.last_time = now
        return False

    def process_frame(self, frame):
        self.frame_count += 1
        if self.frame_count % 30 == 0:
            print(f"[INFO] {self.frame_count} frames read from {self.url}")

        if frame is None or self.analyzer is None:
            return

        try:
            result = self.analyzer.predict_frame(frame)
        except Exception as e:
            print(f"[ERROR] AI inference failed on {self.url}: {e}")
            return

        hornet_detected = self.analyzer.has_hornet(result, min_confidence=0.3)

        if hornet_detected:
            now = time.time()

            # Always draw bounding boxes and save snapshot (keeps overlay up to date)
            annotated = frame.copy()
            for det in result["detections"]:
                if det["class"] == 1 and det["confidence"] >= 0.3:
                    x1, y1, x2, y2 = [int(v) for v in det["coords"]]
                    cv2.rectangle(annotated, (x1, y1), (x2, y2), (0, 0, 255), 3)
                    label = f"Hornet {det['confidence']:.0%}"
                    cv2.putText(annotated, label, (x1, y1 - 10),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 255), 2)

            if self.camera_id:
                snap_dir = Path("snapshots") / str(self.camera_id)
                snap_dir.mkdir(parents=True, exist_ok=True)
                cv2.imwrite(str(snap_dir / "latest.jpg"), annotated)

            # Only save to DB once per cooldown to avoid duplicates
            if now - self._last_hornet_alert >= self._alert_cooldown:
                self._last_hornet_alert = now
                print(f"[ALERT] Hornet detected on {self.url}")

                snap_filename = f"{int(now)}.jpg"
                snap_path = None
                if self.camera_id:
                    snap_dir = Path("snapshots") / str(self.camera_id)
                    snap_dir.mkdir(parents=True, exist_ok=True)
                    unique_path = snap_dir / snap_filename
                    cv2.imwrite(str(unique_path), annotated)
                    snap_path = str(unique_path)

                if self.on_hornet_detected and self.camera_id:
                    try:
                        self.on_hornet_detected(
                            camera_id=self.camera_id,
                            detection=result,
                            snapshot_path=snap_path,
                        )
                    except Exception as e:
                        print(f"[ERROR] Detection handler failed for {self.url}: {e}")

    def stop(self):
        self._stop.set()

    def run(self):
        self.connect()

        while not self._stop.is_set():
            if not self.is_valid():
                self.reconnect()

                if self.retry_count > self.max_retry:
                    print(f"[ERROR] Too many retries for {self.url}")
                    self.is_dead = True
                    break
                continue

            if self.fake:
                time.sleep(self.interval)
                self.retry_count = 0
                self.process_frame(None)
                continue

            try:
                ret, frame = self.cap.read()
            except Exception as e:
                print(f"[ERROR] Failed to read frame from {self.url}: {e}")
                self.reconnect()
                continue

            if not ret:
                self.reconnect()
                continue

            if self.should_skip():
                continue

            self.retry_count = 0
            self.process_frame(frame)

        self.cleanup()

    def cleanup(self):
        if self.cap:
            self.cap.release()
