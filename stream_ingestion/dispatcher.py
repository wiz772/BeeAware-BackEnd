import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from camera_worker import CameraWorker

_RETRY_INTERVAL = 60


class Dispatcher:
    def __init__(self, analyzer=None, on_hornet_detected=None):
        self.analyzer = analyzer
        self.on_hornet_detected = on_hornet_detected
        self.workers = {}
        self.threads = {}
        self._watchdog_stop = threading.Event()
        self._watchdog = threading.Thread(target=self._watchdog_loop, daemon=True)
        self._watchdog.start()

    def add_camera(self, url, camera_id=None):
        if url in self.workers and not self.workers[url].is_dead:
            print(f"[DISPATCHER] Camera already running: {url}")
            return
        worker = CameraWorker(
            url,
            camera_id=camera_id,
            analyzer=self.analyzer,
            on_hornet_detected=self.on_hornet_detected,
        )
        self.workers[url] = worker
        thread = threading.Thread(target=worker.run, name=f"camera-{len(self.threads)+1}")
        self.threads[url] = thread
        thread.start()

    def start_all(self):
        for url, worker in self.workers.items():
            if url not in self.threads:
                thread = threading.Thread(target=worker.run, name=f"camera-{len(self.threads)+1}")
                self.threads[url] = thread
                thread.start()

    def remove_camera(self, url):
        if url not in self.workers:
            return
        self.workers[url].stop()
        self.workers.pop(url, None)
        self.threads.pop(url, None)
        print(f"[DISPATCHER] Camera stopped: {url}")

    def stop_all(self):
        self._watchdog_stop.set()
        for url in list(self.workers.keys()):
            self.remove_camera(url)

    def join_all(self):
        for t in self.threads.values():
            t.join()

    def _watchdog_loop(self):
        while not self._watchdog_stop.is_set():
            dead = [(url, w) for url, w in self.workers.items() if w.is_dead]
            for url, worker in dead:
                cam_id = worker.camera_id
                print(f"[DISPATCHER] Restarting dead worker for {url}...")
                self.workers.pop(url, None)
                self.threads.pop(url, None)
                self.add_camera(url, camera_id=cam_id)
            if dead:
                print(f"[DISPATCHER] Restarted {len(dead)} dead worker(s)")
            self._watchdog_stop.wait(_RETRY_INTERVAL)