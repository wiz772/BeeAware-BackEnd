import argparse
import sys
from pathlib import Path

from .config import TRAINED_MODEL, BASE_MODEL, DEVICE
from .model import get_analyzer_model


CLASS_NAMES = ["bees", "hornet", "others"]
HORNET_CLASS_ID = 1


class Analyzer:
    def __init__(self):
        self.model = get_analyzer_model()

    def predict(self, image_path: Path):
        results = self.model(str(image_path))[0]
        return self._parse_results(results, str(image_path))

    def predict_frame(self, frame):
        results = self.model(frame)[0]
        return self._parse_results(results, "frame")

    def _parse_results(self, results, source: str):
        detections = []
        for box in results.boxes:
            cls_id = int(box.cls[0])
            detections.append({
                "class": cls_id,
                "class_name": CLASS_NAMES[cls_id] if cls_id < len(CLASS_NAMES) else "unknown",
                "confidence": float(box.conf[0]),
                "coords": box.xyxy[0].tolist(),
            })
        return {
            "source": source,
            "model_used": str(TRAINED_MODEL if TRAINED_MODEL.exists() else BASE_MODEL),
            "detections": detections,
        }

    def has_hornet(self, result: dict, min_confidence: float = 0.3) -> bool:
        return any(
            d["class"] == HORNET_CLASS_ID and d["confidence"] >= min_confidence
            for d in result["detections"]
        )


_analyzer_instance = None


def get_analyzer() -> Analyzer:
    global _analyzer_instance
    if _analyzer_instance is None:
        _analyzer_instance = Analyzer()
    return _analyzer_instance


def predict_image(image_path: Path) -> dict:
    return get_analyzer().predict(image_path)


def run_server(host: str = "0.0.0.0", port: int = 8001):
    from fastapi import FastAPI, File, UploadFile
    import uvicorn

    app = FastAPI(title="BeeAware Analyzer", version="1.0.0")

    analyzer = get_analyzer()

    @app.post("/predict")
    async def predict(file: UploadFile = File(...)):
        contents = await file.read()
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)
        temp_path = temp_dir / file.filename
        temp_path.write_bytes(contents)
        try:
            result = analyzer.predict(temp_path)
        finally:
            temp_path.unlink(missing_ok=True)
        return result

    @app.get("/health")
    def health():
        return {"status": "ok", "device": DEVICE}

    print(f"[Analyzer] Server running on {host}:{port}")
    uvicorn.run(app, host=host, port=port)


def main():
    parser = argparse.ArgumentParser(description="BeeAware Analyzer")
    parser.add_argument("--image", type=str, help="Run inference on a single image")
    parser.add_argument("--serve", action="store_true", help="Start analyzer server")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Server host")
    parser.add_argument("--port", type=int, default=8001, help="Server port")

    args = parser.parse_args()

    if args.serve:
        run_server(host=args.host, port=args.port)
    elif args.image:
        path = Path(args.image)
        if not path.exists():
            print(f"Image not found: {path}", file=sys.stderr)
            sys.exit(1)
        result = predict_image(path)
        print(result)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
