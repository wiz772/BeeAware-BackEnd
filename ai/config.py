import torch
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

DATA_YAML = SCRIPT_DIR / "datasets" / "hornet_dataset" / "data.yaml"
BASE_MODEL = SCRIPT_DIR / "yolov8n.pt"
TRAINED_MODEL = SCRIPT_DIR / "runs" / "detect" / "train" / "weights" / "best.pt"
DATA_DIR = SCRIPT_DIR / "datasets" / "hornet_dataset"

USE_CUDA = torch.cuda.is_available()
DEVICE = "cuda" if USE_CUDA else "cpu"
