import os
import tempfile
import shutil

from ultralytics import YOLO
from .config import BASE_MODEL, TRAINED_MODEL, DEVICE


def _resolve_ascii_path(path):
    """Workaround for torch.load failing on Windows with non-ASCII paths."""
    try:
        import ctypes
        from ctypes import wintypes
        GetShortPathNameW = ctypes.windll.kernel32.GetShortPathNameW
        GetShortPathNameW.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
        buf = ctypes.create_unicode_buffer(260)
        n = GetShortPathNameW(str(path), buf, 260)
        if n > 0 and n < 260:
            short = buf.value
            if short:
                return short
    except Exception:
        pass
    return str(path)


def load_model(for_training: bool = False, device: str = DEVICE) -> YOLO:
    if for_training:
        model_path = str(BASE_MODEL)
    elif TRAINED_MODEL.exists():
        model_path = _resolve_ascii_path(TRAINED_MODEL)
    else:
        model_path = str(BASE_MODEL)

    m = YOLO(model_path)

    if device.startswith("cuda"):
        try:
            m.to(device)
        except Exception:
            pass
    return m


_model_instance = None


def get_analyzer_model() -> YOLO:
    global _model_instance
    if _model_instance is None:
        _model_instance = load_model(for_training=False, device=DEVICE)
    return _model_instance


def clear_model_cache():
    global _model_instance
    _model_instance = None
