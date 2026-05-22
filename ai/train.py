import random
import shutil
from pathlib import Path

from .config import DATA_YAML, DATA_DIR, DEVICE
from .model import load_model


def prepare_dataset_splits(base_dir: Path, val_split: float = 0.2, test_split: float = 0.0, seed: int = 0):
    train_images = base_dir / "train" / "images"
    train_labels = base_dir / "train" / "labels"
    val_images = base_dir / "valid" / "images"
    val_labels = base_dir / "valid" / "labels"
    test_images = base_dir / "test" / "images"
    test_labels = base_dir / "test" / "labels"

    if val_images.exists():
        return

    if not train_images.exists():
        print(f"Train images folder not found: {train_images}")
        return

    img_exts = {".jpg", ".jpeg", ".png", ".bmp"}
    imgs = [p for p in sorted(train_images.iterdir()) if p.suffix.lower() in img_exts]
    if not imgs:
        print(f"No images found in {train_images}")
        return

    random.seed(seed)
    random.shuffle(imgs)

    n = len(imgs)
    n_val = int(n * val_split)
    n_test = int(n * test_split)

    val_sel = set(imgs[:n_val])
    test_sel = set(imgs[n_val:n_val + n_test]) if n_test > 0 else set()

    val_images.mkdir(parents=True, exist_ok=True)
    val_labels.mkdir(parents=True, exist_ok=True)
    if n_test > 0:
        test_images.mkdir(parents=True, exist_ok=True)
        test_labels.mkdir(parents=True, exist_ok=True)

    for p in imgs:
        if p in val_sel:
            dst_img = val_images / p.name
            shutil.copy2(p, dst_img)
            lbl = train_labels / (p.stem + '.txt')
            if lbl.exists():
                shutil.copy2(lbl, val_labels / lbl.name)
        elif p in test_sel:
            dst_img = test_images / p.name
            shutil.copy2(p, dst_img)
            lbl = train_labels / (p.stem + '.txt')
            if lbl.exists():
                shutil.copy2(lbl, test_labels / lbl.name)


def train_model(epochs: int = 100):
    prepare_dataset_splits(DATA_DIR, val_split=0.2, test_split=0.0, seed=0)

    model = load_model(for_training=True, device=DEVICE)
    model.train(
        data=str(DATA_YAML),
        epochs=epochs,
        project=str(DATA_DIR.parent.parent / "runs" / "detect"),
        name="train",
        exist_ok=True,
        split=0.2,
        resume=True,
        device=DEVICE,
    )


def main():
    import argparse
    parser = argparse.ArgumentParser(description="BeeAware Training")
    parser.add_argument("--epochs", type=int, default=100, help="Number of epochs")
    args = parser.parse_args()

    train_model(epochs=args.epochs)


if __name__ == "__main__":
    main()
