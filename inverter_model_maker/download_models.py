#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Download local AI/OCR models for Model Maker.

v4.2 default:
  python download_models.py rapidocr

This downloads small PP-OCR ONNX models for RapidOCR and updates
model_maker_web_v4/mm_settings.json. Large VLM/LLM downloads are kept as
legacy explicit commands only.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# v4.2 fast OCR models. Source: https://huggingface.co/monkt/paddleocr-onnx
RAPIDOCR_REPO = "monkt/paddleocr-onnx"
RAPIDOCR_REVISION = "main"
RAPIDOCR_LANGS_DEFAULT = ("english", "korean", "latin")
RAPIDOCR_DET_FILES = ("detection/v5/det.onnx", "detection/v5/config.json")

# Legacy large models. Do not download these from INSTALL_???.bat by default.
PHI_REPO = "microsoft/Phi-4-mini-instruct"
PHI_REVISION = "main"
QWEN_REPO = "QuantTrio/Qwen3-VL-32B-Instruct-AWQ"
QWEN_REVISION = "main"
NEMOTRON_REPO = "nvidia/Llama-3.1-Nemotron-Nano-VL-8B-V1"
NEMOTRON_REVISION = "main"

MODELS_DIR = Path("C:/models")
RAPIDOCR_DIR = MODELS_DIR / "paddleocr-onnx"
PROJECT_DIR = Path(__file__).resolve().parent
V4_SETTINGS_PATH = PROJECT_DIR / "model_maker_web_v4" / "mm_settings.json"


def _snapshot(repo_id: str, local_dir: str | Path, revision: str):
    """Download a full Hugging Face repo snapshot."""
    from huggingface_hub import snapshot_download
    snapshot_download(
        repo_id=repo_id,
        local_dir=str(local_dir),
        revision=revision,
        ignore_patterns=["*.msgpack", "flax_model*", "tf_model*", "rust_model*"],
    )


def _hf_download(repo_id: str, filename: str, local_root: Path, revision: str) -> Path:
    """Download one repo file while preserving the repository subfolder layout."""
    from huggingface_hub import hf_hub_download
    dest = local_root / filename.replace("/", os.sep)
    if dest.exists() and dest.stat().st_size > 0:
        print(f"[Skip] {filename}")
        return dest
    print(f"[Downloading] {filename}")
    downloaded = Path(hf_hub_download(repo_id=repo_id, filename=filename, revision=revision))
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Avoid shutil.copy2 import cost until needed.
    import shutil
    shutil.copy2(downloaded, dest)
    return dest


def _has_weights(path: str | Path) -> bool:
    import glob
    path = str(path)
    return bool(glob.glob(os.path.join(path, "*.safetensors")) + glob.glob(os.path.join(path, "*.bin")))


def _load_settings() -> dict:
    if not V4_SETTINGS_PATH.exists():
        return {}
    try:
        with open(V4_SETTINGS_PATH, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_settings(data: dict) -> None:
    V4_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(V4_SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def _update_v4_settings(default_lang: str = "english") -> None:
    settings = _load_settings()
    layout = dict(settings.get("layout", {}))
    layout.setdefault("enabled", True)
    layout.setdefault("min_valid_rows", 5)
    layout.setdefault("min_accept_rows", 30)
    layout.setdefault("image_dpi", 200)

    rapid = dict(settings.get("rapidocr", {}))
    rapid.update({
        "enabled": True,
        "det_model_path": str((RAPIDOCR_DIR / "detection" / "v5" / "det.onnx").as_posix()),
        "rec_model_path": str((RAPIDOCR_DIR / "languages" / default_lang / "rec.onnx").as_posix()),
        "rec_keys_path": str((RAPIDOCR_DIR / "languages" / default_lang / "dict.txt").as_posix()),
        "lang": default_lang,
        "device": "cpu",
    })

    legacy = dict(settings.get("legacy_nemotron", settings.get("nemotron_ocr", {})))
    legacy.setdefault("enabled", False)
    legacy.setdefault("model_path", "C:/models/Nemotron-Nano-VL-8B")
    legacy.setdefault("device", "auto")
    legacy.setdefault("image_dpi", 200)
    legacy.setdefault("page_timeout", 120)
    legacy["enabled"] = False

    settings["layout"] = layout
    settings["rapidocr"] = rapid
    settings["legacy_nemotron"] = legacy
    settings.pop("nemotron_ocr", None)
    _save_settings(settings)
    print(f"[Done] Updated {V4_SETTINGS_PATH}")


def download_rapidocr(langs: tuple[str, ...] = RAPIDOCR_LANGS_DEFAULT):
    """Download small RapidOCR/PP-OCR ONNX models for v4.2."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[Info] RapidOCR model repo: {RAPIDOCR_REPO}")
    print(f"[Info] Target: {RAPIDOCR_DIR}")
    print("[Info] Downloading PP-OCRv5 detection plus language recognition models")

    for filename in RAPIDOCR_DET_FILES:
        _hf_download(RAPIDOCR_REPO, filename, RAPIDOCR_DIR, RAPIDOCR_REVISION)

    for lang in langs:
        for name in ("rec.onnx", "dict.txt", "config.json"):
            _hf_download(RAPIDOCR_REPO, f"languages/{lang}/{name}", RAPIDOCR_DIR, RAPIDOCR_REVISION)

    default_lang = "english" if "english" in langs else langs[0]
    _update_v4_settings(default_lang=default_lang)
    print("[Done] RapidOCR ONNX models are ready")


def download_phi():
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    phi_path = MODELS_DIR / "Phi-4-mini-instruct"
    if phi_path.exists() and _has_weights(phi_path):
        print("[Skip] Phi-4-mini-instruct already exists")
        return
    print(f"[Downloading] {PHI_REPO} (revision={PHI_REVISION}) ...")
    _snapshot(PHI_REPO, phi_path, PHI_REVISION)
    print("[Done] Phi-4-mini-instruct downloaded")


def download_qwen():
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    qwen_path = MODELS_DIR / "Qwen3-VL-32B-4bit"
    if qwen_path.exists() and _has_weights(qwen_path):
        print("[Skip] Qwen3-VL-32B-4bit already exists")
        return
    print(f"[Downloading] {QWEN_REPO} (revision={QWEN_REVISION}) ...")
    print("  Size: ~22GB - legacy/debug only")
    _snapshot(QWEN_REPO, qwen_path, QWEN_REVISION)
    print("[Done] Qwen3-VL-32B-4bit downloaded")


def download_nemotron():
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    nem_path = MODELS_DIR / "Nemotron-Nano-VL-8B"
    if nem_path.exists() and _has_weights(nem_path):
        print("[Skip] Nemotron-Nano-VL-8B already exists")
        return
    print(f"[Downloading] {NEMOTRON_REPO} (revision={NEMOTRON_REVISION}) ...")
    print("  Size: ~16GB - legacy/debug only")
    _snapshot(NEMOTRON_REPO, nem_path, NEMOTRON_REVISION)
    print("[Done] Nemotron-Nano-VL-8B downloaded")


def main(argv: list[str]) -> int:
    mode = argv[1] if len(argv) > 1 else "rapidocr"
    try:
        if mode in ("rapidocr", "ocr", "all"):
            download_rapidocr()
        elif mode == "rapidocr_en":
            download_rapidocr(("english",))
        elif mode == "rapidocr_ko":
            download_rapidocr(("english", "korean"))
        elif mode == "phi":
            download_phi()
        elif mode == "qwen":
            download_qwen()
        elif mode == "nemotron":
            download_nemotron()
        elif mode == "legacy_all":
            download_phi(); print(); download_qwen(); print(); download_nemotron()
        else:
            print(f"Unknown mode: {mode}")
            print("Usage: python download_models.py [rapidocr|rapidocr_en|rapidocr_ko|phi|qwen|nemotron|legacy_all]")
            return 1
    except Exception as e:
        print(f"[Error] Model download failed: {e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
