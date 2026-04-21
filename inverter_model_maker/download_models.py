#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Download Phi-4-mini-instruct, Qwen3-VL-32B-4bit, and Nemotron-Nano-VL-8B from HuggingFace
Uses snapshot_download() to avoid model-class import issues.
Usage: python download_models.py [phi|qwen|nemotron|all]
"""
import os
import sys

# Pinned revisions — update these if a specific commit is required
PHI_REPO = "microsoft/Phi-4-mini-instruct"
PHI_REVISION = "main"

QWEN_REPO = "QuantTrio/Qwen3-VL-32B-Instruct-AWQ"
QWEN_REVISION = "main"

NEMOTRON_REPO = "nvidia/Llama-3.1-Nemotron-Nano-VL-8B-V1"
NEMOTRON_REVISION = "main"

MODELS_DIR = "C:/models"


def _snapshot(repo_id: str, local_dir: str, revision: str):
    """Download all repo files using huggingface_hub.snapshot_download."""
    from huggingface_hub import snapshot_download
    snapshot_download(
        repo_id=repo_id,
        local_dir=local_dir,
        revision=revision,
        local_dir_use_symlinks=False,
        ignore_patterns=["*.msgpack", "flax_model*", "tf_model*", "rust_model*"],
    )


def _has_weights(path: str) -> bool:
    import glob
    return bool(
        glob.glob(os.path.join(path, "*.safetensors"))
        + glob.glob(os.path.join(path, "*.bin"))
    )


def download_phi():
    os.makedirs(MODELS_DIR, exist_ok=True)
    phi_path = os.path.join(MODELS_DIR, "Phi-4-mini-instruct")
    if os.path.exists(phi_path) and _has_weights(phi_path):
        print("[Skip] Phi-4-mini-instruct already exists")
        return
    print(f"[Downloading] {PHI_REPO} (revision={PHI_REVISION}) ...")
    try:
        _snapshot(PHI_REPO, phi_path, PHI_REVISION)
        print("[Done] Phi-4-mini-instruct downloaded")
    except Exception as e:
        print(f"[Error] Phi download failed: {e}")
        sys.exit(1)


def download_qwen():
    os.makedirs(MODELS_DIR, exist_ok=True)
    qwen_path = os.path.join(MODELS_DIR, "Qwen3-VL-32B-4bit")
    if os.path.exists(qwen_path) and _has_weights(qwen_path):
        print("[Skip] Qwen3-VL-32B-4bit already exists")
        return
    print(f"[Downloading] {QWEN_REPO} (revision={QWEN_REVISION}) ...")
    print("  Size: ~22GB — this will take a while")
    try:
        _snapshot(QWEN_REPO, qwen_path, QWEN_REVISION)
        print("[Done] Qwen3-VL-32B-4bit downloaded")
    except Exception as e:
        print(f"[Error] Qwen3-VL download failed: {e}")
        print(f"[Info] Try: https://huggingface.co/{QWEN_REPO}")
        sys.exit(1)


def download_nemotron():
    os.makedirs(MODELS_DIR, exist_ok=True)
    nem_path = os.path.join(MODELS_DIR, "Nemotron-Nano-VL-8B")
    if os.path.exists(nem_path) and _has_weights(nem_path):
        print("[Skip] Nemotron-Nano-VL-8B already exists")
        return
    print(f"[Downloading] {NEMOTRON_REPO} (revision={NEMOTRON_REVISION}) ...")
    print("  Size: ~16GB — this will take a while")
    try:
        _snapshot(NEMOTRON_REPO, nem_path, NEMOTRON_REVISION)
        print("[Done] Nemotron-Nano-VL-8B downloaded")
    except Exception as e:
        print(f"[Error] Nemotron download failed: {e}")
        print(f"[Info] Try: https://huggingface.co/{NEMOTRON_REPO}")
        sys.exit(1)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode == "phi":
        download_phi()
    elif mode == "qwen":
        download_qwen()
    elif mode == "nemotron":
        download_nemotron()
    elif mode == "all":
        download_phi()
        print()
        download_qwen()
        print()
        download_nemotron()
    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python download_models.py [phi|qwen|nemotron|all]")
        sys.exit(1)
