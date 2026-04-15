#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Download Phi-4-mini-instruct and Qwen3-VL-32B-4bit from HuggingFace
Usage: python download_models.py [phi|qwen|all]
"""
import os
import sys

def download_phi():
    """Download Phi-4-mini-instruct model"""
    from transformers import AutoTokenizer, AutoModelForCausalLM

    models_dir = "C:/models"
    os.makedirs(models_dir, exist_ok=True)

    phi_path = os.path.join(models_dir, "Phi-4-mini-instruct")
    if not os.path.exists(phi_path):
        print("[Downloading] Phi-4-mini-instruct...")
        try:
            AutoTokenizer.from_pretrained(
                "microsoft/Phi-4-mini-instruct",
                trust_remote_code=True,
                cache_dir=models_dir
            )
            AutoModelForCausalLM.from_pretrained(
                "microsoft/Phi-4-mini-instruct",
                trust_remote_code=True,
                device_map="cpu",
                torch_dtype="auto",
                cache_dir=models_dir
            )
            print("[Done] Phi-4-mini-instruct downloaded")
        except Exception as e:
            print(f"[Error] Phi download failed: {e}")
            sys.exit(1)
    else:
        print("[Skip] Phi-4-mini-instruct already exists")

def download_qwen():
    """Download Qwen3-VL-32B-4bit model"""
    from transformers import AutoProcessor, AutoModelForVision2Seq

    models_dir = "C:/models"
    os.makedirs(models_dir, exist_ok=True)

    qwen_path = os.path.join(models_dir, "Qwen3-VL-32B-4bit")
    if not os.path.exists(qwen_path):
        print("[Downloading] Qwen3-VL-32B-4bit...")
        try:
            AutoProcessor.from_pretrained(
                "Qwen/Qwen3-VL-32B-4bit",
                trust_remote_code=True,
                cache_dir=models_dir
            )
            AutoModelForVision2Seq.from_pretrained(
                "Qwen/Qwen3-VL-32B-4bit",
                trust_remote_code=True,
                device_map="auto",
                torch_dtype="auto",
                cache_dir=models_dir
            )
            print("[Done] Qwen3-VL-32B-4bit downloaded")
        except Exception as e:
            print(f"[Error] Qwen3-VL download failed: {e}")
            print("[Info] Try downloading manually from HuggingFace:")
            print("  https://huggingface.co/Qwen/Qwen3-VL-32B-4bit")
            sys.exit(1)
    else:
        print("[Skip] Qwen3-VL-32B-4bit already exists")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode == "phi":
        download_phi()
    elif mode == "qwen":
        download_qwen()
    elif mode == "all":
        download_phi()
        print()
        download_qwen()
    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python download_models.py [phi|qwen|all]")
        sys.exit(1)
