"""
imagePreprocessing.py — Download and preprocess images from a list of URLs.

Receives a list of (url, raw_bytes) or just URLs from the streaming pipeline,
applies the standard Trusted Zone transforms, and returns clean PIL Images
ready to be consumed by clipModel.py.

Interface
─────────
    from scripts.imagePreprocessing import preprocess_urls

    clean: list[tuple[str, Image.Image]] = preprocess_urls(urls)
    # Each tuple is (original_url, preprocessed_PIL_image)
    # URLs that fail download or validation are silently dropped.
"""

from __future__ import annotations

import io
import urllib.request
from typing import Optional

from PIL import Image

# ── Constants ──────────────────────────────────────────────────────────────────

IMAGE_SIZE: tuple[int, int] = (224, 224)

# ── Internal helpers ───────────────────────────────────────────────────────────

def _download(url: str, timeout: int = 15) -> Optional[bytes]:
    """
    Download raw bytes from *url*.
    Returns None on any network or HTTP error.
    """
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as exc:
        print(f"  [preprocess] Download failed for '{url}': {exc}")
        return None


def _preprocess(raw_bytes: bytes) -> Optional[Image.Image]:
    """
    Apply the standard Trusted Zone image transforms:
      1. Verify integrity with Pillow.
      2. Convert to RGB colour space.
      3. Resize to IMAGE_SIZE (224x224).

    Returns a PIL Image ready for CLIP, or None if the image is corrupt.
    """
    try:
        # Step 1 — integrity check (advances the stream pointer)
        img = Image.open(io.BytesIO(raw_bytes))
        img.verify()

        # Step 2 & 3 — re-open after verify(), convert and resize
        img = Image.open(io.BytesIO(raw_bytes))
        img = img.convert("RGB")
        img = img.resize(IMAGE_SIZE)
        return img

    except Exception as exc:
        print(f"  [preprocess] Image validation failed: {exc}")
        return None

# ── Public interface ───────────────────────────────────────────────────────────

def preprocess_urls(urls: list[str]) -> list[tuple[str, Image.Image]]:
    """
    Download and preprocess a batch of image URLs.

    Called by sparkStreaming.py for each micro-batch.
    Returns only the successfully processed images so that
    clipModel.py always receives a clean, ready-to-embed list.

    Parameters
    ----------
    urls : list[str]
        Image URLs extracted from the Kafka micro-batch.

    Returns
    -------
    list[tuple[str, PIL.Image.Image]]
        Pairs of (original_url, preprocessed_image) for every URL
        that could be downloaded and validated.  Failed URLs are
        dropped and logged but do not raise exceptions.
    """
    results: list[tuple[str, Image.Image]] = []

    for url in urls:
        raw = _download(url)
        if raw is None:
            continue

        img = _preprocess(raw)
        if img is None:
            continue

        results.append((url, img))

    print(f"  [preprocess] {len(results)}/{len(urls)} images ready after preprocessing.")
    return results