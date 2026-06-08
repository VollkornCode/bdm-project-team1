from __future__ import annotations

import io
import urllib.request
from typing import Optional

from PIL import Image


IMAGE_SIZE: tuple[int, int] = (224, 224)


def _download(url: str, timeout: int = 15) -> Optional[bytes]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as exc:
        print(f"  [preprocess] Download failed for '{url}': {exc}")
        return None


def _preprocess(raw_bytes: bytes) -> Optional[Image.Image]:
    try:
        img = Image.open(io.BytesIO(raw_bytes))
        img.verify()
        img = Image.open(io.BytesIO(raw_bytes))
        img = img.convert("RGB")
        img = img.resize(IMAGE_SIZE)
        return img

    except Exception as exc:
        print(f"  [preprocess] Image validation failed: {exc}")
        return None


def preprocess_urls(urls: list[str]) -> list[tuple[str, Image.Image]]:
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