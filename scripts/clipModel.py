from __future__ import annotations

from typing import Optional
import numpy as np
from PIL import Image
from pymilvus import Collection, connections
from transformers import CLIPModel, CLIPProcessor

CLIP_MODEL_NAME = "openai/clip-vit-base-patch32"
EMBEDDING_DIM   = 512


def _load_clip(device: str = "cpu") -> tuple[CLIPProcessor, CLIPModel]:
    """Load the CLIP processor and model. Downloads on first run (~350 MB)."""
    print(f"  [clipModel] Loading CLIP model '{CLIP_MODEL_NAME}' on {device}…")
    processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)
    model     = CLIPModel.from_pretrained(CLIP_MODEL_NAME).to(device)
    model.eval()
    print("  [clipModel] CLIP model ready.")
    return processor, model


def _embed(images: list[Image.Image], processor: CLIPProcessor, model: CLIPModel, device: str) -> np.ndarray:

    import torch

    inputs = processor(images=images, return_tensors="pt", padding=True).to(device)
    with torch.no_grad():
        features = model.get_image_features(**inputs)
        features = features / features.norm(dim=-1, keepdim=True)   # L2 normalise
    return features.cpu().numpy().astype("float32")


def _connect_milvus(host: str, port: int, collection_name: str) -> Collection:
    connections.connect(alias="default", host=host, port=port)
    col = Collection(collection_name)
    col.load()
    return col


def _search_top1(collection: Collection, embedding: list[float]) -> Optional[str]:
    try:
        results = collection.search(
            data=[embedding],
            anns_field="embedding",
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=1,
            output_fields=["filename"],
        )
        if results and results[0]:
            return results[0][0].entity.get("filename")
    except Exception as exc:
        print(f"  [clipModel] Milvus search error: {exc}")
    return None


class CLIPMilvusClient:

    def __init__(
        self,
        milvus_host: str       = "milvus",
        milvus_port: int       = 19530,
        collection_name: str   = "recipe_images",
        device: str            = "cpu",
    ) -> None:
        self.device          = device
        self.milvus_host     = milvus_host
        self.milvus_port     = milvus_port
        self.collection_name = collection_name

        self._processor: Optional[CLIPProcessor] = None
        self._model:     Optional[CLIPModel]     = None
        self._collection: Optional[Collection]  = None

    def _ensure_clip(self) -> None:
        if self._processor is None:
            self._processor, self._model = _load_clip(self.device)

    def _ensure_milvus(self) -> None:
        if self._collection is None:
            self._collection = _connect_milvus(
                self.milvus_host, self.milvus_port, self.collection_name
            )

    def query(self, clean_images: list[tuple[str, Image.Image]]) -> list[dict]:
        if not clean_images:
            return []

        self._ensure_clip()
        self._ensure_milvus()

        urls   = [url for url, _ in clean_images]
        images = [img for _, img in clean_images]

        print(f"  [clipModel] Embedding {len(images)} images…")
        embeddings = _embed(images, self._processor, self._model, self.device)

        results: list[dict] = []
        for url, emb in zip(urls, embeddings):
            filename = _search_top1(self._collection, emb.tolist())
            if filename is None:
                print(f"  [clipModel] No Milvus match for '{url}' — skipping.")
                continue
            results.append({"image_url": url, "filename": filename})

        print(f"  [clipModel] {len(results)}/{len(images)} matches found in Milvus.")
        return results