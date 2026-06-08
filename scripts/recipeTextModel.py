from __future__ import annotations

from typing import Optional

import numpy as np
from pymilvus import Collection, connections
from transformers import AutoModel, AutoTokenizer


TEXT_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM   = 384


def _load_text_model(device: str = "cpu") -> tuple[AutoTokenizer, AutoModel]:
    print(f"  [recipeTextModel] Loading model '{TEXT_MODEL_NAME}' on {device}…")
    tokenizer = AutoTokenizer.from_pretrained(TEXT_MODEL_NAME)
    model     = AutoModel.from_pretrained(TEXT_MODEL_NAME).to(device)
    model.eval()
    print("  [recipeTextModel] Text model ready.")
    return tokenizer, model


def _mean_pool(token_embeddings: "torch.Tensor", attention_mask: "torch.Tensor") -> "torch.Tensor":
    import torch
    mask_expanded = (
        attention_mask.unsqueeze(-1)
        .expand(token_embeddings.size())
        .float()
    )
    return torch.sum(token_embeddings * mask_expanded, dim=1) / torch.clamp(
        mask_expanded.sum(dim=1), min=1e-9
    )


def _embed(
    texts: list[str],
    tokenizer: AutoTokenizer,
    model: AutoModel,
    device: str,
) -> np.ndarray:
    import torch

    encoded = tokenizer(
        texts,
        padding=True,
        truncation=True,
        max_length=256,
        return_tensors="pt",
    ).to(device)

    with torch.no_grad():
        outputs = model(**encoded)

    pooled = _mean_pool(outputs.last_hidden_state, encoded["attention_mask"])
    normalised = pooled / pooled.norm(dim=-1, keepdim=True)
    return normalised.cpu().numpy().astype("float32")


def _connect_milvus(host: str, port: int, collection_name: str) -> Collection:
    connections.connect(alias="default", host=host, port=port)
    col = Collection(collection_name)
    col.load()
    return col


def _search_top1(collection: Collection, embedding: list[float]) -> Optional[int]:
    try:
        results = collection.search(
            data=[embedding],
            anns_field="embedding",
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=1,
            output_fields=["recipe_id"],
        )
        if results and results[0]:
            return int(results[0][0].entity.get("recipe_id"))
    except Exception as exc:
        print(f"  [recipeTextModel] Milvus search error: {exc}")
    return None


class RecipeMilvusClient:

    def __init__(
        self,
        milvus_host: str       = "milvus",
        milvus_port: int       = 19530,
        collection_name: str   = "recipe_texts",
        device: str            = "cpu",
    ) -> None:
        self.device          = device
        self.milvus_host     = milvus_host
        self.milvus_port     = milvus_port
        self.collection_name = collection_name

        self._tokenizer:  Optional[AutoTokenizer] = None
        self._model:      Optional[AutoModel]     = None
        self._collection: Optional[Collection]    = None

    def _ensure_model(self) -> None:
        if self._tokenizer is None:
            self._tokenizer, self._model = _load_text_model(self.device)

    def _ensure_milvus(self) -> None:
        if self._collection is None:
            self._collection = _connect_milvus(
                self.milvus_host, self.milvus_port, self.collection_name
            )

    def query(self, clean_queries: list[str]) -> list[dict]:
        """Embed incoming queries and search for the most similar recipe."""
        if not clean_queries:
            return []

        self._ensure_model()
        self._ensure_milvus()

        print(f"  [recipeTextModel] Embedding {len(clean_queries)} user text queries…")
        embeddings = _embed(clean_queries, self._tokenizer, self._model, self.device)

        results: list[dict] = []
        for query_text, emb in zip(clean_queries, embeddings):
            recipe_id = _search_top1(self._collection, emb.tolist())
            if recipe_id is None:
                print(f"  [recipeTextModel] No match found for search input — skipping.")
                continue
            results.append({"search_input": query_text, "matched_recipe_id": recipe_id})

        print(f"  [recipeTextModel] {len(results)}/{len(clean_queries)} matches found.")
        return results