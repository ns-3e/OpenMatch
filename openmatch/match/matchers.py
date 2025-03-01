from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer
import numpy as np
from functools import lru_cache

class EmbeddingMatcher:
    """Matcher using sentence embeddings for semantic similarity."""
    
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        import torch
        # Force CPU usage to avoid memory issues
        device = "cpu"
        torch.set_num_threads(1)  # Limit CPU threads
        self.model = SentenceTransformer(model_name, device=device)
        
    @lru_cache(maxsize=1000)
    def compute_embedding(self, text: str) -> np.ndarray:
        """Compute embedding for a single text with caching."""
        return self.model.encode([text])[0]
        
    def compute_embeddings(self, texts: List[str]) -> np.ndarray:
        """Compute embeddings for multiple texts."""
        return self.model.encode(texts)
        
    def compute_similarity(self, text1: str, text2: str) -> float:
        """Compute similarity between two texts."""
        emb1 = self.compute_embedding(text1)
        emb2 = self.compute_embedding(text2)
        return float(np.dot(emb1, emb2) / (np.linalg.norm(emb1) * np.linalg.norm(emb2)))

class MatcherFactory:
    """Factory for creating appropriate matchers based on match type."""
    
    @staticmethod
    def create_matcher(match_type: str, **kwargs) -> Any:
        if match_type == "EMBEDDING":
            return EmbeddingMatcher(
                model_name=kwargs.get('embedding_model', "Salesforce/SFR-Embedding-2_R")
            )
        elif match_type == "FUZZY":
            from .rules import FuzzyMatcher
            return FuzzyMatcher()
        else:
            return None
