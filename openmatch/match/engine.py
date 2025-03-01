from typing import Dict, List, Optional, Any, Tuple, Set, Union
import numpy as np
from functools import lru_cache
from sentence_transformers import SentenceTransformer
import faiss
import psutil
import gc
import xxhash
import time
import math
from dataclasses import dataclass
from collections import defaultdict
from .config import MatchConfig, MatchType
from .settings import DatabaseConfig, VectorBackend
from .rules import MatchRule
from datetime import datetime
from tqdm import tqdm
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn
from rich.console import Console
from rich.live import Live
from rich.table import Table

class MemoryError(Exception):
    """Raised when memory usage exceeds threshold."""
    pass

@dataclass
class LSHVector:
    """Locality-Sensitive Hashing vector for rapid approximate matching."""
    signature: bytes
    record_idx: int
    confidence: float = 1.0
    
    def __hash__(self):
        return hash(self.signature)
    
    def __eq__(self, other):
        return isinstance(other, LSHVector) and self.signature == other.signature

class MatchEngine:
    """Core matching engine implementation with optimizations for large datasets."""
    
    def __init__(self, config: MatchConfig, db_config: DatabaseConfig):
        self.config = config
        self.db_config = db_config
        self.rules = [MatchRule(rule_config) for rule_config in config.rules]
        self.memory_threshold = 0.90  # 90% memory usage threshold
        self.embedding_batch_size = 128  # Increased batch size
        self.blocking_cache = {}  # Cache for blocking key values
        self.lsh_tables = {}  # LSH tables for approximate matching
        self.hash_functions = 8  # Number of hash functions for LSH
        self.performance_stats = defaultdict(float)  # Performance monitoring
        self.last_memory_check = time.time()  # Initialize memory check time
        self.memory_check_interval = 5  # Check memory every 5 seconds
        self.embedding_model = None  # Initialize as None
        self.model_id = f"{config.blocking.embedding_model}_{int(time.time())}"  # Unique model ID
        
        # Initialize components
        self._initialize_embedding_model()
    
    def _check_memory(self, force=False):
        """Check if memory usage is below threshold, with rate limiting."""
        current_time = time.time()
        if not force and current_time - self.last_memory_check < self.memory_check_interval:
            return
            
        self.last_memory_check = current_time
        memory = psutil.virtual_memory()
        memory_percent = memory.percent / 100.0  # Convert to decimal
        
        if memory_percent > self.memory_threshold:
            gc.collect()  # Try to free memory
            memory = psutil.virtual_memory()
            memory_percent = memory.percent / 100.0
            if memory_percent > self.memory_threshold:
                raise MemoryError(f"Memory usage ({memory_percent*100:.1f}%) exceeds threshold ({self.memory_threshold*100}%)")
        
    def _initialize_embedding_model(self):
        """Initialize the SentenceTransformer model for embeddings."""
        try:
            # Force CPU on macOS to avoid MPS memory issues
            import platform
            device = "cpu" if platform.system() == "Darwin" else ("cuda" if self.config.use_gpu else "cpu")
            
            try:
                self.embedding_model = SentenceTransformer(
                    self.config.blocking.embedding_model,
                    device=device
                )
            except Exception as e:
                print(f"Warning: Failed to load primary model: {str(e)}")
                print("Falling back to smaller model...")
                self.embedding_model = SentenceTransformer(
                    "sentence-transformers/all-MiniLM-L6-v2",
                    device="cpu"  # Always use CPU for fallback model
                )
            
            # Verify model loaded correctly
            if self.embedding_model is None:
                raise ValueError("Failed to initialize embedding model")
                
            # Precompute and cache embeddings for common values
            self._precompute_common_embeddings()
        except Exception as e:
            print(f"Critical: Could not initialize any embedding model: {str(e)}")
            self.embedding_model = None  # Ensure it's None if initialization failed
    
    def _precompute_common_embeddings(self):
        """Precompute and cache embeddings for common values."""
        # Common values that might appear in records
        common_values = [
            "unknown", "n/a", "na", "none", "null", 
            "male", "female", "m", "f",
            "active", "inactive", "pending",
            "usa", "united states", "canada", "uk", "australia"
        ]
        
        # Precompute embeddings
        for value in common_values:
            _ = self.compute_embedding(value)
    
    @lru_cache(maxsize=10000)  # Increased cache size
    def compute_embedding(self, text: str) -> np.ndarray:
        """Compute embedding for a text string with caching."""
        self._check_memory()
        try:
            if not text or text.lower() in ['null', 'none', 'na', 'n/a', '']:
                # Return zero vector for empty or null values
                return np.zeros(self.embedding_model.get_sentence_embedding_dimension())
                
            return self.embedding_model.encode([text])[0]
        except Exception as e:
            print(f"Warning: Failed to compute embedding: {str(e)}")
            # Return zero vector as fallback
            return np.zeros(self.embedding_model.get_sentence_embedding_dimension())
    
    def compute_embeddings_batch(self, texts: List[str]) -> np.ndarray:
        """Compute embeddings for multiple texts in batches with optimized memory usage."""
        self._check_memory()
        
        # Filter out null values
        valid_texts = []
        valid_indices = []
        
        for i, text in enumerate(texts):
            if text and text.lower() not in ['null', 'none', 'na', 'n/a', '']:
                valid_texts.append(text)
                valid_indices.append(i)
        
        if not valid_texts:
            # Return zero vectors if no valid texts
            return np.zeros((len(texts), self.embedding_model.get_sentence_embedding_dimension()))
        
        # Process in smaller batches to manage memory
        all_embeddings = np.zeros((len(texts), self.embedding_model.get_sentence_embedding_dimension()))
        
        for i in range(0, len(valid_texts), self.embedding_batch_size):
            batch_texts = valid_texts[i:i + self.embedding_batch_size]
            batch_indices = valid_indices[i:i + self.embedding_batch_size]
            
            try:
                batch_embeddings = self.embedding_model.encode(batch_texts)
                
                # Place embeddings in the correct positions
                for j, idx in enumerate(batch_indices):
                    all_embeddings[idx] = batch_embeddings[j]
                    
                self._check_memory(force=False)  # Check memory periodically, not after each batch
            except Exception as e:
                print(f"Warning: Failed to compute batch embeddings: {str(e)}")
        
        return all_embeddings
    
    def compute_blocking_tensor(self, record: Dict[str, Any]) -> np.ndarray:
        """Compute blocking tensor for a record with improved memory efficiency."""
        self._check_memory(force=False)
        
        # Check for cached tensor
        record_id = record.get('id') or hash(frozenset(record.items()))
        cache_key = f"tensor_{record_id}"
        if cache_key in self.blocking_cache:
            return self.blocking_cache[cache_key]
        
        # Extract texts for all fuzzy fields in one go
        field_texts = []
        field_names = []
        
        for rule in self.rules:
            for field in rule.config.fields:
                if field.match_type == MatchType.FUZZY and field.name in record:
                    field_texts.append(str(record[field.name]))
                    field_names.append(field.name)
        
        if not field_texts:
            raise ValueError("No fuzzy fields found for blocking tensor computation")
        
        # Compute embeddings in a single batch
        embeddings = self.compute_embeddings_batch(field_texts)
        
        # Concatenate and normalize
        tensor = np.concatenate(embeddings)
        normalized_tensor = tensor / (np.linalg.norm(tensor) + 1e-8)
        
        # Cache the tensor
        if len(self.blocking_cache) < 100000:  # Limit cache size
            self.blocking_cache[cache_key] = normalized_tensor
            
        return normalized_tensor
    
    def compute_lsh_signature(self, record: Dict[str, Any], field_name: str) -> bytes:
        """Compute LSH signature for a field value."""
        if field_name not in record:
            return b''
            
        value = str(record[field_name]).lower()
        if not value or value in ['null', 'none', 'na', 'n/a']:
            return b''
            
        # Generate multiple hashes for robustness
        signature_parts = []
        for i in range(self.hash_functions):
            # Use different seed for each hash function
            h = xxhash.xxh64(value, seed=i).digest()
            signature_parts.append(h)
            
        return b''.join(signature_parts)
    
    def add_to_lsh_tables(self, record: Dict[str, Any], idx: int):
        """Add a record to LSH tables for approximate matching."""
        for rule in self.rules:
            for field in rule.config.fields:
                if field.match_type in [MatchType.FUZZY, MatchType.EXACT]:
                    signature = self.compute_lsh_signature(record, field.name)
                    if not signature:
                        continue
                        
                    field_table = self.lsh_tables.setdefault(field.name, {})
                    vectors = field_table.setdefault(signature, set())
                    vectors.add(LSHVector(signature=signature, record_idx=idx))
    
    def find_lsh_candidates(self, record: Dict[str, Any]) -> Set[int]:
        """Find candidate matches using LSH tables."""
        candidates = set()
        
        for rule in self.rules:
            rule_candidates = set()
            
            for field in rule.config.fields:
                if field.match_type in [MatchType.FUZZY, MatchType.EXACT]:
                    signature = self.compute_lsh_signature(record, field.name)
                    if not signature:
                        continue
                        
                    field_table = self.lsh_tables.get(field.name, {})
                    vectors = field_table.get(signature, set())
                    
                    # Collect candidates from this field
                    field_candidates = {vector.record_idx for vector in vectors}
                    
                    if not rule_candidates:
                        rule_candidates = field_candidates
                    else:
                        # Require matching on all fields in the rule (AND logic)
                        rule_candidates &= field_candidates
            
            # Combine candidates from different rules (OR logic)
            candidates |= rule_candidates
        
        return candidates
    
    def get_blocking_key(self, record: Dict[str, Any]) -> str:
        """Generate adaptive blocking key from configured blocking fields."""
        # Check if key is already in cache
        record_id = record.get('id') or hash(frozenset(record.items()))
        cache_key = f"block_{record_id}"
        if cache_key in self.blocking_cache:
            return self.blocking_cache[cache_key]
        
        key_parts = []
        for field in self.config.blocking.blocking_keys:
            value = record.get(field, "")
            
            # Handle different field types appropriately
            if isinstance(value, (int, float)):
                # For numeric fields, create range-based blocks
                # Use exponential binning for more even distribution
                if value == 0:
                    binned_value = "0"
                else:
                    magnitude = math.floor(math.log10(abs(value) + 1))
                    bin_size = 10 ** magnitude
                    binned_value = str(int(value // bin_size) * bin_size)
                key_parts.append(binned_value)
            elif isinstance(value, str):
                if not value or value.lower() in ['null', 'none', 'na', 'n/a']:
                    key_parts.append("missing")
                else:
                    # Use more sophisticated tokenization for strings
                    # 1. Convert to lowercase
                    value = value.lower()
                    # 2. Keep only the first token or first 3 characters
                    if ' ' in value:
                        value = value.split()[0]  # First token
                    value = value[:3]  # First 3 chars
                    key_parts.append(value)
            else:
                key_parts.append("unknown")
        
        # Create the blocking key
        blocking_key = "|".join(key_parts)
        
        # Cache the result if cache isn't too large
        if len(self.blocking_cache) < 100000:  # Limit cache size
            self.blocking_cache[cache_key] = blocking_key
            
        return blocking_key
    
    def add_record(self, record: Dict[str, Any], record_id: Any = None) -> None:
        """Add a record to the matching engine with optimizations for large datasets."""
        self._check_memory()
        
        # Add to LSH tables
        if record_id is not None:
            self.add_to_lsh_tables(record, record_id)
        
        # Add to vector index
        try:
            blocking_tensor = self.compute_blocking_tensor(record)
            self.index.add(np.array([blocking_tensor]))
        except Exception as e:
            print(f"Warning: Failed to add record to vector index: {str(e)}")
    
    def add_records_batch(self, records: List[Dict[str, Any]]) -> None:
        """Add multiple records in batch for better performance."""
        self._check_memory()
        
        # Extract all tensors first
        tensors = []
        for i, record in enumerate(records):
            try:
                # Add to LSH tables
                self.add_to_lsh_tables(record, i)
                
                # Compute tensor
                tensor = self.compute_blocking_tensor(record)
                tensors.append(tensor)
            except Exception as e:
                print(f"Warning: Failed to process record {i}: {str(e)}")
        
        if tensors:
            # Convert to numpy array
            tensors_array = np.vstack(tensors)
            
            # Train index if not trained yet
            if not self.index_trained and hasattr(self.index, 'train'):
                self.train_index(tensors_array)
            
            # Add to index
            self.index.add(tensors_array)
    
    def find_candidates(
        self, 
        query_record: Dict[str, Any], 
        k: int = 100,
        use_lsh: bool = True
    ) -> List[Tuple[int, float]]:
        """Find candidate matches using vector similarity search in the database."""
        self._check_memory()
        
        candidates = set()
        
        # 1. Use LSH for approximate matching (very fast)
        if use_lsh:
            lsh_candidates = self.find_lsh_candidates(query_record)
            candidates.update(lsh_candidates)
        
        # 2. Use vector search if we don't have enough candidates or LSH is disabled
        if len(candidates) < k or not use_lsh:
            try:
                query_tensor = self.compute_blocking_tensor(query_record)
                
                # Use database vector search
                if self.db_config.vector_backend == VectorBackend.PGVECTOR:
                    # Convert numpy array to list for database
                    query_vector = query_tensor.tolist()
                    
                    # Use the database's vector search function
                    results = self.session.execute(text(f"""
                        SELECT * FROM {self.db_config.schema}.find_similar_records(
                            :query_vector::vector,
                            :threshold,
                            :max_results
                        )
                    """), {
                        'query_vector': query_vector,
                        'threshold': 0.7,  # Minimum similarity threshold
                        'max_results': k
                    })
                    
                    # Add vector search results to candidates
                    for row in results:
                        candidates.add((int(row.record_id), float(row.similarity)))
                        
            except Exception as e:
                print(f"Warning: Vector search failed: {str(e)}")
        
        # Convert candidates to the expected format
        valid_results = list(candidates)
        if not all(isinstance(c, tuple) for c in valid_results):
            # Convert any non-tuple candidates to tuples with default distance
            valid_results = [(c, 1.0) if not isinstance(c, tuple) else c for c in valid_results]
            
        # Sort by similarity (higher is better)
        valid_results.sort(key=lambda x: x[1], reverse=True)
        
        # Limit to k results
        return valid_results[:k]
    
    def match_records(
        self,
        record1: Dict[str, Any],
        record2: Dict[str, Any],
        fast_mode: bool = False
    ) -> Tuple[MatchType, float, Optional[str]]:
        """Match two records and return match type, score, and the rule ID that caused the match."""
        try:
            # Check memory usage periodically
            self._check_memory()
            
            # Try each rule in order
            for rule in self.rules:
                match_type, score = rule.apply(record1, record2, fast_mode)
                if match_type != MatchType.NO_MATCH:
                    return match_type, score, rule.config.rule_id
            
            # No rules matched
            return MatchType.NO_MATCH, 0.0, None
            
        except Exception as e:
            print(f"Warning: Match operation failed: {str(e)}")
            return MatchType.ERROR, 0.0, None
    
    def process_batch(
        self, 
        records: List[Dict[str, Any]], 
        batch_size: Optional[int] = None,
        use_progressive_blocking: bool = True
    ) -> List[Tuple[int, int, MatchType, float]]:
        """Process a batch of records using optimized blocking and progressive matching."""
        self._check_memory()
        batch_size = batch_size or self.config.batch_size
        matches = []
        start_time = time.time()
        
        # Initialize statistics
        stats = {
            'total_records': len(records),
            'exact_matches': 0,
            'potential_matches': 0,
            'processed_pairs': 0
        }
        
        console = Console()
        
        # Create progress display
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            
            main_task = progress.add_task(f"[cyan]Processing {len(records)} records...", total=100)
            
            # Index all records for vector search
            if len(records) > 1000:  # Only build index for larger batches
                index_task = progress.add_task("[yellow]Indexing records...", total=len(records))
                self.add_records_batch(records)
                progress.update(index_task, completed=len(records))
            
            if use_progressive_blocking:
                # Progressive blocking approach
                # 1. First do exact blocking on key fields (fastest)
                exact_blocks = defaultdict(list)
                blocking_task = progress.add_task("[green]Building blocking groups...", total=len(records))
                
                for idx, record in enumerate(records):
                    for field in self.config.blocking.blocking_keys:
                        if field in record and record[field]:
                            # Create exact match key
                            key = f"{field}:{record[field]}"
                            exact_blocks[key].append((idx, record))
                    progress.update(blocking_task, advance=1)
                
                # Process exact blocks (these are guaranteed matches on at least one field)
                total_comparisons = sum(len(block) * (len(block) - 1) // 2 for block in exact_blocks.values())
                exact_task = progress.add_task("[blue]Processing exact matches...", total=total_comparisons)
                
                for key, block_records in exact_blocks.items():
                    if len(block_records) > 1:  # Only process blocks with potential matches
                        for i, (idx1, record1) in enumerate(block_records):
                            for idx2, record2 in block_records[i+1:]:
                                match_type, confidence, rule_id = self.match_records(record1, record2)
                                if match_type != MatchType.NO_MATCH:
                                    matches.append((
                                        idx1,
                                        idx2,
                                        match_type,
                                        float(confidence)
                                    ))
                                    if match_type == MatchType.EXACT:
                                        stats['exact_matches'] += 1
                                    else:
                                        stats['potential_matches'] += 1
                                stats['processed_pairs'] += 1
                                progress.update(exact_task, advance=1)
                                progress.update(main_task, completed=min(100, int((stats['processed_pairs'] / total_comparisons) * 100)))
                
                # 2. Then do approximate blocking with LSH for fuzzy matches
                if not matches or len(matches) < (len(records) * 0.01):  # If very few exact matches
                    approx_task = progress.add_task("[magenta]Processing approximate matches...", total=len(records))
                    # Group records by LSH signature
                    for idx, record in enumerate(records):
                        if not any(idx == match[0] or idx == match[1] for match in matches):
                            # Only process records that aren't matched yet
                            candidates = self.find_candidates(record, k=min(50, len(records)))
                            
                            for candidate_idx, _ in candidates:
                                if (candidate_idx < len(records) and candidate_idx != idx and 
                                    not any((idx == match[0] and candidate_idx == match[1]) or 
                                           (idx == match[1] and candidate_idx == match[0]) 
                                           for match in matches)):
                                    
                                    record2 = records[candidate_idx]
                                    match_type, confidence, rule_id = self.match_records(record, record2)
                                    if match_type != MatchType.NO_MATCH:
                                        matches.append((
                                            idx,
                                            candidate_idx,
                                            match_type,
                                            float(confidence)
                                        ))
                                        if match_type == MatchType.EXACT:
                                            stats['exact_matches'] += 1
                                        else:
                                            stats['potential_matches'] += 1
                                    stats['processed_pairs'] += 1
                        progress.update(approx_task, advance=1)
                        progress.update(main_task, completed=min(100, int((idx / len(records)) * 100)))

            # Print final statistics
            console.print("\n[bold]Match Statistics:[/bold]")
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Metric", style="dim")
            table.add_column("Value", justify="right")
            table.add_row("Total Records", str(stats['total_records']))
            table.add_row("Processed Pairs", str(stats['processed_pairs']))
            table.add_row("Exact Matches", str(stats['exact_matches']))
            table.add_row("Potential Matches", str(stats['potential_matches']))
            table.add_row("Processing Time", f"{time.time() - start_time:.2f}s")
            console.print(table)

        return matches
        
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        stats = dict(self.performance_stats)
        stats['avg_match_time_ms'] = (stats.get('match_time', 0) / max(1, stats.get('matches_processed', 1))) * 1000
        return stats

    def _process_record_batch(
        self,
        records: List[Tuple[int, int, Dict, Dict, str, str, str]]
    ) -> List[Dict[str, Any]]:
        """Process a batch of records and return match results with model and rule IDs."""
        matches = []
        
        for record_id1, record_id2, data1, data2, system1, system2, block_key in records:
            try:
                match_type, score, rule_id = self.match_records(data1, data2)
                
                if match_type != MatchType.NO_MATCH and score >= self.config.min_match_score:
                    matches.append({
                        'record_id_1': record_id1,
                        'record_id_2': record_id2,
                        'match_score': float(score),
                        'match_type': match_type.value,
                        'match_model_id': self.model_id,
                        'match_rule_id': rule_id,
                        'match_details': {
                            'block_key': block_key,
                            'source_systems': [system1, system2],
                            'timestamp': datetime.utcnow().isoformat()
                        }
                    })
                    
            except Exception as e:
                print(f"Warning: Failed to process record pair ({record_id1}, {record_id2}): {str(e)}")
                continue
                
        return matches
