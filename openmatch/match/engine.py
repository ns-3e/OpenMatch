"""
Match engine implementation for OpenMatch.
"""

from typing import Dict, List, Optional, Any, Tuple, Set
from concurrent.futures import ThreadPoolExecutor
import logging
from functools import lru_cache
import pandas as pd
from collections import defaultdict
import numpy as np

from openmatch.match.config import MatchConfig, BlockingConfig
from openmatch.match.rules import MatchRules
from openmatch.utils.logging import setup_logging


class MatchEngine:
    """Main engine for record matching."""
    
    def __init__(
        self,
        config: MatchConfig,
        logger: Optional[logging.Logger] = None
    ):
        """Initialize the match engine.
        
        Args:
            config: Match configuration
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        
        # Validate configuration
        errors = config.validate()
        if errors:
            raise ValueError(f"Invalid configuration: {', '.join(errors)}")
            
        # Initialize caching if enabled
        if config.enable_caching:
            self.match_field = lru_cache(maxsize=config.cache_size)(self._match_field)
        else:
            self.match_field = self._match_field

    def match_records(
        self,
        record1: Dict[str, Any],
        record2: Dict[str, Any]
    ) -> Tuple[float, Dict[str, float]]:
        """Match two records and return overall score and field scores.
        
        Args:
            record1: First record to compare
            record2: Second record to compare
            
        Returns:
            Tuple of (overall_score, field_scores)
        """
        field_scores = {}
        total_weight = self.config.get_total_weight()
        
        for field_name, field_config in self.config.field_configs.items():
            value1 = record1.get(field_name)
            value2 = record2.get(field_name)
            
            score = self.match_field(value1, value2, field_config)
            field_scores[field_name] = score
            
        # Calculate overall score based on aggregation method
        if self.config.score_aggregation == "weighted_average":
            if total_weight == 0:
                overall_score = 0.0
            else:
                overall_score = sum(
                    score * self.config.get_field_weight(field)
                    for field, score in field_scores.items()
                ) / total_weight
        elif self.config.score_aggregation == "min":
            overall_score = min(field_scores.values())
        elif self.config.score_aggregation == "max":
            overall_score = max(field_scores.values())
        else:
            raise ValueError(
                f"Unsupported score aggregation method: {self.config.score_aggregation}"
            )
            
        return overall_score, field_scores

    def find_matches(
        self,
        records: List[Dict[str, Any]],
        comparison_records: Optional[List[Dict[str, Any]]] = None
    ) -> List[Tuple[Dict[str, Any], Dict[str, Any], float, Dict[str, float]]]:
        """Find matches between records.
        
        Args:
            records: Records to match
            comparison_records: Optional separate set of records to compare against
                             If not provided, matches within records
                             
        Returns:
            List of (record1, record2, overall_score, field_scores) tuples
        """
        if comparison_records is None:
            comparison_records = records
            
        # Apply blocking if configured
        if self.config.blocking:
            record_blocks = self._apply_blocking(records, comparison_records)
        else:
            # No blocking - compare all records
            record_blocks = [(records, comparison_records)]
            
        matches = []
        
        # Process each block
        for block_records, block_comparisons in record_blocks:
            block_matches = self._process_block(block_records, block_comparisons)
            matches.extend(block_matches)
            
        # Sort matches by score descending
        matches.sort(key=lambda x: x[2], reverse=True)
        
        return matches

    def _match_field(
        self,
        value1: Any,
        value2: Any,
        field_config: Any
    ) -> float:
        """Match two field values according to configuration."""
        return MatchRules.match_field(value1, value2, field_config)

    def _apply_blocking(
        self,
        records: List[Dict[str, Any]],
        comparison_records: List[Dict[str, Any]]
    ) -> List[Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]]:
        """Apply blocking to reduce comparison space."""
        if not self.config.blocking:
            return [(records, comparison_records)]
            
        blocks = defaultdict(lambda: ([], []))
        
        # Helper function to get block key
        def get_block_key(record: Dict[str, Any]) -> str:
            if self.config.blocking.method == "standard":
                # Concatenate blocking key values
                return "|".join(
                    str(record.get(key, ""))
                    for key in self.config.blocking.blocking_keys
                )
            elif self.config.blocking.method == "sorted_neighborhood":
                # Use first blocking key for sorting
                return str(record.get(self.config.blocking.blocking_keys[0], ""))
            else:
                raise ValueError(
                    f"Unsupported blocking method: {self.config.blocking.method}"
                )
                
        # Assign records to blocks
        for record in records:
            block_key = get_block_key(record)
            blocks[block_key][0].append(record)
            
        for record in comparison_records:
            block_key = get_block_key(record)
            blocks[block_key][1].append(record)
            
        # Handle sorted neighborhood method
        if (self.config.blocking.method == "sorted_neighborhood" and
            self.config.blocking.parameters.get("window_size", 0) > 0):
            window_size = self.config.blocking.parameters["window_size"]
            sorted_keys = sorted(blocks.keys())
            expanded_blocks = defaultdict(lambda: ([], []))
            
            for i, key in enumerate(sorted_keys):
                # Add records from window
                start_idx = max(0, i - window_size)
                end_idx = min(len(sorted_keys), i + window_size + 1)
                
                for j in range(start_idx, end_idx):
                    window_key = sorted_keys[j]
                    expanded_blocks[key][0].extend(blocks[window_key][0])
                    expanded_blocks[key][1].extend(blocks[window_key][1])
                    
            blocks = expanded_blocks
            
        return [
            (block_records, block_comparisons)
            for block_records, block_comparisons in blocks.values()
            if block_records and block_comparisons
        ]

    def _process_block(
        self,
        records: List[Dict[str, Any]],
        comparison_records: List[Dict[str, Any]]
    ) -> List[Tuple[Dict[str, Any], Dict[str, Any], float, Dict[str, float]]]:
        """Process a block of records to find matches."""
        matches = []
        
        # Use parallel processing if enabled
        if self.config.parallel_processing:
            with ThreadPoolExecutor(
                max_workers=self.config.num_workers
            ) as executor:
                # Create comparison pairs
                pairs = [
                    (r1, r2)
                    for r1 in records
                    for r2 in comparison_records
                    if r1 is not r2  # Avoid self-matches
                ]
                
                # Process pairs in parallel
                future_to_pair = {
                    executor.submit(self.match_records, r1, r2): (r1, r2)
                    for r1, r2 in pairs
                }
                
                for future in future_to_pair:
                    try:
                        overall_score, field_scores = future.result()
                        if overall_score >= self.config.min_overall_score:
                            r1, r2 = future_to_pair[future]
                            matches.append((r1, r2, overall_score, field_scores))
                    except Exception as e:
                        self.logger.error(f"Error processing pair: {e}")
        else:
            # Sequential processing
            for r1 in records:
                for r2 in comparison_records:
                    if r1 is r2:  # Skip self-matches
                        continue
                        
                    try:
                        overall_score, field_scores = self.match_records(r1, r2)
                        if overall_score >= self.config.min_overall_score:
                            matches.append((r1, r2, overall_score, field_scores))
                    except Exception as e:
                        self.logger.error(f"Error matching records: {e}")
                        
        return matches

    def match_dataframe(
        self,
        df1: pd.DataFrame,
        df2: Optional[pd.DataFrame] = None,
        id_field: str = "id"
    ) -> pd.DataFrame:
        """Match records in pandas DataFrames.
        
        Args:
            df1: First DataFrame
            df2: Optional second DataFrame (if None, matches within df1)
            id_field: Field containing record ID
            
        Returns:
            DataFrame with match results
        """
        # Convert DataFrames to records
        records1 = df1.to_dict("records")
        records2 = df2.to_dict("records") if df2 is not None else None
        
        # Find matches
        matches = self.find_matches(records1, records2)
        
        # Convert results to DataFrame
        results = []
        for r1, r2, score, field_scores in matches:
            result = {
                "id1": r1[id_field],
                "id2": r2[id_field],
                "overall_score": score
            }
            result.update({
                f"{field}_score": field_score
                for field, field_score in field_scores.items()
            })
            results.append(result)
            
        return pd.DataFrame(results)
