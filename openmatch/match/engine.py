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
import uuid
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from openmatch.match.config import MatchConfig, BlockingConfig, MetadataConfig
from openmatch.match.rules import MatchRules
from openmatch.utils.logging import setup_logging


class MatchEngine:
    """Main engine for record matching."""
    
    def __init__(
        self,
        config: MatchConfig,
        metadata_config: Optional[MetadataConfig] = None,
        db_engine: Optional[sa.engine.Engine] = None,
        logger: Optional[logging.Logger] = None
    ):
        """Initialize the match engine.
        
        Args:
            config: Match configuration
            metadata_config: Optional metadata table configuration
            db_engine: SQLAlchemy database engine for metadata storage
            logger: Optional logger instance
        """
        self.config = config
        self.metadata_config = metadata_config or MetadataConfig()
        self.db_engine = db_engine
        self.logger = logger or logging.getLogger(__name__)
        self.batch_id = str(uuid.uuid4())
        
        # Validate configuration
        errors = config.validate()
        if errors:
            raise ValueError(f"Invalid configuration: {', '.join(errors)}")
            
        # Initialize caching if enabled
        if config.enable_caching:
            self.match_field = lru_cache(maxsize=config.cache_size)(self._match_field)
        else:
            self.match_field = self._match_field
            
        # Initialize metadata tables if database engine is provided
        if db_engine and metadata_config:
            self._initialize_metadata_tables()

    def _initialize_metadata_tables(self):
        """Initialize metadata tables in the database."""
        try:
            # Execute DDL statements
            with self.db_engine.begin() as conn:
                for ddl in self.metadata_config.get_ddl_statements():
                    conn.execute(sa.text(ddl))
            self.logger.info("Successfully initialized metadata tables")
        except Exception as e:
            self.logger.error(f"Error initializing metadata tables: {e}")
            raise

    def _get_processed_source_ids(self, source_system: str) -> Set[str]:
        """Get set of source IDs that have already been processed."""
        if not self.db_engine:
            return set()
            
        query = f"""
        SELECT source_id FROM {self.metadata_config.get_table_name('xref')}
        WHERE source_system = :source_system
        """
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(
                    sa.text(query),
                    {"source_system": source_system}
                )
                return {row[0] for row in result}
        except Exception as e:
            self.logger.error(f"Error retrieving processed source IDs: {e}")
            return set()

    def _get_candidate_masters(
        self,
        blocking_key: str,
        min_score: float = 0.0
    ) -> List[Dict[str, Any]]:
        """Get candidate master records for matching based on blocking key."""
        if not self.db_engine:
            return []
            
        query = f"""
        SELECT m.master_id, m.golden_record, m.match_score
        FROM {self.metadata_config.get_table_name('master')} m
        JOIN {self.metadata_config.get_table_name('xref')} x
            ON m.master_id = x.master_id
        WHERE x.status = 'ACTIVE'
        AND m.match_score >= :min_score
        """
        
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(
                    sa.text(query),
                    {"min_score": min_score}
                )
                return [dict(row) for row in result]
        except Exception as e:
            self.logger.error(f"Error retrieving candidate masters: {e}")
            return []

    def _store_match_result(
        self,
        source_id: str,
        source_system: str,
        master_id: Optional[str],
        match_score: float,
        field_scores: Dict[str, float],
        match_rule: str,
        source_data: Dict[str, Any]
    ):
        """Store match result in metadata tables."""
        if not self.db_engine:
            return
            
        try:
            with self.db_engine.begin() as conn:
                # Insert match result
                match_id = str(uuid.uuid4())
                conn.execute(
                    sa.text(f"""
                    INSERT INTO {self.metadata_config.get_table_name('results')}
                    (match_id, source_id, master_id, match_score, field_scores,
                     match_rule, batch_id, created_at)
                    VALUES
                    (:match_id, :source_id, :master_id, :match_score, :field_scores,
                     :match_rule, :batch_id, CURRENT_TIMESTAMP)
                    """),
                    {
                        "match_id": match_id,
                        "source_id": source_id,
                        "master_id": master_id,
                        "match_score": match_score,
                        "field_scores": field_scores,
                        "match_rule": match_rule,
                        "batch_id": self.batch_id
                    }
                )
                
                # Insert or update xref record
                conn.execute(
                    sa.text(f"""
                    INSERT INTO {self.metadata_config.get_table_name('xref')}
                    (xref_id, source_id, source_system, master_id, match_score,
                     created_at, last_updated, source_data)
                    VALUES
                    (:xref_id, :source_id, :source_system, :master_id, :match_score,
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :source_data)
                    ON CONFLICT (source_id, source_system)
                    DO UPDATE SET
                        master_id = EXCLUDED.master_id,
                        match_score = EXCLUDED.match_score,
                        last_updated = CURRENT_TIMESTAMP,
                        source_data = EXCLUDED.source_data
                    """),
                    {
                        "xref_id": str(uuid.uuid4()),
                        "source_id": source_id,
                        "source_system": source_system,
                        "master_id": master_id,
                        "match_score": match_score,
                        "source_data": source_data
                    }
                )
                
        except Exception as e:
            self.logger.error(f"Error storing match result: {e}")

    def _update_match_statistics(
        self,
        total_records: int,
        matched_records: int,
        potential_matches: int,
        new_masters: int,
        updated_masters: int,
        start_time: datetime,
        performance_metrics: Dict[str, Any]
    ):
        """Update match statistics for the current batch."""
        if not self.db_engine:
            return
            
        try:
            with self.db_engine.begin() as conn:
                conn.execute(
                    sa.text(f"""
                    INSERT INTO {self.metadata_config.get_table_name('stats')}
                    (stat_id, batch_id, start_time, end_time, total_records,
                     matched_records, potential_matches, new_masters, updated_masters,
                     avg_match_score, performance_metrics)
                    VALUES
                    (:stat_id, :batch_id, :start_time, CURRENT_TIMESTAMP, :total_records,
                     :matched_records, :potential_matches, :new_masters, :updated_masters,
                     :avg_match_score, :performance_metrics)
                    """),
                    {
                        "stat_id": str(uuid.uuid4()),
                        "batch_id": self.batch_id,
                        "start_time": start_time,
                        "total_records": total_records,
                        "matched_records": matched_records,
                        "potential_matches": potential_matches,
                        "new_masters": new_masters,
                        "updated_masters": updated_masters,
                        "avg_match_score": matched_records / total_records if total_records > 0 else 0,
                        "performance_metrics": performance_metrics
                    }
                )
        except Exception as e:
            self.logger.error(f"Error updating match statistics: {e}")

    def find_matches_incremental(
        self,
        records: List[Dict[str, Any]],
        source_system: str,
        batch_size: int = 1000
    ) -> List[Tuple[Dict[str, Any], Dict[str, Any], float, Dict[str, float]]]:
        """Find matches incrementally, only processing new records.
        
        Args:
            records: New records to match
            source_system: Source system identifier
            batch_size: Size of batches for processing
            
        Returns:
            List of (record1, record2, overall_score, field_scores) tuples
        """
        start_time = datetime.now()
        performance_metrics = defaultdict(float)
        
        # Get already processed source IDs
        processed_ids = self._get_processed_source_ids(source_system)
        
        # Filter out already processed records
        new_records = [
            r for r in records
            if str(r.get('id', '')) not in processed_ids
        ]
        
        if not new_records:
            self.logger.info("No new records to process")
            return []
            
        self.logger.info(f"Processing {len(new_records)} new records")
        
        matches = []
        total_matches = 0
        new_masters = 0
        updated_masters = 0
        
        # Process records in batches
        for i in range(0, len(new_records), batch_size):
            batch = new_records[i:i + batch_size]
            batch_start = datetime.now()
            
            # Apply blocking if configured
            if self.config.blocking:
                record_blocks = self._apply_blocking(batch, [])
            else:
                record_blocks = [(batch, [])]
                
            # Process each block
            for block_records, _ in record_blocks:
                for record in block_records:
                    # Get candidate masters based on blocking key
                    blocking_key = self._get_block_key(record)
                    candidates = self._get_candidate_masters(
                        blocking_key,
                        min_score=self.config.min_overall_score
                    )
                    
                    best_match = None
                    best_score = 0
                    best_field_scores = {}
                    
                    # Compare with candidates
                    for candidate in candidates:
                        score, field_scores = self.match_records(
                            record,
                            candidate['golden_record']
                        )
                        
                        if score >= self.config.min_overall_score and score > best_score:
                            best_match = candidate
                            best_score = score
                            best_field_scores = field_scores
                    
                    # Store match result
                    if best_match:
                        matches.append((
                            record,
                            best_match['golden_record'],
                            best_score,
                            best_field_scores
                        ))
                        updated_masters += 1
                    else:
                        # Create new master record
                        new_master_id = str(uuid.uuid4())
                        self._store_match_result(
                            source_id=str(record.get('id', '')),
                            source_system=source_system,
                            master_id=new_master_id,
                            match_score=1.0,  # Perfect score for new master
                            field_scores={},
                            match_rule="NEW_MASTER",
                            source_data=record
                        )
                        new_masters += 1
                    
                    total_matches += 1
            
            # Update performance metrics
            batch_duration = (datetime.now() - batch_start).total_seconds()
            performance_metrics['avg_batch_duration'] += batch_duration
            performance_metrics['records_per_second'] = len(batch) / batch_duration
            
            self.logger.info(
                f"Processed batch {i//batch_size + 1}, "
                f"records {i} to {min(i + batch_size, len(new_records))}"
            )
        
        # Update final statistics
        self._update_match_statistics(
            total_records=len(new_records),
            matched_records=total_matches,
            potential_matches=len(matches),
            new_masters=new_masters,
            updated_masters=updated_masters,
            start_time=start_time,
            performance_metrics=dict(performance_metrics)
        )
        
        return matches

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
