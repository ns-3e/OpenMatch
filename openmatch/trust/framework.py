"""
Trust framework implementation for OpenMatch.
"""

from typing import Dict, List, Any, Optional, Union
import logging
from functools import lru_cache
import pandas as pd

from .config import TrustConfig
from .scoring import TrustScoring
from .rules import TrustRules


class TrustFramework:
    """Main framework for data trust and survivorship."""
    
    def __init__(
        self,
        config: TrustConfig,
        logger: Optional[logging.Logger] = None
    ):
        """Initialize trust framework.
        
        Args:
            config: Trust framework configuration
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        
        # Validate configuration
        errors = config.validate()
        if errors:
            raise ValueError(f"Invalid configuration: {', '.join(errors)}")
            
        # Initialize components
        self.scoring = TrustScoring(config)
        self.rules = TrustRules(config)
        
        # Initialize caching if enabled
        if config.enable_caching:
            self.process_record = lru_cache(maxsize=config.cache_size)(
                self._process_record
            )
        else:
            self.process_record = self._process_record

    def process_records(
        self,
        records: Union[List[Dict[str, Any]], pd.DataFrame],
        source: str
    ) -> List[Dict[str, Any]]:
        """Process records through trust framework.
        
        Args:
            records: List of records or DataFrame to process
            source: Source system name
            
        Returns:
            List of processed records with trust scores
        """
        # Convert DataFrame to records if needed
        if isinstance(records, pd.DataFrame):
            records = records.to_dict("records")
            
        processed = []
        for record in records:
            try:
                processed_record = self.process_record(record, source)
                if processed_record:
                    processed.append(processed_record)
            except Exception as e:
                self.logger.error(f"Error processing record: {e}")
                
        return processed

    def _process_record(
        self,
        record: Dict[str, Any],
        source: str
    ) -> Optional[Dict[str, Any]]:
        """Process a single record through trust framework."""
        # Calculate scores
        scores = self.scoring.calculate_record_scores(record, source)
        
        # Check trust threshold
        if scores["trust_score"] < self.config.min_trust_score:
            return None
            
        # Add scores to record
        record.update({
            "trust_score": scores["trust_score"],
            "quality_score": scores["quality_score"],
            "dimension_scores": scores["dimension_scores"],
            "source": source
        })
        
        return record

    def merge_records(
        self,
        records: List[Dict[str, Any]],
        trust_scores: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """Merge records using survivorship rules.
        
        Args:
            records: List of records to merge
            trust_scores: Optional dict mapping record IDs to trust scores
            
        Returns:
            Merged record with winning values
        """
        return self.rules.apply_survivorship_rules(records, trust_scores)

    def resolve_conflicts(
        self,
        values: List[Any],
        sources: List[str],
        trust_scores: Optional[Dict[str, float]] = None
    ) -> Any:
        """Resolve conflicts between multiple values.
        
        Args:
            values: List of values to resolve
            sources: List of source systems
            trust_scores: Optional dict mapping sources to trust scores
            
        Returns:
            Resolved value
        """
        return self.rules.resolve_conflicts(values, sources, trust_scores)

    def process_dataframe(
        self,
        df: pd.DataFrame,
        source: str,
        inplace: bool = False
    ) -> pd.DataFrame:
        """Process DataFrame through trust framework.
        
        Args:
            df: DataFrame to process
            source: Source system name
            inplace: Whether to modify DataFrame in place
            
        Returns:
            Processed DataFrame with trust scores
        """
        if not inplace:
            df = df.copy()
            
        # Process records
        processed = self.process_records(df, source)
        
        # Convert back to DataFrame
        result = pd.DataFrame(processed)
        
        # Preserve index if possible
        if len(result) == len(df):
            result.index = df.index
            
        return result

    def merge_dataframes(
        self,
        dfs: List[pd.DataFrame],
        sources: List[str],
        id_field: str = "id"
    ) -> pd.DataFrame:
        """Merge multiple DataFrames using survivorship rules.
        
        Args:
            dfs: List of DataFrames to merge
            sources: List of source system names
            id_field: Field containing record ID
            
        Returns:
            Merged DataFrame
        """
        # Process each DataFrame
        processed_dfs = []
        for df, source in zip(dfs, sources):
            processed = self.process_dataframe(df, source)
            processed_dfs.append(processed)
            
        # Combine all records
        all_records = []
        for df in processed_dfs:
            all_records.extend(df.to_dict("records"))
            
        # Group records by ID
        grouped = {}
        for record in all_records:
            record_id = record.get(id_field)
            if record_id:
                if record_id not in grouped:
                    grouped[record_id] = []
                grouped[record_id].append(record)
                
        # Merge each group
        merged = []
        for record_id, group in grouped.items():
            trust_scores = {
                r["source"]: r["trust_score"]
                for r in group
                if "trust_score" in r
            }
            merged_record = self.merge_records(group, trust_scores)
            merged_record[id_field] = record_id
            merged.append(merged_record)
            
        return pd.DataFrame(merged)

    def audit_merge(
        self,
        records: List[Dict[str, Any]],
        merged_record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate audit information for a merge operation.
        
        Args:
            records: Original records that were merged
            merged_record: Result of merge operation
            
        Returns:
            Audit information including winning sources and scores
        """
        audit = {
            "total_records": len(records),
            "sources": list(set(r["source"] for r in records if "source" in r)),
            "field_sources": {},
            "field_scores": {}
        }
        
        # Track winning source and score for each field
        for field in merged_record:
            if field in ["trust_score", "quality_score", "dimension_scores", "source"]:
                continue
                
            merged_value = merged_record[field]
            matching_records = [
                r for r in records
                if r.get(field) == merged_value
            ]
            
            if matching_records:
                winning_record = max(
                    matching_records,
                    key=lambda r: r.get("trust_score", 0.0)
                )
                audit["field_sources"][field] = winning_record.get("source")
                audit["field_scores"][field] = winning_record.get("trust_score", 0.0)
                
        return audit

    def validate_source(self, source: str) -> bool:
        """Check if a source system is configured."""
        return source in self.config.sources

    def get_source_config(self, source: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a source system."""
        source_config = self.config.sources.get(source)
        if source_config:
            return {
                "name": source_config.name,
                "reliability_score": source_config.reliability_score,
                "priority": source_config.priority,
                "update_frequency": source_config.update_frequency,
                "field_mappings": source_config.field_mappings,
                "validation_rules": {
                    field: {
                        "required": rule.required,
                        "data_type": rule.data_type,
                        "min_length": rule.min_length,
                        "max_length": rule.max_length,
                        "pattern": rule.pattern,
                        "allowed_values": rule.allowed_values
                    }
                    for field, rule in source_config.validation_rules.items()
                }
            }
        return None
