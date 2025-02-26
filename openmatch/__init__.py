"""
OpenMatch - Enterprise-Grade Master Data Management Library
"""

from typing import Dict, List, Optional, Union, Any
import logging
from dataclasses import dataclass

from openmatch.match import MatchEngine, MatchConfig
from openmatch.merge import MergeProcessor, MergeStrategy
from openmatch.trust import TrustFramework, TrustConfig
from openmatch.lineage import LineageTracker
from openmatch.utils.logging import setup_logging
from openmatch.config import (
    SurvivorshipRules,
    DataModelConfig,
    ValidationRules,
    PhysicalModelConfig
)

__version__ = "0.1.0"

@dataclass
class MDMResults:
    """Results from MDM processing."""
    total_records: int
    match_groups_count: int
    avg_group_size: float
    golden_records: List[Dict[str, Any]]
    review_required: List[Dict[str, Any]]
    source_counts: Dict[str, int]
    quality_metrics: Dict[str, float]
    match_scores: List[float]
    source_records: Dict[str, List[Dict[str, Any]]]

class MDMPipeline:
    """Main pipeline for MDM processing."""
    
    def __init__(
        self,
        data_model: DataModelConfig,
        trust_config: TrustConfig,
        survivorship_rules: SurvivorshipRules,
        match_config: MatchConfig,
        db_config: Dict[str, str],
        logger: Optional[logging.Logger] = None
    ):
        """Initialize the MDM pipeline.
        
        Args:
            data_model: Data model configuration
            trust_config: Trust framework configuration
            survivorship_rules: Survivorship rules
            match_config: Match configuration
            db_config: Database configuration
            logger: Optional logger instance
        """
        self.data_model = data_model
        self.trust_config = trust_config
        self.survivorship_rules = survivorship_rules
        self.match_config = match_config
        self.db_config = db_config
        self.logger = logger or logging.getLogger(__name__)
        
        # Initialize components
        self.match_engine = MatchEngine(match_config)
        self.merge_processor = MergeProcessor()
        self.trust_framework = TrustFramework(trust_config)
        self.lineage = LineageTracker()
        
    def process_records(
        self,
        records: List[Dict[str, Any]]
    ) -> MDMResults:
        """Process records through the MDM pipeline.
        
        Args:
            records: List of records to process
            
        Returns:
            MDM processing results
        """
        # Calculate trust scores
        trust_scores = {
            record["id"]: self.trust_framework.calculate_trust_scores(record)
            for record in records
        }
        
        # Find matches
        matches = self.match_engine.find_matches(records)
        
        # Merge matched records
        golden_records = self.merge_processor.merge_matches(matches, trust_scores)
        
        # Track lineage
        for golden_record in golden_records:
            self.lineage.track_merge(
                source_records=self.merge_processor.get_source_records(golden_record["id"]),
                target_id=golden_record["id"],
                user_id="system",
                confidence_score=1.0,
                details={}
            )
        
        # Calculate metrics
        source_counts = {}
        for record in records:
            source = record.get("source", "unknown")
            source_counts[source] = source_counts.get(source, 0) + 1
        
        # Prepare results
        results = MDMResults(
            total_records=len(records),
            match_groups_count=len(golden_records),
            avg_group_size=len(records) / len(golden_records) if golden_records else 0,
            golden_records=golden_records,
            review_required=[],  # TODO: Implement review logic
            source_counts=source_counts,
            quality_metrics={},  # TODO: Implement quality metrics
            match_scores=[score for _, _, score in matches],
            source_records={
                golden_id: list(self.merge_processor.get_source_records(golden_id))
                for golden_id in [r["id"] for r in golden_records]
            }
        )
        
        return results
    
    def get_golden_record(
        self,
        record_id: Union[str, int],
        include_lineage: bool = True
    ) -> Optional[Dict]:
        """Retrieve a golden record by ID.
        
        Args:
            record_id: ID of the golden record to retrieve
            include_lineage: Whether to include lineage information
            
        Returns:
            Golden record dictionary if found, None otherwise
        """
        golden_record = self.merge_processor.get_golden_record(record_id)
        
        if golden_record and include_lineage and self.lineage:
            golden_record["lineage"] = self.lineage.get_record_history(
                record_id
            )
            
        return golden_record
