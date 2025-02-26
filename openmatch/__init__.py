"""
OpenMatch - Enterprise-Grade Master Data Management Library
"""

from typing import Dict, List, Optional, Union
import logging

from openmatch.match import MatchEngine, MatchConfig
from openmatch.merge import MergeProcessor, MergeStrategy
from openmatch.trust import TrustFramework, TrustConfig
from openmatch.lineage import LineageTracker
from openmatch.utils.logging import setup_logging

__version__ = "0.1.0"

class MDMPipeline:
    """Main pipeline class for MDM processing."""
    
    def __init__(
        self,
        trust_config: Optional[TrustConfig] = None,
        match_config: Optional[MatchConfig] = None,
        merge_strategy: Optional[MergeStrategy] = None,
        enable_lineage: bool = True,
        logger: Optional[logging.Logger] = None
    ):
        """Initialize the MDM pipeline.
        
        Args:
            trust_config: Configuration for trust scoring and survivorship
            match_config: Configuration for match rules and thresholds
            merge_strategy: Strategy for merging matched records
            enable_lineage: Whether to track record lineage
            logger: Optional logger instance
        """
        self.logger = logger or setup_logging(__name__)
        
        # Initialize components
        self.match_engine = MatchEngine(match_config)
        self.merge_processor = MergeProcessor(merge_strategy)
        self.trust_framework = TrustFramework(trust_config)
        self.lineage_tracker = LineageTracker() if enable_lineage else None
        
        self.logger.info("MDM Pipeline initialized")
    
    def process_records(
        self,
        records: List[Dict],
        batch_size: int = 1000,
        return_details: bool = False
    ) -> Dict:
        """Process a batch of records through the MDM pipeline.
        
        Args:
            records: List of record dictionaries to process
            batch_size: Size of batches for processing
            return_details: Whether to return detailed match/merge info
            
        Returns:
            Dictionary containing:
                - golden_records: List of golden records
                - xrefs: Cross-reference mappings
                - lineage: Record lineage info (if enabled)
                - trust_scores: Trust scores for records
                - details: Additional processing details (if return_details=True)
        """
        self.logger.info(f"Processing {len(records)} records")
        
        # Match similar records
        matches = self.match_engine.find_matches(records, batch_size)
        
        # Score records for trust/survivorship
        trust_scores = self.trust_framework.score_records(records)
        
        # Merge matched records
        golden_records = self.merge_processor.merge_matches(
            matches,
            trust_scores=trust_scores
        )
        
        # Track lineage if enabled
        lineage = None
        if self.lineage_tracker:
            lineage = self.lineage_tracker.track_merge(
                source_records=records,
                golden_records=golden_records
            )
            
        result = {
            "golden_records": golden_records,
            "xrefs": self.merge_processor.get_xrefs(),
            "lineage": lineage,
            "trust_scores": trust_scores
        }
        
        if return_details:
            result["details"] = {
                "match_results": matches,
                "merge_details": self.merge_processor.get_merge_details()
            }
            
        self.logger.info(
            f"Processed {len(records)} records into {len(golden_records)} "
            "golden records"
        )
        
        return result
    
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
        
        if golden_record and include_lineage and self.lineage_tracker:
            golden_record["lineage"] = self.lineage_tracker.get_record_history(
                record_id
            )
            
        return golden_record
