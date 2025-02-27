"""
OpenMatch - An open source Master Data Management (MDM) solution
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from .config import TrustConfig, SurvivorshipRules, MetadataConfig
from .match import MatchConfig, MatchEngine
from .trust import TrustFramework
from .lineage import LineageTracker

@dataclass
class MDMResults:
    """Results from MDM processing."""
    golden_records: List[Dict[str, Any]]
    source_records: Dict[str, List[Dict[str, Any]]]
    match_groups_count: int
    total_records: int
    review_required: List[Dict[str, Any]]
    source_counts: Dict[str, int]
    quality_metrics: Dict[str, float]
    avg_group_size: float = 0.0

class MDMPipeline:
    """Main MDM pipeline for processing records."""
    
    def __init__(
        self,
        match_config: MatchConfig,
        trust_config: TrustConfig,
        survivorship_rules: SurvivorshipRules,
        metadata_config: Optional[MetadataConfig] = None
    ):
        self.match_engine = MatchEngine(match_config)
        self.trust_framework = TrustFramework(trust_config)
        self.survivorship_rules = survivorship_rules
        self.metadata_config = metadata_config
        self.lineage_tracker = LineageTracker()

    def process_records(self, records: List[Dict[str, Any]]) -> MDMResults:
        """
        Process a batch of records through the MDM pipeline.
        
        Args:
            records: List of records to process
            
        Returns:
            MDMResults object containing golden records and related data
        """
        # Initialize results
        results = MDMResults(
            golden_records=[],
            source_records={},
            match_groups_count=0,
            total_records=len(records),
            review_required=[],
            source_counts={},
            quality_metrics={}
        )
        
        try:
            # Count records by source
            for record in records:
                source = record.get('source')
                if source:
                    results.source_counts[source] = results.source_counts.get(source, 0) + 1
            
            # Find matches
            match_groups = self.match_engine.find_matches(records)
            results.match_groups_count = len(match_groups)
            
            # Process each match group
            for group in match_groups:
                # Calculate trust scores
                trust_scores = self.trust_framework.calculate_trust_scores(group)
                
                # Apply survivorship rules
                golden_record = self.survivorship_rules.apply_rules(group, trust_scores)
                
                # Track lineage
                self.lineage_tracker.track_merge(group, golden_record)
                
                # Add to results
                results.golden_records.append(golden_record)
                results.source_records[golden_record['id']] = group
                
                # Check if manual review is needed
                if self.needs_review(group, trust_scores):
                    results.review_required.append({
                        'golden_record_id': golden_record['id'],
                        'group': group,
                        'trust_scores': trust_scores
                    })
            
            # Calculate average group size
            if results.match_groups_count > 0:
                results.avg_group_size = len(records) / results.match_groups_count
            
            # Calculate quality metrics
            results.quality_metrics = self.calculate_quality_metrics(results)
            
            return results
            
        except Exception as e:
            # Log error and re-raise
            print(f"Error processing records: {str(e)}")
            raise

    def needs_review(self, group: List[Dict[str, Any]], trust_scores: Dict[str, float]) -> bool:
        """
        Determine if a match group needs manual review.
        
        Args:
            group: List of matched records
            trust_scores: Trust scores for the records
            
        Returns:
            True if manual review is needed, False otherwise
        """
        # Example review criteria - can be customized
        if len(group) > 5:  # Large groups may need review
            return True
            
        # Low trust scores may need review
        if any(score < 0.5 for score in trust_scores.values()):
            return True
            
        return False

    def calculate_quality_metrics(self, results: MDMResults) -> Dict[str, float]:
        """
        Calculate quality metrics for the processed records.
        
        Args:
            results: MDM processing results
            
        Returns:
            Dictionary of quality metrics
        """
        metrics = {}
        
        # Matching effectiveness
        if results.total_records > 0:
            metrics['match_rate'] = len(results.golden_records) / results.total_records
            metrics['review_rate'] = len(results.review_required) / results.total_records
        
        # Data completeness (example)
        complete_records = sum(
            1 for record in results.golden_records
            if all(field in record for field in ['first_name', 'last_name', 'email'])
        )
        metrics['completeness'] = complete_records / len(results.golden_records) if results.golden_records else 0
        
        return metrics
