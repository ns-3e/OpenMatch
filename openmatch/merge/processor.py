"""
Merge processor implementation for OpenMatch.
"""

from typing import Dict, List, Optional, Set, Tuple, Union
import logging
import uuid
from datetime import datetime

from openmatch.merge.strategies import MergeStrategy, DefaultMergeStrategy
from openmatch.utils.logging import setup_logging


class MergeProcessor:
    """Processor for merging records and managing golden records."""
    
    def __init__(
        self,
        strategy: Optional[MergeStrategy] = None,
        id_field: str = "id",
        source_field: str = "source",
        timestamp_field: str = "last_updated",
        logger: Optional[logging.Logger] = None
    ):
        """Initialize the merge processor.
        
        Args:
            strategy: Strategy for merging records
            id_field: Field containing record ID
            source_field: Field containing record source
            timestamp_field: Field containing record timestamp
            logger: Optional logger instance
        """
        self.strategy = strategy or DefaultMergeStrategy()
        self.id_field = id_field
        self.source_field = source_field
        self.timestamp_field = timestamp_field
        self.logger = logger or setup_logging(__name__)
        
        # Store golden records
        self.golden_records = {}  # golden_id -> record
        
        # Track cross-references
        self.xrefs = {}  # source_id -> golden_id
        self.sources = {}  # golden_id -> set(source_ids)
        
        # Track merge details
        self.merge_details = {}  # golden_id -> merge info
        
    def _generate_golden_id(self) -> str:
        """Generate a new golden record ID.
        
        Returns:
            Unique golden record ID
        """
        return f"GOLDEN_{uuid.uuid4().hex[:8]}"
        
    def merge_matches(
        self,
        matches: List[Tuple[Dict, Dict, float]],
        trust_scores: Optional[Dict[str, Dict[str, float]]] = None
    ) -> List[Dict]:
        """Merge matched records into golden records.
        
        Args:
            matches: List of (record1, record2, score) tuples
            trust_scores: Optional trust scores for records
            
        Returns:
            List of golden records
        """
        # Group matches into clusters
        clusters = self._group_matches(matches)
        golden_records = []
        
        # Process each cluster
        for cluster in clusters:
            # Generate golden ID
            golden_id = self._generate_golden_id()
            
            # Get trust scores for cluster records
            cluster_scores = None
            if trust_scores:
                cluster_scores = {
                    record[self.id_field]: trust_scores.get(
                        record[self.id_field],
                        {"total": 0.0}
                    )
                    for record in cluster
                }
            
            # Merge records
            golden_record = self.strategy.merge_records(
                cluster,
                golden_id,
                trust_scores=cluster_scores
            )
            
            # Add metadata
            golden_record[self.id_field] = golden_id
            golden_record[self.source_field] = "GOLDEN"
            golden_record[self.timestamp_field] = datetime.now().isoformat()
            
            # Store golden record
            self.golden_records[golden_id] = golden_record
            golden_records.append(golden_record)
            
            # Update cross-references
            source_ids = {r[self.id_field] for r in cluster}
            self.sources[golden_id] = source_ids
            for source_id in source_ids:
                self.xrefs[source_id] = golden_id
            
            # Track merge details
            self.merge_details[golden_id] = {
                "timestamp": datetime.now(),
                "source_records": cluster,
                "trust_scores": cluster_scores,
                "merge_strategy": self.strategy.__class__.__name__
            }
            
        self.logger.info(
            f"Created {len(golden_records)} golden records from "
            f"{len(matches)} matches"
        )
        
        return golden_records
        
    def _group_matches(
        self,
        matches: List[Tuple[Dict, Dict, float]]
    ) -> List[List[Dict]]:
        """Group matches into clusters for merging.
        
        Args:
            matches: List of (record1, record2, score) tuples
            
        Returns:
            List of record clusters
        """
        # Build graph of matches
        import networkx as nx
        graph = nx.Graph()
        
        for record1, record2, score in matches:
            id1 = record1[self.id_field]
            id2 = record2[self.id_field]
            
            # Add nodes with record data
            graph.add_node(id1, record=record1)
            graph.add_node(id2, record=record2)
            
            # Add edge with similarity score
            graph.add_edge(id1, id2, weight=score)
            
        # Find connected components (clusters)
        clusters = []
        for component in nx.connected_components(graph):
            cluster = [
                graph.nodes[node]["record"]
                for node in component
            ]
            clusters.append(cluster)
            
        return clusters
        
    def get_golden_record(self, record_id: str) -> Optional[Dict]:
        """Get golden record by ID.
        
        Args:
            record_id: ID of golden record
            
        Returns:
            Golden record if found, None otherwise
        """
        return self.golden_records.get(record_id)
        
    def get_xrefs(self) -> Dict[str, str]:
        """Get all cross-references.
        
        Returns:
            Dictionary mapping source IDs to golden IDs
        """
        return self.xrefs.copy()
        
    def get_source_records(self, golden_id: str) -> Set[str]:
        """Get source record IDs for a golden record.
        
        Args:
            golden_id: ID of golden record
            
        Returns:
            Set of source record IDs
        """
        return self.sources.get(golden_id, set())
        
    def get_merge_details(
        self,
        golden_id: Optional[str] = None
    ) -> Union[Dict, List[Dict]]:
        """Get merge details for golden record(s).
        
        Args:
            golden_id: Optional golden record ID
            
        Returns:
            Merge details for specified record or all records
        """
        if golden_id:
            return self.merge_details.get(golden_id, {})
        return list(self.merge_details.values())
        
    def rollback_merge(self, golden_id: str) -> bool:
        """Rollback a merge operation.
        
        Args:
            golden_id: ID of golden record to rollback
            
        Returns:
            True if rollback successful, False otherwise
        """
        if golden_id not in self.golden_records:
            return False
            
        # Remove golden record
        del self.golden_records[golden_id]
        
        # Remove cross-references
        source_ids = self.sources.pop(golden_id, set())
        for source_id in source_ids:
            self.xrefs.pop(source_id, None)
            
        # Remove merge details
        self.merge_details.pop(golden_id, None)
        
        self.logger.info(f"Rolled back merge for golden record {golden_id}")
        return True
