from datetime import datetime
from typing import Dict, List, Optional, Set, Any
from enum import Enum
from dataclasses import dataclass
import networkx as nx

class RelationType(Enum):
    SAME_AS = "same_as"
    PARENT_OF = "parent_of"
    CHILD_OF = "child_of"
    RELATED_TO = "related_to"
    DERIVED_FROM = "derived_from"
    SUPERSEDES = "supersedes"
    REPLACED_BY = "replaced_by"

@dataclass
class CrossReference:
    source_id: str
    target_id: str
    relation_type: RelationType
    source_system: Optional[str] = None
    target_system: Optional[str] = None
    confidence_score: Optional[float] = None
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    metadata: Dict[str, Any] = None

class CrossReferenceManager:
    def __init__(self):
        # Graph to store relationships
        self.graph = nx.MultiDiGraph()
        # Store system-specific identifiers
        self.system_ids: Dict[str, Dict[str, str]] = {}
        # Store temporal validity
        self.temporal_xrefs: Dict[str, List[CrossReference]] = {}

    def add_xref(
        self,
        source_id: str,
        target_id: str,
        relation_type: RelationType,
        source_system: Optional[str] = None,
        target_system: Optional[str] = None,
        confidence_score: Optional[float] = None,
        valid_from: Optional[datetime] = None,
        valid_to: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add a cross-reference between two entities."""
        xref = CrossReference(
            source_id=source_id,
            target_id=target_id,
            relation_type=relation_type,
            source_system=source_system,
            target_system=target_system,
            confidence_score=confidence_score,
            valid_from=valid_from or datetime.utcnow(),
            valid_to=valid_to,
            metadata=metadata or {}
        )
        
        # Add to graph
        self.graph.add_edge(
            source_id,
            target_id,
            key=relation_type.value,
            attr_dict=xref.__dict__
        )
        
        # Store temporal information
        if source_id not in self.temporal_xrefs:
            self.temporal_xrefs[source_id] = []
        self.temporal_xrefs[source_id].append(xref)
        
        # Store system mappings
        if source_system and target_system:
            if source_system not in self.system_ids:
                self.system_ids[source_system] = {}
            self.system_ids[source_system][source_id] = target_id

    def get_related_entities(
        self,
        entity_id: str,
        relation_type: Optional[RelationType] = None,
        at_time: Optional[datetime] = None,
        max_depth: int = 1
    ) -> Set[str]:
        """Get related entities with optional filtering."""
        related = set()
        
        # Get all edges from the entity
        edges = self.graph.out_edges(entity_id, data=True)
        
        for source, target, data in edges:
            edge_type = RelationType(data["key"])
            
            # Filter by relation type if specified
            if relation_type and edge_type != relation_type:
                continue
                
            # Filter by time if specified
            if at_time:
                valid_from = data["attr_dict"]["valid_from"]
                valid_to = data["attr_dict"]["valid_to"]
                
                if valid_from and valid_from > at_time:
                    continue
                if valid_to and valid_to < at_time:
                    continue
                    
            related.add(target)
            
            # Recurse if needed
            if max_depth > 1:
                next_related = self.get_related_entities(
                    target,
                    relation_type,
                    at_time,
                    max_depth - 1
                )
                related.update(next_related)
                
        return related

    def get_system_id(
        self,
        entity_id: str,
        target_system: str,
        at_time: Optional[datetime] = None
    ) -> Optional[str]:
        """Get system-specific identifier for an entity."""
        if target_system not in self.system_ids:
            return None
            
        system_map = self.system_ids[target_system]
        return system_map.get(entity_id)

    def get_relationship_graph(
        self,
        entity_ids: List[str],
        relation_types: Optional[List[RelationType]] = None,
        at_time: Optional[datetime] = None
    ) -> nx.MultiDiGraph:
        """Get a subgraph of relationships between specified entities."""
        # Create subgraph with specified entities
        subgraph = self.graph.subgraph(entity_ids)
        
        if relation_types or at_time:
            # Create filtered graph
            filtered = nx.MultiDiGraph()
            filtered.add_nodes_from(subgraph.nodes(data=True))
            
            # Add edges that match filters
            for source, target, key, data in subgraph.edges(data=True, keys=True):
                edge_type = RelationType(key)
                
                # Filter by relation type
                if relation_types and edge_type not in relation_types:
                    continue
                    
                # Filter by time
                if at_time:
                    valid_from = data["attr_dict"]["valid_from"]
                    valid_to = data["attr_dict"]["valid_to"]
                    
                    if valid_from and valid_from > at_time:
                        continue
                    if valid_to and valid_to < at_time:
                        continue
                        
                filtered.add_edge(source, target, key=key, attr_dict=data["attr_dict"])
                
            return filtered
            
        return subgraph

    def find_path(
        self,
        source_id: str,
        target_id: str,
        relation_types: Optional[List[RelationType]] = None,
        at_time: Optional[datetime] = None
    ) -> Optional[List[str]]:
        """Find a path between two entities with optional filters."""
        try:
            if relation_types or at_time:
                # Create filtered graph
                filtered = nx.MultiDiGraph()
                filtered.add_nodes_from(self.graph.nodes())
                
                for source, target, key, data in self.graph.edges(data=True, keys=True):
                    edge_type = RelationType(key)
                    
                    # Apply filters
                    if relation_types and edge_type not in relation_types:
                        continue
                        
                    if at_time:
                        valid_from = data["attr_dict"]["valid_from"]
                        valid_to = data["attr_dict"]["valid_to"]
                        
                        if valid_from and valid_from > at_time:
                            continue
                        if valid_to and valid_to < at_time:
                            continue
                            
                    filtered.add_edge(source, target)
                    
                return nx.shortest_path(filtered, source_id, target_id)
                
            return nx.shortest_path(self.graph, source_id, target_id)
            
        except nx.NetworkXNoPath:
            return None

    def export_xrefs(self, entity_id: str) -> Dict[str, Any]:
        """Export all cross-references for an entity."""
        xrefs = self.temporal_xrefs.get(entity_id, [])
        
        return {
            "entity_id": entity_id,
            "cross_references": [
                {
                    "source_id": xref.source_id,
                    "target_id": xref.target_id,
                    "relation_type": xref.relation_type.value,
                    "source_system": xref.source_system,
                    "target_system": xref.target_system,
                    "confidence_score": xref.confidence_score,
                    "valid_from": xref.valid_from.isoformat() if xref.valid_from else None,
                    "valid_to": xref.valid_to.isoformat() if xref.valid_to else None,
                    "metadata": xref.metadata
                }
                for xref in xrefs
            ]
        }
