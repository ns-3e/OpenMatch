"""
Database schema definitions for MDM operations.
"""
from datetime import datetime
from typing import List
from sqlalchemy import (
    Table, Column, Integer, String, DateTime, 
    ForeignKey, JSON, Boolean, Float, Text,
    UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

class MasterRecord(Base):
    """Master record table containing the golden record for each entity."""
    __tablename__ = 'master_records'
    __table_args__ = (
        Index('idx_master_entity_type', 'entity_type'),
        UniqueConstraint('source_record_ids', name='uq_master_source_records'),
        {'schema': 'mdm'}
    )

    id = Column(Integer, primary_key=True)
    entity_type = Column(String(50), nullable=False)  # e.g., 'person', 'organization'
    golden_record = Column(JSONB, nullable=False)  # Stores the consolidated record data
    confidence_score = Column(Float)
    record_count = Column(Integer, default=1)  # Number of source records
    source_record_ids = Column(JSONB)  # Array of source record IDs
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    
    # Relationships
    source_records = relationship("SourceRecord", back_populates="master_record")

class SourceRecord(Base):
    """Source records linked to master records."""
    __tablename__ = 'source_records'
    __table_args__ = {'schema': 'mdm'}

    id = Column(Integer, primary_key=True)
    master_record_id = Column(Integer, ForeignKey('mdm.master_records.id'))
    source_system = Column(String(100), nullable=False)
    source_id = Column(String(255), nullable=False)
    record_data = Column(JSONB, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    
    # Relationships
    master_record = relationship("MasterRecord", back_populates="source_records")
    
    __table_args__ = (
        UniqueConstraint('source_system', 'source_id', name='uq_source_record'),
        Index('idx_source_system_id', 'source_system', 'source_id'),
        {'schema': 'mdm'}
    )

class MatchResult(Base):
    """Stores match results between records."""
    __tablename__ = 'match_results'
    __table_args__ = {'schema': 'mdm'}

    id = Column(Integer, primary_key=True)
    source_record_id = Column(Integer, ForeignKey('mdm.source_records.id'))
    matched_record_id = Column(Integer, ForeignKey('mdm.source_records.id'))
    match_score = Column(Float, nullable=False)
    match_details = Column(JSONB)  # Stores detailed matching information
    match_rule_id = Column(String(100))  # Reference to the rule that created the match
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), default='PENDING')  # PENDING, CONFIRMED, REJECTED
    
    __table_args__ = (
        Index('idx_match_source_record', 'source_record_id'),
        Index('idx_match_matched_record', 'matched_record_id'),
        {'schema': 'mdm'}
    )

class MergeHistory(Base):
    """Tracks merge operations and their details."""
    __tablename__ = 'merge_history'
    __table_args__ = {'schema': 'mdm'}

    id = Column(Integer, primary_key=True)
    master_record_id = Column(Integer, ForeignKey('mdm.master_records.id'))
    merged_record_ids = Column(JSONB)  # List of merged record IDs
    merge_rule_id = Column(String(100))
    merge_details = Column(JSONB)  # Details about the merge operation
    created_at = Column(DateTime, default=datetime.utcnow)
    operator = Column(String(100))  # User or system that performed the merge
    
    __table_args__ = (
        Index('idx_merge_master_record', 'master_record_id'),
        {'schema': 'mdm'}
    )

class RuleSet(Base):
    """Stores matching and merging rules."""
    __tablename__ = 'rule_sets'
    __table_args__ = {'schema': 'mdm'}

    id = Column(Integer, primary_key=True)
    rule_id = Column(String(100), unique=True)
    rule_type = Column(String(20))  # MATCH or MERGE
    entity_type = Column(String(50))
    rule_config = Column(JSONB)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_rule_type_entity', 'rule_type', 'entity_type'),
        {'schema': 'mdm'}
    ) 