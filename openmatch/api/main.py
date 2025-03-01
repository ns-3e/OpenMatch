"""
FastAPI backend for OpenMatch MDM operations.
"""
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel
import time

from openmatch.match import (
    MatchEngine, MatchConfig, BlockingConfig,
    FieldConfig, MatchRuleConfig, MatchType
)
from openmatch.connectors import (
    DatabaseConnector, MasterRecord,
    SourceRecord, MatchResult, MergeHistory
)
from openmatch.model import DataModelConfig, DataModelManager

app = FastAPI(
    title="OpenMatch MDM API",
    description="API for managing master data matching operations",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for request/response
class JobStatus(BaseModel):
    job_id: int
    job_type: str
    status: str
    start_time: str
    end_time: Optional[str]
    metrics: Optional[Dict]

class MatchJobConfig(BaseModel):
    batch_size: int = 10000
    max_workers: Optional[int] = None
    use_processes: bool = False
    blocking_keys: List[str]
    match_rules: List[Dict]

class DataModelConfigUpdate(BaseModel):
    entities: Dict
    source_systems: Dict
    physical_model: Dict
    metadata: Dict

# Database dependency
def get_db():
    db = DatabaseConnector()
    try:
        yield db.session
    finally:
        db.session.close()

@app.get("/api/jobs", response_model=List[JobStatus])
def get_jobs(db: Session = Depends(get_db)):
    """Get all job instances and their status."""
    result = db.execute("""
        SELECT * FROM mdm.job_instances 
        ORDER BY start_time DESC LIMIT 100
    """).fetchall()
    return [JobStatus(**row) for row in result]

@app.get("/api/jobs/{job_id}", response_model=JobStatus)
def get_job(job_id: int, db: Session = Depends(get_db)):
    """Get status of a specific job."""
    result = db.execute(
        "SELECT * FROM mdm.job_instances WHERE job_id = :job_id",
        {"job_id": job_id}
    ).fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobStatus(**result)

@app.post("/api/jobs/match", response_model=JobStatus)
def start_match_job(config: MatchJobConfig, db: Session = Depends(get_db)):
    """Start a new match job with the given configuration."""
    try:
        # Create match engine config
        match_config = MatchConfig(
            blocking=BlockingConfig(blocking_keys=config.blocking_keys),
            rules=[MatchRuleConfig(**rule) for rule in config.match_rules]
        )
        
        # Initialize engine
        engine = MatchEngine(match_config)
        
        # Start job and return status
        job = engine.process_batch(
            batch_size=config.batch_size,
            max_workers=config.max_workers,
            use_processes=config.use_processes
        )
        return JobStatus(**job)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/match")
def get_match_statistics(db: Session = Depends(get_db)):
    """Get match statistics from materialized view."""
    return db.execute("SELECT * FROM mdm.match_statistics").fetchall()

@app.get("/api/stats/blocking")
def get_blocking_statistics(db: Session = Depends(get_db)):
    """Get blocking effectiveness statistics."""
    return db.execute("SELECT * FROM mdm.blocking_statistics").fetchall()

@app.get("/api/model/config")
def get_data_model_config():
    """Get current data model configuration."""
    return DataModelConfig.load()

@app.put("/api/model/config")
def update_data_model_config(config: DataModelConfigUpdate):
    """Update data model configuration."""
    try:
        model_config = DataModelConfig(**config.dict())
        model_config.save()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/model/entities")
def get_entities(db: Session = Depends(get_db)):
    """Get all entity definitions."""
    config = DataModelConfig.load()
    return config.entities

@app.get("/api/model/source-systems")
def get_source_systems(db: Session = Depends(get_db)):
    """Get all configured source systems."""
    config = DataModelConfig.load()
    return config.source_systems

@app.get("/api/records/master")
def get_master_records(
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """Get paginated master records."""
    records = db.query(MasterRecord).offset(offset).limit(limit).all()
    return records

@app.get("/api/records/source")
def get_source_records(
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """Get paginated source records."""
    records = db.query(SourceRecord).offset(offset).limit(limit).all()
    return records

@app.get("/api/records/matches")
def get_match_results(
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """Get paginated match results."""
    matches = db.query(MatchResult).offset(offset).limit(limit).all()
    return matches

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 