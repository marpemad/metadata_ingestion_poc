from typing import List, Dict, Any
from pydantic import BaseModel, Field, validator
import yaml

class Source(BaseModel):
    id: str
    enabled: bool = True
    type: str
    domain: str
    entity: str
    options: Dict[str, Any] = Field(default_factory=dict)
    raw_partitions: List[str] = Field(default_factory=lambda: ["ingest_date"])
    hub_primary_keys: List[str] = Field(default_factory=list)

class SourceSystem(BaseModel):
    version: int
    defaults: Dict[str, Any]
    sources: List[Source]

def load_sources(path: str) -> SourceSystem:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return SourceSystem(**data)
