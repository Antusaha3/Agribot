# api/schemas.py
from pydantic import BaseModel
from typing import List, Optional

class AskRequest(BaseModel):
    question: str
    session_id: Optional[str] = None

class AskResponse(BaseModel):
    answer: str
    mode: str
    sources: List[str]

class IngestRequest(BaseModel):
    urls: Optional[List[str]] = None
    files: Optional[List[str]] = None  # paths or S3 keys
