"""
State models for the Lendingwise AI pipeline.
"""

from typing import Dict, Any, Optional
from pydantic import BaseModel


class IngestionState(BaseModel):
    """State for document ingestion from SQS/S3."""
    s3_bucket: str
    s3_key: str
    # Full s3 path to the sidecar metadata JSON (e.g., s3://bucket/.../metadata/<file>.json)
    metadata_s3_path: Optional[str] = None
    FPCID: Optional[str] = None
    LMRId: Optional[str] = None
    document_name: Optional[str] = None
    document_type: Optional[str] = None
    agent_name: Optional[str] = None
    agent_type: Optional[str] = None
    tool: Optional[str] = None
    source_url: Optional[str] = None
    content_type: Optional[str] = None
    uploaded_at: Optional[str] = None
    size_bytes: Optional[int] = None
    etag: Optional[str] = None
    prefix_parts: Optional[Dict[str, Any]] = None
    raw_metadata: Optional[Dict[str, Any]] = None


class OCRState(BaseModel):
    """State for OCR processing results."""
    bucket: str
    key: str
    mode: str
    doc_category: str
    document_name: Optional[str] = None
    ocr_json: Dict[str, Any]


class ClassificationState(BaseModel):
    """State for document classification results."""
    expected_category: str
    detected_doc_type: str
    passed: bool
    message: str


class ExtractionState(BaseModel):
    """State for field extraction results."""
    passed: bool
    message: str
    extracted: Optional[Dict[str, Any]] = None


class PipelineState(BaseModel):
    """Main pipeline state containing all sub-states."""
    ingestion: Optional[IngestionState] = None
    ocr: Optional[OCRState] = None
    classification: Optional[ClassificationState] = None
    extraction: Optional[ExtractionState] = None
