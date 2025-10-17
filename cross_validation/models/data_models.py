"""
Data models for cross validation system.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass
class BorrowerData:
    """Borrower data from tblfile database table."""
    FPCID: Optional[str] = None
    LMRId: Optional[str] = None
    borrowerName: Optional[str] = None  # Note: DB uses borrowerName, not borrowerFName
    borrowerMName: Optional[str] = None
    borrowerLName: Optional[str] = None
    borrowerDOB: Optional[str] = None
    borrowerPOB: Optional[str] = None
    driverLicenseNumber: Optional[str] = None
    driverLicenseState: Optional[str] = None


@dataclass
class DocumentDetails:
    """Details of a verified document from S3."""
    document_name: str
    agent_name: Optional[str]
    tool: Optional[str]
    file_s3_location: Optional[str]
    metadata_s3_path: Optional[str]
    verified_result_s3_path: str
    verified_details: Optional[Dict[str, Any]]


@dataclass
class FieldResult:
    """Result for a single field validation."""
    field: str
    status: str  # "MATCH", "PARTIAL", "MISMATCH"
    reference: str  # "-" if no DB reference
    documents: Dict[str, str]  # {doc_name: value}
    consensus: Optional[str] = None  # For cross-document mode
    issue: Optional[str] = None
    note: Optional[str] = None


@dataclass
class ValidationSummary:
    """Summary of validation results."""
    fpcid: str
    lmrid: str
    status: str  # "PASS" or "FAIL"
    score: int  # 0-100
    threshold: int  # 85
    message: str
    timestamp: str
    note: Optional[str] = None


@dataclass
class RecommendationResult:
    """Recommendation based on validation."""
    action: str  # "APPROVE", "REJECT", "REVIEW"
    confidence: str  # "HIGH", "MEDIUM", "LOW"
    notes: str


@dataclass
class VerificationReport:
    """Complete verification report for a FPCID/LMRId pair (simplified for frontend)."""
    validation_summary: ValidationSummary
    documents_validated: List[str]
    field_results: List[FieldResult]
    summary: Dict[str, Any]  # {total_fields, matched, partial, failed, issues}
    recommendation: RecommendationResult


# Legacy models for backward compatibility
@dataclass
class FieldMatch:
    """Represents a field and its values across sources."""
    field_name: str
    sources: Dict[str, Optional[str]] = field(default_factory=dict)  # source_name -> value
    matched: bool = True
    mismatch_reason: Optional[str] = None
