"""
Simplified report generation for cross validation system.
"""

import json
import os
from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import asdict

from ..models.data_models import VerificationReport, FieldResult, ValidationSummary, RecommendationResult
from ..s3_operations.s3_client import upload_json_to_s3, extract_date_from_s3_path


def write_enhanced_cross_validation_report_to_s3(
    report: VerificationReport, 
    s3_client,
    first_document_s3_path: Optional[str] = None,
    bucket: str = "lendingwise-aiagent"
) -> Optional[str]:
    """
    Write simplified cross-validation report to S3.
    
    Args:
        report: VerificationReport object
        s3_client: boto3 S3 client
        first_document_s3_path: S3 path of first document (for date extraction)
        bucket: S3 bucket name
        
    Returns:
        S3 key where the report was saved, or None if failed.
    """
    try:
        # Extract date from document S3 path
        year = month = day = None
        if first_document_s3_path:
            year, month, day = extract_date_from_s3_path(first_document_s3_path)
        
        # Fallback to current date if not found
        if not all([year, month, day]):
            now = datetime.now()
            year = str(now.year)
            month = f"{now.month:02d}"
            day = f"{now.day:02d}"
            print(f"[WARN] Using current date for cross-validation report: {year}-{month}-{day}")
        
        # Build S3 key path using checklistId (or fallback to lmrid if not available)
        checklistId = report.validation_summary.checklistId or report.validation_summary.lmrid
        s3_key = (
            f"LMRFileDocNew/{report.validation_summary.fpcid}/{year}/{month}/{day}/"
            f"{report.validation_summary.lmrid}/upload/cross-validation-result/"
            f"{report.validation_summary.fpcid}_{checklistId}_cross-validation.json"
        )
        
        # Create simplified report
        simplified_report = create_simplified_cross_validation_report(report)
        
        # Upload to S3
        success = upload_json_to_s3(s3_client, bucket, s3_key, simplified_report)
        
        if success:
            print(f"[✓] Cross-validation report uploaded to s3://{bucket}/{s3_key}")
            return s3_key
        else:
            print(f"[ERROR] Failed to upload cross-validation report to S3")
            return None
            
    except Exception as e:
        print(f"[ERROR] Failed to create cross-validation report: {e}")
        import traceback
        traceback.print_exc()
        return None


def create_simplified_cross_validation_report(report: VerificationReport) -> Dict[str, Any]:
    """Create a simplified, frontend-ready cross-validation report."""
    
    # Convert dataclass to dict for JSON serialization
    def convert_to_dict(obj):
        """Convert dataclass to dict recursively."""
        if hasattr(obj, '__dict__'):
            result = {}
            for key, value in obj.__dict__.items():
                if isinstance(value, list):
                    result[key] = [convert_to_dict(item) if hasattr(item, '__dict__') else item for item in value]
                elif hasattr(value, '__dict__'):
                    result[key] = convert_to_dict(value)
                else:
                    result[key] = value
            return result
        return obj
    
    report_dict = convert_to_dict(report)
    
    # Return the simplified structure
    return report_dict


def get_recommendation_status(overall_status: str, match_percentage: float) -> str:
    """Get recommendation status based on validation results."""
    if overall_status == "VERIFIED" and match_percentage >= 95:
        return "APPROVE"
    elif overall_status == "VERIFIED" and match_percentage >= 85:
        return "APPROVE_WITH_MINOR_ISSUES"
    elif overall_status == "PARTIAL" and match_percentage >= 70:
        return "CONDITIONAL_APPROVAL"
    elif match_percentage >= 50:
        return "MANUAL_REVIEW_REQUIRED"
    else:
        return "REJECT"


def get_next_action(overall_status: str, mismatch_count: int) -> str:
    """Get next action based on validation results."""
    if overall_status == "VERIFIED" and mismatch_count == 0:
        return "No action required - validation passed"
    elif overall_status == "VERIFIED" and mismatch_count > 0:
        return "Minor review recommended for mismatched fields"
    elif overall_status == "PARTIAL":
        return "Manual review required for mismatched fields"
    else:
        return "Full manual review required - significant issues detected"


def is_critical_field(field_name: str) -> bool:
    """Check if a field is considered critical."""
    critical_fields = ["firstname", "lastname", "dob", "ssn", "socialsecuritynumber", "dateofbirth"]
    return field_name.lower() in critical_fields


# Legacy functions for backward compatibility
def write_json_report(report: VerificationReport, output_dir: str) -> None:
    """Write simple JSON report to output directory."""
    if not output_dir:
        return
        
    os.makedirs(output_dir, exist_ok=True)
    safe_FPCID = str(report.FPCID).replace("/", "_")
    safe_LMRId = str(report.LMRId).replace("/", "_")
    out_path = os.path.join(output_dir, f"{safe_FPCID}__{safe_LMRId}.json")
    
    simple_report = {
        "FPCID": report.FPCID,
        "LMRId": report.LMRId,
        "overall_status": report.overall_status,
        "summary": report.summary,
        "timestamp": datetime.now().isoformat(),
        "field_matches": [
            {
                "field_name": fm.field_name,
                "matched": fm.matched,
                "sources": fm.sources
            }
            for fm in report.field_matches
        ]
    }
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(simple_report, f, indent=2, ensure_ascii=False)
    
    print(f"[REPORT] Simple JSON report written to {out_path}")


def write_comprehensive_json_report(report: VerificationReport, output_dir: str) -> None:
    """Write comprehensive JSON report (simplified version)."""
    if not output_dir:
        return
        
    os.makedirs(output_dir, exist_ok=True)
    safe_FPCID = str(report.FPCID).replace("/", "_")
    safe_LMRId = str(report.LMRId).replace("/", "_")
    out_path = os.path.join(output_dir, f"detailed_validation_report_{safe_FPCID}_{safe_LMRId}.json")
    
    # Create simplified comprehensive report
    comprehensive_report = create_simplified_cross_validation_report(report)
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(comprehensive_report, f, indent=2, ensure_ascii=False)
    
    print(f"[REPORT] Comprehensive JSON report written to {out_path}")


def write_markdown_report(report: VerificationReport, output_dir: str = "cross_validation/reports") -> None:
    """Write simple markdown report."""
    if not output_dir:
        return
        
    os.makedirs(output_dir, exist_ok=True)
    safe_FPCID = str(report.FPCID).replace("/", "_")
    safe_LMRId = str(report.LMRId).replace("/", "_")
    out_path = os.path.join(output_dir, f"{safe_FPCID}__{safe_LMRId}.md")
    
    # Generate simple markdown content
    lines = []
    lines.append(f"# Cross-Validation Report")
    lines.append(f"")
    lines.append(f"**FPCID:** {report.FPCID}")
    lines.append(f"**LMRId:** {report.LMRId}")
    lines.append(f"**Status:** {report.overall_status}")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"")
    lines.append(f"## Summary")
    lines.append(f"{report.summary}")
    lines.append(f"")
    lines.append(f"## Field Results")
    
    for fm in report.field_matches:
        status_icon = "✅" if fm.matched else "❌"
        lines.append(f"- {status_icon} **{fm.field_name}**")
        for source, value in fm.sources.items():
            lines.append(f"  - {source}: {value}")
        if not fm.matched and fm.mismatch_reason:
            lines.append(f"  - *Reason: {fm.mismatch_reason}*")
        lines.append(f"")
    
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    
    print(f"[REPORT] Markdown report written to {out_path}")