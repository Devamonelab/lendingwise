"""
Validation Check node for verifying extracted document field values.

This node validates document fields against document-specific rules including:
- Expiration date checks
- Required field presence
- Format validations (SSN, license numbers, etc.)
- Logical validations (issue date < expiration date)
- Age validations

Validation failures result in "human verification needed" status rather than
complete rejection, allowing documents to proceed through the pipeline while
flagging them for manual review.
"""

import os
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Tuple, Optional

from ..config.state_models import PipelineState
from ..utils.helpers import log_agent_event


# Validation result structure
class ValidationResult:
    def __init__(self):
        self.passed = True
        self.warnings: List[str] = []
        self.errors: List[str] = []
        self.info: List[str] = []
    
    def add_warning(self, message: str):
        """Add a warning (allows pass but flags for review)."""
        self.warnings.append(message)
        self.passed = False  # Warnings require human verification
    
    def add_error(self, message: str):
        """Add an error (requires human verification)."""
        self.errors.append(message)
        self.passed = False
    
    def add_info(self, message: str):
        """Add informational message."""
        self.info.append(message)
    
    def has_issues(self) -> bool:
        """Check if there are any warnings or errors."""
        return len(self.warnings) > 0 or len(self.errors) > 0
    
    def get_all_messages(self) -> List[str]:
        """Get all messages combined."""
        messages = []
        if self.errors:
            messages.extend([f"❌ {msg}" for msg in self.errors])
        if self.warnings:
            messages.extend([f"⚠️  {msg}" for msg in self.warnings])
        if self.info:
            messages.extend([f"ℹ️  {msg}" for msg in self.info])
        return messages


# ============================================================================
# Date Validation Utilities
# ============================================================================

def parse_date(date_str: Any) -> Optional[datetime]:
    """
    Parse various date formats into datetime object.
    Supports: MM/DD/YYYY, YYYY-MM-DD, MM-DD-YYYY, etc.
    """
    if not date_str or date_str == "":
        return None
    
    date_str = str(date_str).strip()
    
    # Common date formats
    formats = [
        "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y",
        "%Y/%m/%d", "%m.%d.%Y", "%d.%m.%Y", "%Y.%m.%d",
        "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y",
        "%m/%d/%y", "%y-%m-%d", "%m-%d-%y",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None


def is_date_expired(date_str: Any) -> Tuple[bool, Optional[datetime]]:
    """Check if a date is expired. Returns (is_expired, parsed_date)."""
    parsed = parse_date(date_str)
    if not parsed:
        return False, None
    
    today = datetime.now()
    return parsed < today, parsed


def is_date_expiring_soon(date_str: Any, days: int = 30) -> Tuple[bool, Optional[datetime]]:
    """Check if date is expiring within specified days."""
    parsed = parse_date(date_str)
    if not parsed:
        return False, None
    
    today = datetime.now()
    threshold = today + timedelta(days=days)
    return today < parsed < threshold, parsed


def validate_date_logic(issue_date: Any, expiration_date: Any) -> Tuple[bool, str]:
    """Validate that issue date is before expiration date."""
    issue = parse_date(issue_date)
    expiry = parse_date(expiration_date)
    
    if not issue or not expiry:
        return True, ""  # Can't validate if dates are missing
    
    if issue >= expiry:
        return False, f"Issue date ({issue_date}) must be before expiration date ({expiration_date})"
    
    return True, ""


# ============================================================================
# Format Validation Utilities
# ============================================================================

def validate_ssn_format(ssn: str) -> Tuple[bool, str]:
    """
    Validate Social Security Number format and range.
    Format: XXX-XX-XXXX or XXXXXXXXX
    Invalid: 000-XX-XXXX, XXX-00-XXXX, 666-XX-XXXX, 9XX-XX-XXXX
    """
    if not ssn:
        return False, "SSN is missing"
    
    # Remove common separators
    ssn_clean = re.sub(r'[-\s]', '', str(ssn))
    
    # Check format
    if not re.match(r'^\d{9}$', ssn_clean):
        return False, f"Invalid SSN format: {ssn}. Expected XXX-XX-XXXX or 9 digits"
    
    # Extract parts
    area = ssn_clean[:3]
    group = ssn_clean[3:5]
    serial = ssn_clean[5:]
    
    # Validate ranges
    if area == "000":
        return False, "Invalid SSN: Area number cannot be 000"
    if area == "666":
        return False, "Invalid SSN: Area number cannot be 666"
    if area.startswith("9"):
        return False, "Invalid SSN: Area number cannot start with 9"
    if group == "00":
        return False, "Invalid SSN: Group number cannot be 00"
    if serial == "0000":
        return False, "Invalid SSN: Serial number cannot be 0000"
    
    return True, ""


def validate_zip_code(zip_code: str) -> Tuple[bool, str]:
    """Validate US ZIP code format (XXXXX or XXXXX-XXXX)."""
    if not zip_code:
        return True, ""  # Optional field
    
    zip_clean = str(zip_code).strip()
    
    if re.match(r'^\d{5}(-\d{4})?$', zip_clean):
        return True, ""
    
    return False, f"Invalid ZIP code format: {zip_code}. Expected XXXXX or XXXXX-XXXX"


def validate_state_code(state: str) -> Tuple[bool, str]:
    """Validate US state code (2-letter abbreviation)."""
    if not state:
        return True, ""  # Optional field
    
    valid_states = {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
        "DC", "PR", "VI", "GU", "AS", "MP"
    }
    
    state_upper = str(state).strip().upper()
    
    if state_upper in valid_states:
        return True, ""
    
    return False, f"Invalid state code: {state}. Expected 2-letter US state abbreviation"


def validate_age_from_dob(dob: Any, min_age: int = 0, max_age: int = 120) -> Tuple[bool, str]:
    """Validate age based on date of birth."""
    parsed_dob = parse_date(dob)
    if not parsed_dob:
        return True, ""  # Can't validate if DOB is missing
    
    today = datetime.now()
    age = (today - parsed_dob).days // 365
    
    if age < min_age:
        return False, f"Age ({age}) is below minimum required age ({min_age})"
    
    if age > max_age:
        return False, f"Age ({age}) seems unrealistic (maximum {max_age})"
    
    return True, ""


# ============================================================================
# Document-Specific Validators
# ============================================================================

def validate_driving_license(extracted: Dict[str, Any]) -> ValidationResult:
    """Validate Driver's License fields."""
    result = ValidationResult()
    
    # Required fields check
    required_fields = ["firstName", "lastName", "dob", "licenseNumber", "expirationDate", "issuingState"]
    for field in required_fields:
        if not extracted.get(field):
            result.add_error(f"Required field missing: {field}")
    
    # Expiration date check
    expiration = extracted.get("expirationDate")
    if expiration:
        is_expired, exp_date = is_date_expired(expiration)
        if is_expired:
            result.add_error(f"Driver's license is expired (expiration date: {expiration})")
        else:
            # Check if expiring soon
            is_expiring, _ = is_date_expiring_soon(expiration, days=30)
            if is_expiring:
                result.add_warning(f"Driver's license is expiring soon (expiration date: {expiration})")
    
    # Issue date vs expiration date logic
    issue_date = extracted.get("issueDate")
    if issue_date and expiration:
        valid, msg = validate_date_logic(issue_date, expiration)
        if not valid:
            result.add_error(msg)
    
    # Age validation (must be at least 16 for driver's license)
    dob = extracted.get("dob")
    if dob:
        valid, msg = validate_age_from_dob(dob, min_age=16, max_age=120)
        if not valid:
            result.add_error(msg)
    
    # State validation
    state = extracted.get("issuingState") or extracted.get("state")
    if state:
        valid, msg = validate_state_code(state)
        if not valid:
            result.add_warning(msg)
    
    # ZIP code validation
    zip_code = extracted.get("zip")
    if zip_code:
        valid, msg = validate_zip_code(zip_code)
        if not valid:
            result.add_warning(msg)
    
    return result


def validate_state_id(extracted: Dict[str, Any]) -> ValidationResult:
    """Validate State ID fields."""
    result = ValidationResult()
    
    # Required fields
    required_fields = ["firstName", "lastName", "dob", "idNumber", "expirationDate", "issuingState"]
    for field in required_fields:
        if not extracted.get(field):
            result.add_error(f"Required field missing: {field}")
    
    # Expiration date check
    expiration = extracted.get("expirationDate")
    if expiration:
        is_expired, _ = is_date_expired(expiration)
        if is_expired:
            result.add_error(f"State ID is expired (expiration date: {expiration})")
        else:
            is_expiring, _ = is_date_expiring_soon(expiration, days=30)
            if is_expiring:
                result.add_warning(f"State ID is expiring soon (expiration date: {expiration})")
    
    # Issue date logic
    issue_date = extracted.get("issueDate")
    if issue_date and expiration:
        valid, msg = validate_date_logic(issue_date, expiration)
        if not valid:
            result.add_error(msg)
    
    # Age validation
    dob = extracted.get("dob")
    if dob:
        valid, msg = validate_age_from_dob(dob, min_age=0, max_age=120)
        if not valid:
            result.add_error(msg)
    
    # State validation
    state = extracted.get("issuingState") or extracted.get("state")
    if state:
        valid, msg = validate_state_code(state)
        if not valid:
            result.add_warning(msg)
    
    return result


def validate_passport(extracted: Dict[str, Any]) -> ValidationResult:
    """Validate Passport fields."""
    result = ValidationResult()
    
    # Required fields
    required_fields = ["passportNumber", "firstName", "lastName", "dateOfBirth", "expirationDate", "issuingCountry"]
    for field in required_fields:
        if not extracted.get(field):
            result.add_error(f"Required field missing: {field}")
    
    # Expiration date check
    expiration = extracted.get("expirationDate")
    if expiration:
        is_expired, _ = is_date_expired(expiration)
        if is_expired:
            result.add_error(f"Passport is expired (expiration date: {expiration})")
        else:
            # Warn if expiring within 6 months (many countries require 6 months validity)
            is_expiring, _ = is_date_expiring_soon(expiration, days=180)
            if is_expiring:
                result.add_warning(f"Passport is expiring soon (expiration date: {expiration}). Many countries require 6 months validity.")
    
    # Issue date logic
    issue_date = extracted.get("issueDate")
    if issue_date and expiration:
        valid, msg = validate_date_logic(issue_date, expiration)
        if not valid:
            result.add_error(msg)
    
    # Age validation
    dob = extracted.get("dateOfBirth")
    if dob:
        valid, msg = validate_age_from_dob(dob, min_age=0, max_age=120)
        if not valid:
            result.add_error(msg)
    
    # Passport number format (basic check - should not be empty and reasonable length)
    passport_num = extracted.get("passportNumber")
    if passport_num:
        passport_str = str(passport_num).strip()
        if len(passport_str) < 6 or len(passport_str) > 12:
            result.add_warning(f"Passport number length seems unusual: {passport_num}")
    
    return result


def validate_social_security_card(extracted: Dict[str, Any]) -> ValidationResult:
    """Validate Social Security Card fields."""
    result = ValidationResult()
    
    # Required fields
    required_fields = ["firstName", "lastName"]
    for field in required_fields:
        if not extracted.get(field):
            result.add_error(f"Required field missing: {field}")
    
    # SSN validation
    ssn = extracted.get("socialSecurityNumber") or extracted.get("number")
    if ssn:
        valid, msg = validate_ssn_format(ssn)
        if not valid:
            result.add_error(msg)
    else:
        result.add_error("Social Security Number is missing")
    
    return result


def validate_birth_certificate(extracted: Dict[str, Any]) -> ValidationResult:
    """Validate Birth Certificate fields."""
    result = ValidationResult()
    
    # Required fields
    required_fields = ["firstName", "lastName", "dateOfBirth", "stateOfBirth"]
    for field in required_fields:
        if not extracted.get(field):
            result.add_error(f"Required field missing: {field}")
    
    # Date of birth validation
    dob = extracted.get("dateOfBirth")
    if dob:
        parsed_dob = parse_date(dob)
        if parsed_dob:
            # Birth date should not be in the future
            if parsed_dob > datetime.now():
                result.add_error(f"Date of birth cannot be in the future: {dob}")
            
            # Age validation (reasonable range)
            valid, msg = validate_age_from_dob(dob, min_age=0, max_age=120)
            if not valid:
                result.add_error(msg)
    
    # State validation
    state = extracted.get("stateOfBirth")
    if state:
        valid, msg = validate_state_code(state)
        if not valid:
            result.add_warning(msg)
    
    return result


def validate_permanent_resident_card(extracted: Dict[str, Any]) -> ValidationResult:
    """Validate Permanent Resident Card (Green Card) fields."""
    result = ValidationResult()
    
    # Required fields
    required_fields = ["firstName", "lastName", "dateOfBirth", "alienNumber", "cardNumber", "expirationDate"]
    for field in required_fields:
        if not extracted.get(field):
            result.add_error(f"Required field missing: {field}")
    
    # Expiration date check
    expiration = extracted.get("expirationDate")
    if expiration:
        is_expired, _ = is_date_expired(expiration)
        if is_expired:
            result.add_error(f"Green Card is expired (expiration date: {expiration})")
        else:
            is_expiring, _ = is_date_expiring_soon(expiration, days=180)
            if is_expiring:
                result.add_warning(f"Green Card is expiring soon (expiration date: {expiration})")
    
    # Age validation
    dob = extracted.get("dateOfBirth")
    if dob:
        valid, msg = validate_age_from_dob(dob, min_age=0, max_age=120)
        if not valid:
            result.add_error(msg)
    
    return result


def validate_employment_authorization(extracted: Dict[str, Any]) -> ValidationResult:
    """Validate Employment Authorization Document (EAD) fields."""
    result = ValidationResult()
    
    # Required fields
    required_fields = ["firstName", "lastName", "dateOfBirth", "cardNumber", "expirationDate"]
    for field in required_fields:
        if not extracted.get(field):
            result.add_error(f"Required field missing: {field}")
    
    # Expiration date check
    expiration = extracted.get("expirationDate")
    if expiration:
        is_expired, _ = is_date_expired(expiration)
        if is_expired:
            result.add_error(f"Employment Authorization Document is expired (expiration date: {expiration})")
        else:
            is_expiring, _ = is_date_expiring_soon(expiration, days=90)
            if is_expiring:
                result.add_warning(f"Employment Authorization Document is expiring soon (expiration date: {expiration})")
    
    # Age validation
    dob = extracted.get("dateOfBirth")
    if dob:
        valid, msg = validate_age_from_dob(dob, min_age=0, max_age=120)
        if not valid:
            result.add_error(msg)
    
    return result


def validate_military_id(extracted: Dict[str, Any]) -> ValidationResult:
    """Validate Military ID fields."""
    result = ValidationResult()
    
    # Required fields
    required_fields = ["firstName", "lastName", "dateOfBirth", "branch", "expirationDate"]
    for field in required_fields:
        if not extracted.get(field):
            result.add_error(f"Required field missing: {field}")
    
    # Expiration date check
    expiration = extracted.get("expirationDate")
    if expiration:
        is_expired, _ = is_date_expired(expiration)
        if is_expired:
            result.add_error(f"Military ID is expired (expiration date: {expiration})")
        else:
            is_expiring, _ = is_date_expiring_soon(expiration, days=60)
            if is_expiring:
                result.add_warning(f"Military ID is expiring soon (expiration date: {expiration})")
    
    # Age validation
    dob = extracted.get("dateOfBirth")
    if dob:
        valid, msg = validate_age_from_dob(dob, min_age=17, max_age=120)
        if not valid:
            result.add_error(msg)
    
    return result


def validate_generic_identity(extracted: Dict[str, Any]) -> ValidationResult:
    """Generic validation for identity documents."""
    result = ValidationResult()
    
    # Basic required fields
    if not extracted.get("firstName") and not extracted.get("lastName"):
        result.add_warning("Name information is missing or incomplete")
    
    # Check for expiration if present
    expiration = extracted.get("expirationDate")
    if expiration:
        is_expired, _ = is_date_expired(expiration)
        if is_expired:
            result.add_warning(f"Document appears to be expired (expiration date: {expiration})")
    
    # DOB validation if present
    dob = extracted.get("dob") or extracted.get("dateOfBirth")
    if dob:
        valid, msg = validate_age_from_dob(dob, min_age=0, max_age=120)
        if not valid:
            result.add_warning(msg)
    
    return result


# ============================================================================
# Main Validation Router
# ============================================================================

VALIDATION_FUNCTIONS = {
    "driving_license": validate_driving_license,
    "mobile_drivers_license": validate_driving_license,  # Same rules as regular DL
    "state_id": validate_state_id,
    "real_id": validate_state_id,  # Same rules as state ID
    "passport": validate_passport,
    "passport_card": validate_passport,  # Similar rules to passport
    "social_security_card": validate_social_security_card,
    "birth_certificate": validate_birth_certificate,
    "permanent_resident_card": validate_permanent_resident_card,
    "employment_authorization_document": validate_employment_authorization,
    "military_id": validate_military_id,
    "veteran_id": validate_military_id,  # Similar rules
    # Add more as needed
}


def validate_document(doc_type: str, extracted: Dict[str, Any]) -> ValidationResult:
    """
    Route to appropriate validator based on document type.
    """
    doc_type_lower = (doc_type or "").lower().strip()
    
    # Get specific validator or use generic
    validator = VALIDATION_FUNCTIONS.get(doc_type_lower, validate_generic_identity)
    
    return validator(extracted)


# ============================================================================
# Main Node Function
# ============================================================================

def ValidationCheck(state: PipelineState) -> PipelineState:
    """
    Validate extracted document fields against document-specific rules.
    
    This node checks:
    - Required field presence
    - Expiration dates
    - Format validations
    - Logical validations
    - Age validations
    
    Documents with validation issues are flagged for human verification
    but still proceed through the pipeline and are saved to S3.
    """
    if state.extraction is None:
        raise ValueError("Extraction state missing; run Extraction node first.")
    
    log_agent_event(state, "Validation Check", "start")
    
    print("\n" + "=" * 80)
    print("🔍 VALIDATION CHECK")
    print("=" * 80)
    
    # Get document type and extracted data
    doc_type = ""
    if state.classification:
        doc_type = state.classification.detected_doc_type or ""
    
    extracted = state.extraction.extracted or {}
    
    print(f"Document Type: {doc_type}")
    print(f"Extracted Fields: {len(extracted)}")
    
    # Perform validation
    validation_result = validate_document(doc_type, extracted)
    
    # Display results
    print("\n" + "-" * 80)
    if validation_result.passed:
        print("✅ VALIDATION PASSED")
        print("All validation checks passed successfully.")
    else:
        print("⚠️  VALIDATION ISSUES DETECTED")
        print("Document requires human verification.")
        print("\nIssues found:")
        for msg in validation_result.get_all_messages():
            print(f"  {msg}")
    
    print("-" * 80)
    
    # Update state - store validation results
    if not hasattr(state, 'validation'):
        # Add validation results to state (we'll store in extraction for now)
        state.extraction.message = "Validation completed"
    
    # Update database with validation results
    try:
        from datetime import datetime, timezone
        import json
        from ..tools.db import update_tblaigents_by_keys
        
        # Determine document status
        if validation_result.passed:
            document_status = "pass"
            doc_verification_result = {
                "status": "pass",
                "message": "Document validation passed. All checks completed successfully.",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        else:
            document_status = "human verification needed"
            doc_verification_result = {
                "status": "human_verification_needed",
                "message": "Document requires human verification due to validation issues.",
                "validation_issues": {
                    "errors": validation_result.errors,
                    "warnings": validation_result.warnings,
                    "info": validation_result.info
                },
                "suggestions": [
                    "Please review the validation issues listed above",
                    "Verify that all required information is present and accurate",
                    "Check expiration dates and ensure document is current",
                    "Contact support if you believe this is an error"
                ],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        
        # Update database
        if state.ingestion:
            update_tblaigents_by_keys(
                FPCID=state.ingestion.FPCID,
                checklistId=state.ingestion.checklistId,
                updates={
                    "document_status": document_status,
                    "doc_verification_result": json.dumps(doc_verification_result),
                    "cross_validation": not validation_result.passed,  # Flag for human review if issues
                },
                document_name=state.ingestion.document_name,
                LMRId=state.ingestion.LMRId,
            )
            print(f"\n[✓] Database updated with validation status: {document_status}")
            
    except Exception as e:
        print(f"\n[WARN] Failed to update database with validation results: {e}")
    
    log_agent_event(state, "Validation Check", "completed", {
        "passed": validation_result.passed,
        "errors": len(validation_result.errors),
        "warnings": len(validation_result.warnings),
        "document_type": doc_type
    })
    
    print("=" * 80 + "\n")
    
    return state

