"""
Field validation logic for cross validation system.
"""

from typing import Dict, List, Optional, Any

from ..models.data_models import BorrowerData, DocumentDetails, FieldMatch, VerificationReport


# Field mapping: canonical_name -> list of possible aliases in S3 documents
FIELD_ALIASES = {
    # Borrower first name
    "borrowerFName": ["firstName", "first_name", "firstname", "borrowerFName", "borrowerfname", "name"],
    # Borrower middle name
    "borrowerMName": ["middleName", "middle_name", "middlename", "borrowerMName", "borrowermname"],
    # Borrower last name
    "borrowerLName": ["lastName", "last_name", "lastname", "borrowerLName", "borrowerlname"],
    # Borrower DOB
    "borrowerDOB": ["dob", "dateOfBirth", "date_of_birth", "birthdate", "borrowerDOB", "borrowerdob"],
    # Borrower POB
    "borrowerPOB": ["pob", "placeOfBirth", "place_of_birth", "birthplace", "borrowerPOB", "borrowerpob"],
    # Driver license number
    "driverLicenseNumber": ["idNumber", "id_number", "licenseNumber", "license_number", "driverLicenseNumber", "dl_number"],
    # Driver license state
    "driverLicenseState": ["issuingState", "issuing_state", "state", "driverLicenseState", "license_state"],
    # Co-borrower first name
    "coBorrowerFName": ["coBorrowerFName", "coborrowerFirstName", "co_borrower_first_name"],
    # Co-borrower middle name
    "coBorrowerMName": ["coBorrowerMName", "coborrowerMiddleName", "co_borrower_middle_name"],
    # Co-borrower last name
    "coBorrowerLName": ["coBorrowerLName", "coborrowerLastName", "co_borrower_last_name"],
    # Co-borrower DOB
    "coborrowerDOB": ["coborrowerDOB", "coBorrowerDOB", "co_borrower_dob"],
    # Co-borrower POB
    "coborrowerPOB": ["coborrowerPOB", "coBorrowerPOB", "co_borrower_pob"],
    # Co-borrower driver license number
    "coBorDriverLicenseNumber": ["coBorDriverLicenseNumber", "co_borrower_license_number"],
    # Co-borrower driver license state
    "coBorDriverLicenseState": ["coBorDriverLicenseState", "co_borrower_license_state"],
}


def normalize_value(value: Any) -> Optional[str]:
    """Normalize a value for comparison."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in {"", "n/a", "na", "none", "null", "not provided"}:
        return None
    # Remove common punctuation for name/ID comparison
    # For dates, try to standardize format
    return s.upper()


def extract_field_from_document(doc_data: Optional[Dict[str, Any]], canonical_field: str) -> Optional[str]:
    """
    Extract a canonical field from document data by checking all possible aliases.
    Returns normalized value or None if not found.
    """
    if not doc_data:
        return None
    
    aliases = FIELD_ALIASES.get(canonical_field, [canonical_field])
    
    # Flatten nested dict if needed
    def flatten_dict(d, parent_key=''):
        items = []
        if isinstance(d, dict):
            for k, v in d.items():
                new_key = f"{parent_key}.{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key))
                else:
                    items.append((new_key, v))
        return items
    
    flat_data = dict(flatten_dict(doc_data))
    
    # Try to find matching field (case-insensitive)
    for alias in aliases:
        for key, value in flat_data.items():
            if key.lower() == alias.lower():
                normalized = normalize_value(value)
                if normalized:
                    return normalized
    
    return None


def extract_fields_from_borrower_data(borrower: BorrowerData) -> Dict[str, Optional[str]]:
    """Extract and normalize all fields from BorrowerData."""
    return {
        "borrowerFName": normalize_value(borrower.borrowerFName),
        "borrowerMName": normalize_value(borrower.borrowerMName),
        "borrowerLName": normalize_value(borrower.borrowerLName),
        "borrowerDOB": normalize_value(borrower.borrowerDOB),
        "borrowerPOB": normalize_value(borrower.borrowerPOB),
        "driverLicenseNumber": normalize_value(borrower.driverLicenseNumber),
        "driverLicenseState": normalize_value(borrower.driverLicenseState),
        "coBorrowerFName": normalize_value(borrower.coBorrowerFName),
        "coBorrowerMName": normalize_value(borrower.coBorrowerMName),
        "coBorrowerLName": normalize_value(borrower.coBorrowerLName),
        "coborrowerDOB": normalize_value(borrower.coborrowerDOB),
        "coborrowerPOB": normalize_value(borrower.coborrowerPOB),
        "coBorDriverLicenseNumber": normalize_value(borrower.coBorDriverLicenseNumber),
        "coBorDriverLicenseState": normalize_value(borrower.coBorDriverLicenseState),
    }


def cross_validate_fields(
    documents: List[DocumentDetails],
    borrower_data: Optional[BorrowerData]
) -> List[FieldMatch]:
    """
    Cross-validate all relevant fields across S3 documents and DB data.
    Returns a list of FieldMatch objects.
    """
    canonical_fields = list(FIELD_ALIASES.keys())
    field_matches: List[FieldMatch] = []
    
    for canonical_field in canonical_fields:
        field_match = FieldMatch(field_name=canonical_field)
        
        # Extract from DB if available
        if borrower_data:
            db_fields = extract_fields_from_borrower_data(borrower_data)
            db_value = db_fields.get(canonical_field)
            if db_value:
                field_match.sources["DB (tblfile)"] = db_value
        
        # Extract from each S3 document
        for doc in documents:
            doc_value = extract_field_from_document(doc.verified_details, canonical_field)
            if doc_value:
                source_name = f"S3 ({doc.document_name})"
                field_match.sources[source_name] = doc_value
        
        # Check if field has any values
        if not field_match.sources:
            # Field not found in any source - skip it
            continue
        
        # Check for mismatches
        unique_values = set(field_match.sources.values())
        if len(unique_values) > 1:
            field_match.matched = False
            values_str = ", ".join([f"{src}={val}" for src, val in field_match.sources.items()])
            field_match.mismatch_reason = f"Conflicting values found: {values_str}"
        else:
            field_match.matched = True
        
        field_matches.append(field_match)
    
    return field_matches


def generate_verification_report(
    FPCID: str,
    LMRId: str,
    documents: List[DocumentDetails],
    borrower_data: Optional[BorrowerData],
    field_matches: List[FieldMatch]
) -> VerificationReport:
    """Generate a complete verification report."""
    
    # Count matches and mismatches
    total_fields_checked = len(field_matches)
    matched_fields = [fm for fm in field_matches if fm.matched]
    mismatched_fields = [fm for fm in field_matches if not fm.matched]
    
    # Determine overall status
    if not mismatched_fields:
        overall_status = "VERIFIED"
        summary = f"✅ All {total_fields_checked} fields matched across all sources."
    elif len(mismatched_fields) < len(matched_fields):
        overall_status = "PARTIAL"
        summary = f"⚠️ {len(matched_fields)}/{total_fields_checked} fields matched. {len(mismatched_fields)} mismatches found."
    else:
        overall_status = "FAILED"
        summary = f"❌ Verification failed: {len(mismatched_fields)}/{total_fields_checked} fields have mismatches."
    
    # Generate detailed findings
    detailed_findings = []
    
    if borrower_data:
        detailed_findings.append("✓ Database record found in tblfile")
    else:
        detailed_findings.append("ℹ️ No database record found in tblfile - validation based on S3 documents only")
    
    detailed_findings.append(f"✓ {len(documents)} document(s) processed from S3")
    detailed_findings.append(f"✓ {total_fields_checked} fields checked for consistency")
    
    if matched_fields:
        detailed_findings.append(f"\n✅ MATCHED FIELDS ({len(matched_fields)}):")
        for fm in matched_fields:
            value = list(fm.sources.values())[0]  # All values are same, take first
            sources_list = ", ".join(fm.sources.keys())
            detailed_findings.append(f"  • {fm.field_name} = '{value}' (Sources: {sources_list})")
    
    if mismatched_fields:
        detailed_findings.append(f"\n❌ MISMATCHED FIELDS ({len(mismatched_fields)}):")
        for fm in mismatched_fields:
            detailed_findings.append(f"  • {fm.field_name}:")
            detailed_findings.append(f"    Reason: {fm.mismatch_reason}")
            for source, value in fm.sources.items():
                detailed_findings.append(f"    - {source}: '{value}'")
    
    return VerificationReport(
        FPCID=FPCID,
        LMRId=LMRId,
        db_data_available=borrower_data is not None,
        documents=documents,
        field_matches=field_matches,
        overall_status=overall_status,
        summary=summary,
        detailed_findings=detailed_findings
    )
