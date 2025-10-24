"""
Extraction node for field extraction and data cleaning.
Deterministic GPT extraction + strict post-clean:
- Removes empty/missing fields from final JSON
- Deduplicates common aliases (dob/dateOfBirth, issueDate/dateIssued)
- Dynamic S3 key path (FPCID, LMRId, UTC date)
"""

import os
import json
from datetime import datetime, timezone
from typing import Dict, Any

from openai import OpenAI

from ..config.state_models import PipelineState, ExtractionState
from ..utils.helpers import log_agent_event
from ..tools.aws_services import get_s3_client
from ..tools.db import update_tblaigents_by_keys


# ----------------------------------------------------------------------
# Document field definitions
# ----------------------------------------------------------------------
DOC_FIELDS = {
    # Core Identity Documents
    "driving_license": [
        "firstName","middleName","lastName","suffix","dob",
        "addressLine1","addressLine2","city","state","zip",
        "countryName","expirationDate","idNumber","licenseNumber","issuingState","issueDate",
        "class","restrictions","endorsements"
    ],
    "mobile_drivers_license": [
        "firstName","middleName","lastName","suffix","dob",
        "licenseNumber","issuingState","issueDate","expirationDate",
        "digitalSignature","qrCode","mobileAppProvider"
    ],
    "state_id": [
        "firstName","middleName","lastName","suffix","dob",
        "addressLine1","addressLine2","city","state","zip",
        "countryName","expirationDate","idNumber","issuingState","issueDate"
    ],
    "real_id": [
        "firstName","middleName","lastName","suffix","dob",
        "addressLine1","addressLine2","city","state","zip",
        "countryName","expirationDate","idNumber","issuingState","issueDate","realIdCompliant"
    ],
    
    # Passport Documents
    "passport": [
        "passportNumber","firstName","middleName","lastName","suffix",
        "issuingCountry","dateOfBirth","issueDate","expirationDate",
        "placeOfBirth","nationality","sex"
    ],
    "passport_card": [
        "passportCardNumber","firstName","middleName","lastName","suffix",
        "issuingCountry","dateOfBirth","issueDate","expirationDate",
        "placeOfBirth","nationality","sex"
    ],
    
    # Birth and Vital Records
    "birth_certificate": [
        "firstName","middleName","lastName","dateOfBirth","stateOfBirth","dateIssued",
        "certificateNumber","registrarSignature","sealOfState"
    ],
    "marriage_certificate": [
        "spouseName1","spouseName2","marriageDate","marriagePlace",
        "certificateNumber","issuingOffice","officiantName","witnessNames"
    ],
    "divorce_decree": [
        "petitionerName","respondentName","divorceDate","courtName",
        "caseNumber","judgeName","finalDecreeDate"
    ],
    
    # Social Security
    "social_security_card": [
        "firstName","middleName","lastName","suffix","socialSecurityNumber","number"
    ],
    
    # Immigration Documents
    "permanent_resident_card": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "alienNumber","cardNumber","categoryCode","countryOfBirth",
        "issuingCountry","expirationDate","residentSince"
    ],
    "certificate_of_naturalization": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "certificateNumber","dateOfNaturalization","placeOfNaturalization",
        "formerNationality","issuingOffice"
    ],
    "certificate_of_citizenship": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "certificateNumber","dateOfCitizenship","placeOfBirth",
        "issuingOffice","parentCitizenship"
    ],
    "employment_authorization_document": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "alienNumber","cardNumber","categoryCode","countryOfBirth",
        "expirationDate","employmentAuthorized"
    ],
    "form_i94": [
        "firstName","middleName","lastName","admissionNumber",
        "dateOfArrival","dateOfDeparture","portOfEntry","classOfAdmission"
    ],
    "us_visa": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "visaNumber","visaType","issueDate","expirationDate",
        "issuingPost","nationality","passportNumber"
    ],
    "reentry_permit": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "permitNumber","issueDate","expirationDate","alienNumber"
    ],
    
    # Military and Government IDs
    "military_id": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "serviceNumber","rank","branch","issueDate","expirationDate",
        "bloodType","sponsor"
    ],
    "veteran_id": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "veteranIdNumber","issueDate","expirationDate","branch","serviceYears"
    ],
    "tribal_id": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "tribalIdNumber","tribeName","issueDate","expirationDate","bloodQuantum"
    ],
    "global_entry_card": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "passId","membershipNumber","issueDate","expirationDate"
    ],
    "tsa_precheck_card": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "knownTravelerNumber","issueDate","expirationDate"
    ],
    "voter_registration": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "voterIdNumber","registrationDate","politicalParty","precinct",
        "addressLine1","addressLine2","city","state","zip"
    ],
    
    # Professional and Educational
    "professional_license": [
        "firstName","middleName","lastName","suffix","licenseNumber",
        "licenseType","profession","issueDate","expirationDate",
        "issuingState","issuingBoard"
    ],
    "student_id": [
        "firstName","middleName","lastName","suffix","studentId",
        "institution","program","issueDate","expirationDate","academicYear"
    ],
    
    # Financial and Proof Documents
    "utility_bill": [
        "utilityBillType","serviceProvider","serviceProviderAddress",
        "accountHolderBillingName","accountHolderBillingAddress",
        "serviceAddress","accountNumber","billDate","dueDate",
        "periodStartDate","periodEndDate","amountDue"
    ],
    "lease_agreement": [
        "tenantName","landlordName","propertyAddress","leaseStartDate",
        "leaseEndDate","monthlyRent","securityDeposit","signatureDate"
    ],
    "bank_statement": [
        "accountHolderName","bankName","accountNumber","routingNumber",
        "statementDate","statementPeriod","accountType","balance"
    ],
    "insurance_card": [
        "policyHolderName","policyNumber","groupNumber","insuranceCompany",
        "effectiveDate","expirationDate","coverageType","dependents"
    ],
    "voided_check": [
        "accountHolderName","bankName","bankAddress","accountNumber",
        "routingNumber","checkNumber","checkDate"
    ],
    "direct_deposit": [
        "employeeName","employeeAddress","employeePhoneNumber","employeeSocialSecurityNumber",
        "bankName","bankAddress","accountNumber","routingNumber","typeOfAccount",
        "partneringInstitutionName","employeeSignature","employeeSignatureDate"
    ],
    
    # International and Digital IDs
    "consular_id": [
        "firstName","middleName","lastName","suffix","dateOfBirth",
        "consularIdNumber","issuingConsulate","nationality","issueDate","expirationDate"
    ],
    "digital_id": [
        "firstName","middleName","lastName","suffix","digitalIdNumber",
        "platform","issueDate","expirationDate","verificationLevel"
    ],
    
    # Visa Types (including H1B)
    "h1b_visa": [
        "firstName","middleName","lastName","suffix",
        "dateOfBirth","issuingCountry","expirationDate","visaNumber","petitionNumber"
    ],
    
    # Generic identity document for unknown types
    "identity_document": [
        "firstName","middleName","lastName","suffix","dob","dateOfBirth",
        "address","addressLine1","addressLine2","city","state","zip","country",
        "idNumber","documentNumber","passportNumber","licenseNumber",
        "issueDate","expirationDate","dateIssued","issuingAuthority",
        "nationality","placeOfBirth","sex","height","weight","eyeColor","hairColor"
    ],
    
    # Generic fallback
    "other": [
        "firstName","middleName","lastName","suffix",
        "dob","dateOfBirth","address","city","state","zip","country",
        "idNumber","passportNumber","accountNumber","ssn",
        "issueDate","expirationDate","dateIssued","residentSince","documentType"
    ],
}


# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------
_EMPTY_STRINGS = {"", "n/a", "na", "none", "null"}  # case-insensitive

def _is_empty_value(v: Any) -> bool:
    """Treat empty strings/whitespace/None/empty collections as empty."""
    if v is None:
        return True
    if isinstance(v, str):
        s = v.strip()
        return s == "" or s.lower() in _EMPTY_STRINGS
    if isinstance(v, (list, dict, set, tuple)):
        return len(v) == 0
    return False

# Alias groups where values represent the same concept; keep only one.
# Order = priority (first wins if both have values).
_ALIAS_GROUPS = [
    ("dob", "dateOfBirth"),
    ("issueDate", "dateIssued"),
]

def _normalize_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    """Lowercase keys for case-insensitive matching (values preserved)."""
    if not isinstance(d, dict):
        return d
    return {k.lower(): _normalize_keys(v) for k, v in d.items()}

def _postprocess_complete_schema(cleaned: Dict[str, Any], fields: list[str]) -> Dict[str, Any]:
    """Ensure all expected fields exist (with empty string if missing)."""
    return {f: cleaned.get(f, "") for f in fields}

def _dedupe_aliases(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Collapse alias groups so only the first non-empty key in each group remains.
    Example: if dob and dateOfBirth both have values, keep dob (first), drop dateOfBirth.
    """
    result = dict(data)
    for group in _ALIAS_GROUPS:
        # find first non-empty in group
        keep_key = None
        keep_val = None
        for k in group:
            if k in result and not _is_empty_value(result[k]):
                keep_key = k
                keep_val = result[k]
                break
        # remove others
        for k in group:
            if k in result:
                if k != keep_key:
                    result.pop(k, None)
        # ensure the kept one is present (if any value existed)
        if keep_key is not None:
            result[keep_key] = keep_val
    return result

def _drop_empty_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    """Remove fields whose value is empty per _is_empty_value."""
    return {k: v for k, v in data.items() if not _is_empty_value(v)}

def _validate_extracted_fields(extracted: Dict[str, Any], expected_fields: list[str], doc_type: str) -> Dict[str, Any]:
    """
    Validate that extracted fields match expected fields for the document type.
    Remove any unexpected fields and ensure only valid fields are present.
    """
    validated = {}
    unexpected_fields = []
    
    for key, value in extracted.items():
        if key in expected_fields:
            validated[key] = value
        else:
            unexpected_fields.append(key)
    
    if unexpected_fields:
        print(f"[WARN] Removed unexpected fields for {doc_type}: {unexpected_fields}")
    
    return validated

def _fallback_extraction(normalized_input: Dict[str, Any], fields: list[str]) -> Dict[str, Any]:
    """
    Fallback extraction method when GPT fails.
    Performs case-insensitive field matching from normalized input.
    """
    flat = {k.lower(): v for k, v in normalized_input.items()}
    result = {}
    
    for field in fields:
        field_lower = field.lower()
        # Try exact match first
        if field_lower in flat:
            result[field] = flat[field_lower]
        else:
            # Try common variations
            variations = [
                field_lower.replace("_", ""),
                field_lower.replace("_", " "),
                field_lower.replace(" ", ""),
                field_lower.replace(" ", "_"),
            ]
            
            found = False
            for variation in variations:
                if variation in flat and not _is_empty_value(flat[variation]):
                    result[field] = flat[variation]
                    found = True
                    break
            
            if not found:
                result[field] = ""
    
    return result


# ----------------------------------------------------------------------
# GPT Extraction
# ----------------------------------------------------------------------
def extract_fields_with_gpt(input_json: Dict[str, Any], doc_type: str) -> Dict[str, Any]:
    """
    Use GPT to strictly extract only the fields defined for the specific document type.
    Ensures proper field extraction according to DOC_FIELDS for verification purposes.
    """
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    
    # Validate doc_type and get corresponding fields
    doc_type_key = (doc_type or "").lower()
    if doc_type_key not in DOC_FIELDS:
        print(f"[WARN] Unknown document type '{doc_type}', using 'other' fields")
        doc_type_key = "other"
    
    fields = DOC_FIELDS[doc_type_key]
    normalized_input = _normalize_keys(input_json)

    prompt = f"""
You are a strict document field extractor for {doc_type.upper()} documents.

CRITICAL REQUIREMENTS:
1. Document Type: {doc_type}
2. ONLY extract these exact fields: {fields}
3. Match field names case-insensitively from the input
4. If a required field is missing or empty, set it to ""
5. DO NOT include any fields not in the required list
6. DO NOT add extra keys, comments, or metadata
7. Return ONLY a valid JSON object with the specified fields

Required Fields for {doc_type}:
{json.dumps(fields, indent=2)}

Input OCR Data:
{json.dumps(normalized_input, indent=2, ensure_ascii=False)}

Extract ONLY the required fields listed above. Ignore all other data.
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            temperature=0,
            messages=[
                {
                    "role": "system", 
                    "content": f"You are a strict field extractor. Return ONLY a JSON object containing exactly these fields: {fields}. No extra fields, no comments, no explanations."
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        raw = json.loads(resp.choices[0].message.content)
        
        # Validate that GPT only returned expected fields
        raw = _validate_extracted_fields(raw, fields, doc_type)
        
    except Exception as e:
        # Safe local fallback if the model fails
        print(f"[WARN] GPT extraction failed for {doc_type}, using fallback mode: {e}")
        raw = _fallback_extraction(normalized_input, fields)

    # 1) Ensure full schema (all required fields present)
    complete = _postprocess_complete_schema(raw, fields)
    # 2) Dedupe alias keys (keep priority key only)
    deduped = _dedupe_aliases(complete)
    # 3) Drop empty values for clean output
    final_clean = _drop_empty_fields(deduped)

    return final_clean


# ----------------------------------------------------------------------
# Helper: map document display/name to doc_type
# ----------------------------------------------------------------------
def _map_document_name_to_doc_type(name: str, classification_type: str = None, fallback: str = "identity_document") -> str:
    """
    Map document name to doc_type with comprehensive coverage of all U.S. identity documents.
    Uses both document name and classification type for better detection.
    """
    n = (name or "").strip().lower()
    c = (classification_type or "").strip().lower()
    
    # Check both name and classification for better accuracy
    combined = f"{n} {c}".strip()
    
    # Core Identity Documents (most specific first)
    if ("driver" in combined and "license" in combined) or "dl" in combined or "driving_license" in combined:
        if "mobile" in combined or "mdl" in combined:
            return "mobile_drivers_license"
        return "driving_license"
    if ("state" in combined and ("id" in combined or "identification" in combined)) and "driver" not in combined:
        if "real" in combined:
            return "real_id"
        return "state_id"
    
    # Passport Documents
    if "passport" in combined and "card" in combined:
        return "passport_card"
    if "passport" in combined and "card" not in combined:
        return "passport"
    
    # Birth and Vital Records
    if ("birth" in combined and "certificate" in combined) or "birth_cert" in combined:
        return "birth_certificate"
    if ("marriage" in combined and "certificate" in combined) or "marriage_cert" in combined:
        return "marriage_certificate"
    if ("divorce" in combined and ("decree" in combined or "certificate" in combined)):
        return "divorce_decree"
    
    # Social Security
    if ("social" in combined and "security" in combined) or "ssn" in combined or "ss_card" in combined:
        return "social_security_card"
    
    # Immigration Documents
    if ("permanent" in combined and "resident" in combined) or "green_card" in combined or "prc" in combined or "i-551" in combined:
        return "permanent_resident_card"
    if ("naturalization" in combined and "certificate" in combined) or "n-550" in combined or "n-570" in combined:
        return "certificate_of_naturalization"
    if ("citizenship" in combined and "certificate" in combined) or "n-560" in combined or "n-561" in combined:
        return "certificate_of_citizenship"
    if ("employment" in combined and "authorization" in combined) or "ead" in combined or "i-766" in combined:
        return "employment_authorization_document"
    if "i-94" in combined or ("arrival" in combined and "departure" in combined):
        return "form_i94"
    if ("visa" in combined and ("us" in combined or "american" in combined)) or "h1b" in combined or "h-1b" in combined:
        if "h1b" in combined or "h-1b" in combined:
            return "h1b_visa"
        return "us_visa"
    if ("reentry" in combined and "permit" in combined) or "i-327" in combined:
        return "reentry_permit"
    
    # Military and Government IDs
    if ("military" in combined and "id" in combined) or "cac" in combined or "common_access" in combined:
        return "military_id"
    if ("veteran" in combined and "id" in combined) or "vic" in combined:
        return "veteran_id"
    if ("tribal" in combined and "id" in combined) or "tribal_card" in combined:
        return "tribal_id"
    if ("global" in combined and "entry" in combined) or "nexus" in combined:
        return "global_entry_card"
    if ("tsa" in combined and "precheck" in combined) or "precheck" in combined:
        return "tsa_precheck_card"
    if ("voter" in combined and ("registration" in combined or "card" in combined)):
        return "voter_registration"
    
    # Professional and Educational
    if ("professional" in combined and "license" in combined) or ("license" in combined and any(prof in combined for prof in ["medical", "legal", "contractor", "nursing", "teaching"])):
        return "professional_license"
    if ("student" in combined and "id" in combined) or "student_card" in combined:
        return "student_id"
    
    # Financial and Proof Documents
    if ("utility" in combined and "bill" in combined) or any(util in combined for util in ["electric", "gas", "water", "internet", "cable"]):
        return "utility_bill"
    if ("lease" in combined and "agreement" in combined) or "rental_agreement" in combined:
        return "lease_agreement"
    if ("bank" in combined and "statement" in combined) or "account_statement" in combined:
        return "bank_statement"
    if ("insurance" in combined and "card" in combined) or any(ins in combined for ins in ["health_insurance", "auto_insurance"]):
        return "insurance_card"
    if ("voided" in combined and "check" in combined) or "void_check" in combined:
        return "voided_check"
    if ("direct" in combined and "deposit" in combined) or "dd_form" in combined:
        return "direct_deposit"
    
    # Consular and International
    if ("consular" in combined and "id" in combined) or "matricula" in combined:
        return "consular_id"
    
    # Digital IDs
    if ("digital" in combined and "id" in combined) or any(platform in combined for platform in ["id.me", "login.gov"]):
        return "digital_id"
    
    # If it's any kind of identity document but we can't determine the specific type
    if any(identity_term in combined for identity_term in ["id", "identification", "license", "card", "certificate", "document"]):
        return "identity_document"
    
    # Final fallback
    return fallback


# ----------------------------------------------------------------------
# Main pipeline node
# ----------------------------------------------------------------------
def Extract(state: PipelineState) -> PipelineState:
    if state.classification is None or state.ocr is None:
        raise ValueError("OCR or Classification missing; run previous nodes first.")

    log_agent_event(state, "Document Data Extraction", "start")
    message = state.classification.message

    document_name = state.ocr.document_name or ""
    classification_type = state.classification.detected_doc_type or ""
    mapped_doc_type = _map_document_name_to_doc_type(
        document_name, 
        classification_type=classification_type, 
        fallback="identity_document"
    )
    
    # Log document type detection for debugging
    print(f"[INFO] Document detection - Name: '{document_name}', Classification: '{classification_type}', Final Type: '{mapped_doc_type}'")

    ocr_struct = state.ocr.ocr_json or {}
    page1 = ocr_struct.get("1") if isinstance(ocr_struct, dict) else None
    input_for_cleaner = page1 if isinstance(page1, dict) else ocr_struct

    # --- field extraction (strict clean) ---
    try:
        cleaned = extract_fields_with_gpt(input_for_cleaner, mapped_doc_type)
        
        # Validate that we have the expected fields for this document type
        expected_fields = DOC_FIELDS.get(mapped_doc_type.lower(), DOC_FIELDS["other"])
        extracted_fields = list(cleaned.keys())
        
        print(f"[INFO] Expected fields for {mapped_doc_type}: {len(expected_fields)}")
        print(f"[INFO] Successfully extracted fields: {len(extracted_fields)}")
        
        if not cleaned or len(cleaned) == 0:
            print(f"[WARN] No fields extracted for {mapped_doc_type}")
            
    except Exception as e:
        print(f"[ERROR] Field extraction failed for {mapped_doc_type}: {str(e)}")
        cleaned = {"error": str(e), "document_type": mapped_doc_type}

    print("PASS" if state.classification.passed else "FAIL")
    if message:
        print(message)
    # Print the already-cleaned JSON (has no empty fields, no dup aliases)
    try:
        print(json.dumps(cleaned, indent=2, ensure_ascii=False))
    except Exception:
        print(str(cleaned))

    state.extraction = ExtractionState(
        passed=state.classification.passed,
        message=message,
        extracted=cleaned,
    )
    log_agent_event(state, "Document Data Extraction", "completed")

    # --- Upload cleaned data to S3 (dynamic path) ---
    key = None
    try:
        s3 = get_s3_client()
        bucket = "lendingwise-aiagent"
        doc_name = (
            (state.ingestion.document_name if getattr(state, "ingestion", None) else None)
            or (state.ocr.document_name if state.ocr else None)
            or "document"
        )
        # Get date from ingestion metadata (same as used during upload)
        fpcid = state.ingestion.FPCID if getattr(state, "ingestion", None) and hasattr(state.ingestion, 'FPCID') else "3363"
        lmrid = state.ingestion.LMRId if getattr(state, "ingestion", None) and hasattr(state.ingestion, 'LMRId') else "1"
        
        # Extract date from ingestion metadata or prefix_parts
        year = month = day = None
        if state.ingestion and hasattr(state.ingestion, '_raw_metadata') and state.ingestion._raw_metadata:
            meta = state.ingestion._raw_metadata
            year = meta.get("year")
            month = meta.get("month") 
            day = meta.get("day")
        
        # If not found in metadata, try prefix_parts
        if not all([year, month, day]) and state.ingestion and hasattr(state.ingestion, 'prefix_parts') and state.ingestion.prefix_parts:
            parts = state.ingestion.prefix_parts
            year = parts.get("year")
            month = parts.get("month")
            day = parts.get("day")
        
        # Fallback to today's date if still not found
        if not all([year, month, day]):
            today = datetime.now(timezone.utc)
            year = year or today.year
            month = month or today.month
            day = day or today.day
            print(f"[WARN] Using fallback date: {year}-{month:02d}-{day:02d}")
        
        # Ensure proper formatting
        year = str(year)
        month = f"{int(month):02d}" if month else "01"
        day = f"{int(day):02d}" if day else "01"
        
        print(f"[DEBUG] Using date from ingestion: {year}-{month}-{day}")
        
        key = (
            f"LMRFileDocNew/{fpcid}/{year}/{month}/{day}/"
            f"{lmrid}/upload/result/result_{doc_name}.json"
        )

        body = json.dumps(cleaned, ensure_ascii=False).encode("utf-8")
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json; charset=utf-8")
        print(f"[✓] Extraction result uploaded to s3://{bucket}/{key}")
    except Exception as e:
        print(f"[WARN] Failed to upload extraction result to S3: {e}")

    # --- Generate doc_verification_result JSON ---
    doc_verification_result_json = None
    
    try:
        if state.classification and state.classification.passed:
            # PASS case: Simple success message
            doc_verification_result_json = json.dumps({
                "status": "pass",
                "message": "Your document is valid. Everything is verified successfully.",
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            print("[✓] Validation passed - generating success message")
        else:
            # FAIL case: Detailed failure with reasons and suggestions
            failure_message = state.classification.message if state.classification else "Validation failed"
            failure_type = "validation_failed"
            failure_stage = "classification"
            suggestions = []
            
            # Determine failure type and suggestions based on message
            message_lower = failure_message.lower()
            
            if "mismatch" in message_lower or "wrong" in message_lower:
                failure_type = "document_mismatch"
                suggestions = [
                    "Upload the correct document type that matches what you selected",
                    "Ensure the document image is clear and readable",
                    "Check that you're uploading the front side of the document"
                ]
            elif "expired" in message_lower:
                failure_type = "expired"
                suggestions = [
                    "Please upload a valid, non-expired document",
                    "If you recently renewed, upload the new document"
                ]
            elif "content" in message_lower:
                failure_type = "content_mismatch"
                suggestions = [
                    "The document content doesn't match what was specified",
                    "Please verify you uploaded the correct document",
                    "Re-upload with a clearer image if text is hard to read"
                ]
            else:
                suggestions = [
                    "Please re-upload a clear photo of your document",
                    "Ensure all text and information is visible",
                    "Contact support if the issue persists"
                ]
            
            doc_verification_result_json = json.dumps({
                "status": "fail",
                "reason": failure_message,
                "type": failure_type,
                "stage": failure_stage,
                "suggestions": suggestions,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            print(f"[✗] Validation failed: {failure_type} - {failure_message}")
    except Exception as e:
        print(f"[WARN] Failed to generate doc_verification_result: {e}")
    
    # --- Update DB record ---
    try:
        ingestion = state.ingestion
        classification = state.classification
        # Note: document_status will be updated by validation_check_node
        # Here we just set initial status based on classification
        document_status = "pass" if classification and classification.passed else "fail"
        cross_validation = False  # Will be set by validation_check_node if needed

        update_tblaigents_by_keys(
            FPCID=(ingestion.FPCID if ingestion and hasattr(ingestion, 'FPCID') else fpcid),
            checklistId=(ingestion.checklistId if ingestion and hasattr(ingestion, 'checklistId') else None),
            updates={
                "file_s3_location": (f"s3://{ingestion.s3_bucket}/{ingestion.s3_key}" if ingestion else None),
                "document_status": document_status,
                "uploadedat": (ingestion.uploaded_at if ingestion else None),
                "metadata_s3_path": (ingestion.metadata_s3_path if ingestion else None),
                "verified_result_s3_path": (f"s3://lendingwise-aiagent/{key}" if key else None),
                "cross_validation": cross_validation,
                "doc_verification_result": doc_verification_result_json,
            },
            document_name=(ingestion.document_name if ingestion else None),
            LMRId=(ingestion.LMRId if ingestion and hasattr(ingestion, 'LMRId') else lmrid),
        )
        print("[✓] DB row updated in stage_newskinny.tblaigents (allowed fields only)")
    except Exception as e:
        print(f"[WARN] Failed to update DB row: {e}")

    return state
