"""
Classification node for document type validation.
Adds strict mismatch check between provided document_name and detected identity subtype.
"""

import os
from typing import Optional, Dict, Any

from ..config.state_models import PipelineState, ClassificationState
from ..utils.helpers import log_agent_event


def _map_display_name_to_identity_subtype(name: Optional[str]) -> str:
    """
    Map a human document display name (from metadata/DB) to a normalized
    identity subtype for all U.S. identity documents.
    """
    n = (name or "").strip().lower()
    if not n:
        return "other_identity"

    # Core Identity Documents (most specific first)
    if ("driver" in n or "driving" in n) and "license" in n:
        return "driving_license"
    if "mobile" in n and ("driver" in n or "license" in n):
        return "mobile_drivers_license"
    if ("state" in n and ("id" in n or "identification" in n)) and "driver" not in n:
        return "state_id"
    if "real" in n and "id" in n:
        return "real_id"
    
    # Passport Documents
    if "passport" in n and "card" in n:
        return "passport_card"
    if "passport" in n and "card" not in n:
        return "passport"
    
    # Birth and Vital Records
    if ("birth" in n and "certificate" in n) or "birth_cert" in n:
        return "birth_certificate"
    if ("marriage" in n and "certificate" in n) or "marriage_cert" in n:
        return "marriage_certificate"
    if ("divorce" in n and ("decree" in n or "certificate" in n)):
        return "divorce_decree"
    
    # Social Security
    if ("social" in n and "security" in n) or "ssn" in n or "ss_card" in n:
        return "social_security_card"
    
    # Indian Identity Documents
    if any(term in n for term in ["aadhaar", "aadhar", "adhar", "uidai"]) or "aadhaar_card" in n:
        return "aadhaar_card"
    
    # Immigration Documents
    if ("permanent" in n and "resident" in n) or "green_card" in n or "prc" in n or "i-551" in n:
        return "permanent_resident_card"
    if ("naturalization" in n and "certificate" in n) or "n-550" in n or "n-570" in n:
        return "certificate_of_naturalization"
    if ("citizenship" in n and "certificate" in n) or "n-560" in n or "n-561" in n:
        return "certificate_of_citizenship"
    if ("employment" in n and "authorization" in n) or "ead" in n or "i-766" in n:
        return "employment_authorization_document"
    if "i-94" in n or ("arrival" in n and "departure" in n):
        return "form_i94"
    if ("visa" in n and ("us" in n or "american" in n)) or "h1b" in n or "h-1b" in n:
        return "us_visa"
    if ("reentry" in n and "permit" in n) or "i-327" in n:
        return "reentry_permit"
    
    # Military and Government IDs
    if ("military" in n and "id" in n) or "cac" in n or "common_access" in n:
        return "military_id"
    if ("veteran" in n and "id" in n) or "vic" in n:
        return "veteran_id"
    if ("tribal" in n and "id" in n) or "tribal_card" in n:
        return "tribal_id"
    if ("global" in n and "entry" in n) or "nexus" in n:
        return "global_entry_card"
    if ("tsa" in n and "precheck" in n) or "precheck" in n:
        return "tsa_precheck_card"
    if ("voter" in n and ("registration" in n or "card" in n)):
        return "voter_registration"
    
    # Professional and Educational
    if ("professional" in n and "license" in n) or ("license" in n and any(prof in n for prof in ["medical", "legal", "contractor", "nursing", "teaching"])):
        return "professional_license"
    if ("student" in n and "id" in n) or "student_card" in n:
        return "student_id"
    
    # Financial and Proof Documents
    if ("utility" in n and "bill" in n) or any(util in n for util in ["electric", "gas", "water", "internet", "cable"]):
        return "utility_bill"
    if ("lease" in n and "agreement" in n) or "rental_agreement" in n:
        return "lease_agreement"
    if ("bank" in n and "statement" in n) or "account_statement" in n:
        return "bank_statement"
    if ("insurance" in n and "card" in n) or any(ins in n for ins in ["health_insurance", "auto_insurance"]):
        return "insurance_card"
    if ("voided" in n and "check" in n) or "void_check" in n:
        return "voided_check"
    if ("direct" in n and "deposit" in n) or "dd_form" in n:
        return "direct_deposit"
    
    # Consular and International
    if ("consular" in n and "id" in n) or "matricula" in n:
        return "consular_id"
    
    # Digital IDs
    if ("digital" in n and "id" in n) or any(platform in n for platform in ["id.me", "login.gov"]):
        return "digital_id"
    
    # If it's any kind of identity document but we can't determine the specific type
    if any(identity_term in n for identity_term in ["id", "identification", "license", "card", "certificate", "document"]):
        return "identity_document"
    
    return "other_identity"


def _guess_identity_subtype_from_ocr(ocr_json: Dict[str, Any]) -> str:
    """
    Guess identity subtype from OCR-structured JSON for all U.S. identity documents.
    Uses enhanced heuristics and LLM classification with comprehensive document type support.
    """
    # Prefer top-level page 1 extracted fields if present
    page1 = None
    if isinstance(ocr_json, dict):
        page1 = ocr_json.get("1") or ocr_json.get(1)
        if not isinstance(page1, dict):
            # Some pipelines might store directly under page-less keys; use entire object
            page1 = ocr_json

    # Compose a compact summary string for heuristic and/or LLM
    keys = []
    values = []
    if isinstance(page1, dict):
        for k, v in list(page1.items())[:50]:  # cap to avoid huge prompts
            try:
                ks = str(k).lower()
                vs = str(v).lower()
            except Exception:
                continue
            keys.append(ks)
            if len(vs) < 120:
                values.append(vs)
    text_blob = " ".join(keys + values)

    # Enhanced heuristics for all U.S. identity documents (cheap, no network)
    blob = text_blob
    
    # Core Identity Documents
    if any(w in blob for w in ["driver", "driving"]) and "license" in blob:
        if "mobile" in blob or "mdl" in blob:
            return "mobile_drivers_license"
        return "driving_license"
    if ("state" in blob and ("id" in blob or "identification" in blob)) and "driver" not in blob:
        if "real" in blob:
            return "real_id"
        return "state_id"
    
    # Passport Documents
    if "passport card" in blob:
        return "passport_card"
    if "passport" in blob:
        return "passport"
    
    # Birth and Vital Records
    if ("birth" in blob and "certificate" in blob) or "birth_cert" in blob:
        return "birth_certificate"
    if ("marriage" in blob and "certificate" in blob):
        return "marriage_certificate"
    if ("divorce" in blob and ("decree" in blob or "certificate" in blob)):
        return "divorce_decree"
    
    # Social Security
    if ("social" in blob and "security" in blob) or "ssn" in blob:
        return "social_security_card"

    # Indian Identity Documents
    if any(term in blob for term in ["aadhaar", "aadhar", "adhar", "uidai", "government of india"]) or "unique identification" in blob:
        return "aadhaar_card"
    
    # Immigration Documents
    if ("permanent" in blob and "resident" in blob) or "green_card" in blob or "i-551" in blob:
        return "permanent_resident_card"
    if ("naturalization" in blob and "certificate" in blob) or any(form in blob for form in ["n-550", "n-570"]):
        return "certificate_of_naturalization"
    if ("citizenship" in blob and "certificate" in blob) or any(form in blob for form in ["n-560", "n-561"]):
        return "certificate_of_citizenship"
    if ("employment" in blob and "authorization" in blob) or "ead" in blob or "i-766" in blob:
        return "employment_authorization_document"
    if "i-94" in blob or ("arrival" in blob and "departure" in blob):
        return "form_i94"
    if ("visa" in blob and ("us" in blob or "american" in blob)) or "h1b" in blob or "h-1b" in blob:
        return "us_visa"
    if ("reentry" in blob and "permit" in blob) or "i-327" in blob:
        return "reentry_permit"
    
    # Military and Government IDs
    if ("military" in blob and "id" in blob) or "cac" in blob or "common_access" in blob:
        return "military_id"
    if ("veteran" in blob and "id" in blob) or "vic" in blob:
        return "veteran_id"
    if ("tribal" in blob and "id" in blob):
        return "tribal_id"
    if ("global" in blob and "entry" in blob) or "nexus" in blob:
        return "global_entry_card"
    if ("tsa" in blob and "precheck" in blob) or "precheck" in blob:
        return "tsa_precheck_card"
    if ("voter" in blob and ("registration" in blob or "card" in blob)):
        return "voter_registration"
    
    # Professional and Educational
    if ("professional" in blob and "license" in blob) or ("license" in blob and any(prof in blob for prof in ["medical", "legal", "contractor", "nursing", "teaching"])):
        return "professional_license"
    if ("student" in blob and "id" in blob):
        return "student_id"
    
    # Financial and Proof Documents
    if ("utility" in blob and "bill" in blob) or any(util in blob for util in ["electric", "gas", "water", "internet", "cable"]):
        return "utility_bill"
    if ("lease" in blob and "agreement" in blob) or "rental_agreement" in blob:
        return "lease_agreement"
    if ("bank" in blob and "statement" in blob):
        return "bank_statement"
    if ("insurance" in blob and "card" in blob):
        return "insurance_card"
    if ("voided" in blob and "check" in blob):
        return "voided_check"
    if ("direct" in blob and "deposit" in blob):
        return "direct_deposit"
    
    # Consular and Digital IDs
    if ("consular" in blob and "id" in blob) or "matricula" in blob:
        return "consular_id"
    if ("digital" in blob and "id" in blob) or any(platform in blob for platform in ["id.me", "login.gov"]):
        return "digital_id"

    # LLM-based classification (comprehensive document type support)
    try:
        from openai import OpenAI  # type: ignore
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            raise RuntimeError("no key")
        client = OpenAI(api_key=api_key)
        
        # Comprehensive list of all supported U.S. identity document types
        allowed_types = [
            "driving_license", "mobile_drivers_license", "state_id", "real_id",
            "passport", "passport_card", "birth_certificate", "marriage_certificate", "divorce_decree",
            "social_security_card", "permanent_resident_card", "certificate_of_naturalization",
            "certificate_of_citizenship", "employment_authorization_document", "form_i94", "us_visa",
            "reentry_permit", "military_id", "veteran_id", "tribal_id", "global_entry_card",
            "tsa_precheck_card", "voter_registration", "professional_license", "student_id",
            "utility_bill", "lease_agreement", "bank_statement", "insurance_card",
            "voided_check", "direct_deposit", "consular_id", "digital_id", "identity_document", "other_identity"
        ]
        
        system = (
            "You are an expert classifier for U.S. identity documents. Return JSON only.\n"
            f"Supported document types: {', '.join(allowed_types)}\n"
            "Analyze the OCR data and classify the document to the most specific type.\n"
            "Consider document layout, field names, issuing authorities, and content patterns."
        )
        
        payload = {
            "document_name": ocr_json.get("document_name"),
            "page1_fields_preview": dict(list(page1.items())[:30]) if isinstance(page1, dict) else {},
            "text_content_sample": text_blob[:500]  # First 500 chars for context
        }
        
        resp = client.chat.completions.create(
            model="gpt-4o",
            response_format={"type": "json_object"},
            temperature=0,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": (
                    "Classify this U.S. identity document to one of the supported types. "
                    "Respond as {\"subtype\":\"document_type\", \"confidence\":\"high|medium|low\", \"reasoning\":\"brief explanation\"}.\n"
                    + str(payload)
                )},
            ],
        )
        content = resp.choices[0].message.content
        import json as _json
        result = _json.loads(content) or {}
        label = result.get("subtype", "other_identity")
        
        # Validate against allowed types
        if label in allowed_types:
            return label
        else:
            # If LLM returned an unexpected type, try to map it or fall back
            return "identity_document" if any(term in label for term in ["id", "license", "card", "certificate"]) else "other_identity"
            
    except Exception as e:
        # Silent fallback to generic identity document if it looks like an ID document
        if any(term in blob for term in ["id", "license", "card", "certificate", "document", "number"]):
            return "identity_document"
        return "other_identity"


def _extract_actual_document_name_from_ocr(ocr_json: Dict[str, Any]) -> str:
    """
    Use LLM to extract the actual document name/type from OCR content.
    This helps identify what document was actually uploaded vs what the user claimed.
    """
    # Get OCR content
    page1 = None
    if isinstance(ocr_json, dict):
        page1 = ocr_json.get("1") or ocr_json.get(1)
        if not isinstance(page1, dict):
            page1 = ocr_json

    # Compose text for analysis
    text_content = []
    if isinstance(page1, dict):
        for k, v in list(page1.items())[:30]:  # Limit to avoid huge prompts
            try:
                text_content.append(f"{k}: {v}")
            except Exception:
                continue
    
    text_blob = "\n".join(text_content)
    
    # Quick heuristic checks first
    blob_lower = text_blob.lower()
    
    # Indian documents
    if any(term in blob_lower for term in ["aadhaar", "aadhar", "adhar", "government of india", "uidai"]):
        return "Aadhaar Card"
    
    # US documents
    if "driver" in blob_lower and "license" in blob_lower:
        return "Driver's License"
    if "passport" in blob_lower and "card" in blob_lower:
        return "Passport Card"
    if "passport" in blob_lower:
        return "Passport"
    if "social security" in blob_lower:
        return "Social Security Card"
    if ("state" in blob_lower and "id" in blob_lower) or "identification" in blob_lower:
        return "State ID"
    
    # Use LLM for more complex detection
    try:
        from openai import OpenAI
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            return "Unknown Document"
        
        client = OpenAI(api_key=api_key)
        
        system_prompt = """You are an expert document identifier. Analyze the OCR text and identify the specific type of identity document.

Return ONLY the document name in this exact format:
- "Driver's License" 
- "State ID"
- "Passport"
- "Passport Card"
- "Aadhaar Card"
- "Social Security Card"
- "Birth Certificate"
- "Marriage Certificate"
- "Permanent Resident Card"
- "Military ID"
- "Professional License"
- "Utility Bill"
- "Bank Statement"
- "Unknown Document" (if you cannot determine)

Look for official headers, issuing authorities, document layouts, and specific field patterns."""

        user_prompt = f"""Identify this document type from the OCR text:

{text_blob[:1000]}

What type of identity document is this?"""

        resp = client.chat.completions.create(
            model="gpt-4o",
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        
        result = resp.choices[0].message.content.strip()
        return result if result else "Unknown Document"
        
    except Exception as e:
        print(f"[WARN] LLM document name extraction failed: {e}")
        return "Unknown Document"


def Classification(state: PipelineState) -> PipelineState:
    """
    Classify document type, validate against expected category, and enforce
    a strict name-vs-identity-subtype check. If mismatched, mark as fail and
    instruct user to re-upload the correct document.
    """
    if state.ocr is None:
        raise ValueError("Nothing ingested; run OCR node first.")

    log_agent_event(state, "Document Classification", "start")

    # Coarse category (bank_statement/identity/property/...) detected by OCR routing
    doc_category = (state.ocr.doc_category or "").strip().lower()
    ocr_json = state.ocr.ocr_json or {}
    doc_type = (ocr_json.get("doc_type") or "").strip().lower()

    # If ingestion agent is Identity Verification Agent, force expected to identity
    try:
        agent_name = (state.ingestion.agent_name or "").strip().lower() if state.ingestion else ""
    except Exception:
        agent_name = ""
    if agent_name == "identity verification agent":
        doc_category = "identity"
    elif not doc_category and doc_type:
        # If no expected category provided (e.g., missing from metadata), treat expected as detected
        doc_category = doc_type

    # First-level pass/fail on coarse category
    category_match = (doc_category == doc_type) and (doc_type != "")

    # Enhanced comprehensive document validation
    name_mismatch = False
    subtype_detected = None
    subtype_expected = None
    detailed_reason = None
    metadata_db_name_mismatch = False
    actual_document_mismatch = False

    ingestion_name = (state.ingestion.document_name if state.ingestion else None) or ""
    
    # 1. Check for mismatch between metadata document_name and DB document_name
    if state.ingestion and state.ingestion.raw_metadata:
        metadata_doc_name = state.ingestion.raw_metadata.get("document_name", "").strip()
        db_doc_name = ingestion_name.strip()
        
        if metadata_doc_name and db_doc_name and metadata_doc_name.lower() != db_doc_name.lower():
            metadata_db_name_mismatch = True
            detailed_reason = (
                f"Document name mismatch: Metadata indicates '{metadata_doc_name}' but "
                f"database context shows '{db_doc_name}'. Please ensure the document name "
                f"is consistent or re-upload with the correct document type."
            )
            print(f"[ERROR] Metadata/DB document name mismatch: '{metadata_doc_name}' vs '{db_doc_name}'")
    
    # 2. Extract actual document name from OCR content and validate
    actual_document_name = None
    if doc_type == "identity" and not metadata_db_name_mismatch:
        actual_document_name = _extract_actual_document_name_from_ocr(ocr_json or {})
        print(f"[INFO] Actual document detected from OCR: '{actual_document_name}'")
        
        # Normalize names for comparison
        claimed_name_normalized = ingestion_name.lower().strip()
        actual_name_normalized = actual_document_name.lower().strip()
        
        # Define document name mappings for flexible matching
        document_aliases = {
            "driver license": ["driver's license", "driving license", "drivers license", "dl"],
            "driver's license": ["driver license", "driving license", "drivers license", "dl"],
            "aadhaar card": ["aadhar card", "adhar card", "aadhaar", "aadhar", "adhar"],
            "passport card": ["passport", "us passport card"],
            "passport": ["us passport", "passport book"],
            "state id": ["state identification", "state identification card", "id card"],
            "social security card": ["ssn card", "social security", "ss card"],
        }
        
        # Check if claimed name matches actual document
        is_valid_match = False
        
        # Direct match
        if claimed_name_normalized in actual_name_normalized or actual_name_normalized in claimed_name_normalized:
            is_valid_match = True
        
        # Check aliases
        if not is_valid_match:
            for canonical_name, aliases in document_aliases.items():
                if claimed_name_normalized in [canonical_name] + aliases:
                    if any(alias in actual_name_normalized for alias in [canonical_name] + aliases):
                        is_valid_match = True
                        break
        
        # Special case for common misspellings
        if not is_valid_match:
            # Handle common typos
            if "licese" in claimed_name_normalized and "license" in actual_name_normalized:
                is_valid_match = True
            elif "licence" in claimed_name_normalized and "license" in actual_name_normalized:
                is_valid_match = True
        
        if not is_valid_match and actual_document_name != "Unknown Document":
            actual_document_mismatch = True
            detailed_reason = (
                f"Document content mismatch: You specified '{ingestion_name}' but the uploaded "
                f"document appears to be a '{actual_document_name}'. Please upload the correct "
                f"document type or update the document name to match the uploaded file."
            )
            print(f"[ERROR] Document content mismatch: Claimed '{ingestion_name}' vs Actual '{actual_document_name}'")
    
    if doc_type == "identity" and ingestion_name and not metadata_db_name_mismatch:
        subtype_expected = _map_display_name_to_identity_subtype(ingestion_name)
        subtype_detected = _guess_identity_subtype_from_ocr(ocr_json or {})
        
        # Log detection results for debugging
        print(f"[INFO] Document name mapping: '{ingestion_name}' -> '{subtype_expected}'")
        print(f"[INFO] OCR content detection: -> '{subtype_detected}'")
        
        # Enhanced mismatch detection logic
        # Allow some flexibility for related document types
        compatible_groups = [
            {"driving_license", "mobile_drivers_license"},  # Mobile DL is compatible with regular DL
            {"state_id", "real_id", "passport_card"},  # State ID, REAL ID, and Passport Card are often similar in format
            {"passport", "passport_card"},  # Passport types are related
            {"certificate_of_naturalization", "certificate_of_citizenship"},  # Citizenship documents
            {"military_id", "veteran_id"},  # Military-related IDs
            {"utility_bill", "lease_agreement", "bank_statement"},  # Proof of residence documents
            {"employment_authorization_document", "permanent_resident_card"},  # Immigration documents
            {"consular_id", "identity_document"},  # International identity documents
        ]
        
        # Check if documents are in compatible groups
        is_compatible = False
        if subtype_expected == subtype_detected:
            is_compatible = True
        else:
            for group in compatible_groups:
                if subtype_expected in group and subtype_detected in group:
                    is_compatible = True
                    break
        
        # Only flag mismatch if both are specific and not compatible
        if (subtype_expected not in {"other_identity", "identity_document"} and 
            subtype_detected not in {"other_identity", "identity_document"} and 
            not is_compatible):
            name_mismatch = True
            detailed_reason = (
                f"Document type mismatch: You indicated '{ingestion_name}' (expected: {subtype_expected}), "
                f"but the uploaded document appears to be a {subtype_detected.replace('_', ' ').title()}. "
                f"Please upload the correct document type or update the document name."
            )
        elif subtype_detected == "other_identity" and subtype_expected != "other_identity":
            # Special case: couldn't detect specific type but user specified one
            print(f"[WARN] Could not definitively identify document type from OCR. Expected: {subtype_expected}")
            # Don't fail in this case, just log the warning

    passed = category_match and not name_mismatch and not metadata_db_name_mismatch and not actual_document_mismatch
    
    # Generate appropriate message based on results
    if passed:
        message = "pass"
        if subtype_detected and subtype_expected:
            print(f"[✓] Document classification successful: {subtype_detected}")
        if actual_document_name:
            print(f"[✓] Document content validation successful: {actual_document_name}")
    else:
        if actual_document_mismatch:
            message = detailed_reason
        elif metadata_db_name_mismatch:
            message = detailed_reason
        elif not category_match:
            message = (
                f"Document category mismatch: Expected '{doc_category}' document, "
                f"but detected '{doc_type}'. Please upload a valid identity document "
                f"(accepted formats: PNG, JPG, JPEG, PDF, DOCX, etc.)."
            )
        else:
            message = detailed_reason or (
                "Document name and content mismatch. Please ensure the uploaded document "
                "matches the document type you specified, or update the document name to match "
                "the uploaded file."
            )
    
    # Enhanced logging for debugging
    print(f"[INFO] Classification Results:")
    print(f"  - Expected Category: {doc_category}")
    print(f"  - Detected Type: {doc_type}")
    print(f"  - Document Name (DB): {ingestion_name}")
    if state.ingestion and state.ingestion.raw_metadata:
        metadata_doc_name = state.ingestion.raw_metadata.get("document_name", "")
        print(f"  - Document Name (Metadata): {metadata_doc_name}")
    if actual_document_name:
        print(f"  - Actual Document (OCR): {actual_document_name}")
    print(f"  - Expected Subtype: {subtype_expected}")
    print(f"  - Detected Subtype: {subtype_detected}")
    print(f"  - Category Match: {category_match}")
    print(f"  - Name Match: {not name_mismatch}")
    print(f"  - Metadata/DB Name Match: {not metadata_db_name_mismatch}")
    print(f"  - Actual Document Match: {not actual_document_mismatch}")
    print(f"  - Final Result: {'PASS' if passed else 'FAIL'}")
    if not passed:
        print(f"  - Reason: {message}")

    state.classification = ClassificationState(
        expected_category=doc_category,
        detected_doc_type=doc_type,
        passed=passed,
        message=message,
    )
    
    # Store additional classification details for debugging
    if hasattr(state.classification, '__dict__'):
        state.classification.__dict__.update({
            'subtype_expected': subtype_expected,
            'subtype_detected': subtype_detected,
            'category_match': category_match,
            'name_mismatch': name_mismatch
        })
    
    log_agent_event(state, "Document Classification", "completed", {
        "passed": passed, 
        "detected": doc_type,
        "subtype_expected": subtype_expected,
        "subtype_detected": subtype_detected,
        "document_name_db": ingestion_name,
        "document_name_metadata": (state.ingestion.raw_metadata.get("document_name", "") if state.ingestion and state.ingestion.raw_metadata else ""),
        "actual_document_name": actual_document_name,
        "metadata_db_name_match": not metadata_db_name_mismatch,
        "actual_document_match": not actual_document_mismatch
    })
    
    return state
