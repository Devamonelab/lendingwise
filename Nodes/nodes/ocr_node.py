"""
OCR node for document processing and data extraction.
"""

import os
import json
from typing import Dict, Any, Optional, List

from ..config.state_models import PipelineState, OCRState
from ..config.settings import OCR_MODE, DOC_CATEGORY
from ..tools.aws_services import run_textract_async_s3, run_analyze_id_s3
from ..tools.ocr_processing import (
    group_blocks_by_page, resolve_kv_pairs_from_page_blocks,
    cells_from_page_blocks, lines_words_from_page_blocks,
    route_document_type_from_ocr, analyze_id_to_kvs
)
from ..tools.llm_services import chat_json, classify_via_image, extract_via_image, remove_raw_text_fields
from ..utils.helpers import get_filename_without_extension, log_agent_event


# Document type prompts
PROMPTS_BY_TYPE: Dict[str, str] = {
    "bank_statement": (
        "You are a financial document parser for BANK STATEMENTS. "
        "Input JSON contains arrays: 'lines', 'cells', and 'kvs'. "
        "Return a single JSON object with anything present (do not limit to a fixed field list). "
        "Prefer structured keys where obvious (e.g., statement_period, balances, transactions). "
        "Additionally, set a top-level field 'document_name' to the exact document title/name as mentioned in the document itself when present; otherwise omit. "
        "Rules: Preserve text exactly; do not normalize; include a 'key_values' array if you see labeled pairs; "
        "omit fields not present. Output a single JSON object only."
    ),
    "identity": (
        "You parse identity documents (driver license, state ID, passport, etc). "
        "Input JSON has 'lines', 'cells', and 'kvs'. "
        "Extract ALL information present in the document. "
        "Return ONE JSON object where each field name (e.g. 'DOB','EYES','HGT','WGT','SEX','CLASS','ISS','EXP','ORGAN DONOR', etc) "
        "is a JSON key, and the field's value is the JSON value. "
        "Rules:\n"
        "- Preserve text exactly as shown, including units, casing, punctuation, and emojis/symbols (e.g. '❤️').\n"
        "- If a field appears with only a symbol (like a heart icon for Organ Donor), include that symbol as its value.\n"
        "- Do not wrap fields as {key:..., value:...} objects — instead use plain JSON key:value pairs.\n"
        "- Do not invent fields; only include what is clearly present.\n"
        "- Additionally, set a top-level field 'document_name' to the exact document title/name as mentioned in the document itself when present; otherwise omit.\n"
        "- Output JSON only, no prose."
    ),
    "property": (
        "You are an exhaustive parser for PROPERTY-related documents (appraisals, deeds, plats, surveys, covenants, tax records). "
        "Input has 'lines', 'cells', and 'kvs'. Extract EVERYTHING present. Return ONE JSON object only.\n\n"
        "Sections (include when present):\n"
        "  title_block: address, parcel/APN, legal description, borrower/owner/lender, zoning, tax info, subdivision, district/county/state, dates, registry numbers.\n"
        "  valuation: approaches (cost/sales/income), opinion of value, effective date, exposure/marketing time, reconciliations.\n"
        "  site: lot size, utilities, zoning compliance, easements, hazards, topography, influences.\n"
        "  improvements: year built, style, condition (C-ratings), renovations, construction details (foundation, roof, HVAC, windows, floors), amenities (garages, decks, fireplaces).\n"
        "  sales_history: full chain with dates, prices, document types, grantors/grantees, book/page.\n"
        "  comparables: reconstruct comparable tables into arrays with adjustments, net/gross, distances, remarks.\n"
        "  key_values: all labeled pairs as {key, value}.\n"
        "  approvals: signers, roles, license numbers, expirations, certifications, supervisory details.\n"
        "  maps_legends: captions, scales, legends, directional notes.\n"
        "  notes: disclaimers, limiting conditions, free text not captured elsewhere.\n\n"
        "Additionally, set a top-level field 'document_name' to the exact document title/name as mentioned in the document itself when present; otherwise omit.\n"
        "Rules: Preserve text EXACTLY; do not normalize; reconstruct tables; include checkboxes and symbols as-is; no prose."
    ),
    "entity": (
        "You are an exhaustive parser for ENTITY/BUSINESS documents (formation, amendments, certificates, annual reports). "
        "Input has 'lines', 'cells', and 'kvs'. Extract EVERYTHING. Return ONE JSON object only.\n\n"
        "Sections (include when present):\n"
        "  header: document titles, form names/codes, jurisdiction, filing office.\n"
        "  entity_profile: legal name(s), prior names/DBAs, entity type/class, jurisdiction of organization, domestication/foreign registration details, formation date, duration.\n"
        "  identifiers: EIN, state ID, SOS#, file#, control#, NAICS, DUNS.\n"
        "  registered_agent: name, ID, addresses, consent statements.\n"
        "  addresses: principal office, mailing, records office (each as full exact text).\n"
        "  management: organizers, incorporators, members/managers, directors, officers (names, roles, addresses, terms).\n"
        "  ownership_capital: shares/units/classes, par value, authorized/issued, ownership table (reconstruct from cells).\n"
        "  purpose_powers: stated purpose, limitations, special provisions.\n"
        "  compliance: annual reports, franchise tax, effective dates, delayed effectiveness.\n"
        "  approvals: signatures, seals, notary blocks, certifications, filing acknowledgments, dates/times.\n"
        "  key_values: every labeled pair as {key, value}.\n"
        "  tables: any tables reconstructed from 'cells'.\n"
        "  notes: free text not captured elsewhere.\n\n"
        "Additionally, set a top-level field 'document_name' to the exact document title/name as mentioned in the document itself when present; otherwise omit.\n"
        "Rules: Preserve text exactly; reconstruct tables; include checkboxes/symbols; no prose."
    ),
    "loan": (
        "You are an exhaustive parser for LOAN documents (notes, disclosures, deeds of trust, LE/CD, riders). "
        "Input has 'lines', 'cells', and 'kvs'. Extract EVERYTHING. Return ONE JSON object only.\n\n"
        "Sections (include when present):\n"
        "  parties: borrower(s), lender, trustee, servicer, MERS, guarantors (names, addresses).\n"
        "  loan_terms: principal, interest rate, APR/APY, rate type, index/margin, caps, payment schedule, maturity, amortization, prepayment, late fees, escrow, balloon, ARM disclosures.\n"
        "  collateral: property address/legal, lien position, riders/addenda.\n"
        "  fees_costs: itemized fees, finance charges, points, credits (reconstruct tables).\n"
        "  disclosures: TILA/RESPA sections, right to cancel, servicing transfer, privacy, HMDA, ECOA.\n"
        "  compliance_numbers: loan #, application #, NMLS IDs, case numbers, MIC/endorsements.\n"
        "  signatures_notary: signature lines, notary acknowledgments, seals, dates/times.\n"
        "  key_values: every labeled pair as {key, value}.\n"
        "  tables: payment schedules, fee tables, escrow analyses reconstructed from 'cells'.\n"
        "  notes: any free text not captured elsewhere.\n\n"
        "Additionally, set a top-level field 'document_name' to the exact document title/name as mentioned in the document itself when present; otherwise omit.\n"
        "Rules: Preserve text exactly; reconstruct tables; include checkboxes/symbols; no prose."
    ),
    "unknown": (
        "You are a cautious yet exhaustive parser for UNKNOWN document types. "
        "Input has 'lines', 'cells', and 'kvs'. Extract EVERYTHING visible without guessing meaning. Return ONE JSON object only.\n\n"
        "Output shape MUST match prior expectations:\n"
        "  key_values: array of {key, value} for any labeled pairs you can see.\n"
        "  free_text: ordered array of textual lines exactly as shown.\n"
        "Additionally (when present):\n"
        "  tables: reconstructed tables from 'cells' (array of row objects).\n"
        "  checkmarks: array of {label, status} for selection elements with 'SELECTED' or 'NOT_SELECTED'.\n"
        "  notes: any content that is ambiguous or uncategorized.\n\n"
        "Additionally, set a top-level field 'document_name' to the exact document title/name as mentioned in the document itself when present; otherwise omit.\n"
        "Rules: Preserve text exactly; do not normalize; no prose."
    ),
}


def llm_extract_page(doc_type: str, page_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract data from page using LLM."""
    llm_input = {
        "lines": page_data.get("lines", []),
        "cells": page_data.get("cells", []),
        "kvs":   page_data.get("kvs", []),
    }
    system = PROMPTS_BY_TYPE.get(doc_type, PROMPTS_BY_TYPE["unknown"])
    out = chat_json("gpt-4o", system, llm_input) or {}
    return remove_raw_text_fields(out)


def run_pipeline(bucket: str, key: str, mode: str = "ocr+llm") -> Dict[str, Any]:
    """
    Execute the OCR extraction pipeline.
    """
    simplified: Dict[str, Any] = {"pages": {}}
    name_no_ext = get_filename_without_extension(key)

    if mode == "ocr+llm":
        # 1) Textract (Tables + Forms)
        raw = run_textract_async_s3(bucket, key)

        # 2) Build per-page simplified view FROM ORIGINAL BLOCKS so relationships remain available
        blocks = raw.get("blocks", [])
        pages_full = group_blocks_by_page(blocks)

        simplified = {"pages": {}}
        for page_num, page_blocks in pages_full.items():
            lw = lines_words_from_page_blocks(page_blocks)
            cells = cells_from_page_blocks(page_blocks)
            kvs = resolve_kv_pairs_from_page_blocks(page_blocks)

            simplified["pages"][page_num] = {
                "lines": lw["lines"],
                "words": lw["words"],
                "cells": cells,
                "kvs": kvs,
            }

        # 3) Route document type from OCR ONLY (page 1)
        doc_type = route_document_type_from_ocr(simplified)
        print(f"[Router] Document type: {doc_type}")

        # 4) OPTIONAL: If identity, try AnalyzeID and MERGE KVs into page 1
        if doc_type == "identity":
            try:
                aid = run_analyze_id_s3(bucket, key)
                aid_kvs = analyze_id_to_kvs(aid)
                first_page_key = sorted(simplified["pages"].keys(), key=lambda x: int(x))[0]
                simplified["pages"][first_page_key].setdefault("kvs", [])
                simplified["pages"][first_page_key]["kvs"].extend(aid_kvs)
            except Exception as e:
                print(f"[AnalyzeID] skipped or failed: {e}")

        image_extracted = {}
    else:  # LLM-only mode
        lower_key = key.lower()
        if lower_key.endswith(".pdf"):
            raw = run_textract_async_s3(bucket, key)
            blocks = raw.get("blocks", [])
            pages_full = group_blocks_by_page(blocks)
            simplified = {"pages": {}}
            for page_num, page_blocks in pages_full.items():
                lw = lines_words_from_page_blocks(page_blocks)
                cells = cells_from_page_blocks(page_blocks)
                kvs = resolve_kv_pairs_from_page_blocks(page_blocks)
                simplified["pages"][page_num] = {
                    "lines": lw["lines"],
                    "words": lw["words"],
                    "cells": cells,
                    "kvs": kvs,
                }
            doc_type = route_document_type_from_ocr(simplified)
            print(f"[Router] Document type: {doc_type}")
            image_extracted = {}
        else:
            # Handle image files with LLM vision
            from ..tools.aws_services import generate_presigned_url
            image_url = generate_presigned_url(bucket, key)
            
            if image_url:
                doc_type = classify_via_image("gpt-4o", image_url)
                print(f"[Router] Document type: {doc_type}")
                image_extracted = extract_via_image("gpt-4o", doc_type, image_url, PROMPTS_BY_TYPE)
                simplified = {"pages": {1: {"lines": [], "words": [], "cells": [], "kvs": []}}}
            else:
                doc_type = "unknown"
                simplified = {"pages": {1: {"lines": [], "words": [], "cells": [], "kvs": []}}}
                image_extracted = {}

    # 5) Extract page-by-page via LLM (works for both modes) + HOIST document_name
    all_structured: Dict[str, Any] = {"doc_type": doc_type}
    top_level_doc_name: Optional[str] = None
    doc_name_candidates: List[str] = []

    for page_num in sorted(simplified["pages"].keys(), key=lambda x: int(x)):
        page_data = simplified["pages"][page_num]
        print(f"[LLM] Extracting page {page_num} as '{doc_type}'...")
        if mode == "llm" and page_num == 1 and image_extracted:
            extracted = image_extracted
        else:
            extracted = llm_extract_page(doc_type, page_data)

        # Hoist `document_name` out of page result (first non-empty wins)
        if isinstance(extracted, dict):
            dn = extracted.get("document_name")
            if dn:
                doc_name_candidates.append(dn)
                extracted.pop("document_name", None)

        all_structured[str(page_num)] = extracted

    # Decide on a single top-level document_name
    if doc_name_candidates:
        top_level_doc_name = doc_name_candidates[0]
        all_structured["document_name"] = top_level_doc_name

    # Do not save OCR outputs locally or to S3 as per requirement
    return {
        "doc_type": doc_type,
        "structured": all_structured,
        "name_no_ext": name_no_ext,
        "mode": mode,
    }


def OCR(state: PipelineState) -> PipelineState:
    """
    Process document through OCR pipeline.
    """
    if state.ingestion is None:
        raise ValueError("Ingestion state missing; run Ingestion node first.")
    if state.tamper_check is None:
        raise ValueError("Tamper check state missing; run TamperCheck node first.")

    # Prefer doc category from ingestion document_type
    doc_category = (state.ingestion.document_type or "").strip()
    if not doc_category:
        # Fallback to env for manual runs
        doc_category = DOC_CATEGORY

    bucket = state.ingestion.s3_bucket
    key = state.ingestion.s3_key
    mode = OCR_MODE

    if not bucket or not key:
        raise ValueError("Missing S3 bucket/key from ingestion.")

    print(f"\n=== PROCEEDING WITH DOCUMENT PROCESSING ===")
    log_agent_event(state, "OCR", "start")
    
    # Check if tamper check passed (warnings are already printed in TamperCheck node)
    tamper_check_passed = state.tamper_check.status in ["OK"]

    # Run OCR pipeline with the requested mode
    result = run_pipeline(bucket, key, mode)
    structured = result.get("structured", {})

    # Populate OCR state
    state.ocr = OCRState(
        bucket=bucket,
        key=key,
        mode=mode,
        doc_category=doc_category,
        document_name=structured.get("document_name"),
        ocr_json=structured,
        tamper_check_passed=tamper_check_passed,
    )
    log_agent_event(state, "OCR", "completed", {"doc_type": result.get("doc_type")})
    return state
