"""
Enhanced cross-validation logic with GPT-4o integration.
"""

import sys
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from ..models.data_models import (
    BorrowerData, 
    DocumentDetails, 
    FieldResult,
    ValidationSummary,
    RecommendationResult,
    VerificationReport
)
from .gpt4o_validator import GPT4oValidator


# Standard reference fields from tblfile
REFERENCE_FIELDS = [
    "borrowerName",  # Note: DB column is borrowerName (First Name)
    "borrowerMName", 
    "borrowerLName",
    "borrowerDOB",
    "driverLicenseNumber",
    "driverLicenseState",
    "borrowerPOB"
]

FIELD_DISPLAY_NAMES = {
    "borrowerName": "First Name",
    "borrowerMName": "Middle Name",
    "borrowerLName": "Last Name",
    "borrowerDOB": "Date of Birth",
    "driverLicenseNumber": "License Number",
    "driverLicenseState": "License State",
    "borrowerPOB": "Place of Birth"
}


class EnhancedValidator:
    """Enhanced validation with GPT-4o."""
    
    def __init__(self):
        """Initialize validator with GPT-4o."""
        self.gpt4o = GPT4oValidator()
    
    def validate(
        self,
        documents: List[DocumentDetails],
        borrower_data: Optional[BorrowerData],
        FPCID: str,
        LMRId: str,
        checklistId: Optional[str] = None,
        document_name: Optional[str] = None
    ) -> VerificationReport:
        """
        Main validation function.
        
        Args:
            documents: List of documents to validate
            borrower_data: Reference data from database (can be None)
            FPCID: Tenant ID
            LMRId: Loan file ID
            
        Returns:
            VerificationReport with simplified structure
        """
        print(f"\n[VALIDATE] Starting enhanced validation for FPCID={FPCID}, LMRId={LMRId}")
        print(f"[VALIDATE] Mode: {'REFERENCE_BASED' if borrower_data else 'CROSS_DOCUMENT_ONLY'}")
        print(f"[VALIDATE] Documents: {len(documents)}")
        
        # Step 1: Extract fields from all documents using GPT-4o
        extracted_docs = self._extract_all_fields(documents)
        
        # Step 2: Validate based on mode
        if borrower_data:
            field_results, score = self._validate_with_reference(
                extracted_docs, 
                borrower_data,
                documents
            )
        else:
            field_results, score = self._validate_cross_document(
                extracted_docs,
                documents
            )
        
        # Step 3: Generate report
        report = self._generate_report(
            FPCID=FPCID,
            LMRId=LMRId,
            checklistId=checklistId,
            document_name=document_name,
            documents=[doc.document_name for doc in documents],
            field_results=field_results,
            score=score,
            has_reference=borrower_data is not None
        )
        
        print(f"[VALIDATE] Validation complete: {report.validation_summary.status} ({score}%)")
        return report
    
    def _extract_all_fields(
        self, 
        documents: List[DocumentDetails]
    ) -> List[Dict[str, any]]:
        """Extract fields from all documents using GPT-4o."""
        extracted = []
        
        for doc in documents:
            print(f"[GPT-4o] Extracting fields from: {doc.document_name}")
            
            if not doc.verified_details:
                print(f"[WARN] No data for {doc.document_name}")
                extracted.append({
                    "document_name": doc.document_name,
                    "standard_fields": {},
                    "additional_fields": {},
                    "error": "No document data available"
                })
                continue
            
            result = self.gpt4o.extract_fields_from_document(
                doc.verified_details,
                doc.document_name
            )
            
            result["document_name"] = doc.document_name
            extracted.append(result)
        
        return extracted
    
    def _validate_with_reference(
        self,
        extracted_docs: List[Dict[str, any]],
        borrower_data: BorrowerData,
        documents: List[DocumentDetails]
    ) -> Tuple[List[FieldResult], int]:
        """
        Validate with database reference (Phase 1 + Phase 2).
        
        Phase 1: Reference fields (70% weight)
        Phase 2: Additional fields (30% weight)
        """
        print(f"[VALIDATE] Phase 1: Validating reference fields against DB")
        
        field_results = []
        reference_dict = {
            "borrowerName": borrower_data.borrowerName,
            "borrowerMName": borrower_data.borrowerMName,
            "borrowerLName": borrower_data.borrowerLName,
            "borrowerDOB": borrower_data.borrowerDOB,
            "driverLicenseNumber": borrower_data.driverLicenseNumber,
            "driverLicenseState": borrower_data.driverLicenseState,
            "borrowerPOB": borrower_data.borrowerPOB
        }
        
        # Phase 1: Reference fields
        phase1_total = 0
        phase1_matched = 0
        
        for field_name in REFERENCE_FIELDS:
            ref_value = reference_dict.get(field_name)
            
            if not ref_value:
                # Skip fields with no reference value
                continue
            
            phase1_total += 1
            
            # Collect values from all documents
            doc_values = {}
            all_match = True
            issues = []
            
            for extracted in extracted_docs:
                doc_name = extracted["document_name"]
                doc_value = extracted.get("standard_fields", {}).get(field_name)
                
                if doc_value:
                    doc_values[doc_name] = doc_value
                    
                    # Compare with reference using GPT-4o
                    comparison = self.gpt4o.compare_values(
                        ref_value, 
                        doc_value, 
                        field_name,
                        doc_name
                    )
                    
                    if not comparison["match"]:
                        all_match = False
                        issues.append(f"{doc_name}: {comparison['reason']}")
                else:
                    doc_values[doc_name] = "-"
            
            # Determine status
            if all_match and doc_values:
                status = "MATCH"
                phase1_matched += 1
                issue = None
            elif not doc_values:
                status = "MATCH"  # No documents have this field, but reference exists
                phase1_matched += 1
                issue = None
            else:
                status = "PARTIAL" if any(issues) else "MISMATCH"
                issue = "; ".join(issues) if issues else "Values don't match reference"
            
            field_results.append(FieldResult(
                field=FIELD_DISPLAY_NAMES.get(field_name, field_name),
                status=status,
                reference=str(ref_value) if ref_value else "-",
                documents=doc_values,
                issue=issue
            ))
        
        # Calculate Phase 1 score (70% weight)
        if phase1_total > 0:
            phase1_percentage = (phase1_matched / phase1_total) * 100
            phase1_contribution = (phase1_percentage / 100) * 70
        else:
            phase1_contribution = 70  # No reference fields to validate
        
        print(f"[VALIDATE] Phase 1 complete: {phase1_matched}/{phase1_total} matched ({phase1_percentage:.1f}%)")
        
        # Phase 2: Additional fields (cross-document consensus)
        print(f"[VALIDATE] Phase 2: Validating additional fields across documents")
        phase2_results, phase2_contribution = self._validate_additional_fields(extracted_docs)
        field_results.extend(phase2_results)
        
        # Calculate overall score
        overall_score = int(phase1_contribution + phase2_contribution)
        
        print(f"[VALIDATE] Phase 1 contribution: {phase1_contribution:.1f}%")
        print(f"[VALIDATE] Phase 2 contribution: {phase2_contribution:.1f}%")
        print(f"[VALIDATE] Overall score: {overall_score}%")
        
        return field_results, overall_score
    
    def _validate_cross_document(
        self,
        extracted_docs: List[Dict[str, any]],
        documents: List[DocumentDetails]
    ) -> Tuple[List[FieldResult], int]:
        """
        Validate using cross-document consensus only (no DB reference).
        All fields have equal weight.
        """
        print(f"[VALIDATE] Cross-document validation (no DB reference)")
        
        field_results = []
        
        # Collect all unique fields across all documents
        all_fields = {}
        
        for extracted in extracted_docs:
            standard = extracted.get("standard_fields", {})
            additional = extracted.get("additional_fields", {})
            
            for field_name, value in {**standard, **additional}.items():
                if value:
                    if field_name not in all_fields:
                        all_fields[field_name] = {}
                    all_fields[field_name][extracted["document_name"]] = value
        
        total_fields = len(all_fields)
        total_score = 0
        
        for field_name, doc_values in all_fields.items():
            # Use GPT-4o to find consensus
            consensus_result = self.gpt4o.find_consensus(field_name, doc_values)
            
            consensus_value = consensus_result.get("consensus")
            agreement_count = consensus_result.get("agreement_count", 0)
            total_docs = consensus_result.get("total_documents", 1)
            all_match = consensus_result.get("all_match", False)
            issue = consensus_result.get("issue")
            
            # Calculate agreement percentage
            agreement_pct = (agreement_count / total_docs) * 100 if total_docs > 0 else 0
            
            # Determine status
            if all_match:
                status = "MATCH"
            elif agreement_pct >= 70:
                status = "PARTIAL"
            else:
                status = "MISMATCH"
            
            # Add to total score
            total_score += agreement_pct
            
            # Format document values
            formatted_docs = {}
            for doc_name in [doc.document_name for doc in documents]:
                formatted_docs[doc_name] = doc_values.get(doc_name, "-")
            
            field_results.append(FieldResult(
                field=FIELD_DISPLAY_NAMES.get(field_name, field_name.replace("_", " ").title()),
                status=status,
                reference="-",
                documents=formatted_docs,
                consensus=f"{consensus_value} ({agreement_count}/{total_docs} documents)" if consensus_value else None,
                issue=issue
            ))
        
        # Calculate average score
        overall_score = int(total_score / total_fields) if total_fields > 0 else 0
        
        print(f"[VALIDATE] Cross-document validation complete: {overall_score}%")
        
        return field_results, overall_score
    
    def _validate_additional_fields(
        self,
        extracted_docs: List[Dict[str, any]]
    ) -> Tuple[List[FieldResult], float]:
        """Validate additional fields across documents (Phase 2)."""
        
        # Collect additional fields
        additional_fields = {}
        
        for extracted in extracted_docs:
            for field_name, value in extracted.get("additional_fields", {}).items():
                if value:
                    if field_name not in additional_fields:
                        additional_fields[field_name] = {}
                    additional_fields[field_name][extracted["document_name"]] = value
        
        if not additional_fields:
            print(f"[VALIDATE] No additional fields found")
            return [], 30.0  # Full marks if no additional fields
        
        field_results = []
        total_agreement = 0
        
        for field_name, doc_values in additional_fields.items():
            consensus_result = self.gpt4o.find_consensus(field_name, doc_values)
            
            agreement_count = consensus_result.get("agreement_count", 0)
            total_docs = consensus_result.get("total_documents", 1)
            all_match = consensus_result.get("all_match", False)
            
            agreement_pct = (agreement_count / total_docs) * 100 if total_docs > 0 else 0
            total_agreement += agreement_pct
            
            status = "MATCH" if all_match else "PARTIAL"
            
            field_results.append(FieldResult(
                field=field_name.replace("_", " ").title(),
                status=status,
                reference="-",
                documents=doc_values,
                note="Cross-document validation"
            ))
        
        # Calculate Phase 2 contribution (30% weight)
        avg_agreement = total_agreement / len(additional_fields)
        phase2_contribution = (avg_agreement / 100) * 30
        
        print(f"[VALIDATE] Phase 2: {len(additional_fields)} additional fields, avg agreement: {avg_agreement:.1f}%")
        
        return field_results, phase2_contribution
    
    def _generate_report(
        self,
        FPCID: str,
        LMRId: str,
        checklistId: Optional[str],
        document_name: Optional[str],
        documents: List[str],
        field_results: List[FieldResult],
        score: int,
        has_reference: bool
    ) -> VerificationReport:
        """Generate simplified validation report."""
        
        # Count statuses
        matched = sum(1 for f in field_results if f.status == "MATCH")
        partial = sum(1 for f in field_results if f.status == "PARTIAL")
        failed = sum(1 for f in field_results if f.status == "MISMATCH")
        
        # Determine pass/fail
        status = "PASS" if score >= 85 else "FAIL"
        
        # Generate message
        if status == "PASS":
            message = "Identity verified successfully"
        else:
            message = "Validation failed - Manual review required"
        
        # Collect issues
        issues = [f.issue for f in field_results if f.issue]
        
        # Determine recommendation
        if score >= 95:
            action = "APPROVE"
            confidence = "HIGH"
            notes = "All critical fields verified. Strong validation score."
        elif score >= 85:
            action = "APPROVE"
            confidence = "MEDIUM-HIGH"
            notes = "Validation passed. Minor discrepancies detected but within acceptable threshold."
        elif score >= 70:
            action = "REVIEW"
            confidence = "MEDIUM"
            notes = "Manual review recommended. Multiple field discrepancies detected."
        else:
            action = "REJECT"
            confidence = "LOW"
            notes = "Critical mismatches detected. Recommend rejection or thorough manual review."
        
        # Create validation summary
        validation_summary = ValidationSummary(
            fpcid=FPCID,
            lmrid=LMRId,
            checklistId=checklistId,
            document_name=document_name,
            status=status,
            score=score,
            threshold=85,
            message=message,
            timestamp=datetime.now().isoformat() + "Z",
            note="No reference data in database - validated using document consensus" if not has_reference else None
        )
        
        # Create recommendation
        recommendation = RecommendationResult(
            action=action,
            confidence=confidence,
            notes=notes
        )
        
        # Create summary dict
        summary = {
            "total_fields": len(field_results),
            "matched": matched,
            "partial": partial,
            "failed": failed,
            "issues": issues
        }
        
        return VerificationReport(
            validation_summary=validation_summary,
            documents_validated=documents,
            field_results=field_results,
            summary=summary,
            recommendation=recommendation
        )

