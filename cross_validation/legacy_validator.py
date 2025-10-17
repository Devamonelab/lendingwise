"""
Legacy validator that integrates existing validation logic with new modular system.
This maintains compatibility with the existing extract_verified_details.py approach.
"""

import pandas as pd
import sqlalchemy
import json
import os
from datetime import datetime
from openai import OpenAI
from typing import Optional, Dict, Any

from .s3_operations import make_s3_client, parse_s3_url, get_json_from_s3
from .reports import write_comprehensive_json_report, write_enhanced_cross_validation_report_to_s3
from .models import VerificationReport, DocumentDetails, FieldMatch


class LegacyValidator:
    """Legacy validation system that maintains existing functionality."""
    
    def __init__(self, db_uri: str, openai_api_key: str, output_dir: str):
        self.db_uri = db_uri
        self.openai_client = OpenAI(api_key=openai_api_key)
        self.output_dir = output_dir
        self.s3_client = None
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize S3 client
        try:
            self.s3_client = make_s3_client()
            print("✅ AWS S3 connection successful")
        except Exception as e:
            print(f"⚠️ AWS S3 connection failed: {e}")
    
    def load_extracted_file_from_s3(self, s3_url: str) -> Optional[pd.DataFrame]:
        """Load JSON data from S3 URL using boto3."""
        try:
            print(f"🔄 Loading data from S3: {s3_url}")
            
            # Parse S3 URL
            bucket, key = parse_s3_url(s3_url)
            print(f"📦 Bucket: {bucket}")
            print(f"🔑 Key: {key}")
            
            # Download file from S3
            data = get_json_from_s3(self.s3_client, bucket, key)
            if not data:
                return None
            
            print(f"✅ Successfully loaded JSON data from S3")
            
            if isinstance(data, dict):
                return pd.DataFrame([data])
            elif isinstance(data, list):
                return pd.DataFrame(data)
            else:
                raise ValueError("Invalid JSON structure")
                
        except Exception as e:
            print(f"❌ Error loading from S3 URL {s3_url}: {e}")
            return None
    
    def smart_compare(self, value1, value2, col_name: str) -> bool:
        """Smart comparison that handles case sensitivity and date formats."""
        # Handle None/NaN values
        if pd.isna(value1) and pd.isna(value2):
            return True
        if pd.isna(value1) or pd.isna(value2):
            return False
        
        # Convert to string for comparison
        str1 = str(value1).strip()
        str2 = str(value2).strip()
        
        # For date fields, normalize dates before comparison
        date_fields = ['borrowerdob', 'issuedate', 'expirationdate', 'date', 'dob', 'birthdate']
        if any(field in col_name.lower() for field in date_fields):
            # Simple date normalization
            return str1.replace('-', '/').replace('.', '/') == str2.replace('-', '/').replace('.', '/')
        
        # For text fields (names, addresses, etc.), do case-insensitive comparison
        text_fields = ['borrowername', 'borrowermname', 'borrowerlname', 'address', 'city', 'state']
        if any(field in col_name.lower() for field in text_fields):
            return str1.lower() == str2.lower()
        
        # For other fields, do exact comparison
        return str1 == str2
    
    def llm_compare(self, field_name: str, extracted_val, db_val) -> Dict[str, Any]:
        """Compare values semantically using LLM for nuanced understanding."""
        if pd.isna(extracted_val) and pd.isna(db_val):
            return {"match_type": "Exact Match", "confidence": 1.0, "reason": "Both null"}
        
        # First check with smart comparison
        if self.smart_compare(extracted_val, db_val, field_name):
            return {"match_type": "Exact Match", "confidence": 1.0, "reason": "Values match"}
        
        prompt = f"""
        Compare the following two field values for semantic equivalence in a data validation context.
        Field: {field_name}
        Extracted Value: {extracted_val}
        Database Value: {db_val}

        Return a JSON with:
        - match_type: one of ["Exact Match", "Semantic Match", "Mismatch", "Unclear"]
        - confidence: float 0-1
        - reason: brief justification
        """

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2
            )
            result = json.loads(response.choices[0].message.content)
        except Exception as e:
            result = {"match_type": "Error", "confidence": 0, "reason": str(e)}

        return result
    
    def process_record(self, record: Dict[str, Any]) -> None:
        """Process a single record from tblaiagents."""
        FPCID = str(record.get('FPCID', ''))
        LMRId = str(record.get('LMRID', ''))
        s3_json_url = record.get('verified_result_s3_path', '')
        
        print(f"\n{'='*80}")
        print(f"🔄 PROCESSING RECORD")
        print(f"Record ID: {record.get('id', 'N/A')}")
        print(f"FPCID: {FPCID}")
        print(f"LMRId: {LMRId}")
        print(f"Document Name: {record.get('document_name', 'N/A')}")
        print(f"Verified Result S3 URL: {s3_json_url}")
        print(f"{'='*80}")
        
        if not FPCID or not LMRId or not s3_json_url:
            print(f"❌ Missing required fields - creating error report")
            self._create_error_report(record, "Missing required fields")
            return
        
        # Load extracted data from S3
        extracted_df = self.load_extracted_file_from_s3(s3_json_url)
        s3_loading_failed = extracted_df is None or extracted_df.empty
        
        if s3_loading_failed:
            print(f"⚠️ Failed to load data from S3 URL - creating dummy data for processing")
            extracted_df = pd.DataFrame({
                'FPCID': [FPCID],
                'LMRId': [LMRId],
                'data_source': ['S3_LOADING_FAILED'],
                'error_message': ['Could not load data from S3 URL']
            })
        else:
            extracted_df['FPCID'] = FPCID
            extracted_df['LMRId'] = LMRId
        
        # Fetch corresponding database record
        db_loading_failed = False
        try:
            engine = sqlalchemy.create_engine(self.db_uri)
            db_df = pd.read_sql(f"SELECT * FROM tblfile WHERE FPCID='{FPCID}' AND LMRId='{LMRId}'", engine)
            if db_df.empty:
                print(f"⚠️ No matching record found in tblfile")
                db_loading_failed = True
        except Exception as e:
            print(f"⚠️ Error fetching database record: {e}")
            db_loading_failed = True
        
        if db_loading_failed:
            print(f"⚠️ Database loading failed - creating dummy database data")
            db_df = pd.DataFrame({
                'FPCID': [FPCID],
                'LMRId': [LMRId],
                'data_source': ['DATABASE_LOADING_FAILED'],
                'error_message': ['Could not load data from database']
            })
        
        # Normalize column names
        extracted_df.columns = extracted_df.columns.str.strip().str.lower()
        db_df.columns = db_df.columns.str.strip().str.lower()
        
        # Find common columns
        common_columns = [
            c for c in extracted_df.columns
            if c in db_df.columns and c not in ['fpcid', 'lmrid']
        ]
        
        # Perform comparison
        results = []
        
        if not common_columns or s3_loading_failed or db_loading_failed:
            print(f"⚠️ Limited comparison possible - creating status report")
            
            status_reason = []
            if s3_loading_failed:
                status_reason.append("S3 data loading failed")
            if db_loading_failed:
                status_reason.append("Database data loading failed")
            if not common_columns and not s3_loading_failed and not db_loading_failed:
                status_reason.append("No common columns found for comparison")
            
            results.append({
                "column": "processing_status",
                "extracted_value": "PROCESSING_ATTEMPTED",
                "database_value": "PROCESSING_ATTEMPTED", 
                "match_type": "Processing Status",
                "confidence": 1.0,
                "reason": "; ".join(status_reason)
            })
        
        # Process common columns if available
        for col in common_columns:
            e_val = extracted_df[col].iloc[0] if not extracted_df.empty else None
            d_val = db_df[col].iloc[0] if not db_df.empty else None
            res = self.llm_compare(col, e_val, d_val)
            results.append({
                "column": col,
                "extracted_value": e_val,
                "database_value": d_val,
                "match_type": res["match_type"],
                "confidence": res["confidence"],
                "reason": res["reason"]
            })
        
        # Generate comprehensive report
        self._generate_comprehensive_report(record, results, s3_loading_failed, db_loading_failed, common_columns)
        
        print(f"\n✅ RECORD PROCESSING COMPLETE")
        print(f"{'='*80}")
    
    def _create_error_report(self, record: Dict[str, Any], error_message: str) -> None:
        """Create error report for failed processing."""
        record_id = record.get('id', f"fpcid_{record.get('FPCID', 'unknown')}_lmrid_{record.get('LMRID', 'unknown')}")
        error_filename = os.path.join(self.output_dir, f"processing_error_{record_id}.json")
        
        error_report = {
            "timestamp": datetime.now().isoformat(),
            "record_id": record.get('id'),
            "fpcid": record.get('FPCID'),
            "lmrid": record.get('LMRID'),
            "error_type": "PROCESSING_FAILED",
            "error_message": error_message,
            "status": "FAILED"
        }
        
        with open(error_filename, "w") as f:
            json.dump(error_report, f, indent=2)
        
        print(f"📁 Error report saved: {os.path.basename(error_filename)}")
    
    def _generate_comprehensive_report(self, record: Dict[str, Any], results: list, 
                                     s3_loading_failed: bool, db_loading_failed: bool, 
                                     common_columns: list) -> None:
        """Generate comprehensive validation report."""
        
        # Prepare detailed field comparisons
        detailed_field_comparisons = []
        for result in results:
            detailed_field_comparisons.append({
                "field_name": result["column"],
                "extracted_value": str(result["extracted_value"]) if pd.notna(result["extracted_value"]) else None,
                "database_value": str(result["database_value"]) if pd.notna(result["database_value"]) else None,
                "match_status": "MATCH" if result["match_type"] in ["Exact Match", "Semantic Match"] else "NO_MATCH",
                "match_type": result["match_type"],
                "confidence": float(result["confidence"]),
                "reason": result["reason"],
                "comparison_details": {
                    "comparison_type": "llm_enhanced",
                    "values_identical": str(result["extracted_value"]).strip() == str(result["database_value"]).strip() if pd.notna(result["extracted_value"]) and pd.notna(result["database_value"]) else False
                }
            })
        
        # Count matches and mismatches
        total_matches = len([f for f in detailed_field_comparisons if f['match_status'] == "MATCH"])
        total_mismatches = len([f for f in detailed_field_comparisons if f['match_status'] == "NO_MATCH"])
        
        # Determine final status
        if s3_loading_failed or db_loading_failed:
            final_status = "DATA_LOADING_FAILED"
            status_reason = f"Data loading issues - S3: {'FAILED' if s3_loading_failed else 'OK'}, DB: {'FAILED' if db_loading_failed else 'OK'}"
        elif not common_columns:
            final_status = "NO_COMPARISON_POSSIBLE"
            status_reason = "No common columns found for comparison"
        elif total_mismatches > 0:
            final_status = "HUMAN_REVIEW"
            status_reason = "Mismatches detected - Manual review required"
        else:
            final_status = "PASS"
            status_reason = "All fields match - Validation passed"
        
        # Create comprehensive report structure
        record_id = record.get('id', f'fpcid_{record.get("FPCID", "unknown")}_lmrid_{record.get("LMRID", "unknown")}')
        comprehensive_report_filename = os.path.join(self.output_dir, f"detailed_validation_report_{record_id}.json")
        
        comprehensive_report = {
            "report_metadata": {
                "report_type": "Detailed Validation Report",
                "generated_at": datetime.now().isoformat(),
                "report_version": "1.0",
                "processing_engine": "LLM_Enhanced_Comparison"
            },
            "record_information": {
                "record_id": record.get('id'),
                "fpcid": record.get('FPCID'),
                "lmrid": record.get('LMRID'),
                "document_name": record.get('document_name', 'N/A'),
                "agent_name": record.get('agent_name', 'N/A'),
                "document_status": record.get('document_status', 'N/A'),
                "verified_result_s3_path": record.get('verified_result_s3_path'),
                "database_table": "tblfile"
            },
            "data_loading_status": {
                "s3_data_loading": "SUCCESS" if not s3_loading_failed else "FAILED",
                "database_loading": "SUCCESS" if not db_loading_failed else "FAILED",
                "s3_error_details": "Could not load data from S3 URL" if s3_loading_failed else None,
                "database_error_details": "Could not load data from database" if db_loading_failed else None
            },
            "validation_summary": {
                "total_fields_compared": len(common_columns),
                "total_matches": total_matches,
                "total_mismatches": total_mismatches,
                "final_status": final_status,
                "status_reason": status_reason,
                "processing_successful": True
            },
            "detailed_field_comparisons": detailed_field_comparisons,
            "fields_summary": {
                "matched_fields": [f for f in detailed_field_comparisons if f["match_status"] == "MATCH"],
                "non_matched_fields": [f for f in detailed_field_comparisons if f["match_status"] == "NO_MATCH"],
                "total_matched": total_matches,
                "total_non_matched": total_mismatches,
                "match_percentage": round((total_matches / len(detailed_field_comparisons) * 100), 2) if detailed_field_comparisons else 0
            },
            "match_statistics": {
                "exact_matches": len([r for r in results if r["match_type"] == "Exact Match"]),
                "semantic_matches": len([r for r in results if r["match_type"] == "Semantic Match"]),
                "mismatches": len([r for r in results if r["match_type"] == "Mismatch"]),
                "unclear_matches": len([r for r in results if r["match_type"] == "Unclear"]),
                "processing_status_entries": len([r for r in results if r["match_type"] == "Processing Status"]),
                "errors": len([r for r in results if r["match_type"] == "Error"]),
                "total_comparisons": len(results)
            },
            "critical_issues": [
                {
                    "field_name": result["column"],
                    "issue_type": "MISMATCH",
                    "extracted_value": str(result["extracted_value"]) if pd.notna(result["extracted_value"]) else None,
                    "database_value": str(result["database_value"]) if pd.notna(result["database_value"]) else None,
                    "confidence": float(result["confidence"]),
                    "reason": result["reason"]
                }
                for result in results if result["match_type"] == "Mismatch"
            ],
            "data_quality_insights": {
                "common_columns_found": len(common_columns),
                "data_source_availability": {
                    "extracted_data_available": not s3_loading_failed,
                    "database_data_available": not db_loading_failed
                },
                "comparison_methodology": {
                    "date_fields_normalized": True,
                    "text_fields_case_insensitive": True,
                    "numeric_fields_exact_match": True,
                    "llm_semantic_analysis": True
                }
            },
            "next_actions": []
        }
        
        # Add next actions based on final status
        if final_status == "DATA_LOADING_FAILED":
            comprehensive_report["next_actions"] = [
                "Check AWS credentials and S3 access permissions" if s3_loading_failed else "S3 loading OK",
                "Verify database connection and query permissions" if db_loading_failed else "Database loading OK",
                "Confirm data sources are accessible",
                "Retry processing after fixing data access issues"
            ]
        elif final_status == "NO_COMPARISON_POSSIBLE":
            comprehensive_report["next_actions"] = [
                "Review data structure compatibility",
                "Check if extracted and database schemas match",
                "Verify field naming conventions",
                "Consider data mapping or transformation"
            ]
        elif final_status == "HUMAN_REVIEW":
            comprehensive_report["next_actions"] = [
                "Manual review of mismatched fields",
                "Verify extraction accuracy for low-confidence matches",
                "Review and approve semantic matches",
                "Update validation rules if needed"
            ]
        else:  # PASS
            comprehensive_report["next_actions"] = [
                "Validation completed successfully",
                "No further action required",
                "Consider this record as verified"
            ]
        
        # Save the comprehensive report locally (for backward compatibility)
        with open(comprehensive_report_filename, "w") as f:
            json.dump(comprehensive_report, f, indent=2)
        
        # Also save enhanced report to S3 if S3 client is available
        if self.s3_client:
            try:
                # Convert to VerificationReport format for S3 upload
                verification_report = self._convert_to_verification_report(record, detailed_field_comparisons, final_status, status_reason)
                s3_key = write_enhanced_cross_validation_report_to_s3(verification_report, self.s3_client)
                if s3_key:
                    print(f"[✓] Enhanced cross-validation report also saved to S3: {s3_key}")
                else:
                    print(f"[WARN] Failed to save enhanced report to S3")
            except Exception as e:
                print(f"[ERROR] Failed to save enhanced report to S3: {e}")
        
        # Print summary
        print(f"\n📊 FINAL VALIDATION RESULTS")
        print(f"📄 Document: {record.get('document_name', 'N/A')}")
        print(f"🤖 Agent: {record.get('agent_name', 'N/A')}")
        print(f"📋 Status: {record.get('document_status', 'N/A')}")
        print(f"🔗 Record ID: {record.get('id', 'N/A')}")
        print("-" * 80)
        print(f"Total fields compared: {len(common_columns)}")
        print(f"Total matches: {total_matches}")
        print(f"Total mismatches: {total_mismatches}")
        print(f"Final status: {final_status}")
        print(f"Status reason: {status_reason}")
        
        print(f"\n📁 OUTPUT FILE GENERATED:")
        print(f"• {os.path.basename(comprehensive_report_filename)} - Comprehensive validation report")
        print(f"\n📂 File saved to: {self.output_dir}")
        
        # Show field details
        matched_fields = [f for f in detailed_field_comparisons if f['match_status'] == "MATCH"]
        non_matched_fields = [f for f in detailed_field_comparisons if f['match_status'] == "NO_MATCH"]
        
        if matched_fields:
            print(f"\n✅ MATCHED FIELDS ({len(matched_fields)}):")
            for field in matched_fields:
                print(f"   • {field['field_name']}: {field['match_type']} (confidence: {field['confidence']:.2f})")
                print(f"     Extracted: '{field['extracted_value']}' | Database: '{field['database_value']}'")
        
        if non_matched_fields:
            print(f"\n❌ NON-MATCHED FIELDS ({len(non_matched_fields)}):")
            for field in non_matched_fields:
                print(f"   • {field['field_name']}: {field['match_type']} (confidence: {field['confidence']:.2f})")
                print(f"     Extracted: '{field['extracted_value']}' | Database: '{field['database_value']}'")
                print(f"     Reason: {field['reason']}")
        
        match_percentage = (len(matched_fields) / len(detailed_field_comparisons) * 100) if detailed_field_comparisons else 0
        print(f"\n📊 OVERALL MATCH RATE: {match_percentage:.1f}% ({len(matched_fields)}/{len(detailed_field_comparisons)} fields)")
        
        print(f"\n💡 NEXT ACTIONS:")
        for action in comprehensive_report["next_actions"]:
            print(f"• {action}")
    
    def _convert_to_verification_report(self, record: Dict[str, Any], detailed_field_comparisons: list, 
                                      final_status: str, status_reason: str) -> VerificationReport:
        """Convert legacy format to VerificationReport for S3 upload."""
        
        # Create DocumentDetails
        documents = [DocumentDetails(
            document_name=record.get('document_name', 'Unknown'),
            agent_name=record.get('agent_name'),
            tool=None,  # Not available in legacy format
            file_s3_location=None,  # Not available in legacy format
            metadata_s3_path=None,  # Not available in legacy format
            verified_result_s3_path=record.get('verified_result_s3_path'),
            verified_details={"legacy_processing": True}  # Placeholder
        )]
        
        # Create FieldMatch objects
        field_matches = []
        for field_comp in detailed_field_comparisons:
            sources = {}
            if field_comp.get('extracted_value') is not None:
                sources["S3 Document"] = field_comp['extracted_value']
            if field_comp.get('database_value') is not None:
                sources["DB (tblfile)"] = field_comp['database_value']
            
            field_matches.append(FieldMatch(
                field_name=field_comp['field_name'],
                matched=field_comp['match_status'] == "MATCH",
                sources=sources,
                mismatch_reason=field_comp.get('reason') if field_comp['match_status'] != "MATCH" else None
            ))
        
        # Map legacy status to verification status
        status_mapping = {
            "PASS": "VERIFIED",
            "HUMAN_REVIEW": "PARTIAL", 
            "DATA_LOADING_FAILED": "FAILED",
            "NO_COMPARISON_POSSIBLE": "FAILED"
        }
        overall_status = status_mapping.get(final_status, "FAILED")
        
        # Create detailed findings
        detailed_findings = [
            f"Legacy validation completed with status: {final_status}",
            f"Reason: {status_reason}",
            f"Total fields compared: {len(detailed_field_comparisons)}",
            f"Matched fields: {len([f for f in detailed_field_comparisons if f['match_status'] == 'MATCH'])}",
            f"Mismatched fields: {len([f for f in detailed_field_comparisons if f['match_status'] == 'NO_MATCH'])}"
        ]
        
        return VerificationReport(
            FPCID=str(record.get('FPCID', '')),
            LMRId=str(record.get('LMRID', '')),
            overall_status=overall_status,
            summary=status_reason,
            db_data_available=final_status not in ["DATA_LOADING_FAILED", "NO_COMPARISON_POSSIBLE"],
            documents=documents,
            field_matches=field_matches,
            detailed_findings=detailed_findings
        )
    
    def fetch_ai_agent_records(self) -> pd.DataFrame:
        """Fetch records from tblaiagents table."""
        engine = sqlalchemy.create_engine(self.db_uri)
        
        try:
            # Check total records in table
            total_query = "SELECT COUNT(*) as total FROM tblaiagents"
            total_records = pd.read_sql(total_query, engine)
            print(f"📊 Total records in tblaiagents: {total_records['total'].iloc[0]}")
            
            # Check records with verified_result_s3_path
            verified_query = "SELECT COUNT(*) as verified_count FROM tblaiagents WHERE verified_result_s3_path IS NOT NULL AND verified_result_s3_path != ''"
            verified_records = pd.read_sql(verified_query, engine)
            print(f"📊 Records with verified_result_s3_path: {verified_records['verified_count'].iloc[0]}")
            
        except Exception as e:
            print(f"❌ Error checking table structure: {e}")
            return pd.DataFrame()
        
        # Fetch the actual records
        query = "SELECT id, FPCID, LMRID, document_name, agent_name, document_status, verified_result_s3_path FROM tblaiagents WHERE verified_result_s3_path IS NOT NULL AND verified_result_s3_path != ''"
        
        try:
            print(f"🔍 Executing query: {query}")
            records = pd.read_sql(query, engine)
            print(f"📊 Query returned {len(records)} records")
            return records
        except Exception as e:
            print(f"❌ Error fetching from tblaiagents: {e}")
            return pd.DataFrame()
    
    def run_validation(self) -> None:
        """Run the validation process."""
        print(f"📁 Using output directory: {self.output_dir}")
        
        # Fetch records from tblaiagents
        ai_agent_records = self.fetch_ai_agent_records()
        
        if ai_agent_records.empty:
            print(f"❌ No records found in tblaiagents")
            return
        
        print(f"📋 Found {len(ai_agent_records)} record(s) to process")
        
        # Process each record
        for index, record in ai_agent_records.iterrows():
            self.process_record(record.to_dict())
        
        # Final summary
        print(f"\n🎉 BATCH PROCESSING COMPLETE")
        print(f"Processed {len(ai_agent_records)} record(s) from tblaiagents")
        print(f"All validation results saved with unique filenames per record")
        print(f"📂 Output location: {self.output_dir}")


def main():
    """Main function for legacy validator."""
    # Configuration
    DB_URI = "mysql+pymysql://root:NewStrong!Passw0rd@127.0.0.1:3306/stage_newskinny"
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OUTPUT_DIR = r"C:\LendingWise\lendingwise-ai\lendingwise-ai\result"
    
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY environment variable is required")
    
    # Create and run validator
    validator = LegacyValidator(DB_URI, OPENAI_API_KEY, OUTPUT_DIR)
    validator.run_validation()


if __name__ == "__main__":
    main()
