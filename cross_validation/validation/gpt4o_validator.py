"""
GPT-4o powered field extraction and validation.
"""

import json
import os
from typing import Dict, List, Optional, Any, Tuple
from openai import OpenAI


class GPT4oValidator:
    """GPT-4o powered validation engine."""
    
    def __init__(self):
        """Initialize GPT-4o client."""
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        self.client = OpenAI(api_key=api_key)
        self.model = "gpt-4o"
    
    def extract_fields_from_document(
        self, 
        document_data: Dict[str, Any], 
        document_name: str
    ) -> Dict[str, Any]:
        """
        Extract standard and additional fields from document using GPT-4o.
        
        Args:
            document_data: Raw document data from S3
            document_name: Name of the document
            
        Returns:
            Dict with standard_fields and additional_fields
        """
        prompt = f"""You are a document analysis expert. Extract identity fields from this document.

Document Type: {document_name}
Document Data:
{json.dumps(document_data, indent=2)}

Extract these standard fields if present (use exact field names):
1. borrowerName (first name - this is the PRIMARY field name in database)
2. borrowerMName (middle name) 
3. borrowerLName (last name)
4. borrowerDOB (date of birth - normalize to MM/DD/YYYY format if possible)
5. driverLicenseNumber (license/ID number - prefer shorter license number if multiple IDs present)
6. driverLicenseState (state code)
7. borrowerPOB (place of birth)

Also extract any additional identity fields like:
- suffix, addressLine1, city, zip, expirationDate, issueDate, etc.

IMPORTANT: Return ONLY valid JSON, no markdown formatting or explanation.

Return format:
{{
  "standard_fields": {{
    "borrowerName": "value or null",
    "borrowerMName": "value or null",
    "borrowerLName": "value or null",
    "borrowerDOB": "value or null",
    "driverLicenseNumber": "value or null",
    "driverLicenseState": "value or null",
    "borrowerPOB": "value or null"
  }},
  "additional_fields": {{
    "suffix": "value or null",
    "addressLine1": "value or null"
  }}
}}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a document field extraction expert. Always return valid JSON only."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            print(f"[GPT-4o] Extracted fields from {document_name}: {len(result.get('standard_fields', {}))} standard, {len(result.get('additional_fields', {}))} additional")
            return result
            
        except Exception as e:
            print(f"[ERROR] GPT-4o field extraction failed for {document_name}: {e}")
            return {
                "standard_fields": {
                    "borrowerName": None,
                    "borrowerMName": None,
                    "borrowerLName": None,
                    "borrowerDOB": None,
                    "driverLicenseNumber": None,
                    "driverLicenseState": None,
                    "borrowerPOB": None
                },
                "additional_fields": {}
            }
    
    def compare_values(
        self, 
        reference_value: str, 
        document_value: str, 
        field_name: str,
        document_name: str
    ) -> Dict[str, Any]:
        """
        Compare reference value with document value using GPT-4o.
        
        Args:
            reference_value: Value from database (reference)
            document_value: Value from document
            field_name: Name of the field
            document_name: Name of the document
            
        Returns:
            Dict with match (bool) and reason (str)
        """
        # Handle null/empty values
        if not reference_value and not document_value:
            return {"match": True, "reason": "Both values are empty"}
        if not reference_value:
            return {"match": True, "reason": "No reference value to compare"}
        if not document_value:
            return {"match": False, "reason": f"{document_name} has no value for this field"}
        
        prompt = f"""Compare these two values for the field '{field_name}':

Reference (Database - Source of Truth): "{reference_value}"
Document ({document_name}): "{document_value}"

Are they the same? Consider:
- Exact matches
- Case differences (JOHN vs John)
- Format differences (07/25/1980 vs 1980-07-25 vs July 25, 1980)
- Abbreviations (DEAN vs D, Street vs St)
- Nicknames (common name variations)
- Semantic equivalence (1150 GOODWIN PL NE vs 1150 Goodwin Place Northeast)

IMPORTANT: Return ONLY valid JSON, no markdown or explanation.

Return format:
{{
  "match": true or false,
  "reason": "brief explanation"
}}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data validation expert. Always return valid JSON only."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            return result
            
        except Exception as e:
            print(f"[ERROR] GPT-4o comparison failed for {field_name}: {e}")
            # Fallback to simple comparison
            match = str(reference_value).strip().upper() == str(document_value).strip().upper()
            return {
                "match": match,
                "reason": "Exact match" if match else "Values differ (GPT-4o unavailable)"
            }
    
    def find_consensus(
        self, 
        field_name: str, 
        document_values: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Find consensus value across documents using GPT-4o.
        
        Args:
            field_name: Name of the field
            document_values: {doc_name: value} mapping
            
        Returns:
            Dict with consensus, agreement_count, total_documents, all_match, issue
        """
        # Filter out None/empty values
        valid_values = {doc: val for doc, val in document_values.items() if val}
        
        if not valid_values:
            return {
                "consensus": None,
                "agreement_count": 0,
                "total_documents": len(document_values),
                "all_match": False,
                "issue": "No values found in any document"
            }
        
        if len(valid_values) == 1:
            doc_name, value = list(valid_values.items())[0]
            return {
                "consensus": value,
                "agreement_count": 1,
                "total_documents": 1,
                "all_match": True,
                "issue": None
            }
        
        prompt = f"""Analyze these values for field '{field_name}' across multiple documents:

{json.dumps(valid_values, indent=2)}

Determine:
1. Are these values semantically the same?
2. What is the consensus/most likely correct value?
3. How many documents agree with the consensus?
4. Do all documents match (considering semantic equivalence)?

Consider:
- Format variations (dates, addresses)
- Abbreviations
- Case differences
- Semantic equivalence

IMPORTANT: Return ONLY valid JSON, no markdown or explanation.

Return format:
{{
  "consensus": "most likely correct value",
  "agreement_count": number of documents agreeing with consensus,
  "total_documents": {len(valid_values)},
  "all_match": true or false,
  "issue": "description if mismatch, else null"
}}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a consensus analysis expert. Always return valid JSON only."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            return result
            
        except Exception as e:
            print(f"[ERROR] GPT-4o consensus failed for {field_name}: {e}")
            # Fallback: find most common value
            from collections import Counter
            values_list = [str(v).strip().upper() for v in valid_values.values()]
            counter = Counter(values_list)
            most_common = counter.most_common(1)[0]
            consensus_value = most_common[0]
            agreement_count = most_common[1]
            all_match = agreement_count == len(valid_values)
            
            return {
                "consensus": consensus_value,
                "agreement_count": agreement_count,
                "total_documents": len(valid_values),
                "all_match": all_match,
                "issue": None if all_match else f"Only {agreement_count}/{len(valid_values)} documents agree"
            }

