"""
Tamper detection functionality for document security analysis.
"""

import os
import hashlib
import fitz  # PyMuPDF
from PIL import Image
import imagehash
import pytesseract
import pandas as pd
from docx import Document
import difflib
import mimetypes
from datetime import datetime
from typing import Dict, Any, List, Optional

from ..config.settings import OUTPUT_DIR


def sha256_hash(file_path: str) -> str:
    """Compute SHA256 hash of any file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def get_metadata(file_path: str) -> Dict[str, Any]:
    """Get basic file metadata."""
    stats = os.stat(file_path)
    return {
        "file": os.path.basename(file_path),
        "size": stats.st_size,
        "created": datetime.fromtimestamp(stats.st_ctime).isoformat(),
        "modified": datetime.fromtimestamp(stats.st_mtime).isoformat(),
    }


def canonicalize_csv(file_path: str) -> str:
    """Canonicalize CSV for stable hashing."""
    try:
        df = pd.read_csv(file_path)
        df = df.sort_index(axis=1).sort_values(by=df.columns[0])
        return hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()
    except Exception as e:
        return f"Error reading CSV: {e}"


def pdf_text_extract(file_path: str) -> str:
    """Extract visible text from PDF."""
    text = ""
    pdf = fitz.open(file_path)
    for page in pdf:
        text += page.get_text("text")
    pdf.close()
    return text.strip()


def detect_pdf_structure_anomalies(file_path: str) -> List[str]:
    """Detect suspicious embedded content or edits in PDF."""
    pdf = fitz.open(file_path)
    issues = []
    for page in pdf:
        if page.annots():
            issues.append(f"Page {page.number} has annotations (possible edit marks).")
        if page.get_links():
            issues.append(f"Page {page.number} contains hyperlinks (unusual for IDs/invoices).")
        raw = page.get_text("rawdict")
        if raw and "/JS" in str(raw):
            issues.append("Embedded JavaScript detected (potential malicious edit).")
    pdf.close()
    return issues


def docx_text_extract(file_path: str) -> str:
    """Extract text from DOCX."""
    try:
        doc = Document(file_path)
        return "\n".join([p.text for p in doc.paragraphs])
    except Exception as e:
        return f"Error reading DOCX: {e}"


def image_phash(file_path: str) -> str:
    """Compute perceptual hash (detects visual tampering)."""
    try:
        img = Image.open(file_path).convert("RGB")
        return str(imagehash.phash(img))
    except Exception as e:
        return f"Error hashing image: {e}"


def image_ocr_text(file_path: str) -> str:
    """Extract text via OCR for visual-text consistency check."""
    try:
        img = Image.open(file_path)
        return pytesseract.image_to_string(img)
    except Exception as e:
        return f"OCR failed: {e}"


def diff_texts(text1: str, text2: str) -> str:
    """Diff two text strings to explain text tampering."""
    d = difflib.unified_diff(text1.splitlines(), text2.splitlines(), lineterm="", n=3)
    return "\n".join(d)


def tamper_check(file_path: str, baseline_texts: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Analyze a file for tampering/fraud and return results.
    Returns dict with status, reasons, and metadata.
    """
    if baseline_texts is None:
        baseline_texts = {}
    
    file_type, _ = mimetypes.guess_type(file_path)
    meta = get_metadata(file_path)
    results = {"file": file_path, "status": "OK", "reasons": [], "meta": meta}

    content_hash = None
    extracted_text = None

    try:
        # Detect file type and extract content
        if file_type and "pdf" in file_type:
            extracted_text = pdf_text_extract(file_path)
            content_hash = hashlib.sha256(extracted_text.encode()).hexdigest()
            pdf_issues = detect_pdf_structure_anomalies(file_path)
            if pdf_issues:
                results["status"] = "SUSPICIOUS"
                results["reasons"].extend(pdf_issues)

        elif file_type and "image" in file_type:
            phash_val = image_phash(file_path)
            text_val = image_ocr_text(file_path)
            extracted_text = text_val
            content_hash = hashlib.sha256((phash_val + text_val).encode()).hexdigest()

        elif file_type and "csv" in file_type:
            content_hash = canonicalize_csv(file_path)

        elif file_type and ("word" in file_type or file_path.endswith(".docx")):
            extracted_text = docx_text_extract(file_path)
            content_hash = hashlib.sha256(extracted_text.encode()).hexdigest()

        else:
            # Generic file fallback
            content_hash = sha256_hash(file_path)

        # Metadata consistency check
        created = datetime.fromisoformat(meta["created"])
        modified = datetime.fromisoformat(meta["modified"])
        if modified < created:
            results["status"] = "SUSPICIOUS"
            results["reasons"].append(
                "File modified timestamp is earlier than creation time — possible manual date manipulation."
            )

        # Baseline comparison (if provided)
        base = baseline_texts.get(os.path.basename(file_path))
        if base and base != content_hash:
            results["status"] = "TAMPERED"
            results["reasons"].append("File hash differs from baseline — content modified.")
            if extracted_text:
                diff = diff_texts(base[:4000], extracted_text[:4000])
                if diff:
                    results["reasons"].append("Text changes detected:\n" + diff[:800])
        elif not base:
            results["reasons"].append("No baseline found (first time scanned).")

        # Image heuristics
        if file_type and "image" in file_type:
            if "Error" not in content_hash:
                results["reasons"].append(
                    "Image analyzed using perceptual hashing and OCR. "
                    "Visual or text differences from the baseline will mark as tampered."
                )

    except Exception as e:
        results["status"] = "ERROR"
        results["reasons"].append(f"Error analyzing file: {e}")

    return results


def create_sample_baseline() -> Optional[str]:
    """
    Create a sample baseline file for testing tamper detection.
    This is a utility function for testing purposes.
    """
    sample_baseline = {
        "sample_document.pdf": "abc123def456ghi789",  # Sample hash
        "test_id.jpg": "xyz789uvw456rst123",  # Sample hash
    }
    
    baseline_file = os.path.join(OUTPUT_DIR, "sample_baseline.json")
    try:
        with open(baseline_file, "w", encoding="utf-8") as f:
            json.dump(sample_baseline, f, indent=2, ensure_ascii=False)
        print(f"Sample baseline file created: {baseline_file}")
        return baseline_file
    except Exception as e:
        print(f"Error creating sample baseline: {e}")
        return None
