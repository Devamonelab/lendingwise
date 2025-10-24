"""
Database operations for cross validation system.
"""

import os
import sys
from typing import Dict, List, Optional, Tuple

import mysql.connector
from mysql.connector import Error

from ..models.data_models import BorrowerData


def connect_db():
    """Create a new DB connection using env-var defaults."""
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "3.129.145.187"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "aiagentdb"),
        password=os.getenv("DB_PASSWORD", "Agents@1252"),
        database=os.getenv("DB_NAME", "stage_newskinny"),
        autocommit=True,
    )


def _coerce_bool(val) -> bool:
    """Convert various types to boolean."""
    if isinstance(val, (int, bool)):
        return bool(val)
    if isinstance(val, (bytes, bytearray)):
        return val not in (b"\x00", b"0", b"", None)
    if isinstance(val, str):
        return val.strip().lower() in {"1", "true", "t", "y", "yes"}
    return False


def fetch_all_statuses_grouped(conn) -> Dict[Tuple[str, str, str, str], bool]:
    """Return ALL-TRUE status for each (FPCID, checklistId, document_name, LMRId) combination."""
    sql = (
        "SELECT FPCID, checklistId, document_name, LMRId, "
        "MIN(COALESCE(cross_validation, 0)) AS all_true "
        "FROM tblaiagents "
        "WHERE checklistId IS NOT NULL AND document_name IS NOT NULL "
        "GROUP BY FPCID, checklistId, document_name, LMRId"
    )
    cur = conn.cursor()
    try:
        cur.execute(sql)
        out: Dict[Tuple[str, str, str, str], bool] = {}
        for FPCID, checklistId, document_name, LMRId, val in cur.fetchall() or []:
            out[(str(FPCID), str(checklistId), str(document_name), str(LMRId))] = _coerce_bool(val)
        return out
    finally:
        try:
            cur.close()
        except Exception:
            pass


def fetch_doc_for_validation(
    conn, FPCID: str, checklistId: str, document_name: str, LMRId: str, require_file_s3: bool = True
) -> Dict[str, Optional[str]]:
    """Fetch a single document row for validation based on FPCID + checklistId + document_name."""
    cols = [
        "document_name",
        "agent_name", 
        "tool",
        "file_s3_location",
        "metadata_s3_path",
        "verified_result_s3_path",
        "uploadedat",
        "date",
        "checklistId",
        "LMRId",
    ]
    where = [
        "FPCID = %s",
        "checklistId = %s",
        "document_name = %s",
        "verified_result_s3_path IS NOT NULL",
        "verified_result_s3_path <> ''",
    ]
    if require_file_s3:
        where.append("file_s3_location IS NOT NULL AND file_s3_location <> ''")

    sql = (
        f"SELECT {', '.join(cols)} FROM tblaiagents "
        f"WHERE {' AND '.join(where)} LIMIT 1"
    )
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, (FPCID, checklistId, document_name))
        row = cur.fetchone()
        if row:
            return {k: (str(v) if v is not None else None) for k, v in row.items()}
        return {}
    finally:
        try:
            cur.close()
        except Exception:
            pass


def fetch_borrower_data_from_tblfile(conn, FPCID: str, LMRId: str) -> Optional[BorrowerData]:
    """
    Fetch borrower data from tblfile for the given FPCID/LMRId pair.
    Returns None if no data found.
    """
    sql = """
        SELECT 
            FPCID, LMRId,
            borrowerName, borrowerMName, borrowerLName, borrowerDOB, borrowerPOB,
            driverLicenseNumber, driverLicenseState
        FROM tblfile
        WHERE FPCID = %s AND LMRId = %s
        LIMIT 1
    """
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, (FPCID, LMRId))
        row = cur.fetchone()
        if not row:
            return None
        
        return BorrowerData(
            FPCID=str(row.get("FPCID")) if row.get("FPCID") else None,
            LMRId=str(row.get("LMRId")) if row.get("LMRId") else None,
            borrowerName=str(row.get("borrowerName")) if row.get("borrowerName") else None,
            borrowerMName=str(row.get("borrowerMName")) if row.get("borrowerMName") else None,
            borrowerLName=str(row.get("borrowerLName")) if row.get("borrowerLName") else None,
            borrowerDOB=str(row.get("borrowerDOB")) if row.get("borrowerDOB") else None,
            borrowerPOB=str(row.get("borrowerPOB")) if row.get("borrowerPOB") else None,
            driverLicenseNumber=str(row.get("driverLicenseNumber")) if row.get("driverLicenseNumber") else None,
            driverLicenseState=str(row.get("driverLicenseState")) if row.get("driverLicenseState") else None,
        )
    except Error as e:
        print(f"[DB] Error fetching from tblfile: {e}", file=sys.stderr)
        return None
    finally:
        try:
            cur.close()
        except Exception:
            pass


def update_is_verified(conn, FPCID: str, checklistId: str, document_name: str, flag: bool, s3_report_path: Optional[str] = None, LMRId: str = None) -> None:
    """
    Update the Is_varified status and cross_validation_report_path for a specific document.
    
    Uses FPCID + checklistId + document_name for precise row matching.
    
    Args:
        conn: Database connection
        FPCID: Tenant ID
        checklistId: Checklist ID
        document_name: Document name
        flag: Verification status (True/False)
        s3_report_path: S3 path to the cross-validation report
        LMRId: Loan file ID (optional, can be updated if provided)
    """
    if not FPCID or not checklistId or not document_name:
        print(f"[DB] update_is_verified skipped: missing required fields - FPCID={FPCID}, checklistId={checklistId}, document_name={document_name}")
        return
    
    # Build SET clause
    set_parts = ["`Is_varified` = %s"]
    params = [int(flag)]
    
    if s3_report_path:
        set_parts.append("`cross_validation_report_path` = %s")
        params.append(s3_report_path)
    
    if LMRId:
        set_parts.append("`LMRId` = %s")
        params.append(LMRId)
    
    # Build WHERE clause - STRICTLY using FPCID + checklistId + document_name
    sql = (
        f"UPDATE tblaiagents SET {', '.join(set_parts)} "
        "WHERE FPCID = %s AND checklistId = %s AND document_name = %s"
    )
    params.extend([FPCID, checklistId, document_name])
    
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
