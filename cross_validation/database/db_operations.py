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


def fetch_all_statuses_grouped(conn) -> Dict[Tuple[str, str], bool]:
    """Return ALL-TRUE status for each (FPCID, LMRId) pair."""
    sql = (
        "SELECT FPCID, LMRId, MIN(COALESCE(cross_validation, 0)) AS all_true "
        "FROM tblaiagents GROUP BY FPCID, LMRId"
    )
    cur = conn.cursor()
    try:
        cur.execute(sql)
        out: Dict[Tuple[str, str], bool] = {}
        for FPCID, LMRId, val in cur.fetchall() or []:
            out[(str(FPCID), str(LMRId))] = _coerce_bool(val)
        return out
    finally:
        try:
            cur.close()
        except Exception:
            pass


def fetch_docs_for_pair(
    conn, FPCID: str, LMRId: str, require_file_s3: bool = True
) -> List[Dict[str, Optional[str]]]:
    """Fetch document rows for a FPCID/LMRId pair where verified_result_s3_path is set."""
    cols = [
        "document_name",
        "agent_name", 
        "tool",
        "file_s3_location",
        "metadata_s3_path",
        "verified_result_s3_path",
        "uploadedat",
        "date",
    ]
    where = [
        "FPCID = %s",
        "LMRId = %s",
        "verified_result_s3_path IS NOT NULL",
        "verified_result_s3_path <> ''",
    ]
    if require_file_s3:
        where.append("file_s3_location IS NOT NULL AND file_s3_location <> ''")

    sql = (
        f"SELECT {', '.join(cols)} FROM tblaiagents "
        f"WHERE {' AND '.join(where)}"
    )
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, (FPCID, LMRId))
        rows = cur.fetchall() or []
        return [
            {k: (str(v) if v is not None else None) for k, v in row.items()}
            for row in rows
        ]
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


def update_is_verified(conn, FPCID: str, LMRId: str, flag: bool, s3_report_path: Optional[str] = None) -> None:
    """
    Update the Is_varified status and cross_validation_report_path for a FPCID/LMRId pair.
    
    Args:
        conn: Database connection
        FPCID: Tenant ID
        LMRId: Loan file ID
        flag: Verification status (True/False)
        s3_report_path: S3 path to the cross-validation report
    """
    if s3_report_path:
        sql = (
            "UPDATE tblaiagents SET `Is_varified` = %s, `cross_validation_report_path` = %s "
            "WHERE FPCID = %s AND LMRId = %s"
        )
        params = (int(flag), s3_report_path, FPCID, LMRId)
    else:
        sql = (
            "UPDATE tblaiagents SET `Is_varified` = %s "
            "WHERE FPCID = %s AND LMRId = %s"
        )
        params = (int(flag), FPCID, LMRId)
    
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
