"""
Simple database helper to insert AI agent results into MySQL-compatible DB.

Environment variables expected (with reasonable defaults for dev):
  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

Target table: stage_newskinny.tblaigents
"""

import os
from typing import Optional, Dict, Any

import mysql.connector
from mysql.connector import Error


def _make_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "3.129.145.187"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "aiagentdb"),
        password=os.getenv("DB_PASSWORD", "Agents@1252"),
        database=os.getenv("DB_NAME", "stage_newskinny"),
        autocommit=True,
    )


def insert_tblaigents(row: Dict[str, Any]) -> None:
    """
    Insert a row into stage_newskinny.tblaiagents.

    Expected keys in `row` (any missing will be stored as NULL):
      id, FPCID, LMRId, document_name, agent_name, tool,
      file_s3_location, date, document_status, uploadedat,
      metadata_s3_path, verified_result_s3_path, created_at
    """
    cols = [
        "id","FPCID","LMRId","document_name","agent_name","tool",
        "file_s3_location","date","document_status","uploadedat",
        "metadata_s3_path","verified_result_s3_path","created_at",
    ]
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO stage_newskinny.tblaiagents ({','.join(cols)}) VALUES ({placeholders})"

    values = [row.get(c) for c in cols]

    conn = None
    cursor = None
    try:
        conn = _make_connection()
        cursor = conn.cursor()
        cursor.execute(sql, values)
        conn.commit()
    except Error as e:
        # Surface but do not crash pipeline
        print(f"[DB] Insert failed: {e}")
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass



def fetch_agent_context(FPCID: str, LMRId: str, document_name: str = None) -> Dict[str, Any]:
    """
    Fetch minimal agent context for a borrower/loan using FPCID + LMRId.
    If document_name is provided, it will try to match the specific document first,
    then fall back to any document for the FPCID/LMRId combination.

    Returns a dict that may contain: document_name, agent_name, tool.
    If no row is found or an error occurs, returns an empty dict.
    """
    conn = None
    cursor = None
    try:
        conn = _make_connection()
        cursor = conn.cursor(dictionary=True)
        
        # First try to find exact match with document_name if provided
        if document_name:
            sql = (
                "SELECT document_name, agent_name, tool FROM stage_newskinny.tblaiagents "
                "WHERE FPCID = %s AND LMRId = %s AND document_name = %s LIMIT 1"
            )
            cursor.execute(sql, [FPCID, LMRId, document_name])
            row = cursor.fetchone()
            if row:
                return row
        
        # Fallback: find any document for this FPCID/LMRId combination
        sql = (
            "SELECT document_name, agent_name, tool FROM stage_newskinny.tblaiagents "
            "WHERE FPCID = %s AND LMRId = %s LIMIT 1"
        )
        cursor.execute(sql, [FPCID, LMRId])
        row = cursor.fetchone()
        return row or {}
    except Error as e:
        print(f"[DB] fetch_agent_context failed: {e}")
        return {}
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass


def update_tblaigents_by_keys(FPCID: str, LMRId: str, updates: Dict[str, Any], document_name: str | None = None) -> None:
    """
    Update only selected nullable fields for an existing row identified by FPCID + LMRId.

    Allowed fields to update:
      file_s3_location, document_status, uploadedat, metadata_s3_path,
      verified_result_s3_path, cross_validation
    """
    if not FPCID or not LMRId:
        print("[DB] update skipped: missing FPCID or LMRId")
        return

    allowed = {
        "file_s3_location": updates.get("file_s3_location"),
        "document_status": updates.get("document_status"),
        "uploadedat": updates.get("uploadedat"),
        "metadata_s3_path": updates.get("metadata_s3_path"),
        "verified_result_s3_path": updates.get("verified_result_s3_path"),
        "cross_validation": updates.get("cross_validation"),
        "doc_verification_result": updates.get("doc_verification_result"),
    }

    set_parts = []
    values = []
    for col, val in allowed.items():
        # Only update columns that are explicitly provided with a non-None value
        if col in updates and val is not None:
            set_parts.append(f"{col} = %s")
            values.append(val)

    if not set_parts:
        print("[DB] update skipped: no allowed fields provided")
        return

    # Build WHERE with optional document_name filter to target the correct row
    where = "WHERE FPCID = %s AND LMRId = %s"
    values.extend([FPCID, LMRId])
    if document_name:
        where += " AND document_name = %s"
        values.append(document_name)

    sql = (
        f"UPDATE stage_newskinny.tblaiagents SET {', '.join(set_parts)} "
        f"{where} LIMIT 1"
    )

    conn = None
    cursor = None
    try:
        conn = _make_connection()
        cursor = conn.cursor()
        cursor.execute(sql, values)
        conn.commit()
    except Error as e:
        print(f"[DB] Update failed: {e}")
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass
