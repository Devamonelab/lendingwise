from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import uuid
import os

app = FastAPI(
    title="AI Agents Database API",
    description="Simple API for creating AI agent records",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins - restrict this in production
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # Allows all headers
)


# Pydantic model for request
class AgentRecordCreate(BaseModel):
    """Model for creating a new agent record"""
    FPCID: str = Field(..., description="FPC ID", example="3580")
    doc_id: str = Field(..., description="Document ID", example="23")
    document_name: str = Field(..., description="Name of the document", example="Driving license")
    agent_name: str = Field(..., description="Name of the agent", example="Identity Verification Agent")
    tool: str = Field(..., description="Tool used (e.g., ocr+llm)", example="ocr+llm")
    date: str = Field(..., description="Date in YYYY-MM-DD format", example="2025-10-20")
    checklistId: str = Field(..., description="Checklist ID", example="163")
    user_id: str = Field(..., description="User ID", example="12")


# Database connection function
def get_database_connection():
    """Connect to the MySQL database and return the connection object."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        if connection.is_connected():
            return connection
    except Error as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection error: {str(e)}"
        )


def ensure_table_exists(connection):
    """Create table tblaiagents if it does not already exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tblaiagents (
        id VARCHAR(36) PRIMARY KEY,
        FPCID VARCHAR(255),
        LMRId VARCHAR(255),
        doc_id VARCHAR(255),
        document_name VARCHAR(255),
        agent_name VARCHAR(255),
        tool VARCHAR(255),
        file_s3_location TEXT DEFAULT NULL,
        date DATE,
        document_status VARCHAR(255) DEFAULT NULL,
        uploadedat DATETIME DEFAULT NULL,
        metadata_s3_path TEXT DEFAULT NULL,
        verified_result_s3_path TEXT DEFAULT NULL,
        cross_validation BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        Is_varified BOOLEAN DEFAULT FALSE,
        checklistId VARCHAR(255),
        user_id VARCHAR(255),
        doc_verification_result TEXT DEFAULT NULL
    ) ENGINE=InnoDB;
    """
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
    except Error as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create table: {str(e)}"
        )


# API Endpoints
@app.get("/")
async def root():
    """Root endpoint - API health check"""
    return {
        "message": "AI Agents Database API is running",
        "version": "1.0.0",
        "status": "healthy"
    }


@app.post("/create-agent-record", status_code=status.HTTP_201_CREATED)
async def create_agent_record(record: AgentRecordCreate):
    """
    Create a new agent record in the database.
    
    All fields are required:
    - **FPCID**: FPC ID
    - **doc_id**: Document ID
    - **document_name**: Name of the document
    - **agent_name**: Name of the agent
    - **tool**: Tool used (e.g., ocr+llm)
    - **date**: Date in YYYY-MM-DD format
    - **checklistId**: Checklist ID
    - **user_id**: User ID
    
    Note: LMRId will be provided via SQS message during document processing
    """
    connection = get_database_connection()
    
    try:
        ensure_table_exists(connection)
        
        insert_query = """
        INSERT INTO tblaiagents (
            id, FPCID, doc_id, document_name, agent_name, tool, date, cross_validation, created_at, Is_varified, checklistId, user_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        record_id = str(uuid.uuid4())
        created_at = datetime.now()
        
        cursor = connection.cursor()
        cursor.execute(insert_query, (
            record_id,
            record.FPCID,
            record.doc_id,
            record.document_name,
            record.agent_name,
            record.tool,
            record.date,
            False,  # default value for cross_validation
            created_at,
            False,  # default value for Is_varified
            record.checklistId,
            record.user_id
        ))
        connection.commit()
        cursor.close()
        
        return {
            "success": True,
            "message": "Agent record created successfully",
            "record_id": record_id,
            "data": {
                "FPCID": record.FPCID,
                "doc_id": record.doc_id,
                "document_name": record.document_name,
                "agent_name": record.agent_name,
                "tool": record.tool,
                "date": record.date,
                "checklistId": record.checklistId,
                "user_id": record.user_id,
                "created_at": created_at.isoformat()
            }
        }
        
    except Error as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to insert data: {str(e)}"
        )
    finally:
        if connection.is_connected():
            connection.close()


@app.get("/api/documents/{fpcid}/{lmrid}")
async def get_all_documents(fpcid: int, lmrid: int):
    """
    Fetch all documents for a specific FPCID and LMRId.
    
    Returns all document records with their validation status, S3 paths, and verification results.
    
    **Path Parameters:**
    - **fpcid**: FPC ID (integer)
    - **lmrid**: LMR ID (integer)
    
    **Returns:**
    - 200: List of all documents for the given FPCID/LMRId
    - 404: No documents found
    - 500: Database error
    """
    connection = None
    try:
        connection = get_database_connection()
        cursor = connection.cursor(dictionary=True)
        
        query = """
            SELECT 
                id,
                FPCID,
                LMRId,
                doc_id,
                document_name,
                agent_name,
                tool,
                document_status,
                file_s3_location,
                verified_result_s3_path,
                metadata_s3_path,
                cross_validation,
                doc_verification_result,
                uploadedat,
                created_at,
                date,
                Is_varified,
                user_id
            FROM tblaiagents
            WHERE FPCID = %s AND LMRId = %s
            ORDER BY created_at DESC
        """
        
        cursor.execute(query, (str(fpcid), str(lmrid)))
        documents = cursor.fetchall()
        cursor.close()
        
        if not documents:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No documents found for FPCID={fpcid} and LMRId={lmrid}"
            )
        
        # Format datetime fields to ISO string
        for doc in documents:
            if doc.get('uploadedat'):
                doc['uploadedat'] = doc['uploadedat'].isoformat()
            if doc.get('created_at'):
                doc['created_at'] = doc['created_at'].isoformat()
            if doc.get('date'):
                doc['date'] = doc['date'].isoformat()
        
        return {
            "fpcid": fpcid,
            "lmrid": lmrid,
            "total_documents": len(documents),
            "documents": documents
        }
        
    except HTTPException:
        raise
    except Error as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )
    finally:
        if connection and connection.is_connected():
            connection.close()


@app.get("/api/documents/{fpcid}/{lmrid}/{doc_id}")
async def get_specific_document(fpcid: int, lmrid: int, doc_id: str):
    """
    Fetch a specific document by FPCID, LMRId, and doc_id.
    
    Returns the document record for the specified doc_id.
    
    **Path Parameters:**
    - **fpcid**: FPC ID (integer)
    - **lmrid**: LMR ID (integer)
    - **doc_id**: Document ID (string, e.g., "23", "24")
    
    **Returns:**
    - 200: Document details including validation status and extracted data
    - 404: Document not found
    - 500: Database error
    """
    connection = None
    try:
        connection = get_database_connection()
        cursor = connection.cursor(dictionary=True)
        
        query = """
            SELECT 
                id,
                FPCID,
                LMRId,
                doc_id,
                document_name,
                agent_name,
                tool,
                document_status,
                file_s3_location,
                verified_result_s3_path,
                metadata_s3_path,
                cross_validation,
                doc_verification_result,
                uploadedat,
                created_at,
                date,
                Is_varified,
                user_id
            FROM tblaiagents
            WHERE FPCID = %s AND LMRId = %s AND doc_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        
        cursor.execute(query, (str(fpcid), str(lmrid), doc_id))
        document = cursor.fetchone()
        cursor.close()
        
        if not document:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Document with doc_id='{doc_id}' not found for FPCID={fpcid} and LMRId={lmrid}"
            )
        
        # Format datetime fields to ISO string
        if document.get('uploadedat'):
            document['uploadedat'] = document['uploadedat'].isoformat()
        if document.get('created_at'):
            document['created_at'] = document['created_at'].isoformat()
        if document.get('date'):
            document['date'] = document['date'].isoformat()
        
        return {
            "fpcid": fpcid,
            "lmrid": lmrid,
            "doc_id": doc_id,
            "document": document
        }
        
    except HTTPException:
        raise
    except Error as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )
    finally:
        if connection and connection.is_connected():
            connection.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

