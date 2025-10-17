from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, Field
from typing import Literal, Dict, Any, Optional
import os
import sys
import tempfile
from pathlib import Path

# Add the project root to Python path to import s3_uploader
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from ocr_test import run_pipeline
except Exception as e:
    run_pipeline = None  # type: ignore
    _import_error = e

try:
    from S3_Sqs.s3_uploader import upload_document
except Exception as e:
    upload_document = None  # type: ignore
    _s3_import_error = e


app = FastAPI(title="LendingWise OCR+LLM API", version="1.0.0")


class ExtractRequest(BaseModel):
    bucket: str = Field(..., description="S3 bucket name")
    key: str = Field(..., description="S3 object key (path to file)")
    mode: Literal["ocr+llm", "llm"] = Field("ocr+llm", description="Extraction mode")


class UploadRequest(BaseModel):
    FPCID: str = Field(..., description="FPCID identifier")
    LMRId: str = Field(..., description="LMRId identifier")
    year: Optional[str] = Field(None, description="Year (YYYY format, defaults to current year)")
    month: Optional[str] = Field(None, description="Month (MM format, defaults to current month)")
    day: Optional[str] = Field(None, description="Day (DD format, defaults to current day)")
    bucket: Optional[str] = Field("lendingwise-aiagent", description="S3 bucket name")
    document_name: Optional[str] = Field(None, description="Logical document name (e.g., 'Adhar card', 'Driving License')")
    send_sqs: bool = Field(False, description="Whether to send SQS message")
    entity_type: str = Field("LLC", description="Entity type")


class UploadResponse(BaseModel):
    success: bool
    document_key: str
    metadata_key: str
    prefix: str
    sqs_message_id: Optional[str] = None
    message: str


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok"}


@app.post("/extract")
def extract(req: ExtractRequest) -> Dict[str, Any]:
    if run_pipeline is None:
        raise HTTPException(status_code=500, detail=f"Pipeline import failed: {_import_error}")
    try:
        result = run_pipeline(req.bucket, req.key, req.mode)
        return {
            "doc_type": result.get("doc_type"),
            "mode": result.get("mode"),
            "result_path": result.get("result_path"),
            "name_no_ext": result.get("name_no_ext"),
            "structured": result.get("structured"),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    FPCID: str = Form(...),
    LMRId: str = Form(...),
    year: Optional[str] = Form(None),
    month: Optional[str] = Form(None),
    day: Optional[str] = Form(None),
    bucket: str = Form("lendingwise-aiagent"),
    document_name: Optional[str] = Form(None),
    send_sqs: bool = Form(False),
    entity_type: str = Form("LLC")
):
    """
    Upload a document to S3 with metadata and optionally send SQS message.
    """
    if upload_document is None:
        raise HTTPException(status_code=500, detail=f"S3 uploader import failed: {_s3_import_error}")
    
    # Validate file
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    
    # Create temporary file
    temp_file = None
    try:
        # Create temporary file with original extension
        file_ext = os.path.splitext(file.filename)[1]
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=file_ext)
        
        # Write uploaded file content to temporary file
        content = await file.read()
        temp_file.write(content)
        temp_file.close()
        
        # Upload to S3
        result = upload_document(
            FPCID=FPCID,
            LMRId=LMRId,
            document_file=temp_file.name,
            year=year,
            month=month,
            day=day,
            bucket=bucket,
            document_name=document_name,
            send_sqs=send_sqs,
            entity_type=entity_type
        )
        
        return UploadResponse(
            success=True,
            document_key=result["document_key"],
            metadata_key=result["metadata_key"],
            prefix=result["prefix"],
            sqs_message_id=result.get("sqs_message_id"),
            message="Document uploaded successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")
    
    finally:
        # Clean up temporary file
        if temp_file and os.path.exists(temp_file.name):
            os.unlink(temp_file.name)


@app.post("/upload-local", response_model=UploadResponse)
def upload_local_file(req: UploadRequest, file_path: str = Form(...)):
    """
    Upload a local file to S3 with metadata and optionally send SQS message.
    """
    if upload_document is None:
        raise HTTPException(status_code=500, detail=f"S3 uploader import failed: {_s3_import_error}")
    
    # Validate file exists
    if not os.path.exists(file_path):
        raise HTTPException(status_code=400, detail=f"File not found: {file_path}")
    
    try:
        result = upload_document(
            FPCID=req.FPCID,
            LMRId=req.LMRId,
            document_file=file_path,
            year=req.year,
            month=req.month,
            day=req.day,
            bucket=req.bucket,
            document_name=req.document_name,
            send_sqs=req.send_sqs,
            entity_type=req.entity_type
        )
        
        return UploadResponse(
            success=True,
            document_key=result["document_key"],
            metadata_key=result["metadata_key"],
            prefix=result["prefix"],
            sqs_message_id=result.get("sqs_message_id"),
            message="Document uploaded successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.get("/upload/status/{message_id}")
def get_upload_status(message_id: str):
    """
    Get the status of an SQS message (placeholder for future implementation).
    """
    return {
        "message_id": message_id,
        "status": "unknown",
        "message": "Status checking not implemented yet"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.api.server:app", host="0.0.0.0", port=8000, reload=True)

