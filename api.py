"""
SBI Statement Parser API

FastAPI server that accepts SBI statement PDFs and returns parsed transactions.

Endpoints:
    POST /parse   - Upload PDF, get transactions as JSON array
    GET  /health  - Health check

Usage:
    uvicorn api:app --host 0.0.0.0 --port 8080
"""

import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

from parse_sbi_statement import parse_pdf, compute_hash, load_password

logger = logging.getLogger(__name__)

app = FastAPI(
    title="SBI Statement Parser",
    description="Parse SBI bank statement PDFs and extract transactions",
    version="1.0.0",
)

# Load password once at startup
PASSWORD = load_password()

MAX_PDF_SIZE = 50 * 1024 * 1024  # 50 MB


def _validate_pdf(file_bytes: bytes, filename: str):
    """Validate file extension and PDF magic bytes."""
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(400, "File must be a PDF")
    if len(file_bytes) > MAX_PDF_SIZE:
        raise HTTPException(400, f"File too large. Maximum size is {MAX_PDF_SIZE // (1024*1024)} MB")
    if not file_bytes[:5].startswith(b"%PDF-"):
        raise HTTPException(400, "File does not appear to be a valid PDF")


def _parse_uploaded_pdf(file_bytes: bytes, filename: str):
    """Save uploaded bytes to a temp file and parse it."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        transactions, stmt_from, stmt_to, page_count = parse_pdf(tmp_path, PASSWORD)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    return transactions, stmt_from, stmt_to, page_count


def _txn_to_dict(txn):
    """Convert transaction to the API response format."""
    h = txn.get("hash", "")
    return {
        "txn_id": h[:16],
        "value_date": txn.get("value_date", ""),
        "post_date": txn.get("post_date", ""),
        "details": txn.get("details", ""),
        "ref_no": txn.get("ref_no", ""),
        "debit": txn.get("debit", ""),
        "credit": txn.get("credit", ""),
        "balance": txn.get("balance", ""),
        "txn_type": txn.get("txn_type", ""),
        "account_source": txn.get("account_source", "sbi_email"),
        "imported_at": txn.get("imported_at", ""),
        "hash": h,
    }


@app.post("/parse")
async def parse_statement(file: UploadFile = File(...)):
    """Upload a PDF, get parsed transactions back as a JSON array."""
    file_bytes = await file.read()
    _validate_pdf(file_bytes, file.filename)

    try:
        transactions, stmt_from, stmt_to, page_count = _parse_uploaded_pdf(
            file_bytes, file.filename
        )
    except RuntimeError as e:
        raise HTTPException(422, str(e))
    except Exception:
        logger.exception("Unexpected parse error for file: %s", file.filename)
        raise HTTPException(500, "Internal server error during parsing")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    for txn in transactions:
        txn["hash"] = compute_hash(txn)
        txn["imported_at"] = now

    return JSONResponse(content=[_txn_to_dict(t) for t in transactions])


@app.get("/health")
async def health():
    return {"status": "ok"}
