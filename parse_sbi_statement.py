"""
SBI Bank Statement PDF Parser

Parses password-protected SBI savings account statements and returns
structured transaction data.

- Extracts table rows exactly as they appear in the PDF.
- Hash-based dedup key (SHA-256 of 5 financial fields).
"""

import gc
import hashlib
import re
import tempfile
from pathlib import Path

from dotenv import load_dotenv
import os
import pdfplumber
import pikepdf


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

COL_TXN_DATE = 0
COL_VALUE_DATE = 1
COL_DESCRIPTION = 2
COL_CHEQUE_NO = 3
COL_DEBIT = 4
COL_CREDIT = 5
COL_BALANCE = 6
MIN_COLS = 7


def load_password():
    env_path = Path(__file__).parent / ".env"
    load_dotenv(env_path)
    password = os.getenv("PDF_PASSWORD")
    if not password:
        raise RuntimeError("PDF_PASSWORD not set in .env")
    return password


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_amount(value):
    if not value or value.strip() == "-":
        return ""
    cleaned = value.strip().replace(",", "")
    try:
        float(cleaned)
    except ValueError:
        return ""
    return cleaned


def is_date(text):
    if not text:
        return False
    try:
        from datetime import datetime
        datetime.strptime(text.strip(), "%d/%m/%Y")
        return True
    except ValueError:
        return False


def is_transaction_row(row):
    if not row or len(row) < MIN_COLS:
        return False
    return is_date(row[COL_TXN_DATE])


def is_summary_row(row):
    if not row:
        return False
    first = str(row[0]) if row[0] else ""
    return "Statement Summary" in first or "Brought Forward" in first


def extract_statement_period(pdf):
    text = pdf.pages[0].extract_text() or ""
    match = re.search(
        r"Statement\s+From\s*:\s*(\d{2}-\d{2}-\d{4})\s+to\s+(\d{2}-\d{2}-\d{4})",
        text, re.IGNORECASE,
    )
    if match:
        return match.group(1), match.group(2)
    return None, None


# ---------------------------------------------------------------------------
# Description helpers
# ---------------------------------------------------------------------------

def extract_ref_number(desc):
    if not desc:
        return ""
    for line in desc.split("\n"):
        match = re.match(r"^(\d{10,13})\b", line.strip())
        if match:
            return match.group(1)
    return ""


def clean_description(desc):
    if not desc:
        return ""
    cleaned = desc.replace("\n", " | ")
    return re.sub(r"\s+", " ", cleaned).strip()


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------

BATCH_SIZE = 15  # pages per batch — keeps peak memory under ~300 MB


def _extract_rows_from_pages(pdf):
    """Extract transaction rows from all pages of an already-opened pdfplumber PDF."""
    rows = []
    for page in pdf.pages:
        tables = page.extract_tables()
        if not tables:
            continue
        for table in tables:
            if not table:
                continue
            for row in table:
                if not row:
                    continue
                if is_summary_row(row):
                    continue
                if not is_transaction_row(row):
                    continue

                desc_raw = row[COL_DESCRIPTION] or ""
                debit = parse_amount(row[COL_DEBIT])
                credit = parse_amount(row[COL_CREDIT])
                balance = parse_amount(row[COL_BALANCE])

                if not debit and not credit and not balance:
                    continue

                rows.append({
                    "value_date": (row[COL_VALUE_DATE] or "").strip(),
                    "post_date": (row[COL_TXN_DATE] or "").strip(),
                    "details": clean_description(desc_raw),
                    "ref_no": extract_ref_number(desc_raw),
                    "debit": debit,
                    "credit": credit,
                    "balance": balance,
                    "txn_type": "debit" if debit else "credit" if credit else "",
                    "account_source": "sbi_email",
                })
    return rows


def parse_pdf(pdf_path, password):
    """Parse all transaction rows from an SBI statement PDF.

    Processes pages in batches via pikepdf to keep memory under control
    for large (100+ page) statements.
    """
    try:
        source = pikepdf.open(pdf_path, password=password)
    except Exception as e:
        err_str = str(e).lower()
        if "password" in err_str or "decrypt" in err_str or "encrypted" in err_str:
            raise RuntimeError(
                f"Wrong password or encrypted PDF: {pdf_path}\n"
                f"  Check PDF_PASSWORD in your .env file."
            ) from e
        raise

    with source:
        page_count = len(source.pages)
        if page_count == 0:
            raise RuntimeError(f"PDF has no pages: {pdf_path}")

        # --- Validate first page and extract statement period ---
        fd, tmp_first = tempfile.mkstemp(suffix=".pdf")
        os.close(fd)
        try:
            first_pdf = pikepdf.new()
            first_pdf.pages.append(source.pages[0])
            first_pdf.save(tmp_first)
            first_pdf.close()

            with pdfplumber.open(tmp_first) as pdf:
                first_text = pdf.pages[0].extract_text() or ""
                if not re.search(r"State Bank|SBI|Account\s*Number", first_text, re.IGNORECASE):
                    raise RuntimeError(
                        f"This doesn't look like an SBI statement: {pdf_path}\n"
                        f"  First page has no SBI/State Bank header."
                    )
                stmt_from, stmt_to = extract_statement_period(pdf)
        finally:
            Path(tmp_first).unlink(missing_ok=True)

        # --- Process pages in batches ---
        transactions = []

        for batch_start in range(0, page_count, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, page_count)

            fd, tmp_batch = tempfile.mkstemp(suffix=".pdf")
            os.close(fd)
            try:
                batch_pdf = pikepdf.new()
                for i in range(batch_start, batch_end):
                    batch_pdf.pages.append(source.pages[i])
                batch_pdf.save(tmp_batch)
                batch_pdf.close()

                with pdfplumber.open(tmp_batch) as pdf:
                    transactions.extend(_extract_rows_from_pages(pdf))
            finally:
                Path(tmp_batch).unlink(missing_ok=True)
                gc.collect()

        for seq, txn in enumerate(transactions):
            txn["_parse_seq"] = seq

    return transactions, stmt_from, stmt_to, page_count


# ---------------------------------------------------------------------------
# Hash
# ---------------------------------------------------------------------------

def compute_hash(txn):
    """SHA-256 of 5 financial fields. Balance is a running total so
    even same-amount transactions on the same day produce unique hashes."""
    raw = "|".join([
        txn["post_date"], txn["value_date"],
        txn["debit"], txn["credit"], txn["balance"],
    ])
    return hashlib.sha256(raw.encode()).hexdigest()[:32]
