"""
Complete cell-by-cell verification of PDF extraction.
Compares every raw PDF cell against parser output.
Accepts PDF path as command-line argument.
"""
import sys
import gc
import pikepdf
import pdfplumber
import tempfile
import os
from pathlib import Path
from parse_sbi_statement import parse_pdf, load_password, parse_amount, is_date, is_summary_row, compute_hash


def extract_raw_rows(pdf_path, password):
    """Extract every raw row from the PDF tables."""
    source = pikepdf.open(pdf_path, password=password)
    fd, tmp = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)
    source.save(tmp)
    source.close()

    raw_rows = []
    with pdfplumber.open(tmp) as pdf:
        for pi, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            if not tables:
                continue
            for ti, table in enumerate(tables):
                if not table:
                    continue
                for ri, row in enumerate(table):
                    raw_rows.append({
                        "page": pi + 1,
                        "table": ti,
                        "row_idx": ri,
                        "cells": row,
                        "num_cols": len(row) if row else 0,
                    })

    Path(tmp).unlink(missing_ok=True)
    return raw_rows


def categorize_rows(raw_rows):
    """Categorize every raw row."""
    header, empty, summary, data, unknown = [], [], [], [], []
    for r in raw_rows:
        cells = r["cells"]
        if not cells or all(c is None or c == "" for c in cells):
            empty.append(r)
        elif is_summary_row(cells):
            summary.append(r)
        elif r["num_cols"] >= 7 and is_date(cells[0]):
            data.append(r)
        elif any(c and "Balance" in str(c) for c in cells):
            header.append(r)
        else:
            unknown.append(r)
    return header, data, empty, summary, unknown


def find_opening_balance(raw_rows, page_count):
    """Extract opening balance from the summary table on last page."""
    # Look for the summary values row on the last page.
    # Format: ['66,300.56CR', '106', '31', '3,54,101.27', '4,41,297.27', '1,53,496.56CR']
    # or:     ['0.00', '1229', '297', '15,17,586.15', '15,19,181.96', '1,595.81CR']
    for r in raw_rows:
        cells = r["cells"]
        if not cells or not cells[0] or r["page"] != page_count:
            continue
        # The summary values row has 6 cells where cell[1] is the debit count (a small integer)
        if len(cells) < 6:
            continue
        try:
            int(str(cells[1]).replace(",", "").strip())  # Dr Count must be an integer
            val = float(str(cells[0]).replace("CR", "").replace(",", "").strip())
            return val, cells
        except (ValueError, TypeError):
            continue
    return None, None


def compare_cell_by_cell(data_rows, transactions):
    """Compare every cell of every data row against parser output."""
    mismatches = []

    for i, (raw, txn) in enumerate(zip(data_rows, transactions)):
        cells = raw["cells"]
        errors = []

        # Col 0: Txn Date -> post_date
        raw_post = (cells[0] or "").strip()
        if raw_post != txn["post_date"]:
            errors.append(f'post_date: raw="{raw_post}" vs parsed="{txn["post_date"]}"')

        # Col 1: Value Date -> value_date
        raw_val = (cells[1] or "").strip()
        if raw_val != txn["value_date"]:
            errors.append(f'value_date: raw="{raw_val}" vs parsed="{txn["value_date"]}"')

        # Col 3: Cheque No
        raw_cheque = (cells[3] or "").strip()
        expected_cheque = raw_cheque if raw_cheque != "-" else ""
        if expected_cheque != txn.get("cheque_no", ""):
            errors.append(
                f'cheque_no: raw="{raw_cheque}" -> expected="{expected_cheque}" '
                f'vs output="{txn.get("cheque_no", "")}"'
            )

        # Col 4: Debit
        raw_debit_str = (cells[4] or "").strip()
        raw_debit = parse_amount(cells[4])
        if raw_debit != txn["debit"]:
            errors.append(
                f'debit: raw="{raw_debit_str}" -> cleaned="{raw_debit}" vs output="{txn["debit"]}"'
            )

        # Col 5: Credit
        raw_credit_str = (cells[5] or "").strip()
        raw_credit = parse_amount(cells[5])
        if raw_credit != txn["credit"]:
            errors.append(
                f'credit: raw="{raw_credit_str}" -> cleaned="{raw_credit}" vs output="{txn["credit"]}"'
            )

        # Col 6: Balance
        raw_bal_str = (cells[6] or "").strip()
        raw_bal = parse_amount(cells[6])
        if raw_bal != txn["balance"]:
            errors.append(
                f'balance: raw="{raw_bal_str}" -> cleaned="{raw_bal}" vs output="{txn["balance"]}"'
            )

        # Col 2: Description - verify no content lost
        raw_desc = cells[2] or ""
        raw_lines = [l.strip() for l in raw_desc.split("\n") if l.strip()]
        for line in raw_lines:
            if line not in txn["details"]:
                errors.append(f'desc content lost: "{line}"')

        # txn_type consistency
        if txn["debit"] and txn["credit"]:
            errors.append("both debit and credit set")
        if txn["debit"] and txn["txn_type"] != "debit":
            errors.append(f'txn_type should be debit, got "{txn["txn_type"]}"')
        if txn["credit"] and txn["txn_type"] != "credit":
            errors.append(f'txn_type should be credit, got "{txn["txn_type"]}"')

        if errors:
            mismatches.append((i, raw, txn, errors))

    return mismatches


def verify_balance_chain(transactions, opening_balance):
    """Verify sequential balance chain."""
    errors = []
    running = opening_balance
    for i, t in enumerate(transactions):
        d = float(t["debit"]) if t["debit"] else 0.0
        c = float(t["credit"]) if t["credit"] else 0.0
        b = float(t["balance"]) if t["balance"] else 0.0
        expected = round(running - d + c, 2)
        if abs(expected - b) > 0.01:
            errors.append(
                f"Row {i}: prev={running:.2f} - {d:.2f} + {c:.2f} = {expected:.2f}, "
                f"but got {b:.2f} (diff={abs(expected - b):.2f})"
            )
        running = b
    return errors, running


def verify_pdf(pdf_path, password):
    """Run full verification on a single PDF. Returns (pass_count, fail_count, total_txns)."""
    filename = Path(pdf_path).name
    print(f"\n{'#' * 100}")
    print(f"  PDF: {filename}")
    print(f"{'#' * 100}")

    pass_count = 0
    fail_count = 0

    # Extract raw rows
    raw_rows = extract_raw_rows(pdf_path, password)
    header, data, empty, summary, unknown = categorize_rows(raw_rows)

    print(f"\nRaw PDF rows: {len(raw_rows)}")
    print(f"  Header rows: {len(header)}")
    print(f"  Data rows:   {len(data)}")
    print(f"  Empty rows:  {len(empty)}")
    print(f"  Summary rows:{len(summary)}")
    print(f"  Unknown:     {len(unknown)}")
    if unknown:
        for u in unknown:
            print(f"    page={u['page']} row={u['row_idx']}: {u['cells']}")

    # Run parser
    transactions, stmt_from, stmt_to, page_count = parse_pdf(pdf_path, password)
    print(f"\nParser output: {len(transactions)} transactions")
    print(f"Statement period: {stmt_from} to {stmt_to}")
    print(f"Pages: {page_count}")

    # Row count
    print(f"\n--- 1. ROW COUNT CHECK ---")
    if len(data) == len(transactions):
        print(f"  PASS: {len(data)} raw data rows == {len(transactions)} parsed transactions")
        pass_count += 1
    else:
        print(f"  FAIL: {len(data)} raw data rows != {len(transactions)} parsed transactions")
        # Show which raw rows were dropped
        if len(data) > len(transactions):
            dropped = len(data) - len(transactions)
            print(f"  {dropped} rows DROPPED by parser. Investigating...")
            # Match by date+balance to find which ones
            parsed_keys = set()
            for t in transactions:
                parsed_keys.add((t["post_date"], t["balance"]))
            for r in data:
                c = r["cells"]
                key = ((c[0] or "").strip(), parse_amount(c[6]))
                if key not in parsed_keys:
                    print(f"    DROPPED: page={r['page']} row={r['row_idx']}: {c[:3]}... bal={c[6]}")
        fail_count += 1

    # Find opening balance from summary
    print(f"\n--- 2. STATEMENT SUMMARY ---")
    opening, summary_cells = find_opening_balance(raw_rows, page_count)
    if summary_cells:
        print(f"  Summary row: {summary_cells}")
        bf = summary_cells[0].replace("CR", "").replace(",", "").strip()
        dr_count_raw = summary_cells[1] if len(summary_cells) > 1 else "?"
        cr_count_raw = summary_cells[2] if len(summary_cells) > 2 else "?"
        total_d_raw = summary_cells[3] if len(summary_cells) > 3 else "?"
        total_c_raw = summary_cells[4] if len(summary_cells) > 4 else "?"
        closing_raw = summary_cells[5] if len(summary_cells) > 5 else "?"
        print(f"  Brought Forward: {bf}")
        print(f"  Dr Count: {dr_count_raw}, Cr Count: {cr_count_raw}")
        print(f"  Total Debits: {total_d_raw}, Total Credits: {total_c_raw}")
        print(f"  Closing Balance: {closing_raw}")

        # Parse expected values
        try:
            expected_dr_count = int(str(dr_count_raw).replace(",", "").strip())
        except (ValueError, TypeError):
            expected_dr_count = None
        try:
            expected_cr_count = int(str(cr_count_raw).replace(",", "").strip())
        except (ValueError, TypeError):
            expected_cr_count = None
        try:
            expected_total_d = float(str(total_d_raw).replace(",", "").strip())
        except (ValueError, TypeError):
            expected_total_d = None
        try:
            expected_total_c = float(str(total_c_raw).replace(",", "").strip())
        except (ValueError, TypeError):
            expected_total_c = None
        try:
            expected_closing = float(str(closing_raw).replace("CR", "").replace(",", "").strip())
        except (ValueError, TypeError):
            expected_closing = None
    else:
        print(f"  WARNING: Could not find summary row")
        opening = None
        expected_dr_count = None
        expected_cr_count = None
        expected_total_d = None
        expected_total_c = None
        expected_closing = None

    # Cell-by-cell comparison
    print(f"\n--- 3. CELL-BY-CELL COMPARISON (all {len(data)} rows x 7 columns) ---")
    mismatches = compare_cell_by_cell(data, transactions)

    if not mismatches:
        print(f"  PASS: All {len(data)} rows match perfectly across all columns")
        pass_count += 1
    else:
        print(f"  FAIL: {len(mismatches)} rows with mismatches")
        for idx, raw, txn, errors in mismatches[:20]:  # Show first 20
            print(f"\n  Row {idx} (page {raw['page']}):")
            print(f"    Raw: {raw['cells'][:4]}... d={raw['cells'][4]} c={raw['cells'][5]} b={raw['cells'][6]}")
            print(f"    Out: date={txn['post_date']} d={txn['debit']} c={txn['credit']} b={txn['balance']}")
            for e in errors:
                print(f"    -> {e}")
        if len(mismatches) > 20:
            print(f"  ... and {len(mismatches) - 20} more")
        fail_count += 1

    # Balance chain
    print(f"\n--- 4. BALANCE CHAIN ---")
    if opening is not None:
        chain_errors, closing = verify_balance_chain(transactions, opening)
        if not chain_errors:
            print(f"  PASS: All {len(transactions)} sequential balances verified")
            print(f"  Opening: {opening:>14,.2f} -> Closing: {closing:>14,.2f}")
            pass_count += 1
        else:
            print(f"  FAIL: {len(chain_errors)} balance breaks")
            for e in chain_errors[:10]:
                print(f"    {e}")
            if len(chain_errors) > 10:
                print(f"    ... and {len(chain_errors) - 10} more")
            closing = float(transactions[-1]["balance"]) if transactions else 0
            fail_count += 1
    else:
        print(f"  SKIP: No opening balance found")
        closing = float(transactions[-1]["balance"]) if transactions else 0

    # Aggregate totals vs statement summary
    print(f"\n--- 5. AGGREGATE TOTALS vs STATEMENT SUMMARY ---")
    debits = [t for t in transactions if t["txn_type"] == "debit"]
    credits_list = [t for t in transactions if t["txn_type"] == "credit"]
    total_d = sum(float(t["debit"]) for t in debits)
    total_c = sum(float(t["credit"]) for t in credits_list)

    checks = []
    if expected_dr_count is not None:
        checks.append(("Debit count", len(debits), expected_dr_count))
    if expected_cr_count is not None:
        checks.append(("Credit count", len(credits_list), expected_cr_count))
    if expected_total_d is not None:
        checks.append(("Total debits", total_d, expected_total_d))
    if expected_total_c is not None:
        checks.append(("Total credits", total_c, expected_total_c))
    if expected_closing is not None:
        checks.append(("Closing balance", closing, expected_closing))

    agg_pass = True
    for label, got, expected in checks:
        if isinstance(got, float):
            ok = abs(got - expected) < 0.01
        else:
            ok = got == expected
        status = "PASS" if ok else "FAIL"
        if not ok:
            agg_pass = False
        if isinstance(got, float):
            print(f"  {status}: {label}: {got:>14,.2f} (expected {expected:>14,.2f})")
        else:
            print(f"  {status}: {label}: {got} (expected {expected})")
    if agg_pass:
        pass_count += 1
    else:
        fail_count += 1

    # Date checks
    print(f"\n--- 6. DATE VALIDITY & ORDER ---")
    date_errors = []
    for i, t in enumerate(transactions):
        if not is_date(t["post_date"]):
            date_errors.append(f"Row {i}: invalid post_date '{t['post_date']}'")
        if not is_date(t["value_date"]):
            date_errors.append(f"Row {i}: invalid value_date '{t['value_date']}'")

    from datetime import datetime
    prev_date = None
    order_errors = []
    for i, t in enumerate(transactions):
        try:
            dt = datetime.strptime(t["post_date"], "%d/%m/%Y")
            if prev_date and dt < prev_date:
                order_errors.append(f"Row {i}: {t['post_date']} < previous {prev_date.strftime('%d/%m/%Y')}")
            prev_date = dt
        except ValueError:
            pass

    if not date_errors and not order_errors:
        print(f"  PASS: All {len(transactions)} rows have valid DD/MM/YYYY dates in order")
        pass_count += 1
    else:
        for e in date_errors:
            print(f"  FAIL: {e}")
        for e in order_errors:
            print(f"  FAIL: {e}")
        fail_count += 1

    # Hash uniqueness
    print(f"\n--- 7. HASH & TXN_ID UNIQUENESS ---")
    hashes = [compute_hash(t) for t in transactions]
    unique_h = set(hashes)
    txn_ids = [h[:16] for h in hashes]
    unique_ids = set(txn_ids)

    if len(unique_h) == len(hashes) and len(unique_ids) == len(txn_ids):
        print(f"  PASS: All {len(hashes)} hashes and txn_ids are unique")
        pass_count += 1
    else:
        if len(unique_h) < len(hashes):
            print(f"  FAIL: {len(hashes) - len(unique_h)} duplicate hashes")
        if len(unique_ids) < len(txn_ids):
            print(f"  FAIL: {len(txn_ids) - len(unique_ids)} duplicate txn_ids")
        fail_count += 1

    # Field completeness
    print(f"\n--- 8. FIELD COMPLETENESS ---")
    required = ["post_date", "value_date", "details", "balance", "txn_type", "account_source"]
    all_complete = True
    for field in required:
        missing = sum(1 for t in transactions if not t.get(field))
        if missing:
            print(f"  FAIL: {field} - {missing} missing")
            all_complete = False
        else:
            print(f"  PASS: {field}")

    amount_missing = sum(1 for t in transactions if not t["debit"] and not t["credit"])
    if amount_missing:
        print(f"  FAIL: debit/credit - {amount_missing} rows with neither")
        all_complete = False
    else:
        print(f"  PASS: debit/credit (every row has at least one)")
    if all_complete:
        pass_count += 1
    else:
        fail_count += 1

    # Final
    print(f"\n{'=' * 60}")
    print(f"  {filename}: {pass_count} PASSED, {fail_count} FAILED")
    if fail_count == 0:
        print(f"  VERDICT: ALL CHECKS PASSED")
    else:
        print(f"  VERDICT: {fail_count} CHECKS FAILED")
    print(f"{'=' * 60}")

    gc.collect()
    return pass_count, fail_count, len(transactions)


def main():
    password = load_password()

    if len(sys.argv) > 1:
        pdfs = sys.argv[1:]
    else:
        pdfs = [
            r"C:\Users\omraj\Downloads\Email_Statement_01022026014858958549.pdf",
            r"C:\Users\omraj\Downloads\Email_Statement_13022026050953912327.pdf",
            r"C:\Users\omraj\Downloads\Email_Statement_03022026112324499727.pdf",
        ]

    results = []
    for pdf_path in pdfs:
        p, f, n = verify_pdf(pdf_path, password)
        results.append((Path(pdf_path).name, p, f, n))

    print(f"\n\n{'#' * 100}")
    print(f"  OVERALL SUMMARY")
    print(f"{'#' * 100}")
    for name, p, f, n in results:
        status = "ALL PASS" if f == 0 else f"{f} FAILED"
        print(f"  {name}: {n} txns, {p} passed, {f} failed -> {status}")


if __name__ == "__main__":
    main()
