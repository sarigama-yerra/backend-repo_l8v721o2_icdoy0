import io
import os
import csv
import datetime
import smtplib
from email.message import EmailMessage
from typing import List, Dict, Optional

from fastapi import FastAPI, File, Form, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

try:
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.platypus import Table, TableStyle, SimpleDocTemplate
    from reportlab.lib import colors
    REPORTLAB_AVAILABLE = True
except Exception:
    REPORTLAB_AVAILABLE = False

try:
    from openpyxl import Workbook
    OPENPYXL_AVAILABLE = True
except Exception:
    OPENPYXL_AVAILABLE = False

app = FastAPI(title="Missing 945 API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- CSV helpers (no pandas) ----------

def load_csv_bytes(b: bytes) -> List[Dict[str, str]]:
    """Parse CSV bytes to a list of dict rows. Tries common encodings."""
    last_err = None
    for enc in ("utf-8", "utf-8-sig", "utf-16", "latin1"):
        try:
            text = b.decode(enc)
            # Normalize newlines
            f = io.StringIO(text)
            reader = csv.DictReader(f)
            # Trim headers
            fieldnames = [str(h).strip() for h in (reader.fieldnames or [])]
            rows: List[Dict[str, str]] = []
            for raw in reader:
                row = {}
                for k, v in raw.items():
                    if k is None:
                        continue
                    row[str(k).strip()] = (v if v is not None else "").strip()
                rows.append(row)
            # Re-map to trimmed headers if needed
            if reader.fieldnames and any(h != str(h).strip() for h in reader.fieldnames):
                remapped = []
                for r in rows:
                    nr = {}
                    for k, v in r.items():
                        nr[str(k).strip()] = v
                    remapped.append(nr)
                rows = remapped
            return rows
        except Exception as e:
            last_err = e
            continue
    raise HTTPException(status_code=400, detail=f"CSV read error: {last_err}")


def to_xlsx_bytes(rows: List[Dict[str, str]], columns: List[str]) -> bytes:
    if not OPENPYXL_AVAILABLE:
        raise HTTPException(status_code=500, detail="Excel export not available")
    wb = Workbook()
    ws = wb.active
    ws.append(columns)
    for r in rows:
        ws.append([r.get(c, "") for c in columns])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


def to_csv_bytes(rows: List[Dict[str, str]], columns: List[str]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, extrasaction='ignore')
    writer.writeheader()
    for r in rows:
        writer.writerow({c: r.get(c, "") for c in columns})
    return buf.getvalue().encode("utf-8")


def to_pdf_bytes(rows: List[Dict[str, str]], columns: List[str]) -> bytes:
    if not REPORTLAB_AVAILABLE:
        raise HTTPException(status_code=500, detail="PDF export not available")
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(letter), leftMargin=12, rightMargin=12, topMargin=12, bottomMargin=12)
    # Limit rows for readability
    preview = rows[:200]
    data = [columns] + [[str(r.get(c, "")) for c in columns] for r in preview]
    table = Table(data, repeatRows=1)
    style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F3F4F6")]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#E5E7EB")),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ])
    table.setStyle(style)
    doc.build([table])
    buf.seek(0)
    return buf.getvalue()


# ---------- Domain logic (no pandas) ----------

def build_report_rows(shipment_history: UploadFile, edib2bi: UploadFile, edi940: UploadFile) -> List[Dict[str, str]]:
    df1 = load_csv_bytes(shipment_history.file.read())
    shipment_history.file.seek(0)
    df2 = load_csv_bytes(edib2bi.file.read())
    edib2bi.file.seek(0)
    df3 = load_csv_bytes(edi940.file.read())
    edi940.file.seek(0)

    # Required columns
    def has_col(rows: List[Dict[str, str]], col: str) -> bool:
        return any(col in r for r in rows) or (len(rows) == 0)

    if not has_col(df1, "Pickticket"):
        raise HTTPException(status_code=400, detail=f"Missing column 'Pickticket' in Shipment_History___Total")
    if not has_col(df2, "AXReferenceID"):
        raise HTTPException(status_code=400, detail=f"Missing column 'AXReferenceID' in EDIB2BiReportV2")
    if not has_col(df3, "PickRoute"):
        raise HTTPException(status_code=400, detail=f"Missing column 'PickRoute' in EDI940Report_withCostV2.0")

    # Index for joins
    by_ax = {r.get("AXReferenceID", ""): r for r in df2}
    by_pickroute = {r.get("PickRoute", ""): r for r in df3}

    # First join: df1 left join df2 on Pickticket = AXReferenceID
    merged = []
    for r1 in df1:
        key = r1.get("Pickticket", "")
        r2 = by_ax.get(key, {})
        merged_row = {
            'Warehouse': r1.get('Warehouse', ''),
            'Pickticket': r1.get('Pickticket', ''),
            'Order': r1.get('Order', ''),
            'Drop Date': r1.get('Drop Date', ''),
            'Ship Date': r1.get('Ship Date', ''),
            'Ship To': r1.get('Ship To', ''),
            'Ship State': r1.get('Ship State', ''),
            'Zip Code': r1.get('Zip Code', ''),
            'Customer PO': r1.get('Customer PO', ''),
            'Ship Via': r1.get('Ship Via', ''),
            'Load ID': r1.get('Load ID', ''),
            'Weight': r1.get('Weight', ''),
            'SKU': r1.get('SKU', ''),
            'Units': r1.get('Units', ''),
            'Price': r1.get('Price', ''),
            'Size Type': r1.get('Size Type', ''),
            'Size': r1.get('Size', ''),
            'Product Type': r1.get('Product Type', ''),
            'InvoiceNumber': r2.get('InvoiceNumber', r1.get('InvoiceNumber', '')),
            'StatusSummary': r2.get('StatusSummary', r1.get('StatusSummary', '')),
            'ERRORDESCRIPTION': r2.get('ERRORDESCRIPTION', r1.get('ERRORDESCRIPTION', '')),
        }
        merged.append(merged_row)

    # Second join: merged left join df3 on Pickticket = PickRoute
    final_rows = []
    for r in merged:
        r3 = by_pickroute.get(r.get('Pickticket', ''), {})
        out = {
            'Pickticket': r.get('Pickticket', ''),
            'Warehouse': r.get('Warehouse', ''),
            'Order': r.get('Order', ''),
            'Drop Date': r.get('Drop Date', ''),
            'Ship Date': r.get('Ship Date', ''),
            'Ship To': r.get('Ship To', ''),
            'Ship State': r.get('Ship State', ''),
            'Zip Code': r.get('Zip Code', ''),
            'Customer PO': r.get('Customer PO', ''),
            'Ship Via': r.get('Ship Via', ''),
            'Load ID': r.get('Load ID', ''),
            'Weight': r.get('Weight', ''),
            'SKU': r.get('SKU', ''),
            'Units': r.get('Units', ''),
            'Price': r.get('Price', ''),
            'Size Type': r.get('Size Type', ''),
            'Size': r.get('Size', ''),
            'Product Type': r.get('Product Type', ''),
            'InvoiceNumber': r.get('InvoiceNumber', ''),
            'StatusSummary': r.get('StatusSummary', ''),
            'ERRORDESCRIPTION': r.get('ERRORDESCRIPTION', ''),
            'PickRoute': r3.get('PickRoute', ''),
            'SalesHeaderStatus': r3.get('SalesHeaderStatus', ''),
            'SalesHeaderDocStatus': r3.get('SalesHeaderDocStatus', ''),
            'PickModeOfDelivery': r3.get('PickModeOfDelivery', ''),
            'PickCreatedDate': r3.get('PickCreatedDate', ''),
            'DeliveryDate': r3.get('DeliveryDate', ''),
        }
        final_rows.append(out)

    # Rename columns
    for r in final_rows:
        r['Received in EDI?'] = r.pop('InvoiceNumber', '')
        r['EDI Processing Status'] = r.pop('StatusSummary', '')
        r['EDI Message'] = r.pop('ERRORDESCRIPTION', '')
        r['Found in AX DATa?'] = r.pop('PickRoute', '')

    # Filter
    filtered = []
    for r in final_rows:
        doc = r.get('SalesHeaderDocStatus')
        proc = r.get('EDI Processing Status')
        if (doc is None or proc is None) or (doc in ['Picking List'] and proc in ['AX Load Failure']):
            filtered.append(r)

    # Dedupe by Pickticket
    seen = set()
    deduped = []
    for r in filtered:
        pt = r.get('Pickticket', '')
        if pt not in seen:
            seen.add(pt)
            deduped.append(r)

    return deduped


@app.get("/")
def root():
    return {"message": "Missing 945 API running (no-pandas)"}


@app.post("/reconcile")
async def reconcile(
    shipment_history: UploadFile = File(..., description="Shipment_History___Total-*.csv"),
    edib2bi: UploadFile = File(..., description="EDIB2BiReportV2*.csv"),
    edi940: UploadFile = File(..., description="EDI940Report_withCostV2.0*.csv"),
    format: str = "xlsx",
):
    format = (format or "xlsx").lower()
    if format not in {"xlsx", "csv", "json", "pdf"}:
        raise HTTPException(status_code=400, detail="format must be one of xlsx,csv,json,pdf")

    rows = build_report_rows(shipment_history, edib2bi, edi940)

    # Determine final column order
    columns = [
        'Pickticket','Warehouse','Order','Drop Date','Ship Date','Ship To',
        'Ship State','Zip Code','Customer PO','Ship Via','Load ID','Weight','SKU','Units','Price','Size Type','Size','Product Type',
        'Received in EDI?','EDI Processing Status','EDI Message',
        'Found in AX DATa?','SalesHeaderStatus','SalesHeaderDocStatus','PickModeOfDelivery','PickCreatedDate','DeliveryDate'
    ]

    stamp = datetime.datetime.now().strftime("%m%d%y")
    base_filename = f"MISSING_945_{stamp}"

    if format == "json":
        return JSONResponse({
            "filename": base_filename + ".json",
            "rows": rows,
        })

    if format == "xlsx":
        data = to_xlsx_bytes(rows, columns)
        media = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = base_filename + ".xlsx"
    elif format == "csv":
        data = to_csv_bytes(rows, columns)
        media = "text/csv"
        filename = base_filename + ".csv"
    else:  # pdf
        data = to_pdf_bytes(rows, columns)
        media = "application/pdf"
        filename = base_filename + ".pdf"

    return StreamingResponse(io.BytesIO(data), media_type=media, headers={
        "Content-Disposition": f'attachment; filename="{filename}"'
    })


@app.post("/send-report")
async def send_report(
    shipment_history: UploadFile = File(..., description="Shipment_History___Total-*.csv"),
    edib2bi: UploadFile = File(..., description="EDIB2BiReportV2*.csv"),
    edi940: UploadFile = File(..., description="EDI940Report_withCostV2.0*.csv"),
    to: str = Form(..., description="Comma-separated recipient emails"),
    subject: Optional[str] = Form(None),
    body: Optional[str] = Form(None),
    format: str = Form("xlsx"),
):
    rows = build_report_rows(shipment_history, edib2bi, edi940)

    columns = [
        'Pickticket','Warehouse','Order','Drop Date','Ship Date','Ship To',
        'Ship State','Zip Code','Customer PO','Ship Via','Load ID','Weight','SKU','Units','Price','Size Type','Size','Product Type',
        'Received in EDI?','EDI Processing Status','EDI Message',
        'Found in AX DATa?','SalesHeaderStatus','SalesHeaderDocStatus','PickModeOfDelivery','PickCreatedDate','DeliveryDate'
    ]

    format = (format or "xlsx").lower()
    if format not in {"xlsx", "csv", "pdf"}:
        raise HTTPException(status_code=400, detail="format must be one of xlsx,csv,pdf")

    stamp = datetime.datetime.now().strftime("%m%d%y")
    filename = f"MISSING_945_{stamp}.{format}"

    if format == "xlsx":
        data = to_xlsx_bytes(rows, columns)
        media = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif format == "csv":
        data = to_csv_bytes(rows, columns)
        media = "text/csv"
    else:
        data = to_pdf_bytes(rows, columns)
        media = "application/pdf"

    # Email configuration via environment
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    smtp_from = os.getenv("SMTP_FROM", smtp_user or "noreply@example.com")

    if not smtp_host or not smtp_user or not smtp_pass:
        raise HTTPException(status_code=500, detail="SMTP not configured. Set SMTP_HOST, SMTP_USER, SMTP_PASS (and optionally SMTP_PORT, SMTP_FROM)")

    msg = EmailMessage()
    msg["From"] = smtp_from
    recipients = [e.strip() for e in to.split(",") if e.strip()]
    if not recipients:
        raise HTTPException(status_code=400, detail="No recipients provided")
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject or f"Missing 945 Report - {stamp}"
    msg.set_content(body or "Attached is today's Missing 945 report.")
    maintype, subtype = media.split('/')
    msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=filename)

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send email: {e}")

    return {"status": "sent", "recipients": recipients, "filename": filename}


@app.get("/test")
def test():
    return {"backend": "✅ Running"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
