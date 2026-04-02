import re
import hashlib
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

def sha256_bytes(content: bytes) -> str:
    
    return hashlib.sha256(content).hexdigest()

def build_record_id(*parts: str) -> str:
    """
    Build a stable ID for one logical source record.
    The file hash is stored separately so content changes can create versions.
    """
    normalized = "||".join((part or "").strip() for part in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

def infer_content_type(url: str, content_type_header: str = "") -> str:
    """
    Infer a simple file type from the URL first, then from the response header.
    """
    url = (url or "").lower()
    header = (content_type_header or "").lower()

    if url.endswith(".pdf"):
        return "pdf"
    if url.endswith(".doc") or url.endswith(".docx"):
        return "doc"
    if url.endswith(".html") or url.endswith(".htm"):
        return "html"

    if "pdf" in header:
        return "pdf"
    if "html" in header:
        return "html"
    if "word" in header:
        return "doc"
    return "unknown"

def date_partitions(start_date: str, end_date: str, size: str):
    """
    Generic partitioner.
    size: "daily" | "weekly" | "monthly"
    """
    size = (size or "monthly").lower()
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current = start
    while current < end:
        if size == "daily":
            nxt = current + timedelta(days=1)
        elif size == "weekly":
            nxt = current + timedelta(days=7)
        else:
            nxt = current + relativedelta(months=1)

        yield current.strftime("%Y-%m-%d"), min(nxt, end).strftime("%Y-%m-%d")
        current = nxt


def normalize_date_string(
    value: str,
    input_format: str = "%d/%m/%Y",
    output_format: str = "%Y-%m-%d",
    logger=None,
    log_event: str = "invalid_date_format",
) -> str:
    """
    Normalize a date string from one format to another.
    If parsing fails, return the cleaned original value.
    """
    cleaned = " ".join((value or "").split()).strip()
    if not cleaned:
        return ""

    try:
        return datetime.strptime(cleaned, input_format).strftime(output_format)
    except ValueError:
        if logger is not None:
            logger.info(
                json.dumps(
                    {
                        "event": log_event,
                        "raw_value": cleaned,
                    }
                )
            )
        return cleaned

def clean_text(value):
    """
    Basic text cleanup for scraped metadata fields.
    """
    if not value:
        return ""

    value = re.sub(r"\s+", " ", value)
    return value.strip()
