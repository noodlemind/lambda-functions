import hashlib
import json
import os
import re
import time
from typing import Any, Dict, Optional

def derive_event_name(source_name: Optional[str], explicit: Optional[str], record: Optional[Dict[str, Any]]) -> str:
    if explicit:
        return explicit
    src = (source_name or "").strip()
    upper = src.upper()
    if upper.startswith("LOAN_"):
        return "LoanOnboardCompleted"
    if upper.startswith("REPORTINGPAYLOAD_"):
        return "ServicerFileReported"
    # fallback to record fields
    if record:
        for k in ("eventName", "event_type", "eventType"):
            if k in record:
                return str(record[k])
    # last resort
    return "UnknownEvent"

def extract_loan(record: Dict[str, Any], loan_field: str = "loanNumber") -> str:
    # Try canonical field, then aliases
    aliases = (loan_field, "LoanNumber", "loan_no", "loanId", "loan_id", "Loan_No")
    for k in aliases:
        if k in record:
            return normalize_loan_10(str(record[k]))
    raise ValueError(f"loan field not found in record; checked {aliases}")

def normalize_loan_10(raw: str) -> str:
    digits = re.sub(r"\D+", "", raw or "")
    if not digits:
        raise ValueError("loan number empty")
    return digits.zfill(10)[-10:]

def generate_loan_number(prefix: str, seq: int, job_id: str) -> str:
    """
    Deterministic 10-digit loan number from a user-supplied prefix + (job_id, seq).
    - prefix: digits; trimmed to <= 9 (<=10 also OK).
    - remaining digits are derived from blake2b(job_id:seq).
    """
    p = re.sub(r"\D+", "", prefix or "")
    if len(p) > 10:
        p = p[:10]
    if len(p) == 10:
        return p
    # remaining digits
    rem = 10 - len(p)
    h = hashlib.blake2b(f"{job_id}:{seq}".encode("utf-8"), digest_size=8).hexdigest()
    num = int(h, 16) % (10 ** rem)
    return (p + str(num).zfill(rem))[:10]

def stable_hash(s: str) -> int:
    return int(hashlib.blake2b(s.encode("utf-8"), digest_size=8).hexdigest(), 16)

def time_budget_seconds(event: Dict[str, Any], context, default: int = 840) -> int:
    req = int(default)
    try:
        rem_ms = context.get_remaining_time_in_millis()
        # keep 5s headroom
        return min(req, max(1, int(rem_ms / 1000) - 5))
    except Exception:
        return req
