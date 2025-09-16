import os
import time
from typing import Any, Dict, Iterable, Tuple

from .lanes import LaneMux
from .publisher_http import SubmitterHttpPublisher
from .publisher_sns import SnsLanePublisher
from .s3_reader import iter_ndjson, iter_json_array_small, parse_s3_uri
from .template import load_template_from_package_or_s3, render_with_loan
from .util import (
    derive_event_name,
    extract_loan,
    generate_loan_number,
    normalize_loan_10,
    stable_hash,
    time_budget_seconds,
)

def _get(d: Dict[str, Any], path: str, default=None):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
        if cur is None:
            return default
    return cur

def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Event keys (subset):
      - mode: "S3_REPLAY" | "TEMPLATE_CLONE"
      - backend: "submitter_http" | "sns" (default submitter_http)
      - http: { base_url, path, max_pool, timeout_s }
      - sns:  { topic_arn }
      - publish: { lane_count, max_workers, time_budget_secs, max_messages_per_invocation }
      - grouping: { loan_field, strict_fifo_per_loan }
      - s3_replay: { s3_uri, format, offset, limit, event_name }
      - template_clone: { template_name | template_s3_uri | template_inline, count, seq_start, loan_number_rule, sequence_prefix, event_name }
      - attributes: dict (merged into attributes for each publish)
    """
    start = time.time()
    job_id = event.get("job_id") or f"JOB-{int(start)}"
    mode = event.get("mode")
    if mode not in ("S3_REPLAY", "TEMPLATE_CLONE"):
        raise ValueError("mode must be S3_REPLAY or TEMPLATE_CLONE")

    backend = event.get("backend", "submitter_http")

    # Publishing dials
    lane_count = int(_get(event, "publish.lane_count", 64))
    max_workers = int(_get(event, "publish.max_workers", lane_count))
    time_budget = time_budget_seconds(event, context, default=_get(event, "publish.time_budget_secs", 840))
    max_messages = _get(event, "publish.max_messages_per_invocation", 0) or 0

    grouping = event.get("grouping", {}) or {}
    loan_field = grouping.get("loan_field", "loanNumber")

    # Global attributes to attach to each publish
    base_attrs = event.get("attributes", {}) or {}
    base_attrs.setdefault("jobId", job_id)

    # Build lane workers
    if backend == "submitter_http":
        http_cfg = event.get("http", {}) or {}
        base_url = http_cfg.get("base_url")
        if not base_url:
            raise ValueError("http.base_url is required for submitter_http backend")
        path = http_cfg.get("path", "/sendMessage")
        max_pool = int(http_cfg.get("max_pool", 256))
        timeout_s = float(http_cfg.get("timeout_s", 3))
        def worker_factory(lane_id: int) -> SubmitterHttpPublisher:
            return SubmitterHttpPublisher(
                base_url=base_url, path=path, max_pool=max_pool, timeout_s=timeout_s
            )
    elif backend == "sns":
        sns_cfg = event.get("sns", {}) or {}
        topic_arn = sns_cfg.get("topic_arn")
        if not topic_arn:
            raise ValueError("sns.topic_arn is required for sns backend")
        def worker_factory(lane_id: int) -> SnsLanePublisher:
            return SnsLanePublisher(topic_arn=topic_arn, batch_size=10)
    else:
        raise ValueError("backend must be submitter_http or sns")

    lanes = LaneMux(lane_count=lane_count, max_workers=max_workers, worker_factory=worker_factory)

    processed = 0
    failed = 0
    next_offset = None

    try:
        if mode == "S3_REPLAY":
            s3r = event.get("s3_replay", {}) or {}
            s3_uri = s3r.get("s3_uri")
            if not s3_uri:
                raise ValueError("s3_replay.s3_uri is required in S3_REPLAY mode")
            fmt = (s3r.get("format") or "ndjson").lower()
            offset = int(s3r.get("offset") or 0)
            _limit = int(s3r.get("limit") or 0)
            src_name = os.path.basename(parse_s3_uri(s3_uri)[1])
            default_event_name = derive_event_name(src_name, s3r.get("event_name"), None)

            if fmt == "ndjson":
                records: Iterable[Tuple[int, Dict[str, Any]]] = iter_ndjson(s3_uri, start_offset=offset)
            elif fmt == "json_array":
                # Warning: loads into memory; for small files only
                arr = iter_json_array_small(s3_uri)
                records = ((i + offset, r) for i, r in enumerate(arr[offset:]))
            else:
                raise ValueError("s3_replay.format must be ndjson or json_array")

            for seq, rec in records:
                # strict per-loan FIFO: submit to the loan's lane (ordered)
                loan = extract_loan(rec, loan_field=loan_field)
                event_name = derive_event_name(src_name, s3r.get("event_name"), rec)
                attrs = dict(base_attrs)
                attrs.update({"eventName": event_name, "loanNumber": loan})
                payload = rec.get("payload", rec)
                lane_id = stable_hash(loan) % lane_count
                lanes.submit(lane_id, {"loan": loan, "event_name": event_name, "payload": payload, "attributes": attrs, "seq": seq})

                processed += 1
                next_offset = seq + 1

                if max_messages and processed >= max_messages:
                    break
                remaining = time_budget - (time.time() - start)
                if remaining <= 5:
                    break

        else:  # TEMPLATE_CLONE
            tcfg = event.get("template_clone", {}) or {}
            # Source template: package (samples/), S3, or inline
            template_name = tcfg.get("template_name") or "Loan_Event_Sample.json"
            template_s3_uri = tcfg.get("template_s3_uri")
            template_inline = tcfg.get("template_inline")
            template, src_name = load_template_from_package_or_s3(template_name, template_s3_uri, template_inline)

            default_event_name = derive_event_name(src_name, tcfg.get("event_name"), template)

            count = int(tcfg.get("count") or 0)
            if count <= 0:
                raise ValueError("template_clone.count must be > 0")
            seq_start = int(tcfg.get("seq_start") or 0)
            seq_prefix = tcfg.get("sequence_prefix")  # digits string or None
            loan_rule = (tcfg.get("loan_number_rule") or "derive_per_seq").lower()

            # publish N clones
            for i in range(seq_start, seq_start + count):
                # compute loan number
                if loan_rule == "derive_per_seq":
                    loan = generate_loan_number(prefix=seq_prefix or "", seq=i, job_id=job_id)
                else:
                    # keep from template; then ensure 10 digits
                    raw_loan = template.get("loanNumber") or template.get("LoanNumber") or ""
                    if not raw_loan:
                        raise ValueError("Template missing loanNumber; set loan_number_rule=derive_per_seq or provide loanNumber in template_inline")
                    loan = normalize_loan_10(raw_loan)

                # render payload (deep replace placeholders)
                payload = render_with_loan(template, loan, i)

                attrs = dict(base_attrs)
                attrs.update({"eventName": default_event_name, "loanNumber": loan})
                lane_id = stable_hash(loan) % lane_count
                lanes.submit(lane_id, {"loan": loan, "event_name": default_event_name, "payload": payload, "attributes": attrs, "seq": i})

                processed += 1
                next_offset = i + 1

                remaining = time_budget - (time.time() - start)
                if remaining <= 5:
                    break

        # drain lanes until deadline
        p2, f2 = lanes.drain_and_close(deadline_epoch=start + time_budget)
        processed = p2  # count final successful sends
        failed += f2

        return {
            "processed": processed,
            "failed": failed,
            "next_offset": next_offset,
            "partial": (time.time() - start) >= (time_budget - 1) or (max_messages and processed >= max_messages),
            "elapsed_ms": int((time.time() - start) * 1000),
        }

    finally:
        lanes.force_close()
