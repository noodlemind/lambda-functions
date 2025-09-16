# Lambda: S3 or Template → Publish (/sendMessage first, SNS optional)

## What this does
A single AWS Lambda (Python 3.12) that:
1) **S3_REPLAY** — Streams a file from S3 and publishes each record.
2) **TEMPLATE_CLONE** — Loads an on-package template (`lambda_function/samples/Loan_Event_Sample.json`) or a template from S3 and publishes **N clones**.

**Publishers**
- **Primary:** `submitter_http` → POST to `/sendMessage` with max parallelism.
- **Secondary (optional):** `sns` → publish to SNS FIFO (boto3).

**Ordering (strict per-loan)**
- All events (including MISMO) are serialized **per loan**. We hash each `loanNumber` to a **lane** and publish sequentially in that lane. Different loans go to different lanes -> high throughput; same loan -> strict order.

**EventName**
- Source file or template name decides the default:
  - `Loan_*` → `LoanOnboardCompleted`
  - `ReportingPayload_*` → `ServicerFileReported`
- You can override via the Lambda event.

**Loan numbers**
- For `TEMPLATE_CLONE`, Lambda replaces `#loanNumberPlacehoder` (and `#loanNumberPlaceholder`) inside the JSON with a **10-digit** `loanNumber`.
- Pass `sequence_prefix` (digits) to guarantee uniqueness across submissions.

---

## Invoke examples

### TEMPLATE_CLONE → /sendMessage
```json
{
  "job_id": "JOB-2025-09-12-001",
  "mode": "TEMPLATE_CLONE",
  "backend": "submitter_http",
  "http": { "base_url": "https://internal/service", "path": "/sendMessage", "max_pool": 256, "timeout_s": 3 },
  "publish": { "lane_count": 64, "max_workers": 64, "time_budget_secs": 840 },
  "grouping": { "loan_field": "loanNumber", "strict_fifo_per_loan": true },
  "template_clone": {
    "template_name": "Loan_Event_Sample.json",
    "count": 250000,
    "seq_start": 0,
    "loan_number_rule": "derive_per_seq",
    "sequence_prefix": "2715",
    "event_name": null
  "attributes": { "source": "clone" }
}
```

---

## Build & deploy
- **Runtime:** Python 3.12
- **Memory:** 1024–2048 MB; Timeout: 870–900 s
- **IAM:**
  - `/sendMessage` only: CloudWatch logs
  - SNS path: `sns:Publish` to topic ARN
  - S3 path: `s3:GetObject` to read S3 files (and `s3:GetObject` for S3 templates)
- **Package:**
  - Zip the `lambda_function/` folder (which now includes the `samples/` assets) into the deployment artifact.
  - No external libraries (only stdlib + boto3/urllib3 present in Lambda).

---

## Throughput tips
- Use 64 lanes/workers to reach ~1.5–2k msg/s (depending on endpoint latency).
- For SNS, prefer `PublishBatch` (10 msgs/call) for efficiency.
- For massive jobs, invoke several Lambdas with non-overlapping offset/limit windows.
