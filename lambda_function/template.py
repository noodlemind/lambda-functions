import json
import os
from typing import Any, Dict, Tuple, Optional
from .util import normalize_loan_10, generate_loan_number

def load_template_from_package_or_s3(template_name: Optional[str] = None, 
                                     template_s3_uri: Optional[str] = None, 
                                     template_inline: Optional[Dict] = None) -> Tuple[Dict[str, Any], str]:
    """Load template from inline data, S3, or local package samples."""
    if template_inline:
        return template_inline, template_name or "inline_template.json"
    
    if template_s3_uri:
        # reuse s3_reader to fetch JSON
        from .s3_reader import parse_s3_uri  # lazy import
        import boto3
        bucket, key = parse_s3_uri(template_s3_uri)
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        return json.loads(data), os.path.basename(key)
    
    # package sample - find samples directory relative to lambda_function package
    # In Lambda, the structure will be: /var/task/lambda_function/ and /var/task/samples/
    # Locally, it's: .../lambda-functions/lambda_function/ and .../lambda-functions/samples/
    lambda_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(lambda_dir)  # Go up from lambda_function/ to lambda-functions/
    samples_dir = os.path.join(base_dir, "samples")
    
    template_file = template_name or "Loan_Event_Sample.json"
    pkg_path = os.path.join(samples_dir, template_file)
    
    if not os.path.exists(pkg_path):
        raise FileNotFoundError(f"Template file not found: {pkg_path}")
    
    with open(pkg_path, "r", encoding="utf-8") as f:
        return json.load(f), os.path.basename(pkg_path)

def _deep_replace(obj: Any, token: str, value: str) -> Any:
    """Recursively replace tokens in nested data structures."""
    if isinstance(obj, dict):
        return {k: _deep_replace(v, token, value) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_replace(v, token, value) for v in obj]
    if isinstance(obj, str):
        return obj.replace(token, value)
    return obj

def render_with_loan(template: Dict[str, Any], loan: str, seq: int) -> Dict[str, Any]:
    """Replace placeholders in template with actual loan number and sequence."""
    # Accept both misspelling and correct placeholder
    t = _deep_replace(template, "#loanNumberPlacehoder", loan)  # Keep misspelling for backward compat
    t = _deep_replace(t, "#loanNumberPlaceholder", loan)
    # Optionally replace other tokens
    t = _deep_replace(t, "{seq}", str(seq))
    t = _deep_replace(t, "{loanNumber}", loan)
    return t
