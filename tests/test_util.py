import pytest

from lambda_function.util import (
    normalize_loan_10,
    generate_loan_number,
    derive_event_name,
    extract_loan,
)
from lambda_function.template import render_with_loan


def test_normalize_loan_10():
    assert normalize_loan_10("12-34") == "0000001234"
    with pytest.raises(ValueError):
        normalize_loan_10("")


def test_generate_loan_number_deterministic():
    loan1 = generate_loan_number("12", 1, "job")
    loan2 = generate_loan_number("12", 1, "job")
    assert loan1 == loan2
    assert loan1.startswith("12")
    assert len(loan1) == 10


def test_derive_event_name_and_extract_loan():
    record = {"loan_no": "123"}
    assert extract_loan(record) == "0000000123"
    with pytest.raises(ValueError):
        extract_loan({})
    assert (
        derive_event_name(
            "loan_file.json",
            None,
            None,
        )
        == "LoanOnboardCompleted"
    )
    assert derive_event_name(None, "Explicit", {}) == "Explicit"
    assert (
        derive_event_name(None, None, {"eventName": "Foo"}) == "Foo"
    )


def test_render_with_loan():
    template = {"loan": "#loanNumberPlaceholder", "seq": "{seq}"}
    rendered = render_with_loan(template, "12345", 7)
    assert rendered["loan"] == "12345"
    assert rendered["seq"] == "7"
