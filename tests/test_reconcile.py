
import sys
from pathlib import Path
import pandas as pd
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import pytest 

from reconcile import normalize_sku, clean

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("sku 005", "SKU-005"),
        ("SKU_005", "SKU-005"),
        ("SKU005", "SKU-005"),
        (" sku-12 ", "SKU-12"),
        ("", None),
        ("   ", None),
        (None, None),
        (float("nan"), None),
        ("abc", "ABC"),
    ],
)
def test_normalize_sku(raw, expected):
    assert normalize_sku(raw) == expected


"""TEST CASES"""

def test_clean_missing_required_columns_raises():
    df = pd.DataFrame({"sku": ["SKU-1"], "quantity": [1]})  # location missing
    with pytest.raises(ValueError) as e:
        clean(df, "week1")
    assert "Missing required columns" in str(e.value)


def test_clean_drops_rows_missing_sku_or_location_and_records_issue():
    df = pd.DataFrame(
        {
            "sku": ["SKU-1", None, "SKU-3"],
            "quantity": [5, 2, 1],
            "location": ["A", "B", ""],  # empty is treated as missing
            "name": ["n1", "n2", "n3"],
        }
    )
    cleaned, issues = clean(df, "week1")

    # Only the valid row remains
    assert len(cleaned) == 1
    assert cleaned.iloc[0]["sku"] == "SKU-1"
    assert cleaned.iloc[0]["location"] == "A"

    # Should include dropped missing key issue
    issue_types = {i["type"] for i in issues}
    assert "DROPPED_MISSING_KEY" in issue_types


def test_clean_flags_non_numeric_quantity():
    df = pd.DataFrame(
        {
            "sku": ["SKU-1", "SKU-2"],
            "quantity": ["10", "oops"],
            "location": ["A", "A"],
            "name": ["n1", "n2"],
        }
    )
    cleaned, issues = clean(df, "week1")

    assert cleaned["quantity"].isna().sum() == 1

    issue_types = {i["type"] for i in issues}
    assert "NULL_OR_NONNUMERIC_QUANTITY" in issue_types


def test_clean_flags_and_rounds_float_quantities():
    df = pd.DataFrame(
        {
            "sku": ["SKU-1", "SKU-2"],
            "quantity": [1.2, 2.7],
            "location": ["A", "A"],
            "name": ["n1", "n2"],
        }
    )
    cleaned, issues = clean(df, "week1")

    # Rounded to nearest int
    assert cleaned.loc[cleaned["sku"] == "SKU-1", "quantity"].iloc[0] == 1
    assert cleaned.loc[cleaned["sku"] == "SKU-2", "quantity"].iloc[0] == 3

    issue_types = {i["type"] for i in issues}
    assert "FLOAT_QUANTITY" in issue_types


def test_clean_flags_negative_quantity():
    df = pd.DataFrame(
        {
            "sku": ["SKU-1"],
            "quantity": [-5],
            "location": ["A"],
            "name": ["n1"],
        }
    )
    cleaned, issues = clean(df, "week1")


    assert len(cleaned) == 1
    issue_types = {i["type"] for i in issues}
    assert "NEGATIVE_QUANTITY" in issue_types


def test_clean_aggregates_duplicate_sku_location_by_sum():
    df = pd.DataFrame(
        {
            "sku": ["SKU-1", "SKU-1", "SKU-2"],
            "quantity": [3, 4, 1],
            "location": ["A", "A", "B"],
            "name": ["n1", "n1_alt", "n2"],
        }
    )

    cleaned, issues = clean(df, "week1")


    row = cleaned[(cleaned["sku"] == "SKU-1") & (cleaned["location"] == "A")].iloc[0]
    assert int(row["quantity"]) == 7

    issue_types = {i["type"] for i in issues}
    assert "DUPLICATE_KEY" in issue_types


def test_clean_records_sku_format_changes():
    df = pd.DataFrame(
        {
            "sku": ["sku 005", "SKU_006", "SKU-007"],
            "quantity": [1, 1, 1],
            "location": ["A", "A", "A"],
            "name": ["n1", "n2", "n3"],
        }
    )
    cleaned, issues = clean(df, "week1")

    # Ensure normalization happened
    assert set(cleaned["sku"].tolist()) == {"SKU-005", "SKU-006", "SKU-007"}
    assert any(i["type"] == "SKU_FORMAT" for i in issues)



def status(row):
    if row["_merge"] == "both":
        if row["qty_1"] == row["qty_2"]:
            return "present_in_both_unchanged"
        return "present_in_both_qty_changed"
    if row["_merge"] == "left_only":
        return "only_in_snapshot_1_removed"
    return "only_in_snapshot_2_added"


def test_reconcile_status_values():
    w1 = pd.DataFrame(
        {"sku": ["SKU-1", "SKU-2"], "quantity": [10, 5], "location": ["A", "A"], "name": ["n1", "n2"]}
    )
    w2 = pd.DataFrame(
        {"sku": ["SKU-1", "SKU-3"], "quantity": [10, 7], "location": ["A", "B"], "name": ["n1", "n3"]}
    )

    merged = w1.merge(w2, on="sku", how="outer", suffixes=("_1", "_2"), indicator=True)
    merged["qty_1"] = merged["quantity_1"]
    merged["qty_2"] = merged["quantity_2"]
    merged["status"] = merged.apply(status, axis=1)

    # SKU-1 in both unchanged
    s1 = merged.loc[merged["sku"] == "SKU-1", "status"].iloc[0]
    assert s1 == "present_in_both_unchanged"

    # SKU-2 removed
    s2 = merged.loc[merged["sku"] == "SKU-2", "status"].iloc[0]
    assert s2 == "only_in_snapshot_1_removed"

    # SKU-3 added
    s3 = merged.loc[merged["sku"] == "SKU-3", "status"].iloc[0]
    assert s3 == "only_in_snapshot_2_added"


def test_name_mismatch_and_location_changed_flags():
    w1 = pd.DataFrame({"sku": ["SKU-1"], "quantity": [10], "location": ["A"], "name": ["Widget"]})
    w2 = pd.DataFrame({"sku": ["SKU-1"], "quantity": [12], "location": ["B"], "name": ["WIDGET PRO"]})

    merged = w1.merge(w2, on="sku", how="outer", suffixes=("_1", "_2"), indicator=True)
    merged["qty_1"] = merged["quantity_1"]
    merged["qty_2"] = merged["quantity_2"]

    both_mask = merged["_merge"] == "both"

    merged["name_mismatch"] = False
    n1 = merged.loc[both_mask, "name_1"].astype("string")
    n2 = merged.loc[both_mask, "name_2"].astype("string")
    merged.loc[both_mask, "name_mismatch"] = (
        n1.notna() & n2.notna() &
        (n1.str.strip().str.lower() != n2.str.strip().str.lower())
    )

    merged["location_changed"] = False
    merged.loc[both_mask, "location_changed"] = (
        merged.loc[both_mask, "location_1"].fillna("") !=
        merged.loc[both_mask, "location_2"].fillna("")
    )

    assert merged["name_mismatch"].iloc[0]
    assert merged["location_changed"].iloc[0]
 