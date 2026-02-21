import json
import re
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR = PROJECT_ROOT / "output"


COLUMN_MAP = {
    "product_name": "name",
    "qty": "quantity",
    "warehouse": "location",
    "updated_at": "last_counted",
}


def load(path: Path, label: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip().str.lower()
    df = df.rename(columns=COLUMN_MAP)
    df["_source"] = label
    return df


def normalize_sku(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None

    # ✅ treat common string nulls as missing
    if s.strip().lower() in {"none", "nan", "null"}:
        return None

    s = s.upper()
    s = s.replace(" ", "").replace("_", "")
    s = re.sub(r"^SKU(\d+)$", r"SKU-\1", s)
    return s


def clean(df: pd.DataFrame, label: str):
    """
    Clean a snapshot:
    - strip string columns
    - normalize SKU
    - parse quantities and flag issues
    - normalize locations
    - aggregate duplicate (sku, location) keys
    """
    df = df.copy()
    issues = []

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    required = ["sku", "quantity", "location"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{label}: Missing required columns: {missing}. Found={list(df.columns)}")

    original_sku = df["sku"].copy()
    df["sku"] = df["sku"].apply(normalize_sku)

    changed = df[df["sku"].fillna("") != original_sku.astype(str).str.strip().str.upper().fillna("")]
    for idx in changed.index[:200]:
        issues.append(
            {
                "source": label,
                "type": "SKU_FORMAT",
                "sku": df.at[idx, "sku"],
                "detail": f"{original_sku[idx]} -> {df.at[idx, 'sku']}",
            }
        )

    # Normalize location
    df["location"] = df["location"].astype(str).str.strip()
    bad_loc = df["location"].isna() | (df["location"].str.len() == 0) | (df["location"].str.lower() == "nan")
    if bad_loc.any():
        issues.append(
            {
                "source": label,
                "type": "MISSING_LOCATION",
                "sku": None,
                "detail": f"{int(bad_loc.sum())} rows missing location",
            }
        )

    # Parse quantity
    df["quantity_raw"] = df["quantity"]
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    bad_qty = df["quantity"].isna()
    if bad_qty.any():
        issues.append(
            {
                "source": label,
                "type": "NULL_OR_NONNUMERIC_QUANTITY",
                "sku": None,
                "detail": f"{int(bad_qty.sum())} rows have non-numeric quantity",
            }
        )

    float_qty = df["quantity"].notna() & (df["quantity"] % 1 != 0)
    if float_qty.any():
        issues.append(
            {
                "source": label,
                "type": "FLOAT_QUANTITY",
                "sku": None,
                "detail": f"{int(float_qty.sum())} rows have non-integer quantity (rounded)",
            }
        )

    df["quantity"] = df["quantity"].round().astype("Int64")

    neg = df["quantity"].notna() & (df["quantity"] < 0)
    if neg.any():
        issues.append(
            {
                "source": label,
                "type": "NEGATIVE_QUANTITY",
                "sku": None,
                "detail": f"{int(neg.sum())} rows have negative quantity",
            }
        )

    # Drop rows missing sku/location
    bad_key = df["sku"].isna() | bad_loc
    if bad_key.any():
        issues.append(
            {
                "source": label,
                "type": "DROPPED_MISSING_KEY",
                "sku": None,
                "detail": f"Dropped {int(bad_key.sum())} rows missing sku/location",
            }
        )
        df = df.loc[~bad_key].copy()

    # Aggregate duplicates
    dup_key = df.duplicated(["sku", "location"], keep=False)
    if dup_key.any():
        issues.append(
            {
                "source": label,
                "type": "DUPLICATE_KEY",
                "sku": None,
                "detail": f"{int(dup_key.sum())} rows have duplicate (sku,location); aggregated by sum",
            }
        )
        df = (
            df.groupby(["sku", "location"], as_index=False)
            .agg(
                quantity=("quantity", "sum"),
                name=("name", "first"),
                last_counted=("last_counted", "first") if "last_counted" in df.columns else ("sku", "first"),
            )
        )

    return df, issues


def status(row) -> str:
    if row["_merge"] == "both":
        if row["qty_1"] == row["qty_2"]:
            return "present_in_both_unchanged"
        return "present_in_both_qty_changed"
    if row["_merge"] == "left_only":
        return "only_in_snapshot_1_removed"
    return "only_in_snapshot_2_added"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    snap1 = DATA_DIR / "snapshot_1.csv"
    snap2 = DATA_DIR / "snapshot_2.csv"

    w1_raw = load(snap1, "week1")
    w2_raw = load(snap2, "week2")

    w1, issues1 = clean(w1_raw, "week1")
    w2, issues2 = clean(w2_raw, "week2")

    print("Distinct SKUs in week1:", w1["sku"].nunique())
    print("Distinct SKUs in week2:", w2["sku"].nunique())

    merged = w1.merge(w2, on="sku", how="outer", suffixes=("_1", "_2"), indicator=True)

    merged["qty_1"] = merged["quantity_1"]
    merged["qty_2"] = merged["quantity_2"]
    merged["qty_delta"] = merged["qty_2"] - merged["qty_1"]

    merged["qty_delta_pct"] = pd.NA
    valid_base = merged["qty_1"].notna() & (merged["qty_1"] != 0)
    merged.loc[valid_base, "qty_delta_pct"] = (
        (merged.loc[valid_base, "qty_delta"] / merged.loc[valid_base, "qty_1"]) * 100
    ).round(2)

    merged["status"] = merged.apply(status, axis=1)

    both_mask = merged["_merge"] == "both"

    merged["name_mismatch"] = False
    if both_mask.any():
        n1 = merged.loc[both_mask, "name_1"].astype("string")
        n2 = merged.loc[both_mask, "name_2"].astype("string")
        merged.loc[both_mask, "name_mismatch"] = (
            n1.notna() & n2.notna() & (n1.str.strip().str.lower() != n2.str.strip().str.lower())
        )

    merged["location_changed"] = False
    if both_mask.any():
        merged.loc[both_mask, "location_changed"] = (
            merged.loc[both_mask, "location_1"].fillna("") != merged.loc[both_mask, "location_2"].fillna("")
        )

    items = merged[
        [
            "sku",
            "name_1",
            "name_2",
            "location_1",
            "location_2",
            "qty_1",
            "qty_2",
            "qty_delta",
            "qty_delta_pct",
            "status",
            "name_mismatch",
            "location_changed",
        ]
    ].sort_values(["status", "sku"]).reset_index(drop=True)

    print(items["status"].value_counts().to_string())
    print(f"\nName mismatches  : {items['name_mismatch'].sum()}")
    print(f"Location changes : {items['location_changed'].sum()}")

    summary = {
        "counts_by_status": {k: int(v) for k, v in items["status"].value_counts().to_dict().items()},
        "changed_rows": int((items["status"] == "present_in_both_qty_changed").sum()),
        "added_rows": int((items["status"] == "only_in_snapshot_2_added").sum()),
        "removed_rows": int((items["status"] == "only_in_snapshot_1_removed").sum()),
        "location_changes": int(items["location_changed"].sum()),
        "name_mismatches": int(items["name_mismatch"].sum()),
    }

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "data_quality": {"snapshot_1": issues1, "snapshot_2": issues2},
    }

    items.to_csv(OUT_DIR / "reconciliation_items.csv", index=False)
    with open(OUT_DIR / "reconciliation_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print("\nSaved outputs to:", OUT_DIR)
    print(json.dumps(summary, indent=2))

    # Plot totals
    total_qty_week1 = items["qty_1"].sum(skipna=True)
    total_qty_week2 = items["qty_2"].sum(skipna=True)

    labels = ["Week 1", "Week 2"]
    values = [total_qty_week1, total_qty_week2]

    plt.figure(figsize=(6, 4))
    bars = plt.bar(labels, values, color=["#55A868", "#C44E52"])
    plt.title("Total inventory quantity by week")
    plt.ylabel("Total quantity (all SKUs)")
    plt.grid(axis="y", alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            height,
            f"{int(height):,}",
            ha="center",
            va="bottom",
            fontsize=10,
        )

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()