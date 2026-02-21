# Inventory Reconciliation – Notes

## Overview
This project reconciles two inventory snapshots (`data/snapshot_1.csv` and `data/snapshot_2.csv`) to identify SKUs that were **added**, **removed**, **unchanged**, or **changed in quantity**. It produces:
- `output/reconciliation_items.csv` (row-level reconciliation table)
- `output/reconciliation_report.json` (summary + data-quality findings)
- A bar chart comparing total quantity between Week 1 and Week 2

All paths are computed relative to the repository to ensure the code runs consistently after cloning from GitHub.

---

## Key decisions and assumptions
- **Repo-friendly paths:** `PROJECT_ROOT = Path(__file__).resolve().parent` so the project does not depend on absolute paths like `/Users/...`.
- **Join key:** Reconciliation uses an **outer merge on `sku`** to surface adds/removals using the `_merge` indicator. (Note: this assumes SKU is unique enough for reconciliation; if inventory is tracked by `(sku, location)`, reconciliation could be extended accordingly.)
- **SKU normalization rules:** `normalize_sku()` reduces SKU variants by:
  - trimming + uppercasing
  - removing spaces/underscores (e.g., `SKU 005`, `SKU_005`)
  - converting `SKU123` → `SKU-123`
  - treating `"none"`, `"nan"`, `"null"` as missing
  This avoids false mismatches across snapshots due to formatting.
- **Quantities:** Coerced to numeric; non-numeric quantities are flagged. Floats are rounded and stored as `Int64` (inventory counts are assumed discrete). Negative quantities are flagged (could be returns/adjustments or bad data—kept for visibility).
- **Duplicate handling:** Duplicate `(sku, location)` rows are flagged and aggregated by **sum(quantity)**. This assumes duplicates are split lines for the same physical inventory bucket.

---

## Data quality issues found (and how they’re handled)
The cleaning step logs structured issues per snapshot (stored in `reconciliation_report.json` under `data_quality`):
- **Missing SKU / invalid SKU strings:** null-like values can appear as empty strings or `"None"/"nan"`. These are treated as missing and dropped because SKU is required to reconcile.
- **Missing location:** empty/`"nan"` locations are flagged and dropped since location is required for meaningful tracking.
- **Non-numeric quantities:** values like `"oops"` become `NaN` and are flagged. Rows are currently retained (quantity becomes `<NA>`) so anomalies are visible in outputs.
- **Float quantities:** flagged and rounded to the nearest integer.
- **Negative quantities:** flagged for review; not automatically removed.
- **Duplicate `(sku, location)` keys:** flagged and aggregated (prevents double counting).

---

## How I approached the problem
1. **Schema harmonization:** Normalize column names and map expected fields (`qty → quantity`, `warehouse → location`, etc.).
2. **Deterministic cleaning:** Apply consistent rules for SKU, quantity, and location across both snapshots while recording issues for auditability.
3. **Reconciliation with status labels:** Use an outer merge to compute `qty_delta`, `qty_delta_pct`, and a categorical `status` for business interpretation.
4. **Additional checks:** Flag `name_mismatch` and `location_changed` for SKUs present in both snapshots.
5. **Deliverables:** Export CSV + JSON report + quick visualization of week-to-week total quantity change.

---

## Testing
Testing is implemented with **pytest** to validate the core business logic without running the full pipeline:
- **Unit tests for SKU normalization:** Verifies that common formatting variants normalize correctly and that null-like strings are treated as missing.
- **Unit tests for `clean()`:**
  - Missing required columns raise a `ValueError`
  - Rows with missing SKU/location are dropped and logged (`DROPPED_MISSING_KEY`)
  - Non-numeric quantities are detected (`NULL_OR_NONNUMERIC_QUANTITY`)
  - Float quantities are flagged and rounded (`FLOAT_QUANTITY`)
  - Negative quantities are flagged (`NEGATIVE_QUANTITY`)
  - Duplicate `(sku, location)` rows are aggregated (`DUPLICATE_KEY`)
- **Reconciliation behavior tests:** Validates status assignment (`added/removed/unchanged/changed`) and correctness of mismatch flags (`name_mismatch`, `location_changed`).

- Run tests from the project root:
- pytest -q
