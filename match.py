# match.py — GSTR-2B vs Tally Purchase Register Reconciliation
import os, re, json
import pandas as pd
from datetime import datetime

# --- Optional: fast fuzzy; auto-fallback if unavailable ---
try:
    from rapidfuzz import fuzz
    def _ratio(a, b): return float(fuzz.token_sort_ratio(a, b))
except Exception:
    from difflib import SequenceMatcher
    def _ratio(a, b): return float(SequenceMatcher(None, a, b).ratio() * 100.0)


# ---------- Helpers ----------

def _norm_str(x):
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)   # remove punctuation
    s = re.sub(r"\s+", " ", s)       # collapse spaces
    return s.strip()


def _parse_gstr2b(path):
    """
    Parse SAJAN_SHREE_GARMENT.xls.
    - invoice sheet: header at row index 2, sorted by Invoice Date ascending.
    - note sheet   : header at row index 2, credit notes listed separately.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    xl = pd.ExcelFile(path)

    # ---- Invoice sheet ----
    df_inv = xl.parse("invoice", header=2)
    # Keep only numeric sno rows (drops any trailing totals / blank rows)
    df_inv = df_inv[pd.to_numeric(df_inv["sno"], errors="coerce").notna()].copy()
    df_inv["Invoice Date"] = pd.to_datetime(
        df_inv["Invoice Date"], dayfirst=True, errors="coerce"
    )
    df_inv["Invoice Value"] = pd.to_numeric(df_inv["Invoice Value"], errors="coerce")
    # Sort by Invoice Date ascending
    df_inv = df_inv.sort_values("Invoice Date").reset_index(drop=True)

    # ---- Note sheet ----
    df_note = xl.parse("note", header=2)
    df_note = df_note[pd.to_numeric(df_note["sno"], errors="coerce").notna()].copy()
    df_note["Note Date"] = pd.to_datetime(
        df_note["Note Date"], dayfirst=True, errors="coerce"
    )
    df_note["Note Value"] = pd.to_numeric(df_note["Note Value"], errors="coerce")
    df_note = df_note.sort_values("Note Date").reset_index(drop=True)

    return df_inv, df_note


def _parse_tally(path):
    """
    Parse GSTR-3B - Voucher Register.xlsx.
    Rows 0-5 are metadata/headers; actual data starts at row index 6.
    Columns: Date | Particulars | Vch Type | Vch No | Debit Amount | Credit Amount
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    xl = pd.ExcelFile(path)
    df = xl.parse(xl.sheet_names[0], header=None)

    df_data = df.iloc[6:].copy()
    df_data.columns = ["Date", "Particulars", "Vch Type", "Vch No",
                       "Debit Amount", "Credit Amount"]
    df_data = df_data.dropna(subset=["Date"]).copy()
    df_data["Date"] = pd.to_datetime(df_data["Date"], errors="coerce")
    df_data["Credit Amount"] = pd.to_numeric(df_data["Credit Amount"], errors="coerce")
    df_data["Debit Amount"]  = pd.to_numeric(df_data["Debit Amount"],  errors="coerce")
    df_data = df_data.reset_index(drop=True)

    return df_data


# ---------- Main ----------

def reconcile(gstr2b_path, tally_path, output_path=None,
              name_threshold=60.0, date_tolerance_days=3):
    """
    Match GSTR-2B invoices against Tally purchase register.

    Matching criteria (applied together for each GSTR-2B row):
      1. Invoice Value == Credit Amount  (within ±1 rupee rounding tolerance)
      2. Invoice Date  == Tally Date     (tier 1=exact, tier 2=within tolerance, tier 3=any)
      3. Supplier Name fuzzy match       (token_sort_ratio >= name_threshold)

    Best match = lowest date tier first, then highest name similarity.
    Each Tally row is used at most once.

    Output sheets:
      Summary          — run stats
      Unmatched_GSTR2B — GSTR-2B invoices with no Tally match
      Unmatched_Tally  — Tally entries with no GSTR-2B match
      Matched          — side-by-side: all GSTR-2B cols + all Tally cols per matched pair
      Credit_Notes     — credit notes from GSTR-2B (listed, not matched to purchases)
    """
    df_inv, df_note = _parse_gstr2b(gstr2b_path)
    df_tally = _parse_tally(tally_path)

    used_tally = set()   # tally row indices already consumed
    matched_rows = []
    unmatched_gstr2b_idx = []

    for idx_g, row_g in df_inv.iterrows():
        inv_val  = row_g["Invoice Value"]
        inv_date = row_g["Invoice Date"]
        sup_name = _norm_str(row_g["Supplier Name"])

        best_idx   = None
        best_tier  = 99
        best_score = -1.0

        for idx_t, row_t in df_tally.iterrows():
            if idx_t in used_tally:
                continue

            tally_val  = row_t["Credit Amount"]
            tally_date = row_t["Date"]
            tally_name = _norm_str(row_t["Particulars"])

            # 1. Amount must match within ±1 rupee
            if pd.isna(inv_val) or pd.isna(tally_val):
                continue
            if abs(inv_val - tally_val) > 1.0:
                continue

            # 2. Name similarity
            name_score = _ratio(sup_name, tally_name)
            if name_score < name_threshold:
                continue

            # 3. Date tier
            if pd.notna(inv_date) and pd.notna(tally_date):
                diff = abs((inv_date - tally_date).days)
                if diff == 0:
                    tier = 1
                elif diff <= date_tolerance_days:
                    tier = 2
                else:
                    tier = 3
            else:
                tier = 3

            # Keep best: lower tier wins; tie-break by higher name score
            if tier < best_tier or (tier == best_tier and name_score > best_score):
                best_tier  = tier
                best_score = name_score
                best_idx   = idx_t

        if best_idx is not None:
            used_tally.add(best_idx)
            t_row = df_tally.loc[best_idx]

            # Build one merged row: all GSTR-2B columns + all Tally columns + quality cols
            g2b_part   = row_g.add_prefix("g2b_")
            tally_part = t_row.add_prefix("tally_")
            merged = pd.concat([g2b_part, tally_part])
            merged["name_similarity"]  = round(best_score, 1)
            merged["date_match_tier"]  = best_tier   # 1=exact, 2=within tolerance, 3=relaxed
            matched_rows.append(merged)
        else:
            unmatched_gstr2b_idx.append(idx_g)

    # Build output frames
    matched_df          = pd.DataFrame(matched_rows) if matched_rows else pd.DataFrame()
    unmatched_gstr2b_df = df_inv.loc[unmatched_gstr2b_idx].copy()
    unmatched_tally_df  = df_tally[~df_tally.index.isin(used_tally)].copy()

    # Summary
    summary = {
        "generated_at":            datetime.now().isoformat(timespec="seconds"),
        "gstr2b_invoices_total":   len(df_inv),
        "gstr2b_credit_notes":     len(df_note),
        "tally_entries_total":     len(df_tally),
        "matched_count":           len(matched_rows),
        "unmatched_gstr2b_count":  len(unmatched_gstr2b_df),
        "unmatched_tally_count":   len(unmatched_tally_df),
        "name_threshold":          name_threshold,
        "date_tolerance_days":     date_tolerance_days,
    }

    if output_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"reconciliation_{ts}.xlsx"

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        pd.DataFrame([summary]).to_excel(
            writer, index=False, sheet_name="Summary")
        unmatched_gstr2b_df.to_excel(
            writer, index=False, sheet_name="Unmatched_GSTR2B")
        unmatched_tally_df.to_excel(
            writer, index=False, sheet_name="Unmatched_Tally")
        matched_df.to_excel(
            writer, index=False, sheet_name="Matched")
        df_note.to_excel(
            writer, index=False, sheet_name="Credit_Notes")

    return output_path, summary


if __name__ == "__main__":
    out, rep = reconcile(
        "SAJAN_SHREE_GARMENT.xls",
        "GSTR-3B - Voucher Register.xlsx",
    )
    print("Wrote:", out)
    print(json.dumps(rep, indent=2, default=str))
