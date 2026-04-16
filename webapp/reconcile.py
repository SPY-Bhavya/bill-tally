# reconcile.py — GSTR-2B vs Tally Purchase Register Reconciliation (core logic)
import os, re
import pandas as pd
from datetime import datetime

try:
    from rapidfuzz import fuzz
    def _ratio(a, b): return float(fuzz.token_sort_ratio(a, b))
except Exception:
    from difflib import SequenceMatcher
    def _ratio(a, b): return float(SequenceMatcher(None, a, b).ratio() * 100.0)


def _norm_str(x):
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _parse_gstr2b(path):
    """
    Parse GSTR-2B Excel (e.g. SAJAN_SHREE_GARMENT.xls).
    Expects sheets named 'invoice' and 'note', both with header at row index 2.
    Sorts invoices by Invoice Date ascending.
    """
    xl = pd.ExcelFile(path)

    # ---- Invoice sheet ----
    df_inv = xl.parse("invoice", header=2)
    df_inv = df_inv[pd.to_numeric(df_inv["sno"], errors="coerce").notna()].copy()
    df_inv["Invoice Date"] = pd.to_datetime(
        df_inv["Invoice Date"], dayfirst=True, errors="coerce"
    )
    df_inv["Invoice Value"] = pd.to_numeric(df_inv["Invoice Value"], errors="coerce")
    df_inv = df_inv.sort_values("Invoice Date").reset_index(drop=True)

    # ---- Note sheet ----
    df_note = xl.parse("note", header=2)
    df_note = df_note[pd.to_numeric(df_note["sno"], errors="coerce").notna()].copy()
    if "Note Date" in df_note.columns:
        df_note["Note Date"] = pd.to_datetime(
            df_note["Note Date"], dayfirst=True, errors="coerce"
        )
        df_note = df_note.sort_values("Note Date").reset_index(drop=True)

    return df_inv, df_note


def _parse_tally(path):
    """
    Parse Tally GSTR-3B Voucher Register Excel.
    Rows 0-5 are metadata; actual data starts at row index 6.
    Assigns columns: Date | Particulars | Vch Type | Vch No | Debit Amount | Credit Amount
    """
    xl = pd.ExcelFile(path)
    df = xl.parse(xl.sheet_names[0], header=None)
    df_data = df.iloc[6:].copy()
    df_data.columns = ["Date", "Particulars", "Vch Type", "Vch No",
                       "Debit Amount", "Credit Amount"]
    df_data = df_data.dropna(subset=["Date"]).copy()
    df_data["Date"] = pd.to_datetime(df_data["Date"], errors="coerce")
    df_data["Credit Amount"] = pd.to_numeric(df_data["Credit Amount"], errors="coerce")
    df_data["Debit Amount"]  = pd.to_numeric(df_data["Debit Amount"],  errors="coerce")
    return df_data.reset_index(drop=True)


def reconcile(gstr2b_path, tally_path, output_path=None,
              name_threshold=60.0, date_tolerance_days=3):
    """
    Match GSTR-2B invoices against Tally purchase register.

    Matching (applied together per GSTR-2B row):
      1. Invoice Value == Credit Amount  (±1 rupee rounding tolerance)
      2. Invoice Date  vs Tally Date     (tier 1=exact, 2=within tolerance, 3=any)
      3. Supplier Name fuzzy match       (token_sort_ratio >= name_threshold)

    Best match = lowest date tier, then highest name score.
    Each Tally row is consumed at most once.

    Returns (output_path, summary_dict).
    """
    if not os.path.exists(gstr2b_path):
        raise FileNotFoundError(f"Portal file not found: {gstr2b_path}")
    if not os.path.exists(tally_path):
        raise FileNotFoundError(f"Tally file not found: {tally_path}")

    df_inv, df_note = _parse_gstr2b(gstr2b_path)
    df_tally        = _parse_tally(tally_path)

    used_tally          = set()
    matched_rows        = []
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

            # 1. Amount match (±1 rupee)
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
                tier = 1 if diff == 0 else (2 if diff <= date_tolerance_days else 3)
            else:
                tier = 3

            if tier < best_tier or (tier == best_tier and name_score > best_score):
                best_tier  = tier
                best_score = name_score
                best_idx   = idx_t

        if best_idx is not None:
            used_tally.add(best_idx)
            t_row  = df_tally.loc[best_idx]
            g2b_part   = row_g.add_prefix("g2b_")
            tally_part = t_row.add_prefix("tally_")
            merged = pd.concat([g2b_part, tally_part])
            merged["name_similarity"] = round(best_score, 1)
            merged["date_match_tier"] = best_tier
            matched_rows.append(merged)
        else:
            unmatched_gstr2b_idx.append(idx_g)

    matched_df          = pd.DataFrame(matched_rows) if matched_rows else pd.DataFrame()
    unmatched_gstr2b_df = df_inv.loc[unmatched_gstr2b_idx].copy()
    unmatched_tally_df  = df_tally[~df_tally.index.isin(used_tally)].copy()

    summary = {
        "generated_at":           datetime.now().isoformat(timespec="seconds"),
        "gstr2b_invoices_total":  len(df_inv),
        "gstr2b_credit_notes":    len(df_note),
        "tally_entries_total":    len(df_tally),
        "matched_count":          len(matched_rows),
        "unmatched_gstr2b_count": len(unmatched_gstr2b_df),
        "unmatched_tally_count":  len(unmatched_tally_df),
        "name_threshold":         name_threshold,
        "date_tolerance_days":    date_tolerance_days,
    }

    if output_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"reconciliation_{ts}.xlsx"

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        pd.DataFrame([summary]).to_excel(writer, index=False, sheet_name="Summary")
        unmatched_gstr2b_df.to_excel(writer, index=False, sheet_name="Unmatched_GSTR2B")
        unmatched_tally_df.to_excel(writer, index=False, sheet_name="Unmatched_Tally")
        matched_df.to_excel(writer, index=False, sheet_name="Matched")
        df_note.to_excel(writer, index=False, sheet_name="Credit_Notes")

    tables = {
        "matched":          _df_to_records(matched_df),
        "unmatched_portal": _df_to_records(unmatched_gstr2b_df),
        "unmatched_tally":  _df_to_records(unmatched_tally_df),
        "credit_notes":     _df_to_records(df_note),
    }

    return output_path, summary, tables


def _df_to_records(df):
    """Convert a DataFrame to a JSON-safe list of dicts. Dates → dd/mm/yyyy strings."""
    if df is None or df.empty:
        return []
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%d/%m/%Y").fillna("")
        else:
            out[col] = out[col].fillna("").astype(str).str.strip()
    return out.to_dict(orient="records")
