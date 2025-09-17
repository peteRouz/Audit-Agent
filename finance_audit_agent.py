from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


# =============================== Utils =================================

def _norm_str(x) -> str:
    """Normalize a single string (for comparisons)."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    s = str(x).replace("\n", " ").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def _norm_series(s: pd.Series) -> pd.Series:
    """Normalize a pandas Series of strings safely."""
    return (
        s.fillna("")
         .astype(str)
         .str.replace("\n", " ", regex=False)
         .str.strip()
         .str.lower()
         .str.replace(r"\s+", " ", regex=True)
    )

def _approx_equal(a: float, b: float, tol: float = 0.02) -> bool:
    try:
        return abs(float(a) - float(b)) <= tol
    except Exception:
        return False

def _safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _to_date(obj, fallback=None):
    """Convert anything to a python date; if invalid/NaT, return fallback."""
    ts = pd.to_datetime(obj, errors="coerce")
    if pd.isna(ts):
        return fallback
    try:
        return ts.date()
    except Exception:
        return fallback


# =========================== Column constants ==========================

# EXACT headers from your sheet (per the last screenshots)
COL_ENTITY      = "Entity"
COL_TT          = "TT"              # transaction type (IO/GL/PB/...)
COL_TRANS_NO    = "TransNo"
COL_TRANS_DATE  = "Trans date"
COL_PERIOD      = "Period"
COL_HD_ACC     = "Hd.acc"
COL_ACC_DESC    = "Acc"             # account description/name
COL_ACC_CODE    = "Acc(T)"          # account code
COL_CAT1        = "Cat1"
COL_CAT2        = "Cat2"
COL_CAT3        = "Cat3"
COL_CAT4        = "Cat4"
COL_CAT5        = "Cat5"
COL_CAT6        = "Cat6"
COL_CAT7        = "Cat7"
COL_SUPPLIER_ID = "Ap/Ar ID"
COL_SUPPLIER_NM = "Ap/Ar ID(T)"
COL_INV_NO      = "Inv No"
COL_TC          = "TC"
COL_TS          = "TS"
COL_CUR         = "Cur"
COL_CUR_AMT     = "Cur. amount"     # amount in trans currency
COL_AMT_EUR     = "Amount"          # amount in EUR / local
COL_TEXT        = "Text"


# ============================== Agent ==================================

@dataclass
class FinanceAuditAgent:
    excel_path: Path
    sheet_name: str
    bank_dir: Path
    invoices_dir: Path
    bank_account_suffix: str
    allowed_tt: List[str]
    period_min: int
    period_max: int
    # Windows
    date_window_days: int = 14          # (kept for UI simple slider)
    date_window_days_gl: int = 90       # GL payment window (± days)
    date_window_days_bank: int = 90     # MT940 window (± days)

    # runtime
    erp_df: pd.DataFrame | None = None
    mt940_index: List[Dict] | None = None

    def __post_init__(self):
        self._load_erp()
        self._index_mt940()

    # --------------------------- Load ERP ------------------------------

    def _load_erp(self):
        try:
            df = pd.read_excel(self.excel_path, sheet_name=self.sheet_name, engine="openpyxl")
        except ImportError:
            raise ImportError("Missing dependency 'openpyxl'. Please add 'openpyxl' to requirements.")
        except Exception as e:
            raise RuntimeError(f"Failed to read ERP Excel: {e}")

        # Normalize headers (strip/newlines/extra spaces)
        def _clean_col(c):
            c = str(c).replace("\n", " ").strip()
            c = re.sub(r"\s+", " ", c)
            # normalize some unicode punctuation to ascii if present
            c = c.replace("’", "'").replace("–", "-").replace("—", "-")
            return c

        df.columns = [_clean_col(c) for c in df.columns]

        expected = [
            COL_ENTITY, COL_TT, COL_TRANS_NO, COL_TRANS_DATE, COL_PERIOD,
            COL_HD_ACC, COL_ACC_DESC, COL_ACC_CODE,
            COL_CAT1, COL_CAT2, COL_CAT3, COL_CAT4, COL_CAT5, COL_CAT6, COL_CAT7,
            COL_SUPPLIER_ID, COL_SUPPLIER_NM,
            COL_INV_NO, COL_TC, COL_TS, COL_CUR, COL_CUR_AMT, COL_AMT_EUR, COL_TEXT
        ]
        missing = [c for c in expected if c not in df.columns]
        if missing:
            raise KeyError(f"Missing expected columns: {missing}. Found: {list(df.columns)}")

        # Types
        for col in [COL_PERIOD, COL_CUR_AMT, COL_AMT_EUR]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df[COL_TRANS_DATE] = pd.to_datetime(df[COL_TRANS_DATE], errors="coerce")
        df[COL_TT] = df[COL_TT].astype(str).str.upper()

        # Filter by TT (keep IO always)
        allowed = set(x.upper() for x in (self.allowed_tt or []))
        if allowed:
            df = df[df[COL_TT].isin(allowed) | df[COL_TT].eq("IO")]

        # Filter by period
        df = df[(df[COL_PERIOD] >= int(self.period_min)) & (df[COL_PERIOD] <= int(self.period_max))]

        self.erp_df = df.reset_index(drop=True)

    # --------------------------- Index MT940 ---------------------------

    def _index_mt940(self):
        """Parse MT940-like files (.sta / .mt940 / .txt). Very simple parser."""
        self.mt940_index = []
        p = Path(self.bank_dir or "")
        if not p.exists():
            return

        files: List[Path] = []
        for ext in ("*.sta", "*.mt940", "*.txt"):
            files += list(p.glob(ext))

        for f in files:
            try:
                text = f.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue

            chunks = re.split(r"(?=:61:)", text)
            for ch in chunks:
                if not ch.strip():
                    continue
                m61 = re.search(r":61:(.*)", ch)
                m86 = re.search(r":86:(.*)", ch, re.DOTALL)

                raw61 = m61.group(1).strip() if m61 else ""
                raw86 = (m86.group(1).strip().replace("\n", " ") if m86 else "")

                # date (YYMMDD)
                tx_date = None
                mdate = re.match(r"(\d{6})", raw61)
                if mdate:
                    yymmdd = mdate.group(1)
                    yy, mm, dd = int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:])
                    year = 2000 + yy if yy < 80 else 1900 + yy
                    try:
                        tx_date = datetime(year, mm, dd).date()
                    except Exception:
                        pass

                # amount
                amt = None
                mam = re.search(r"[CD]([\d,\.]+)", raw61)
                if mam:
                    s = mam.group(1).replace(",", ".")
                    try:
                        amt = float(s)
                    except Exception:
                        pass

                # account from :86:
                acc = None
                miban = re.search(r"[A-Z]{2}\d{2}[A-Z0-9]{10,}", raw86)
                if miban:
                    acc = miban.group(0)
                elif self.bank_account_suffix and self.bank_account_suffix in raw86:
                    acc = f"...{self.bank_account_suffix}"

                self.mt940_index.append({
                    "file": str(f.name),
                    "date": tx_date,
                    "amount": amt,
                    "ref": raw86[:500],
                    "account": acc
                })

    # ----------------------------- Debug -------------------------------

    def index_summary(self) -> Dict:
        if self.erp_df is None:
            return {}
        counts = self.erp_df[COL_TT].value_counts(dropna=False).to_dict()
        return {
            "rows": int(len(self.erp_df)),
            "by_TT": counts,
            "period_range": [int(self.period_min), int(self.period_max)],
        }

    # ------------------------ IO helpers -------------------------------

    def _get_io_row(self, io_trans_no: int) -> pd.Series:
        df_io = self.erp_df[self.erp_df[COL_TT].eq("IO")]
        row = df_io[df_io[COL_TRANS_NO].astype(str).eq(str(io_trans_no))]
        if row.empty:
            raise ValueError(f"IO {io_trans_no} not found.")
        return row.iloc[0]

    def _get_io_block(self, io_trans_no: int) -> pd.DataFrame:
        return self.erp_df[(self.erp_df[COL_TT].eq("IO")) &
                           (self.erp_df[COL_TRANS_NO].astype(str).eq(str(io_trans_no)))].copy()

    def _get_ap_account_from_io(self, io_trans_no: int) -> Dict[str, Optional[str]]:
        """
        Trade payables on IO: prefer row whose description mentions 'trade payables';
        fallback to the largest EUR credit (Amount < 0).
        """
        block = self._get_io_block(io_trans_no)
        if block.empty:
            return {"code": None, "desc": None}

        desc_ser = _norm_series(block[COL_ACC_DESC]) if COL_ACC_DESC in block.columns else pd.Series("", index=block.index)
        text_ser = _norm_series(block[COL_TEXT]) if COL_TEXT in block.columns else pd.Series("", index=block.index)
        has_tp = desc_ser.str.contains("trade payables", na=False) | text_ser.str.contains("trade payables", na=False)

        cand = block[has_tp]
        if cand.empty:
            cand = block.sort_values(COL_AMT_EUR).head(1)  # most negative (credit)

        r = cand.iloc[0]
        code = str(r.get(COL_ACC_CODE, "")).strip() or None
        desc = str(r.get(COL_ACC_DESC, "")).strip() or str(r.get(COL_TEXT, "")).strip() or None
        return {"code": code, "desc": desc}

    # ------------------------- GL-only match ---------------------------

    def _find_gl_payment(self, io_row: pd.Series, ap_account: Dict) -> Optional[Dict]:
        df = self.erp_df.copy()

        io_date = _to_date(io_row.get(COL_TRANS_DATE), fallback=datetime.today().date())
        date_min = io_date - pd.Timedelta(days=self.date_window_days_gl)
        date_max = io_date + pd.Timedelta(days=self.date_window_days_gl)

        # GL only in window
        df = df[(df[COL_TT] == "GL")]
        df_dates = pd.to_datetime(df[COL_TRANS_DATE], errors="coerce")
        df = df[(df_dates >= date_min) & (df_dates <= date_max)]

        # supplier match
        io_sup_id = _norm_str(io_row.get(COL_SUPPLIER_ID, ""))
        io_sup_nm = _norm_str(io_row.get(COL_SUPPLIER_NM, ""))

        df["_sup_id"] = _norm_series(df[COL_SUPPLIER_ID])
        df["_sup_nm"] = _norm_series(df[COL_SUPPLIER_NM])
        df = df[(df["_sup_id"] == io_sup_id) | (df["_sup_nm"] == io_sup_nm)]

        if df.empty:
            return None

        io_amt_eur = abs(_safe_float(io_row.get(COL_AMT_EUR, 0.0)))
        io_amt_cur = abs(_safe_float(io_row.get(COL_CUR_AMT, 0.0)))
        ap_code = (ap_account.get("code") or "").strip()
        ap_desc_norm = _norm_str(ap_account.get("desc") or "")

        groups = []
        for gl_no, g in df.groupby(COL_TRANS_NO):
            g = g.copy()

            is_code = g[COL_ACC_CODE].astype(str).str.strip().eq(ap_code) if ap_code else False
            is_desc = _norm_series(g[COL_ACC_DESC]).str.contains(re.escape(ap_desc_norm), na=False) if ap_desc_norm else False
            ap_mask = is_code | is_desc

            # debit on AP (EUR > 0)
            g_ap = g[ap_mask & (g[COL_AMT_EUR] > 0)]
            ap_debit_eur = _safe_float(g_ap[COL_AMT_EUR].sum())
            ap_debit_cur = _safe_float(g_ap[COL_CUR_AMT].sum())

            bank_like = _norm_series(g[COL_ACC_DESC]).str.contains(
                "bank|iban|clearing|ing|santander|hsbc|unicredit", na=False
            )
            if COL_TEXT in g.columns:
                bank_like = bank_like | _norm_series(g[COL_TEXT]).str.contains(
                    "bank|iban|clearing|ing|santander|hsbc|unicredit", na=False
                )

            score = 0.0
            if _approx_equal(ap_debit_eur, io_amt_eur, 0.05):
                score += 3.5
            if _approx_equal(abs(ap_debit_cur), io_amt_cur, 0.05):
                score += 2.0
            if bank_like.any():
                score += 1.0

            gl_date = _to_date(g.iloc[0].get(COL_TRANS_DATE), fallback=io_date)
            days = abs((gl_date - io_date).days)
            score += max(0, 1.0 - (days / 90.0))

            groups.append({
                "gl_trans_no": gl_no,
                "score": score,
                "gl_date": gl_date,
                "ap_debit_eur": ap_debit_eur,
                "ap_debit_cur": ap_debit_cur,
                "rows": g,
                "has_bank_line": bool(bank_like.any()),
            })

        if not groups:
            return None

        best = max(groups, key=lambda x: x["score"])
        if best["ap_debit_eur"] <= 0 and best["ap_debit_cur"] == 0:
            return None
        return best

    # --------------------------- MT940 match ---------------------------

    def _match_mt940(self, gl_best: Optional[Dict], io_row: pd.Series) -> List[Dict]:
        if not self.mt940_index:
            return []

        base_date = (gl_best.get("gl_date") if (gl_best and gl_best.get("gl_date"))
                     else _to_date(io_row.get(COL_TRANS_DATE), fallback=datetime.today().date()))
        w = int(self.date_window_days_bank or 10)
        dmin = base_date - timedelta(days=w)
        dmax = base_date + timedelta(days=w)

        target_amt = abs(_safe_float(io_row.get(COL_AMT_EUR, 0.0)))
        if gl_best and _safe_float(gl_best.get("ap_debit_eur", 0.0)) > 0:
            target_amt = abs(_safe_float(gl_best.get("ap_debit_eur", 0.0)))

        inv = _norm_str(io_row.get(COL_INV_NO, ""))
        sup = _norm_str(io_row.get(COL_SUPPLIER_NM, ""))

        candidates = []
        for row in self.mt940_index:
            dt = row.get("date")
            if not dt or not (dmin <= dt <= dmax):
                continue
            amt = abs(_safe_float(row.get("amount", 0.0)))

            score = 0.0
            if _approx_equal(amt, target_amt, 0.05):
                score += 3.5
            ref = _norm_str(row.get("ref", ""))
            if inv and inv in ref:
                score += 1.0
            if sup and any(w in ref for w in sup.split()):
                score += 0.5
            if self.bank_account_suffix and str(self.bank_account_suffix) in (row.get("account") or ""):
                score += 0.5

            candidates.append({
                "date": row.get("date"),
                "amount": row.get("amount"),
                "reference": row.get("ref"),
                "file": row.get("file"),
                "account": row.get("account"),
                "score": score
            })

        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:3]

    # ------------------------ Invoice PDF ------------------------------

    def _find_invoice_pdf(self, invoice_no: str | int) -> Optional[str]:
        if not invoice_no:
            return None
        inv = str(invoice_no).strip()
        p = Path(self.invoices_dir or ".")
        for name in (f"{inv}.pdf", f"INV_{inv}.pdf", f"Invoice_{inv}.pdf"):
            f = p / name
            if f.exists():
                return str(f)
        return None

    # ---------------------------- Public API ---------------------------

    def explain_from_io(self, io_trans_no: int) -> Dict:
        io = self._get_io_row(io_trans_no)
        io_block = self._get_io_block(io_trans_no)

        sup = str(io.get(COL_SUPPLIER_NM, "")).strip()
        inv = str(io.get(COL_INV_NO, "")).strip()
        cur = str(io.get(COL_CUR, "")).strip()
        amt_cur = abs(_safe_float(io.get(COL_CUR_AMT, 0.0)))
        amt_eur = abs(_safe_float(io.get(COL_AMT_EUR, 0.0)))
        post_date = _to_date(io.get(COL_TRANS_DATE), fallback=None)

        # expense (debit) line on IO
        io_debits = io_block[io_block[COL_AMT_EUR] > 0]
        if not io_debits.empty:
            exp_line = io_debits.iloc[0]
            exp_acc_code = str(exp_line.get(COL_ACC_CODE, "")).strip()
            exp_acc_desc = str(exp_line.get(COL_ACC_DESC, "")).strip() or str(exp_line.get(COL_TEXT, "")).strip()
        else:
            exp_acc_code = ""
            exp_acc_desc = ""

        # trade payables (credit on IO)
        ap_acc = self._get_ap_account_from_io(io_trans_no)
        ap_code = ap_acc.get("code") or ""
        ap_desc = ap_acc.get("desc") or ""

        # find GL
        gl_best = self._find_gl_payment(io, ap_acc)

        # ERP tables
        table_erp: List[Dict] = []
        for _, r in io_block.iterrows():
            table_erp.append({
                "source": "ERP - IO",
                "TT": r[COL_TT],
                "trans_no": str(r[COL_TRANS_NO]),
                "date": str(_to_date(r[COL_TRANS_DATE], fallback="")),
                "account": str(r.get(COL_ACC_CODE, "")),
                "account_desc": str(r.get(COL_ACC_DESC, "")),
                "supplier": str(r.get(COL_SUPPLIER_NM, "")),
                "invoice": str(r.get(COL_INV_NO, "")),
                "cur": str(r.get(COL_CUR, "")),
                "curr_amount": r.get(COL_CUR_AMT, ""),
                "amount_eur": r.get(COL_AMT_EUR, ""),
            })

        if gl_best:
            g = gl_best["rows"]
            for _, r in g.iterrows():
                table_erp.append({
                    "source": "ERP - Payment",
                    "TT": r[COL_TT],
                    "trans_no": str(r[COL_TRANS_NO]),
                    "date": str(_to_date(r[COL_TRANS_DATE], fallback="")),
                    "account": str(r.get(COL_ACC_CODE, "")),
                    "account_desc": str(r.get(COL_ACC_DESC, "")),
                    "supplier": str(r.get(COL_SUPPLIER_NM, "")),
                    "invoice": str(r.get(COL_INV_NO, "")),
                    "cur": str(r.get(COL_CUR, "")),
                    "curr_amount": r.get(COL_CUR_AMT, ""),
                    "amount_eur": r.get(COL_AMT_EUR, ""),
                })

        # Narrative (EN)
        parts = []
        if post_date:
            parts.append(
                f"Invoice {inv} from {sup} for {amt_cur:,.2f} {cur} ({amt_eur:,.2f} EUR posted) was recorded on {post_date}."
            )
        else:
            parts.append(
                f"Invoice {inv} from {sup} for {amt_cur:,.2f} {cur} ({amt_eur:,.2f} EUR posted)."
            )

        if exp_acc_code or exp_acc_desc:
            parts.append(f"The IO debits {exp_acc_code} – {exp_acc_desc} and credits {ap_code} – {ap_desc}.")
        else:
            parts.append(f"The IO credits {ap_code} – {ap_desc} (trade payables).")

        status = "OK"
        if gl_best:
            parts.append(
                f"Payment confirmed in GL {gl_best['gl_trans_no']} on {gl_best['gl_date']}: "
                f"debit {ap_code} – {ap_desc} {abs(gl_best['ap_debit_eur']):,.2f} EUR and credit bank/clearing."
            )
        else:
            status = "ERP payment (GL) not found"
            parts.append("No GL transaction found debiting the same trade payables account for the invoice amount.")

        # MT940 (best candidate)
        bank_rows = []
        mt_candidates = self._match_mt940(gl_best, io)
        if mt_candidates:
            best = mt_candidates[0]
            bank_rows.append({
                "date": str(best["date"]),
                "amount": best["amount"],
                "reference": best["reference"],
                "file": best["file"],
                "account": best.get("account") or f"...{self.bank_account_suffix}",
                "score": round(best["score"], 2),
            })
            parts.append(
                f"Bank statement {best['file']} on {best['date']} shows {abs(_safe_float(best['amount'])):,.2f} EUR "
                f"(ref: {best['reference'][:80]}…)."
            )

        summary_text = " ".join(parts)
        invoice_pdf = self._find_invoice_pdf(inv)

        return {
            "summary_text": summary_text,
            "Status": status,
            "table_rows_erp": table_erp,
            "table_rows_bank": bank_rows,
            "Invoice_PDF": invoice_pdf,
        }
