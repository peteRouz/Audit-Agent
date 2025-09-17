from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


# ----------------------------- utils ---------------------------------
def _norm_str(x) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


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


# ------------------------ main agent ---------------------------------
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
    date_window_days: int = 14  # UI simple slider
    date_window_days_gl: int = 90  # GL payment search window
    date_window_days_bank: int = 90  # MT940 search window

    # runtime
    erp_df: pd.DataFrame | None = None
    mt940_index: List[Dict] | None = None

    def __post_init__(self):
        self._load_erp()
        self._index_mt940()

    # -------------------- Loaders / Indexers --------------------------
    def _load_erp(self):
        # Read Excel with robust engine selection
        try:
            df = pd.read_excel(self.excel_path, sheet_name=self.sheet_name, engine="openpyxl")
        except ImportError:
            raise ImportError("Missing dependency 'openpyxl'. Please add 'openpyxl' to requirements.")
        except Exception as e:
            raise RuntimeError(f"Failed to read ERP Excel: {e}")

        # Normalize expected columns (keep original headers as in your file)
        expected = [
            "Entity", "TT", "TransNo", "Trans dat", "Perioc",
            "Hdl.acc", "Acc(T)", "Acc(I)",
            "Cat1", "Cat2", "Cat3", "Cat4", "Cat5",
            "Ca", "CaV",
            "Ap/Ar I", "Ap/Ar ID(T)",
            "Inv N",
            "Cur", "Cur. amount", "Amount"
        ]
        missing = [c for c in expected if c not in df.columns]
        if missing:
            # don't hard fail; warn in downstream errors
            pass

        # Ensure numeric types
        for col in ["Perioc", "Cur. amount", "Amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "Trans dat" in df.columns:
            df["Trans dat"] = pd.to_datetime(df["Trans dat"], errors="coerce")

        # Filter by allowed TTs and period range
        if "TT" in df.columns:
            df["T"] = df["TT"].astype(str).str.upper()

        allowed = set(x.upper() for x in (self.allowed_tt or []))
        if allowed:
            df = df[df["TT"].isin(allowed) | df["TT"].eq("IO")]  # always keep IO

        if "Perioc" in df.columns:
            df = df[(df["Perioc"] >= int(self.period_min)) & (df["Perioc"] <= int(self.period_max))]

        self.erp_df = df.reset_index(drop=True)

    def _index_mt940(self):
        """Parse MT940-like files (.sta / .mt940 / .txt). Very simple parser."""
        self.mt940_index = []
        if not self.bank_dir or not Path(self.bank_dir).exists():
            return

        suffix = str(self.bank_account_suffix or "").strip()
        files = []
        for ext in ("*.sta", "*.mt940", "*.txt"):
            files += list(Path(self.bank_dir).glob(ext))

        for f in files:
            try:
                text = f.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue

            # split on transactions using :61:
            chunks = re.split(r"(?=:61:)", text)
            for ch in chunks:
                if not ch.strip():
                    continue
                # extract :61: (date/amount) and :86: (ref/desc)
                m61 = re.search(r":61:(.*)", ch)
                m86 = re.search(r":86:(.*)", ch, re.DOTALL)

                raw61 = m61.group(1).strip() if m61 else ""
                raw86 = m86.group(1).strip().replace("\n", " ") if m86 else ""

                # very rough date (YYMMDD at start of :61:)
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

                # amount: look for C/D then amount with comma/decimal
                amt = None
                mam = re.search(r"[CD]([\d,\.]+)", raw61)
                if mam:
                    s = mam.group(1).replace(",", ".")
                    try:
                        amt = float(s)
                    except Exception:
                        pass

                # IBAN/account from :86: if present
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

    # ------------------------ Public helpers ---------------------------
    def index_summary(self) -> Dict:
        if self.erp_df is None:
            return {}
        counts = self.erp_df["T"].value_counts(dropna=False).to_dict()
        return {
            "rows": int(len(self.erp_df)),
            "by_T": counts,
            "period_range": [int(self.period_min), int(self.period_max)],
        }

    # ---------------------- IO utilities -------------------------------
    def _get_io_row(self, io_trans_no: int) -> pd.Series:
        df = self.erp_df
        df_io = df[df["T"].astype(str).str.upper().eq("IO")]
        row = df_io[df_io["TransNo"].astype(str).eq(str(io_trans_no))]
        if row.empty:
            raise ValueError(f"IO {io_trans_no} not found.")
        return row.iloc[0]

    def _get_io_block(self, io_trans_no: int) -> pd.DataFrame:
        df = self.erp_df
        return df[(df["T"].astype(str).str.upper() == "IO") &
                  (df["TransNo"].astype(str) == str(io_trans_no))].copy()

    def _get_ap_account_from_io(self, io_trans_no: int) -> Dict[str, Optional[str]]:
        """Find trade payables account on the IO (prefer description 'trade payables', else largest credit)."""
        block = self._get_io_block(io_trans_no)
        if block.empty:
            return {"code": None, "desc": None}

        # 1) prefer description contains 'trade payables'
        cand = block[_norm_str(block["Acc(I)"]).str.contains("trade payables", na=False)]
        if cand.empty:
            # 2) largest credit by EUR amount (negative)
            cand = block.sort_values("Amount").head(1)

        r = cand.iloc[0]
        code = str(r.get("Acc(T)", "")).strip() or None
        desc = str(r.get("Acc(I)", "")).strip() or None
        return {"code": code, "desc": desc}

    # ---------------------- GL-only matching ---------------------------
    def _find_gl_payment(self, io_row: pd.Series, ap_account: Dict) -> Optional[Dict]:
        df = self.erp_df.copy()

        # Date window around IO date
        io_date = pd.to_datetime(io_row["Trans dat"]).normalize()
        date_min = io_date - pd.Timedelta(days=self.date_window_days_gl)
        date_max = io_date + pd.Timedelta(days=self.date_window_days_gl)

        # Filter GL only and in window
        df = df[df["T"].astype(str).str.upper().eq("GL")]
        df["Trans dat"] = pd.to_datetime(df["Trans dat"], errors="coerce")
        df = df[(df["Trans dat"] >= date_min) & (df["Trans dat"] <= date_max)]

        # Supplier match by ID and/or name
        io_sup_id = _norm_str(io_row.get("Ap/Ar I", ""))
        io_sup_nm = _norm_str(io_row.get("Ap/Ar ID(T)", ""))

        df["_sup_id"] = df["Ap/Ar I"].apply(_norm_str)
        df["_sup_nm"] = df["Ap/Ar ID(T)"].apply(_norm_str)
        df = df[(df["_sup_id"] == io_sup_id) | (df["_sup_nm"] == io_sup_nm)]

        if df.empty:
            return None

        io_amt_eur = abs(_safe_float(io_row.get("Amount", 0.0)))
        io_amt_cur = abs(_safe_float(io_row.get("Cur. amount", 0.0)))
        ap_code = (ap_account.get("code") or "").strip()
        ap_desc_norm = _norm_str(ap_account.get("desc") or "")

        groups = []
        for gl_no, g in df.groupby("TransNo"):
            g = g.copy()

            # identify AP debit lines (same account as IO) in EUR > 0
            is_code = g["Acc(T)"].astype(str).str.strip().eq(ap_code) if ap_code else False
            is_desc = _norm_str(g["Acc(I)"]).str.contains(re.escape(ap_desc_norm), na=False) if ap_desc_norm else False
            ap_mask = is_code | is_desc

            g_ap = g[ap_mask & (g["Amount"] > 0)]
            ap_debit_eur = _safe_float(g_ap["Amount"].sum())
            ap_debit_cur = _safe_float(g_ap["Cur. amount"].sum())

            # bank/clearing heuristic
            bank_like = _norm_str(g["Acc(I)"]).str.contains("bank|iban|clearing|ing|santander|hsbc|unicredit", na=False)

            # score
            score = 0.0
            if _approx_equal(ap_debit_eur, io_amt_eur, 0.05):
                score += 3.5
            if _approx_equal(abs(ap_debit_cur), io_amt_cur, 0.05):
                score += 2.0
            if bank_like.any():
                score += 1.0

            gl_date = pd.to_datetime(g.iloc[0]["Trans dat"]).normalize()
            days = abs((gl_date - io_date).days)
            score += max(0, 1.0 - (days / 90.0))  # up to +1

            groups.append({
                "gl_trans_no": gl_no,
                "score": score,
                "gl_date": gl_date.date(),
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

    # -------------------- MT940 matching (simple) ----------------------
    def _match_mt940(self, gl_best: Optional[Dict], io_row: pd.Series) -> List[Dict]:
        """Match bank entries by amount/date window ±N and presence of supplier/invoice ref."""
        if not self.mt940_index:
            return []

        # prefer GL date, fallback to IO date
        base_date = gl_best["gl_date"] if gl_best else pd.to_datetime(io_row["Trans dat"]).date()
        w = int(self.date_window_days_bank or 10)
        dmin = base_date - timedelta(days=w)
        dmax = base_date + timedelta(days=w)

        # target amount in EUR (from IO or GL AP debit)
        target_amt = abs(_safe_float(io_row.get("Amount", 0.0)))
        if gl_best and _safe_float(gl_best.get("ap_debit_eur", 0.0)) > 0:
            target_amt = abs(_safe_float(gl_best.get("ap_debit_eur", 0.0)))

        inv = _norm_str(io_row.get("Inv N", ""))
        sup = _norm_str(io_row.get("Ap/Ar ID(T)", ""))

        candidates = []
        for row in self.mt940_index:
            dt = row.get("date")
            if not dt or not (dmin <= dt <= dmax):
                continue
            amt = abs(_safe_float(row.get("amount", 0.0)))

            # score
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
                "account": row.get("account"),
                "file": row.get("file"),
                "score": score
            })

        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:3]  # top 3

    # --------------------- Invoice PDF helper --------------------------
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

    # --------------------------- Main API ------------------------------
    def explain_from_io(self, io_trans_no: int) -> Dict:
        io = self._get_io_row(io_trans_no)
        io_block = self._get_io_block(io_trans_no)

        # Values
        sup = str(io.get("Ap/Ar ID(T)", "")).strip()
        inv = str(io.get("Inv N", "")).strip()
        cur = str(io.get("Cur", "")).strip()
        amt_cur = abs(_safe_float(io.get("Cur. amount", 0.0)))
        amt_eur = abs(_safe_float(io.get("Amount", 0.0)))
        post_date = pd.to_datetime(io.get("Trans dat")).date()

        # Expense (debit) line from IO
        io_debits = io_block[io_block["Amount"] > 0]
        if not io_debits.empty:
            exp_line = io_debits.iloc[0]
            exp_acc_code = str(exp_line.get("Acc(T)", "")).strip()
            exp_acc_desc = str(exp_line.get("Acc(I)", "")).strip()
        else:
            exp_acc_code = ""
            exp_acc_desc = ""

        # Trade payables account (credit on IO)
        ap_acc = self._get_ap_account_from_io(io_trans_no)
        ap_code = ap_acc.get("code") or ""
        ap_desc = ap_acc.get("desc") or ""

        # Find GL payment
        gl_best = self._find_gl_payment(io, ap_acc)

        # Build ERP tables
        table_erp: List[Dict] = []
        for _, r in io_block.iterrows():
            table_erp.append({
                "source": "ERP - IO",
                "T": r["T"],
                "trans_no": str(r["TransNo"]),
                "date": str(pd.to_datetime(r["Trans dat"]).date()) if pd.notna(r["Trans dat"]) else "",
                "account": str(r.get("Acc(T)", "")),
                "account_desc": str(r.get("Acc(I)", "")),
                "supplier": str(r.get("Ap/Ar ID(T)", "")),
                "invoice": str(r.get("Inv N", "")),
                "cur": str(r.get("Cur", "")),
                "curr_amount": r.get("Cur. amount", ""),
                "amount_eur": r.get("Amount", ""),
            })

        if gl_best:
            g = gl_best["rows"]
            for _, r in g.iterrows():
                table_erp.append({
                    "source": "ERP - Payment",
                    "T": r["T"],
                    "trans_no": str(r["TransNo"]),
                    "date": str(pd.to_datetime(r["Trans dat"]).date()) if pd.notna(r["Trans dat"]) else "",
                    "account": str(r.get("Acc(T)", "")),
                    "account_desc": str(r.get("Acc(I)", "")),
                    "supplier": str(r.get("Ap/Ar ID(T)", "")),
                    "invoice": str(r.get("Inv N", "")),
                    "cur": str(r.get("Cur", "")),
                    "curr_amount": r.get("Cur. amount", ""),
                    "amount_eur": r.get("Amount", ""),
                })

        # Narrative (English)
        parts = []
        parts.append(
            f"Invoice {inv} from {sup} for {amt_cur:,.2f} {cur} ({amt_eur:,.2f} EUR posted) was recorded on {post_date}."
        )
        if exp_acc_code or exp_acc_desc:
            parts.append(
                f"The IO debits {exp_acc_code} – {exp_acc_desc} and credits {ap_code} – {ap_desc}."
            )
        else:
            parts.append(f"The IO credits {ap_code} – {ap_desc} (trade payables).")

        status = "OK"
        if gl_best:
            parts.append(
                f"Payment confirmed in GL {gl_best['gl_trans_no']} on {gl_best['gl_date']}: "
                f"debit {ap_code} – {ap_desc} {abs(gl_best['ap_debit_eur']):,.2f} EUR and credit bank/clearing. "
            )
        else:
            status = "ERP payment (GL) not found"
            parts.append("No GL transaction found debiting the same trade payables account for the invoice amount.")

        # MT940 matching (optional)
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

        # Invoice PDF
        invoice_pdf = self._find_invoice_pdf(inv)

        return {
            "summary_text": summary_text,
            "Status": status,
            "table_rows_erp": table_erp,
            "table_rows_bank": bank_rows,
            "Invoice_PDF": invoice_pdf,
        }
