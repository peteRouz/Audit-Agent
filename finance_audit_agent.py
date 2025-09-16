import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Iterable

import numpy as np
import pandas as pd


class FinanceAuditAgent:
    """
    Finance Audit Agent
    Liga ERP (JE) -> (opcional) Faturas PDF -> Banco (MT940).

    Principais:
      - explain_trans(trans_no)
      - explain_from_io(io_trans_no): IO -> (GL/PB) -> Banco + PDF
    """

    def __init__(
        self,
        excel_path: Path,
        sheet_name: str = "VT - JE Template (JPF)",
        bank_dir: Path = Path("."),
        invoices_dir: Path = Path("."),
        bank_account_suffix: str = "1478",
        allowed_tt: Optional[List[str]] = None,
        period_min: int = 202501,
        period_max: int = 202512,
        date_window_days: int = 14,
    ) -> None:
        self.excel_path = Path(excel_path)
        self.sheet_name = sheet_name
        self.bank_dir = Path(bank_dir)
        self.invoices_dir = Path(invoices_dir)
        self.bank_account_suffix = bank_account_suffix
        self.allowed_tt = set(allowed_tt or ["IO", "PB", "GL"])
        self.period_min = int(period_min)
        self.period_max = int(period_max)
        self.date_window_days = int(date_window_days)

        # Índices carregados
        self.je_index = self._load_je_index()    # agregado por trans_no (para acesso rápido)
        self.bank_df = self._load_bank_entries() # movimentos MT940 parseados
        self.bank_by_amount = (
            self.bank_df.groupby("abs_amount_r2") if not self.bank_df.empty else None
        )
        self._mt940_cache: Optional[List[dict]] = None  # cache parser MT940

    # ---------------------------------------------------------------------
    # Loader do ERP
    # ---------------------------------------------------------------------
    def _load_je_index(self) -> pd.DataFrame:
        df = pd.read_excel(self.excel_path, sheet_name=self.sheet_name)

        # Supplier e conta contábil (cabecalhos possíveis)
        supplier_candidates = ["Ap/Ar ID(T)", "Ap/Ar ID", "Supplier", "Supplier name", "Name"]
        supplier_col = next((c for c in supplier_candidates if c in df.columns), None)

        # Conta contábil (o teu ficheiro mostra "Hd.a")
        account_candidates = ["Hd.a", "Account", "GL account", "G/L Account"]
        account_col = next((c for c in account_candidates if c in df.columns), None)

        cols_map = {
            "Entity": "entity",
            "TT": "tt",
            "TransNo": "trans_no",
            "Trans date": "trans_date",
            "Period": "period",
            "Inv No": "invoice_no",
            "Cur": "currency",
            "Cur. amount": "amount_cur",
            "Amount": "amount_eur",
            "Text": "text",
        }
        if supplier_col:
            cols_map[supplier_col] = "supplier"
        if account_col:
            cols_map[account_col] = "gl_account"

        # cria colunas em falta para não rebentar
        for k in cols_map.keys():
            if k not in df.columns:
                df[k] = None

        df = df[list(cols_map.keys())].rename(columns=cols_map)

        # Normalizações
        df["period"] = pd.to_numeric(df["period"], errors="coerce")
        df = df[(df["period"] >= self.period_min) & (df["period"] <= self.period_max)]
        df["trans_date"] = pd.to_datetime(df["trans_date"], errors="coerce").dt.date
        if "supplier" not in df.columns:
            df["supplier"] = None
        if "gl_account" not in df.columns:
            df["gl_account"] = None

        # Guarda CÓPIA CRUA (linha a linha) para análises
        self.je_lines = df.copy()

        # Índice agregado apenas para TT permitidos
        df_idx = df[df["tt"].isin(self.allowed_tt)].copy()
        agg = {
            "entity": "first",
            "tt": lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0],
            "trans_date": "min",
            "period": "first",
            "invoice_no": lambda s: s.dropna().astype(str).iloc[0] if s.notna().any() else None,
            "currency": lambda s: s.dropna().iloc[0] if s.notna().any() else None,
            "amount_eur": lambda s: s.abs().max(),  # melhor proxy para IO multi-linha
            "text": lambda s: s.dropna().iloc[0] if s.notna().any() else None,
            "supplier": lambda s: s.dropna().iloc[0] if s.notna().any() else None,
            "gl_account": lambda s: s.dropna().astype(str).iloc[0] if s.notna().any() else None,
        }
        je_index = df_idx.groupby("trans_no", as_index=False).agg(agg).sort_values("trans_no")
        return je_index

    # ---------------------------------------------------------------------
    # Parser & loader de MT940
    # ---------------------------------------------------------------------
    @staticmethod
    def _parse_mt940_file(path: Path) -> list:
        """Parser simples :61: e :86:"""
        entries: List[dict] = []
        account: Optional[str] = None
        current: Optional[dict] = None

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        def flush():
            nonlocal current
            if current is not None:
                if current.get("description"):
                    current["description"] = re.sub(r"\s+", " ", current["description"]).strip()
                entries.append(current)
                current = None

        for raw in lines:
            line = raw.rstrip("\n")
            if line.startswith(":25:"):
                account = line[4:].strip()
            elif line.startswith(":61:"):
                flush()
                body = line[4:].strip()
                m_date = re.match(r"(\d{6})", body)
                date_obj = None
                if m_date:
                    yy, mm, dd = int(body[0:2]), int(body[2:4]), int(body[4:6])
                    year = 2000 + yy if yy < 50 else 1900 + yy
                    try:
                        date_obj = datetime(year, mm, dd).date()
                    except ValueError:
                        date_obj = None
                m_dc = re.search(r"[DC]", body)
                dc = m_dc.group(0) if m_dc else None
                m_amt = re.search(r"([0-9]+,[0-9]{2})", body)
                amount = None
                if m_amt:
                    amt_str = m_amt.group(1).replace(".", "").replace(",", ".")
                    amount = float(amt_str)
                    if dc == "D":
                        amount = -amount
                current = {
                    "account": account,
                    "date": date_obj,
                    "amount": amount,
                    "dc": dc,
                    "description": "",
                    "file": path.name,
                }
            elif line.startswith(":86:"):
                if current is None:
                    continue
                desc = line[4:].strip()
                current["description"] = (current.get("description", "") + " " + desc).strip()
            else:
                if current is not None and line.strip():
                    current["description"] += " " + line.strip()
        flush()
        return entries

    def _load_bank_entries(self) -> pd.DataFrame:
        files = [p for p in self.bank_dir.glob("**/*") if p.suffix.lower() in [".sta", ".mt940", ".txt"]]
        all_entries: List[dict] = []
        for p in files:
            try:
                all_entries.extend(self._parse_mt940_file(p))
            except Exception as e:
                print(f"[WARN] Falha a parsear {p.name}: {e}")

        dfb = pd.DataFrame(all_entries)
        if dfb.empty:
            return dfb

        # filtro por sufixo da conta
        def ends_with_suffix(acc: Any, suffix: str) -> bool:
            if not isinstance(acc, str):
                acc = str(acc)
            m = re.search(r"(\d{4})(?!.*\d)", acc or "")
            if m:
                return m.group(1) == suffix
            return acc.replace(" ", "").endswith(suffix)

        if self.bank_account_suffix:
            dfb = dfb[dfb["account"].apply(lambda a: ends_with_suffix(a, self.bank_account_suffix))]

        dfb["abs_amount_r2"] = dfb["amount"].abs().round(2)
        dfb["date"] = pd.to_datetime(dfb["date"], errors="coerce").dt.date
        return dfb

    # ---------------------------------------------------------------------
    # Utilitários & invoices
    # ---------------------------------------------------------------------
    @staticmethod
    def _supplier_hint_from_text(text: str) -> str:
        if not isinstance(text, str):
            return ""
        tokens = re.findall(r"[A-Za-z]{4,}", text)
        return tokens[0] if tokens else ""

    def _find_invoice_pdf(self, invoice_no: Optional[str]) -> Optional[str]:
        inv = (invoice_no or "").strip()
        if not inv:
            return None
        path = self.invoices_dir / f"{inv}.pdf"
        return str(path) if path.exists() else None

    # ---------------------------------------------------------------------
    # IO → GL/PB (ERP)
    # ---------------------------------------------------------------------
    def _find_payment_for_io(
        self,
        io_row: pd.Series,
        max_days: int = 60,
        tt_ok: Iterable[str] = ("GL", "PB"),
    ) -> Optional[Dict[str, Any]]:
        """
        Matching do pagamento no ERP a partir de LINHAS CRUAS:
          - filtra TT (GL/PB) na janela de datas
          - filtro por invoice/supplier quando possível
          - agrega por trans_no e aceita se:
              sum_match ≈ IO  OU  max_abs ≈ IO
        """
        io_amt = float(io_row["amount_eur"])
        io_date = pd.to_datetime(io_row["trans_date"]).date() if pd.notna(io_row["trans_date"]) else None
        io_inv = str(io_row.get("invoice_no") or "").strip()
        io_sup = (str(io_row.get("supplier") or "").strip()).lower()
        if io_date is None:
            return None

        lines = self.je_lines.copy()
        lines["trans_date"] = pd.to_datetime(lines["trans_date"], errors="coerce").dt.date
        lines["invoice_norm"] = lines["invoice_no"].fillna("").astype(str).str.strip()
        lines["supplier_norm"] = lines.get("supplier", pd.Series([None]*len(lines))).fillna("").astype(str).str.lower().str.strip()

        dt_min = io_date - timedelta(days=max_days)
        dt_max = io_date + timedelta(days=max_days)
        pay = lines[(lines["tt"].isin(set(tt_ok))) & (lines["trans_date"].between(dt_min, dt_max))].copy()
        if pay.empty:
            return None

        by_ref = pay[
            ((pay["invoice_norm"] == io_inv) & (io_inv != "")) |
            ((pay["supplier_norm"] == io_sup) & (io_sup != ""))
        ].copy()

        def _agg(df_):
            return pd.Series({
                "trans_date": df_["trans_date"].min(),
                "sum_match": df_["amount_eur"].sum(),
                "max_abs": df_["amount_eur"].abs().max(),
                "tt": df_["tt"].iloc[0],
            })

        grouped = (by_ref if not by_ref.empty else pay).groupby("trans_no").apply(_agg).reset_index()
        is_ok = (
            np.isclose(grouped["sum_match"].abs(), abs(io_amt), atol=0.1) |
            np.isclose(grouped["max_abs"].abs(), abs(io_amt), atol=0.1)
        )
        grouped = grouped[is_ok]
        if grouped.empty:
            return None

        grouped["date_delta"] = (pd.to_datetime(grouped["trans_date"]) - pd.to_datetime(io_date)).abs()
        best = grouped.sort_values("date_delta").iloc[0].to_dict()

        return {
            "trans_no": int(best["trans_no"]),
            "trans_date": pd.to_datetime(best["trans_date"]).date().isoformat(),
            "amount_eur": float(best["sum_match"]) if np.isclose(abs(best["sum_match"]), abs(io_amt), atol=0.1) else float(best["max_abs"]),
            "tt": best["tt"],
            "invoice_no": io_inv or None,
            "supplier": io_row.get("supplier"),
        }

    # ---------------------------------------------------------------------
    # Banco (MT940)
    # ---------------------------------------------------------------------
    def _parse_mt940_dir(self) -> List[dict]:
        if self._mt940_cache is not None:
            return self._mt940_cache
        entries: List[dict] = []
        for f in sorted(self.bank_dir.glob("*")):
            if f.suffix.lower() not in {".sta", ".mt940", ".txt"}:
                continue
            try:
                entries.extend(self._parse_mt940_file(f))
            except Exception as e:
                print(f"[WARN] Falha a parsear {f.name}: {e}")
        for e in entries:
            if isinstance(e.get("date"), datetime):
                e["date"] = e["date"].date().isoformat()
            elif hasattr(e.get("date"), "isoformat"):
                e["date"] = e["date"].isoformat()
            else:
                e["date"] = str(e.get("date")) if e.get("date") else None
        self._mt940_cache = entries
        return entries

    def _find_payment_in_mt940(
        self,
        io_row: pd.Series,
        gl_row: Optional[Dict[str, Any]] = None,
        max_days: int = 10,
    ) -> Optional[Dict[str, Any]]:
        io_amt = float(io_row["amount_eur"])
        io_inv = str(io_row.get("invoice_no") or "").strip()
        io_sup = (str(io_row.get("supplier") or "").strip()).lower()

        ref_date = None
        if gl_row and gl_row.get("trans_date"):
            ref_date = datetime.fromisoformat(gl_row["trans_date"]).date()
        else:
            ref_date = pd.to_datetime(io_row["trans_date"]).date() if pd.notna(io_row["trans_date"]) else None
        if ref_date is None:
            return None

        entries = self._parse_mt940_dir()
        dt_min = ref_date - timedelta(days=max_days)
        dt_max = ref_date + timedelta(days=max_days)

        best = None
        best_score = -1
        for e in entries:
            try:
                d = datetime.fromisoformat(e["date"]).date()
            except Exception:
                continue
            if not (dt_min <= d <= dt_max):
                continue

            score = 0
            if np.isclose(abs(e.get("amount", 0.0)), abs(io_amt), atol=0.05):
                score += 2
            text = (e.get("description") or "").lower()
            if io_inv and io_inv.lower() in text:
                score += 3
            sup_key = io_sup.replace(" ", "")
            if io_sup and (io_sup[:14] in text or sup_key[:14] in text.replace(" ", "")):
                score += 1

            if score > best_score:
                best, best_score = e, score

        if best and best_score >= 2:
            return {
                "date": best["date"],
                "amount": best.get("amount"),
                "reference": (best.get("description") or "")[:150],
                "mt940_file": best.get("file"),
                "account": best.get("account"),
            }
        return None

    # ---------------------------------------------------------------------
    # Públicos
    # ---------------------------------------------------------------------
    def _io_lines_details(self, io_trans_no: int) -> Dict[str, Any]:
        """Extrai detalhes ricos da IO a partir das linhas cruas (contas, descrições, moeda/valor, IVA RC)."""
        rows = self.je_lines[(self.je_lines["trans_no"] == io_trans_no) & (self.je_lines["tt"] == "IO")].copy()
        if rows.empty:
            return {}

        # principais campos
        supplier = rows["supplier"].dropna().astype(str).iloc[0] if rows["supplier"].notna().any() else None
        currency = rows["currency"].dropna().astype(str).iloc[0] if rows["currency"].notna().any() else None

        # linha de débito principal (maior valor positivo)
        rows["amount_eur_num"] = pd.to_numeric(rows["amount_eur"], errors="coerce")
        debit_rows = rows[rows["amount_eur_num"] > 0].copy()
        credit_rows = rows[rows["amount_eur_num"] < 0].copy()

        main_debit = None
        if not debit_rows.empty:
            main_debit = debit_rows.sort_values("amount_eur_num", ascending=False).iloc[0]
        vat_flag = rows["text"].fillna("").str.lower().str.contains("vat").any() or \
                   rows["text"].fillna("").str.lower().str.contains("reverse").any()

        payables_credit = None
        if not credit_rows.empty:
            # tenta apanhar a linha de "Trade payables", senão pega a maior em valor absoluto
            cand = credit_rows.copy()
            mask_tp = cand["text"].fillna("").str.lower().str.contains("payable")
            if mask_tp.any():
                payables_credit = cand[mask_tp].sort_values("amount_eur_num").iloc[0]
            else:
                payables_credit = cand.sort_values("amount_eur_num").iloc[0]

        return {
            "supplier": supplier,
            "currency": currency,
            "main_debit_account": str(main_debit["gl_account"]) if main_debit is not None else None,
            "main_debit_desc": str(main_debit["text"]) if main_debit is not None else None,
            "main_debit_amount_cur": float(main_debit["amount_cur"]) if (main_debit is not None and pd.notna(main_debit["amount_cur"])) else None,
            "main_debit_amount_eur": float(main_debit["amount_eur"]) if (main_debit is not None and pd.notna(main_debit["amount_eur"])) else None,
            "vat_reverse_charge": bool(vat_flag),
            "payables_credit_amount_eur": float(payables_credit["amount_eur"]) if payables_credit is not None else None,
        }

    def explain_trans(self, trans_no: int) -> Dict[str, Any]:
        """Mantido (modo simples)."""
        recs = self.je_index[self.je_index["trans_no"] == trans_no]
        if recs.empty:
            return {"error": f"TransNo {trans_no} não encontrado (ou fora de TT {sorted(self.allowed_tt)})."}
        row = recs.iloc[0]

        amount = float(row["amount_eur"]) if pd.notna(row["amount_eur"]) else None
        je_date = row["trans_date"]
        invoice_no = str(row["invoice_no"]).strip() if pd.notna(row["invoice_no"]) else ""
        text = row["text"] or ""
        supplier = row.get("supplier")

        response: Dict[str, Any] = {
            "JE": {
                "trans_no": int(row["trans_no"]),
                "entity": row["entity"],
                "tt": row["tt"],
                "trans_date": je_date.isoformat() if je_date else None,
                "period": int(row["period"]) if pd.notna(row["period"]) else None,
                "invoice_no": invoice_no or None,
                "currency": row["currency"],
                "amount_eur": amount,
                "text": text,
                "supplier": supplier,
            },
            "Invoice": None,
            "Payment": None,
            "Sources": {"erp_excel": self.excel_path.name, "mt940_files": []},
            "Status": "FALTA_PAGAMENTO",
        }

        pdf = self._find_invoice_pdf(invoice_no if invoice_no else None)
        if pdf:
            response["Invoice"] = {"file": Path(pdf).name}
        return response

    def explain_from_io(self, io_trans_no: int) -> Dict[str, Any]:
        """
        IO -> (GL/PB) -> Banco (MT940) + PDF + resumo audit-ready
        """
        io_recs = self.je_index[(self.je_index["trans_no"] == io_trans_no) & (self.je_index["tt"] == "IO")]
        if io_recs.empty:
            return {"error": f"TransNo {io_trans_no} não é IO ou não existe."}
        io = io_recs.iloc[0]

        io_payload = {
            "trans_no": int(io["trans_no"]),
            "trans_date": pd.to_datetime(io["trans_date"]).date().isoformat() if pd.notna(io["trans_date"]) else None,
            "invoice_no": (str(io.get("invoice_no") or "").strip() or None),
            "amount_eur": float(io["amount_eur"]),
            "text": str(io.get("text") or "").strip() or None,
            "supplier": str(io.get("supplier") or "").strip() or None,
        }

        # Detalhes ricos da IO (contas/descrição/moeda/IVA)
        io_details = self._io_lines_details(io_trans_no)

        # ERP pagamento
        glpb = self._find_payment_for_io(io, max_days=60, tt_ok=("GL", "PB"))
        # Banco
        bank = self._find_payment_in_mt940(io, glpb, max_days=10)
        # PDF
        pdf = self._find_invoice_pdf(io_payload["invoice_no"])

        status = "OK" if (glpb and bank) else ("PARCIAL" if (glpb or bank) else "FALTA_PAGAMENTO")

        # Narrativa audit-ready
        parts = []
        sup_txt = io_details.get("supplier") or io_payload.get("supplier") or "(s/ fornecedor)"
        parts.append(
            f"A fatura {io_payload.get('invoice_no') or '(s/ nº)'} do fornecedor {sup_txt} "
            f"no montante de {io_payload['amount_eur']:.2f} EUR foi registada a {io_payload['trans_date']}."
        )

        # adicionar breakdown de IO
        md_acc = io_details.get("main_debit_account")
        md_desc = io_details.get("main_debit_desc")
        md_cur_val = io_details.get("main_debit_amount_cur")
        currency = io_details.get("currency") or ""
        if md_acc or md_desc or md_cur_val:
            seg = "A IO debita"
            if md_acc:
                seg += f" a conta {md_acc}"
            if md_desc:
                seg += f" ({md_desc})"
            if md_cur_val is not None:
                seg += f" no valor de {md_cur_val:.2f} {currency}"
            parts.append(seg + ".")
        if io_details.get("vat_reverse_charge"):
            parts.append("O lançamento apresenta IVA (reverse charge) nas linhas de suporte.")
        if io_details.get("payables_credit_amount_eur") is not None:
            parts.append("O fornecedor é creditado pelo valor respetivo em 'Trade payables'.")

        # pagamento ERP
        if glpb:
            parts.append(
                f"Pagamento ERP identificado ({glpb['tt']} {glpb['trans_no']}) em {glpb['trans_date']} "
                f"no montante de {glpb['amount_eur']:.2f} EUR."
            )
        else:
            parts.append("Pagamento ERP não encontrado (GL/PB).")

        # banco
        if bank:
            parts.append(
                f"Confirmação no extrato ({bank.get('mt940_file')}), data {bank.get('date')}, "
                f"ref «{(bank.get('reference') or '')[:50]}»."
            )
        else:
            parts.append("Movimento correspondente não confirmado no MT940 (±10 dias).")

        # Tabelas separadas (ERP e Banco) + trans_no sem separadores
        erp_rows = [
            {"Fonte": "ERP - IO",
             "trans_no": str(io_payload["trans_no"]),
             "trans_date": io_payload["trans_date"],
             "invoice_no": io_payload["invoice_no"],
             "amount_eur": io_payload["amount_eur"],
             "supplier": sup_txt}
        ]
        if glpb:
            erp_rows.append({
                "Fonte": "ERP - Pagamento",
                "trans_no": str(glpb["trans_no"]),
                "trans_date": glpb["trans_date"],
                "invoice_no": glpb.get("invoice_no"),
                "amount_eur": glpb["amount_eur"],
                "supplier": sup_txt,
                "tt": glpb["tt"],
            })
        bank_rows = [bank] if bank else []

        return {
            "summary_text": " ".join(parts),
            "Status": status,
            "IO": io_payload,
            "IO_details": io_details,
            "GL_or_PB": glpb,
            "Payment": bank,
            "Invoice_PDF": pdf,
            "Sources": {
                "erp_excel": str(self.excel_path.name),
                "mt940_files": sorted([p.name for p in self.bank_dir.glob('*') if p.is_file()]),
            },
            "table_rows_erp": erp_rows,
            "table_rows_bank": bank_rows,
        }

    # ---------------------------------------------------------------------
    # Debug
    # ---------------------------------------------------------------------
    def index_summary(self) -> Dict[str, Any]:
        by_tt = self.je_index["tt"].value_counts().to_dict()
        return {
            "rows_total": int(len(self.je_index)),
            "by_tt": by_tt,
            "period_min": self.period_min,
            "period_max": self.period_max,
            "bank_files": sorted(self.bank_df["file"].unique().tolist()) if not self.bank_df.empty else [],
        }
