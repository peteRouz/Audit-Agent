import urllib.parse
from pathlib import Path

import pandas as pd
import streamlit as st

from finance_audit_agent import FinanceAuditAgent

st.set_page_config(page_title="Finance Audit Copilot", layout="wide")
st.title("🛡️ Finance Audit Agent")

with st.sidebar:
    st.header("Configuration")

    excel_path = st.text_input(
        "Excel (ERP):",
        value="erp/VT - JE Template (JPF)_202501-06_All transactions.xlsx",
        help="Relative or absolute path to the Excel file with the transactions."
    )
    sheet_name = st.text_input("Excel sheet name:", value="VT - JE Template (JPF)")

    bank_dir = st.text_input("MT940 folder", value="bank", help="Folder with .sta / .mt940 / .txt files")
    invoices_dir = st.text_input("Invoices folder (PDF)", value="invoices")

    bank_last4 = st.text_input("Bank account (last 4 digits):", value="1478")

    period_min = st.number_input("Minimum period (YYYYMM):", value=202501, step=1)
    period_max = st.number_input("Maximum period (YYYYMM):", value=202512, step=1)

    ALL_TT = ["AT","CP","CR","GL","IC","IO","IP","PB","PY","RJ","RR","SO","TA"]
    tt_permitidos = st.multiselect(
        "Allowed transaction types (TT):",
        options=ALL_TT,
        default=["IO", "GL", "PB"],
        help="For the demo, IO + GL + PB covers invoice and payment."
    )

    date_window_days = st.slider("Date window (simple mode)", 1, 30, 14)

    carregar = st.button("🔄 Load data")

# State
if "agent" not in st.session_state:
    st.session_state.agent = None

if carregar:
    try:
        agent = FinanceAuditAgent(
            excel_path=Path(excel_path),
            sheet_name=sheet_name,
            bank_dir=Path(bank_dir),
            invoices_dir=Path(invoices_dir),
            bank_account_suffix=bank_last4,
            allowed_tt=tt_permitidos,
            period_min=int(period_min),
            period_max=int(period_max),
            date_window_days=int(date_window_days),
        )
        st.session_state.agent = agent
        st.success("✅ Data loaded. You can now search your IO.")
    except Exception as e:
        st.session_state.agent = None
        st.error(f"Error loading data: {e}")

st.subheader("Search IO")
trans_no = st.text_input("Transaction number (TransNo):", value="")

col1, col2 = st.columns([1, 1])
with col1:
    run_chain = st.button("🚀 Audit IO → GL/PB → Bank")
with col2:
    show_debug = st.checkbox("Show technical details (debug)", value=False)

if run_chain:
    if not st.session_state.agent:
        st.warning("Load the data first (button in the sidebar).")
    elif not trans_no.strip():
        st.warning("Enter an IO transaction number.")
    else:
        try:
            with st.spinner("Searching IO, matching GL/PB, and confirming in the bank..."):
                result = st.session_state.agent.explain_from_io(int(trans_no.strip()))
            st.success("Done.")

            # Summary + Status
            st.markdown(f"**Summary:** {result.get('summary_text','(no summary)')}")
            st.markdown(f"**Status:** {result.get('Status','?')}")

            # ---------------- ERP TABLE ----------------
            erp_rows = result.get("table_rows_erp") or []
            if erp_rows:
                st.markdown("#### ERP")
                df_erp = pd.DataFrame(erp_rows)
                # force trans_no as string (no separators)
                if "trans_no" in df_erp.columns:
                    df_erp["trans_no"] = df_erp["trans_no"].astype(str)
                st.table(df_erp)

            # ---------------- BANK TABLE ----------------
            bank_rows = result.get("table_rows_bank") or []
            if bank_rows:
                st.markdown("#### Bank (MT940)")
                df_bank = pd.DataFrame(bank_rows)
                st.table(df_bank)

                # Bank payment receipt request (mailto)
                pay = bank_rows[0]
                to = st.text_input("Bankdesk email", value="bankdesk@company.com")
                subj = f"Bank payment receipt – {pay.get('date','')} – {abs(float(pay.get('amount',0))):.2f} EUR"
                body_lines = [
                    "Hello Bankdesk,",
                    "",
                    "Could you please send the payment receipt for this transaction for audit purposes?",
                    "",
                    f"• Date: {pay.get('date','')}",
                    f"• Amount: {abs(float(pay.get('amount',0))):.2f} EUR",
                    f"• Reference/description: {pay.get('reference','')}",
                    f"• Account: {pay.get('account','')}",
                    f"• MT940 file: {pay.get('mt940_file','')}",
                    "",
                    "Thank you."
                ]
                body = urllib.parse.quote("\n".join(body_lines))
                mailto = f"mailto:{urllib.parse.quote(to)}?subject={urllib.parse.quote(subj)}&body={body}"
                st.markdown(f"[📨 Request bank payment receipt]({mailto})")

            # Invoice download (if present)
            pdf_path = result.get("Invoice_PDF")
            if pdf_path:
                try:
                    with open(pdf_path, "rb") as f:
                        st.download_button("📎 Download Invoice (PDF)", f, file_name=Path(pdf_path).name)
                except Exception:
                    st.info("The indicated PDF file could not be opened.")

        except Exception as e:
            st.error(f"Execution error: {e}")

if show_debug and st.session_state.agent:
    st.divider()
    st.caption("Debug / Index counts")
    st.json(st.session_state.agent.index_summary())
