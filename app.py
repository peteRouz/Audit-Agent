import streamlit as st
import pandas as pd
from pathlib import Path

from finance_audit_agent import FinanceAuditAgent

st.set_page_config(page_title="Finance Audit Agent", layout="wide")

st.title("Finance Audit Agent")

# ---------------- Sidebar: configuration ----------------
with st.sidebar:
    st.header("Configuration")

    excel_path = st.text_input(
        "Excel (ERP):",
        value="erp/VT - JE Template (JPF)_202501-06_All transactions.xlsx",
        help="Absolute or relative path to the Excel with ERP transactions.",
    )
    sheet_name = st.text_input(
        "Excel sheet name:",
        value="VT - JE Template (JPF)",
    )

    bank_dir = st.text_input(
        "MT940 folder",
        value="bank",
        help="Folder containing .sta / .mt940 / .txt files",
    )
    invoices_dir = st.text_input(
        "Invoices folder (PDF)",
        value="invoices",
    )

    bank_last4 = st.text_input(
        "Bank account (last 4 digits):",
        value="1478",
    )

    period_min = st.text_input("Minimum period (YYYYMM):", value="202501")
    period_max = st.text_input("Maximum period (YYYYMM):", value="202512")

    allowed_tt = st.multiselect(
        "Allowed transaction types (TT):",
        options=["IO", "GL", "PB"],
        default=["IO", "GL", "PB"],
    )

    date_window_days = st.slider(
        "Date window (simple mode)",
        min_value=1, max_value=120, value=30,
        help="Generic window used by the agent (GL/bank use ±90 by default in the backend).",
    )

    bankdesk_email = st.text_input(
        "Bankdesk email:",
        value="bankdesk@company.com",
        help="Used to render the 'Request bank receipt' mailto button.",
    )

    if st.button("🔄 Load data"):
        try:
            agent = FinanceAuditAgent(
                excel_path=Path(excel_path),
                sheet_name=sheet_name,
                bank_dir=Path(bank_dir),
                invoices_dir=Path(invoices_dir),
                bank_account_suffix=bank_last4.strip(),
                allowed_tt=allowed_tt,
                period_min=int(period_min),
                period_max=int(period_max),
                date_window_days=int(date_window_days),
                bankdesk_email=bankdesk_email.strip(),
            )
            st.session_state["agent"] = agent
            st.success("Data loaded.")
        except Exception as e:
            st.session_state.pop("agent", None)
            st.error(f"Error loading data: {e}")

# ---------------- Main area ----------------
st.subheader("Search IO")

trans_no = st.text_input("Transaction number (TransNo):", value="")

debug = st.checkbox("Show technical details (debug)")

col_btn, _ = st.columns([1, 3])
with col_btn:
    run = st.button("🚀 Audit IO → GL/PB → Bank")

if run:
    if "agent" not in st.session_state:
        st.error("Please load data first (left sidebar).")
        st.stop()

    agent: FinanceAuditAgent = st.session_state["agent"]

    try:
        trans_no_int = int(str(trans_no).strip())
    except Exception:
        st.error("Please enter a valid TransNo (integer).")
        st.stop()

    try:
        result = agent.explain_from_io(trans_no_int)
        st.success("Done.")

        # Narrative + status
        st.markdown(f"**Summary:** {result.get('summary_text','')}")
        st.markdown(f"**Status:** {result.get('Status','')}")

        # ERP table
        st.subheader("ERP")
        erp_rows = result.get("table_rows_erp") or []
        if erp_rows:
            df_erp = pd.DataFrame(erp_rows)
            st.table(df_erp)
        else:
            st.info("No ERP rows to display.")

        # Bank table
        bank_rows = result.get("table_rows_bank") or []
        if bank_rows:
            st.subheader("Bank (MT940)")
            df_bank = pd.DataFrame(bank_rows)
            st.table(df_bank)

        # Download invoice (if exists)
        pdf_path = result.get("Invoice_PDF")
        if pdf_path:
            try:
                with open(pdf_path, "rb") as f:
                    st.download_button(
                        "📎 Download Invoice (PDF)",
                        f,
                        file_name=Path(pdf_path).name,
                        mime="application/pdf",
                    )
            except Exception as e:
                st.warning(f"Invoice PDF found but could not be opened: {e}")

        # Request bank receipt (mailto)
        mailto = result.get("bankdesk_mailto")
        if mailto:
            # Streamlit >=1.25
            try:
                st.link_button("📨 Request bank receipt", mailto)
            except AttributeError:
                # Fallback if link_button is not available
                st.markdown(f"[📨 Request bank receipt]({mailto})")

        # Debug info
        if debug:
            st.divider()
            st.markdown("**Index summary (debug):**")
            st.json(agent.index_summary())

    except Exception as e:
        st.error(f"Execution error: {e}")
