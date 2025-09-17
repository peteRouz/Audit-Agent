import sys, streamlit as st, numpy
st.caption(f"Python {sys.version}")
st.caption(f"Numpy {numpy.__version__}")

import urllib.parse
from pathlib import Path

import pandas as pd
import streamlit as st

from finance_audit_agent import FinanceAuditAgent

st.set_page_config(page_title="Finance Audit Copilot", layout="wide")
st.title("🛡️ Finance Audit Agent")

with st.sidebar:
    st.header("Configuração")

    excel_path = st.text_input(
        "Excel (ERP):",
        value="erp/VT - JE Template (JPF)_202501-06_All transactions.xlsx",
        help="Caminho relativo ou absoluto para o Excel com as transações."
    )
    sheet_name = st.text_input("Sheet do Excel:", value="VT - JE Template (JPF)")

    bank_dir = st.text_input("Pasta MT940", value="bank", help="Pasta com .sta / .mt940 / .txt")
    invoices_dir = st.text_input("Pasta de Faturas (PDF)", value="invoices")

    bank_last4 = st.text_input("Conta bancária (últimos 4 dígitos):", value="1478")

    period_min = st.number_input("Período mínimo (YYYYMM):", value=202501, step=1)
    period_max = st.number_input("Período máximo (YYYYMM):", value=202512, step=1)

    ALL_TT = ["AT","CP","CR","GL","IC","IO","IP","PB","PY","RJ","RR","SO","TA"]
    tt_permitidos = st.multiselect(
        "Tipos de transação (TT) permitidos:",
        options=ALL_TT,
        default=["IO", "GL", "PB"],
        help="Para a demo, IO + GL + PB cobre fatura e pagamento."
    )

    date_window_days = st.slider("Janela de dias (modo simples)", 1, 30, 14)

    carregar = st.button("🔄 Carregar dados")

# Estado
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
        st.success("✅ Dados carregados. Podes procurar a tua IO.")
    except Exception as e:
        st.session_state.agent = None
        st.error(f"Erro ao carregar dados: {e}")

st.subheader("Pesquisar IO")
trans_no = st.text_input("Nr. da transação (TransNo):", value="")

col1, col2 = st.columns([1, 1])
with col1:
    run_chain = st.button("🚀 Auditar IO → GL/PB → Banco")
with col2:
    show_debug = st.checkbox("Mostrar detalhes técnicos (debug)", value=False)

if run_chain:
    if not st.session_state.agent:
        st.warning("Carrega os dados primeiro (botão na barra lateral).")
    elif not trans_no.strip():
        st.warning("Indica um número de transação IO.")
    else:
        try:
            with st.spinner("A procurar IO, a tentar match GL/PB e a confirmar no banco..."):
                result = st.session_state.agent.explain_from_io(int(trans_no.strip()))
            st.success("Concluído.")

            # Resumo + Status
            st.markdown(f"**Resumo:** {result.get('summary_text','(sem resumo)')}")
            st.markdown(f"**Status:** {result.get('Status','?')}")

            # ---------------- ERP TABLE ----------------
            erp_rows = result.get("table_rows_erp") or []
            if erp_rows:
                st.markdown("#### ERP")
                df_erp = pd.DataFrame(erp_rows)
                # força trans_no como string (sem separadores)
                if "trans_no" in df_erp.columns:
                    df_erp["trans_no"] = df_erp["trans_no"].astype(str)
                st.table(df_erp)

            # ---------------- BANK TABLE ----------------
            bank_rows = result.get("table_rows_bank") or []
            if bank_rows:
                st.markdown("#### Banco (MT940)")
                df_bank = pd.DataFrame(bank_rows)
                st.table(df_bank)

                # Botão de pedido de comprovativo bancário (mailto)
                pay = bank_rows[0]
                to = st.text_input("Email do Bankdesk", value="bankdesk@empresa.com")
                subj = f"Comprovativo bancário - {pay.get('date','')} - {abs(float(pay.get('amount',0))):.2f} EUR"
                body_lines = [
                    "Olá Bankdesk,",
                    "",
                    "Podem por favor enviar o comprovativo bancário deste pagamento para auditoria?",
                    "",
                    f"• Data: {pay.get('date','')}",
                    f"• Montante: {abs(float(pay.get('amount',0))):.2f} EUR",
                    f"• Referência/descrição: {pay.get('reference','')}",
                    f"• Conta: {pay.get('account','')}",
                    f"• Ficheiro MT940: {pay.get('mt940_file','')}",
                    "",
                    "Obrigado."
                ]
                body = urllib.parse.quote("\n".join(body_lines))
                mailto = f"mailto:{urllib.parse.quote(to)}?subject={urllib.parse.quote(subj)}&body={body}"
                st.markdown(f"[📨 Solicitar comprovativo bancário]({mailto})")

            # Download da fatura (se existir)
            pdf_path = result.get("Invoice_PDF")
            if pdf_path:
                try:
                    with open(pdf_path, "rb") as f:
                        st.download_button("📎 Download Fatura (PDF)", f, file_name=Path(pdf_path).name)
                except Exception:
                    st.info("Ficheiro PDF indicado não pôde ser aberto.")

        except Exception as e:
            st.error(f"Erro ao executar: {e}")

if show_debug and st.session_state.agent:
    st.divider()
    st.caption("Debug / Contagens de índice")
    st.json(st.session_state.agent.index_summary())
