
# Finance Audit Copilot — Demo (IO → GL/PB → Banco)

1) `pip install -r requirements.txt`
2) `streamlit run app.py`
3) Na app:
   - Aponta para o Excel, pasta MT940 e pasta de invoices
   - Usa o botão **Explicar a partir de IO** com um TransNo IO (ex.: 6000000329)
   - O agente encontra a GL/PB e confirma o pagamento no MT940 (janela ±dias)
