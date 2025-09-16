
import argparse
import json
from pathlib import Path
from finance_audit_agent import FinanceAuditAgent

def main():
    ap = argparse.ArgumentParser(description="Finance Audit Copilot — CLI")
    ap.add_argument("--excel", required=True, help="Caminho para o Excel do ERP")
    ap.add_argument("--sheet", default="VT - JE Template (JPF)", help="Nome da sheet")
    ap.add_argument("--bank", required=True, help="Pasta com MT940 (.sta/.mt940)")
    ap.add_argument("--invoices", default=".", help="Pasta com PDFs de faturas")
    ap.add_argument("--suffix", default="1478", help="Últimos 4 dígitos da conta bancária")
    ap.add_argument("--pmin", type=int, default=202501, help="Período mínimo (YYYYMM)")
    ap.add_argument("--pmax", type=int, default=202506, help="Período máximo (YYYYMM)")
    ap.add_argument("--tt", nargs="+", default=["IO","PB","GL"], help="TT permitidos (ex.: IO PB GL)")
    ap.add_argument("--window", type=int, default=14, help="Janela (dias) para procurar pagamento (modo simples)")
    ap.add_argument("--io", action="store_true", help="Usar modo IO→GL/PB→Banco")
    ap.add_argument("--bankwin", type=int, default=2, help="Janela ±dias para modo IO")
    ap.add_argument("transno", type=int, help="Nr. da transação (TransNo)")

    args = ap.parse_args()
    agent = FinanceAuditAgent(
        excel_path=Path(args.excel),
        sheet_name=args.sheet,
        bank_dir=Path(args.bank),
        invoices_dir=Path(args.invoices),
        bank_account_suffix=args.suffix,
        allowed_tt=args.tt,
        period_min=args.pmin,
        period_max=args.pmax,
        date_window_days=args.window,
    )
    if args.io:
        rep = agent.explain_from_io(args.transno, bank_days_window=args.bankwin)
    else:
        rep = agent.explain_trans(args.transno)
    print(json.dumps(rep, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
