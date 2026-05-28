import os
import sys
import argparse
import pandas as pd


DEFAULT_PKL = r"C:\Users\gabriel.vinicius\Documents\Vscode\MicroOnibus\checkpoint_ml.pkl"
DEFAULT_XLSX = r"C:\Users\gabriel.vinicius\Documents\Vscode\MicroOnibus\checkpoint_ml.xlsx"


def carregar_para_dataframe(obj):
    if isinstance(obj, pd.DataFrame):
        return obj
    if isinstance(obj, (list, tuple)):
        return pd.DataFrame(obj)
    if isinstance(obj, dict):
        return pd.DataFrame([obj])
    return pd.DataFrame(obj)


def exportar(pkl_path: str, xlsx_path: str) -> int:
    if not os.path.exists(pkl_path):
        print(f"Arquivo nao encontrado: {pkl_path}")
        return 1
    try:
        dados = pd.read_pickle(pkl_path)
        df = carregar_para_dataframe(dados)
        df.to_excel(xlsx_path, index=False, engine="openpyxl")
        print(f"OK: {xlsx_path}")
        return 0
    except Exception as e:
        print(f"Erro ao exportar: {e}")
        return 2


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Exporta um arquivo .pkl para .xlsx (Excel).",
    )
    parser.add_argument(
        "pkl",
        nargs="?",
        default=DEFAULT_PKL,
        help="Caminho do arquivo .pkl (default: checkpoint_ml.pkl)",
    )
    parser.add_argument(
        "xlsx",
        nargs="?",
        default=DEFAULT_XLSX,
        help="Caminho do arquivo .xlsx de saida (default: checkpoint_ml.xlsx)",
    )
    args = parser.parse_args(argv)
    return exportar(args.pkl, args.xlsx)


if __name__ == "__main__":
    raise SystemExit(main())
