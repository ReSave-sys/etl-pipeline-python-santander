import pandas as pd # type: ignore
from pathlib import Path

# =========================
# CONFIGURAÇÕES
# =========================
DATA_DIR = Path
INPUT_FILE = DATA_DIR / "users.csv"
OUTPUT_FILE = DATA_DIR / "mensagens_geradas.csv"


# =========================
# EXTRAÇÃO
# =========================
def extract_data(file_path: Path) -> pd.DataFrame:
    print("🔍 Extraindo dados...")
    return pd.read_csv(file_path)


# =========================
# TRANSFORMAÇÃO
# =========================
def generate_message(nome: str) -> str:
    return (
        f"Olá {nome}! 💙\n"
        "Temos uma oferta especial preparada para você.\n"
        "Aproveite condições exclusivas disponíveis por tempo limitado!"
    )


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print("🧠 Transformando dados...")
    df["Mensagem"] = df["Nome"].apply(generate_message)
    return df


# =========================
# CARREGAMENTO
# =========================
def load_data(df: pd.DataFrame, output_path: Path) -> None:
    print("💾 Salvando dados transformados...")
    df.to_csv(output_path, index=False)


# =========================
# EXECUÇÃO DO PIPELINE
# =========================
def main():
    df = extract_data(INPUT_FILE)
    df = transform_data(df)
    load_data(df, OUTPUT_FILE)
    print("✅ Pipeline ETL executado com sucesso!")


if __name__ == "__main__":
    main()
