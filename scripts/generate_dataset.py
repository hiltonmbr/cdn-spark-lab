"""Gerador de datasets sintéticos para cdn-spark-lab.

Gera três tabelas Parquet baseadas em uma narrativa de vendas corporativas/RH:
`empresas` (dimensão pequena, 50 linhas -> demonstração de broadcast join), `funcionarios`
(dimensão média, 5K-20K linhas -> demonstração de SortMergeJoin) e `vendas` (tabela
fato, particionada por ano/mes). Cada linha de `vendas` também carrega um
`id_empresa` desnormalizado (sempre igual ao empregador do seu funcionário) para que
os laboratórios possam demonstrar um broadcast join limpo de único salto sem forçar
um join através de `funcionarios` primeiro.

Nenhum acesso à internet é necessário — tudo é gerado localmente com uma semente
fixa, para que todo aluno obtenha dados byte-idênticos independentemente da data de execução.

Uso:
    uv run python scripts/generate_dataset.py --scale small
    uv run python scripts/generate_dataset.py --scale large
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

SEED = 42
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

SCALES = {
    # (n_vendas, n_funcionarios)
    "small": (500_000, 5_000),
    "large": (10_000_000, 20_000),
}

REGIOES = ["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"]

SETORES = [
    "Varejo", "Tecnologia", "Saude", "Educacao", "Financas", "Industria",
    "Agronegocio", "Logistica", "Alimenticio", "Construcao Civil",
    "Energia", "Telecomunicacoes", "Turismo e Hotelaria", "Consultoria",
    "Juridico",
]

N_EMPRESAS = 50  # exemplo do próprio slide: "uma tabela pequena com 50 linhas"

CARGO_SALARIO_BASE = {
    "Vendedor Junior": 2_500,
    "Vendedor Pleno": 4_000,
    "Vendedor Senior": 6_500,
    "Coordenador Comercial": 9_000,
    "Gerente Comercial": 13_000,
    "Diretor Comercial": 22_000,
}
CARGOS = list(CARGO_SALARIO_BASE.keys())

# Corte fixo (não `today()`) — mantém a geração determinística independentemente
# da data em que o aluno executar `make generate-data`.
DATA_ADMISSAO_START = "2015-01-01"
DATA_ADMISSAO_END = "2026-07-01"


def generate_empresas(rng: np.random.Generator, fake: Faker) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id_empresa": np.arange(1, N_EMPRESAS + 1, dtype=np.int32),
            "nome_empresa": [fake.company() for _ in range(N_EMPRESAS)],
            "setor": rng.choice(SETORES, size=N_EMPRESAS),
            "regiao": rng.choice(REGIOES, size=N_EMPRESAS),
        }
    )


def generate_funcionarios(n: int, rng: np.random.Generator, fake: Faker) -> pd.DataFrame:
    cargos = rng.choice(CARGOS, size=n)
    salario_base = np.array([CARGO_SALARIO_BASE[c] for c in cargos])
    salario = np.round(salario_base * rng.uniform(0.85, 1.25, size=n), 2)

    start_epoch = pd.Timestamp(DATA_ADMISSAO_START).value // 10**9
    end_epoch = pd.Timestamp(DATA_ADMISSAO_END).value // 10**9
    admissao_epoch = rng.integers(start_epoch, end_epoch, size=n)

    return pd.DataFrame(
        {
            "id_funcionario": np.arange(1, n + 1, dtype=np.int64),
            "nome_funcionario": [fake.name() for _ in range(n)],
            "id_empresa": rng.integers(1, N_EMPRESAS + 1, size=n, dtype=np.int32),
            "cargo": cargos,
            "salario": salario,
            "data_admissao": pd.to_datetime(admissao_epoch, unit="s").normalize(),
        }
    )


def generate_vendas(n: int, funcionarios: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    n_funcionarios = len(funcionarios)
    id_funcionario = rng.integers(1, n_funcionarios + 1, size=n, dtype=np.int64)
    # Desnormalizado de propósito — sempre o empregador real do funcionário, para que
    # `vendas` suporte um broadcast join limpo de único salto com `empresas`
    # sem forçar toda consulta a passar por `funcionarios` primeiro.
    funcionario_to_empresa = funcionarios.set_index("id_funcionario")["id_empresa"]
    id_empresa = funcionario_to_empresa.loc[id_funcionario].to_numpy()

    anos = rng.choice([2024, 2025, 2026], size=n, p=[0.25, 0.35, 0.40])
    meses = rng.integers(1, 13, size=n, dtype=np.int32)
    dias = rng.integers(1, 28, size=n, dtype=np.int32)
    return pd.DataFrame(
        {
            "id_venda": np.arange(1, n + 1, dtype=np.int64),
            "id_funcionario": id_funcionario,
            "id_empresa": id_empresa.astype(np.int32),
            "valor": np.round(rng.lognormal(mean=4.0, sigma=1.0, size=n), 2),
            "ano": anos.astype(np.int32),
            "mes": meses,
            "dia": dias,
        }
    )


def write_bronze(df: pd.DataFrame, name: str, partition_cols: list[str] | None = None) -> Path:
    out_dir = DATA_DIR / "bronze" / name
    out_dir.mkdir(parents=True, exist_ok=True)
    if partition_cols:
        for keys, group in df.groupby(partition_cols):
            keys = keys if isinstance(keys, tuple) else (keys,)
            sub_dir = out_dir
            for col, val in zip(partition_cols, keys):
                sub_dir = sub_dir / f"{col}={val}"
            sub_dir.mkdir(parents=True, exist_ok=True)
            # coerce_timestamps="us": pandas/pyarrow usam nanossegundos como padrão
            # para timestamps Parquet em colunas datetime64 (ex.: data_admissao),
            # o que o leitor Parquet do Spark 3.5.x rejeita diretamente
            # ("Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))").
            # Precisão de microssegundos é o que o Spark espera e é mais que
            # suficiente para uma data de admissão.
            group.drop(columns=partition_cols).to_parquet(
                sub_dir / "part-000.parquet", index=False,
                coerce_timestamps="us", allow_truncated_timestamps=True,
            )
    else:
        df.to_parquet(
            out_dir / "part-000.parquet", index=False,
            coerce_timestamps="us", allow_truncated_timestamps=True,
        )
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", choices=SCALES.keys(), default="small")
    args = parser.parse_args()

    n_vendas, n_funcionarios = SCALES[args.scale]
    rng = np.random.default_rng(SEED)
    fake = Faker("pt_BR")
    Faker.seed(SEED)

    print(
        f"🏭 Generating dataset (scale={args.scale}): "
        f"{n_vendas:,} vendas, {n_funcionarios:,} funcionarios, {N_EMPRESAS} empresas"
    )

    empresas = generate_empresas(rng, fake)
    funcionarios = generate_funcionarios(n_funcionarios, rng, fake)
    vendas = generate_vendas(n_vendas, funcionarios, rng)

    write_bronze(empresas, "empresas")
    write_bronze(funcionarios, "funcionarios")
    write_bronze(vendas, "vendas", partition_cols=["ano", "mes"])

    print(f"✅ Bronze layer written to {DATA_DIR / 'bronze'}")
    print(f"   empresas/       (unpartitioned, {N_EMPRESAS} rows)")
    print(f"   funcionarios/   (unpartitioned, {n_funcionarios:,} rows)")
    print(f"   vendas/         (partitioned by ano/mes, {n_vendas:,} rows)")


if __name__ == "__main__":
    main()
