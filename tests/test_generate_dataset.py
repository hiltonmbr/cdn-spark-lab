"""Testes unitários para scripts/generate_dataset.py — sem necessidade de Docker."""

import json
import re
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, "scripts")
import generate_dataset


def test_avaliacoes_app_has_nested_dispositivo_field():
    rng = np.random.default_rng(42)
    df = generate_dataset.generate_avaliacoes_app(200, rng)

    assert list(df.columns) == [
        "id_avaliacao", "id_empresa", "canal", "nota", "comentario", "data", "dispositivo",
    ]
    assert (df["canal"] == "App").all()
    assert df["nota"].between(1, 5).all()
    primeiro = df["dispositivo"].iloc[0]
    assert isinstance(primeiro, dict)
    assert set(primeiro.keys()) == {"os", "versao_app"}
    assert primeiro["os"] in generate_dataset.DISPOSITIVOS_OS
    assert primeiro["versao_app"] in generate_dataset.VERSOES_APP


def test_avaliacoes_app_versao_4_9_1_e_enviesada_para_notas_baixas():
    rng = np.random.default_rng(42)
    df = generate_dataset.generate_avaliacoes_app(5_000, rng)
    df = df.assign(versao_app=df["dispositivo"].apply(lambda d: d["versao_app"]))

    media_bugada = df.loc[df["versao_app"] == "4.9.1", "nota"].mean()
    media_outras = df.loc[df["versao_app"] != "4.9.1", "nota"].mean()
    assert media_bugada < media_outras - 1.0


def test_avaliacoes_site_schema():
    rng = np.random.default_rng(42)
    df = generate_dataset.generate_avaliacoes_site(200, rng)

    assert list(df.columns) == [
        "id_avaliacao", "id_empresa", "canal", "nota", "comentario", "data",
    ]
    assert (df["canal"] == "Site").all()
    assert df["nota"].between(1, 5).all()


def test_avaliacoes_callcenter_tempo_atendimento_correlaciona_com_nota():
    rng = np.random.default_rng(42)
    df = generate_dataset.generate_avaliacoes_callcenter(2_000, rng)

    assert "tempo_atendimento_min" in df.columns
    tempo_nota_1 = df.loc[df["nota"] == 1, "tempo_atendimento_min"].mean()
    tempo_nota_5 = df.loc[df["nota"] == 5, "tempo_atendimento_min"].mean()
    assert tempo_nota_1 > tempo_nota_5


def test_id_avaliacao_ranges_nao_se_sobrepoem():
    rng = np.random.default_rng(42)
    ids_app = set(generate_dataset.generate_avaliacoes_app(100, rng)["id_avaliacao"])
    ids_site = set(generate_dataset.generate_avaliacoes_site(100, rng)["id_avaliacao"])
    ids_callcenter = set(generate_dataset.generate_avaliacoes_callcenter(100, rng)["id_avaliacao"])

    assert ids_app.isdisjoint(ids_site)
    assert ids_app.isdisjoint(ids_callcenter)
    assert ids_site.isdisjoint(ids_callcenter)


def test_nenhum_comentario_contem_virgula_ou_ponto_e_virgula():
    """O CSV do call center usa vírgula como decimal e ';' como delimitador — se
    algum comentário tivesse uma dessas, a demonstração de 'leitura quebrada' do
    notebook 09 ficaria inconsistente entre execuções."""
    for comentarios in generate_dataset.COMENTARIOS_POR_NOTA.values():
        for comentario in comentarios:
            assert "," not in comentario
            assert ";" not in comentario


def test_write_avaliacoes_app_produz_jsonl_valido_com_dispositivo_aninhado(tmp_path, monkeypatch):
    monkeypatch.setattr(generate_dataset, "DATA_DIR", tmp_path)
    rng = np.random.default_rng(42)
    df = generate_dataset.generate_avaliacoes_app(10, rng)

    out_dir = generate_dataset.write_avaliacoes_app(df)
    linhas = (out_dir / "part-000.jsonl").read_text(encoding="utf-8").splitlines()

    assert len(linhas) == 10
    primeiro = json.loads(linhas[0])
    assert set(primeiro["dispositivo"].keys()) == {"os", "versao_app"}
    assert primeiro["data"] == df["data"].iloc[0].strftime("%Y-%m-%d")


def test_write_avaliacoes_site_produz_csv_utf8_com_virgula(tmp_path, monkeypatch):
    monkeypatch.setattr(generate_dataset, "DATA_DIR", tmp_path)
    rng = np.random.default_rng(42)
    df = generate_dataset.generate_avaliacoes_site(10, rng)

    out_dir = generate_dataset.write_avaliacoes_site(df)
    conteudo = (out_dir / "part-000.csv").read_text(encoding="utf-8")

    primeira_linha = conteudo.splitlines()[0]
    assert primeira_linha == "id_avaliacao,id_empresa,canal,nota,comentario,data"


def test_write_avaliacoes_callcenter_produz_csv_legado_ponto_e_virgula_latin1(tmp_path, monkeypatch):
    monkeypatch.setattr(generate_dataset, "DATA_DIR", tmp_path)
    rng = np.random.default_rng(42)
    df = generate_dataset.generate_avaliacoes_callcenter(10, rng)

    out_dir = generate_dataset.write_avaliacoes_callcenter(df)
    conteudo = (out_dir / "part-000.csv").read_text(encoding="latin-1")
    linhas = conteudo.splitlines()

    assert linhas[0] == "id_avaliacao;id_empresa;canal;nota;comentario;data;tempo_atendimento_min"
    campos = linhas[1].split(";")
    assert "," in campos[-1]  # decimal com vírgula em tempo_atendimento_min
    assert re.match(r"^\d{2}/\d{2}/\d{4}$", campos[5])  # data em dd/mm/aaaa, não ISO


def test_main_nao_muda_geracao_das_tabelas_existentes(tmp_path, monkeypatch):
    """Regressão: empresas/funcionarios devem continuar byte-idênticas ao que
    eram antes de avaliacoes existir — os notebooks 00-08 já revisados citam
    valores exatos derivados delas. Valores golden capturados do dataset
    "small" gerado pela versão do script SEM avaliacoes."""
    monkeypatch.setattr(generate_dataset, "DATA_DIR", tmp_path)
    monkeypatch.setattr(sys, "argv", ["generate_dataset.py", "--scale", "small"])

    generate_dataset.main()

    empresas = pd.read_parquet(tmp_path / "bronze" / "empresas" / "part-000.parquet")
    funcionarios = pd.read_parquet(tmp_path / "bronze" / "funcionarios" / "part-000.parquet")

    primeira_empresa = empresas.iloc[0]
    assert primeira_empresa["nome_empresa"] == "Alves e Filhos"
    assert primeira_empresa["setor"] == "Tecnologia"
    assert primeira_empresa["regiao"] == "Sudeste"

    primeiro_funcionario = funcionarios.iloc[0]
    assert primeiro_funcionario["nome_funcionario"] == "Luigi Camargo"
    assert primeiro_funcionario["id_empresa"] == 24
    assert primeiro_funcionario["cargo"] == "Gerente Comercial"

    # avaliacoes também devem ter sido escritas pelo mesmo main()
    assert (tmp_path / "bronze" / "avaliacoes_app" / "part-000.jsonl").exists()
    assert (tmp_path / "bronze" / "avaliacoes_site" / "part-000.csv").exists()
    assert (tmp_path / "bronze" / "avaliacoes_callcenter" / "part-000.csv").exists()
