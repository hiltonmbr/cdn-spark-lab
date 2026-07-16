"""Testes unitários para scripts/generate_dataset.py — sem necessidade de Docker."""

import sys

import numpy as np

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
