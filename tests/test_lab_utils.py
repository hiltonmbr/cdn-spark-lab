"""Unit tests for scripts/lab_utils.py — no Docker required."""

import sys

sys.path.insert(0, "scripts")
import lab_utils


def test_get_local_session_creates_working_session():
    spark = lab_utils.get_local_session("test-local")
    try:
        assert spark.range(5).count() == 5
    finally:
        spark.stop()


def test_layer_path_local():
    expected = str(lab_utils.DATA_DIR / "bronze" / "vendas")
    assert lab_utils.layer_path("local", "bronze", "vendas") == expected


def test_layer_path_connect():
    assert lab_utils.layer_path("connect", "silver", "funcionarios") == "/data/silver/funcionarios"


def test_layer_path_hdfs():
    assert (
        lab_utils.layer_path("hdfs", "gold", "vendas_por_regiao")
        == "webhdfs://localhost:14000/datalake/gold/vendas_por_regiao"
    )


def test_layer_path_s3():
    assert lab_utils.layer_path("s3", "bronze", "empresas") == "s3a://bronze/empresas"


def test_run_gold_benchmark_local():
    spark = lab_utils.get_local_session("test-benchmark")
    try:
        result = lab_utils.run_gold_benchmark(spark, "test-local", "local")
        assert result.row_count > 0
        assert result.seconds > 0
    finally:
        spark.stop()
