#!/usr/bin/env python3
# -- coding: utf-8 --

"""
Full load da tabela db_loja.cliente usando SDK MinIO (sem s3a).
Fluxo:
  1) Lê Postgres -> DataFrame Spark (usa extract_clientes)
  2) Escreve CSV local temporário (1 arquivo com header)
  3) Faz upload com MinIO SDK para:
     raw/full/data=YYYYMMDD/cliente_full_YYYYMMDD_HHMMSS.csv
Requisitos:
  pip install minio pyspark
  spark-submit com pacote JDBC do Postgres.
Execução (exemplo):
  export PG_HOST=db
  export PG_PORT=5432
  export PG_DB=mydb
  export PG_USER=myuser
  export PG_PASS=mypassword

  export MINIO_ENDPOINT=minio:9000         # host:porta (sem http)
  export MINIO_ACCESS_KEY=minioadmin
  export MINIO_SECRET_KEY=minioadmin
  export MINIO_SECURE=false                # 'true' se HTTPS

  export TARGET_BUCKET=raw                 # bucket minúsculo
  spark-submit \
    --packages org.postgresql:postgresql:42.7.3 \
    src/ingest_full_load_clientes.py
"""

import os
import shutil
import glob
import tempfile
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from minio import Minio
from extract_clientes_postgres import extract_clientes


def build_spark() -> SparkSession:
    """Cria SparkSession (somente JDBC Postgres; sem s3a)."""
    spark = (
        SparkSession.builder
        .appName("ingest_full_load_clientes_sdk")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")
    return spark


def get_minio_client() -> Minio:
    """Instancia o cliente MinIO a partir de env vars."""
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")   # ex.: "minio:9000"
    access   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret   = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    secure   = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access, secret_key=secret, secure=secure)


def ensure_bucket(minio_client: Minio, bucket: str) -> None:
    """Garante que o bucket exista (cria se necessário)."""
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)


def main():
    # ---------- Datas para particionamento e watermark ----------
    now_utc = datetime.now(timezone.utc)
    dt_partition = now_utc.strftime("%Y%m%d")
    ts_watermark = now_utc.strftime("%Y%m%d_%H%M%S")

    # ---------- Parâmetros do destino ----------
    bucket = os.getenv("TARGET_BUCKET", "raw").lower()  # buckets devem ser minúsculos
    object_prefix = f"full/data={dt_partition}"
    final_object_name = f"{object_prefix}/cliente_full_{ts_watermark}.csv"

    # ---------- Spark & Extract ----------
    spark = build_spark()
    df = extract_clientes(spark)  # usa sua função já testada
    df.show(3, truncate=False)

    # ---------- Diretórios locais temporários ----------
    workdir = tempfile.mkdtemp(prefix="full_load_clientes_")
    tmp_dir = os.path.join(workdir, f"tmp{ts_watermark}")
    os.makedirs(tmp_dir, exist_ok=True)

    try:
        # ---------- Escreve 1 CSV local com header ----------
        (
            df.coalesce(1)
              .write.mode("overwrite")
              .option("header", "true")
              .csv(tmp_dir)
        )

        # Localiza o único part-*.csv gerado pelo Spark
        part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
        if not part_files:
            raise RuntimeError("part-*.csv não encontrado após a escrita local.")
        part_csv = part_files[0]

        # Renomeia/copia para o nome final local
        local_final = os.path.join(workdir, f"cliente_full_{ts_watermark}.csv")
        shutil.move(part_csv, local_final)

        # ---------- Upload via MinIO SDK ----------
        minio_client = get_minio_client()
        ensure_bucket(minio_client, bucket)

        # OBS: pastas são lógicas em S3/MinIO; usamos barras no object_name
        print(f"[UPLOAD] Enviando para {bucket}/{final_object_name}")
        minio_client.fput_object(
            bucket_name=bucket,
            object_name=final_object_name,
            file_path=local_final,
            content_type="text/csv"
        )
        print(f"[OK] Upload concluído: s3://{bucket}/{final_object_name}")

    finally:
        # ---------- Limpeza local ----------
        try:
            shutil.rmtree(workdir, ignore_errors=True)
        except Exception:
            pass

    spark.stop()


if __name__ == "__main__":
    main()