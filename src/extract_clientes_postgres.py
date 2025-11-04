#!/usr/bin/env python3
# -- coding: utf-8 --

"""
Script: extract_clientes_postgres.py
Função principal: extract_clientes(spark=None)
 - Conecta ao PostgreSQL e retorna um DataFrame Spark com toda a tabela db_loja.cliente.
 - Se 'spark' não for passado, cria uma SparkSession mínima com o driver do Postgres.
"""

import os
from pyspark.sql import SparkSession


def extract_clientes(spark: SparkSession | None = None):
    """
    Conecta ao PostgreSQL e retorna um DataFrame com a tabela db_loja.cliente.
    Parâmetros de conexão vêm de variáveis de ambiente:
        PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS, PG_TABLE
    """
    # Variáveis de ambiente (ajuste conforme seu docker-compose)
    PG_HOST = os.getenv("PG_HOST", "db")
    PG_PORT = os.getenv("PG_PORT", "5432")
    PG_DB   = os.getenv("PG_DB", "mydb")
    PG_USER = os.getenv("PG_USER", "myuser")
    PG_PASS = os.getenv("PG_PASS", "mypassword")
    PG_TABLE = os.getenv("PG_TABLE", "db_loja.cliente")

    # Cria SparkSession se não foi injetada
    created_here = False
    if spark is None:
        spark = (
            SparkSession.builder
            .appName("extract_clientes_postgres")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .getOrCreate()
        )
        created_here = True

    # JDBC
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    jdbc_props = {
        "user": PG_USER,
        "password": PG_PASS,
        "driver": "org.postgresql.Driver",
    }

    print(f"[EXTRACT] Lendo '{PG_TABLE}' em {PG_DB}@{PG_HOST}:{PG_PORT} ...")
    df = spark.read.jdbc(url=jdbc_url, table=PG_TABLE, properties=jdbc_props)
    print("[EXTRACT] DataFrame lido com sucesso.")

    # Se criamos a sessão aqui só para teste interativo, não finalizamos (deixe o caller decidir)
    return df


if __name__ == "__main__":
    # Modo teste: cria sessão, extrai e mostra amostra
    spark = (
        SparkSession.builder
        .appName("extract_clientes_postgres_test")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    df = extract_clientes(spark)
    df.show(5, truncate=False)
    spark.stop()
