#!/usr/bin/env python3
# -- coding: utf-8 --

"""
Ingestão CDC diretamente do WAL lógico (logical replication slot) -> MinIO.
- Lê mudanças do slot via PEEK (não consome).
- Extrai APENAS eventos da tabela db_loja.cliente.
- Escreve 1 CSV local e faz upload p/ raw/inc/data=YYYYMMDD/cliente_cdc_YYYYMMDD_HHMMSS.csv
- Se upload OK, consome as mesmas mensagens (GET com upto_nchanges) para avançar o slot.

Requisitos:
  pip install psycopg2-binary minio
  Slot e publication já criados (ex.: plugin wal2json ou test_decoding)

Ambiente:
  PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS
  CDC_SLOT   (default: data_sync_slot)
  MINIO_ENDPOINT (ex.: minio:9000), MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE=false
  TARGET_BUCKET  (default: raw)
"""

import os, io, re, csv, json, glob, shutil, tempfile
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from minio import Minio


# --------------------- Helpers ---------------------

def pg_connect():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "db"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "mydb"),
        user=os.getenv("PG_USER", "myuser"),
        password=os.getenv("PG_PASS", "mypassword"),
    )
    conn.autocommit = True  # exigido pelas funções do slot lógico
    return conn

def get_minio() -> Minio:
    return Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )

def ensure_bucket(mc: Minio, bucket: str):
    if not mc.bucket_exists(bucket):
        mc.make_bucket(bucket)

# --------------- CDC read (PEEK / GET) ----------------

def peek_changes(conn, slot: str) -> List[Dict[str, Any]]:
    """Retorna lista de mensagens [{'lsn':..., 'data':...}] sem avançar o slot."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT lsn, data FROM pg_logical_slot_peek_changes(%s, NULL, NULL);", (slot,))
        return cur.fetchall()

def consume_changes(conn, slot: str, n_messages: int):
    """Avança o slot consumindo exatamente n_messages."""
    if n_messages <= 0:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_logical_slot_get_changes(%s, NULL, %s);", (slot, n_messages))

# --------------- Parsers ----------------

def parse_wal2json_row(row_data: str) -> List[Dict[str, Any]]:
    """
    wal2json format-version>=2:
      {"change":[
         {"kind":"insert","schema":"db_loja","table":"cliente","columnnames":[...],"columnvalues":[...]},
         {"kind":"update",...}, {"kind":"delete",...}
      ]}
    Retorna lista de eventos normalizados.
    """
    events = []
    obj = json.loads(row_data)
    for ch in obj.get("change", []):
        if ch.get("schema") != "db_loja" or ch.get("table") != "cliente":
            continue
        kind = ch.get("kind")
        if kind == "insert":
            cols = ch.get("columnnames", [])
            vals = ch.get("columnvalues", [])
            events.append({"op": "I", **dict(zip(cols, vals))})
        elif kind == "update":
            cols = ch.get("columnnames", [])
            vals = ch.get("columnvalues", [])
            # em wal2json v2 há oldkeys p/ PK; aqui priorizamos valores novos
            evt = {"op": "U", **dict(zip(cols, vals))}
            # opcional: anexar oldkeys
            if "oldkeys" in ch:
                ok = ch["oldkeys"]
                for k, v in zip(ok.get("keynames", []), ok.get("keyvalues", [])):
                    evt[f"old_{k}"] = v
            events.append(evt)
        elif kind == "delete":
            # delete carrega oldkeys
            evt = {"op": "D"}
            ok = ch.get("oldkeys", {})
            for k, v in zip(ok.get("keynames", []), ok.get("keyvalues", [])):
                evt[k] = v
            events.append(evt)
    return events

def looks_like_json(s: str) -> bool:
    s = s.strip()
    return s.startswith("{") and s.endswith("}")

def parse_test_decoding_row(row_data: str) -> List[Dict[str, Any]]:
    """
    Parser simples para 'test_decoding' (texto). Exemplo:
      table db_loja.cliente: INSERT: id[integer]:1 nome[text]:'Cliente 1' ...
    Obs.: É um parser best-effort; wal2json é fortemente recomendado.
    """
    events = []
    s = row_data.strip()
    if not s.startswith("table "):
        return events
    try:
        # identificar tabela
        # "table db_loja.cliente: INSERT: ..."
        head, rest = s.split(":", 1)
        _, fqtn = head.split(" ", 1)
        if fqtn.strip() != "db_loja.cliente":
            return events
        # identificar operação
        rest = rest.strip()
        if rest.upper().startswith("INSERT:"):
            op = "I"; kv_part = rest[len("INSERT:"):].strip()
        elif rest.upper().startswith("UPDATE:"):
            op = "U"; kv_part = rest[len("UPDATE:"):].strip()
        elif rest.upper().startswith("DELETE:"):
            op = "D"; kv_part = rest[len("DELETE:"):].strip()
        else:
            return events

        # Extrai pares col[valtype]:value (bem rudimentar)
        # Ex.: id[integer]:1 nome[text]:'Cliente 1'
        pairs = re.findall(r"(\w+)\[[^\]]+\]:('(?:\\'|[^'])*'|[^ ]+)", kv_part)
        row = {"op": op}
        for col, val in pairs:
            v = val
            if v.startswith("'") and v.endswith("'"):
                v = v[1:-1].replace("\\'", "'")
            row[col] = v
        events.append(row)
    except Exception:
        pass
    return events

def normalize_messages(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Recebe lista de mensagens do slot (cada uma pode conter várias mudanças).
    Devolve lista "achatada" de eventos normalizados (dicionários).
    """
    all_events: List[Dict[str, Any]] = []
    for r in rows:
        data = r["data"]
        if looks_like_json(data):
            all_events.extend(parse_wal2json_row(data))
        else:
            all_events.extend(parse_test_decoding_row(data))
    return all_events

# --------------- CSV + Upload ----------------

def write_csv_and_upload(events: List[Dict[str, Any]], bucket: str, mc: Minio) -> Tuple[str, int]:
    """
    Escreve 1 CSV (colunas dinâmicas com união dos campos) e envia p/ MinIO.
    Retorna (object_key, n_rows).
    """
    if not events:
        return "", 0

    # caminho de destino (Hive-style inc/)
    now = datetime.now(timezone.utc)
    dt_partition = now.strftime("%Y%m%d")
    ts = now.strftime("%Y%m%d_%H%M%S")
    prefix = f"inc/data={dt_partition}"
    key = f"{prefix}/cliente_cdc_{ts}.csv"

    # descobre o conjunto de colunas (op + todas as chaves vistas)
    cols = ["op"]
    for ev in events:
        for k in ev.keys():
            if k not in cols:
                cols.append(k)

    # CSV local temporário
    workdir = tempfile.mkdtemp(prefix="cdc_slot_")
    local_csv = os.path.join(workdir, f"cliente_cdc_{ts}.csv")

    with open(local_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for ev in events:
            w.writerow({c: ev.get(c, "") for c in cols})

    # upload
    ensure_bucket(mc, bucket)
    mc.fput_object(bucket, key, local_csv, content_type="text/csv")

    # limpeza
    try:
        shutil.rmtree(workdir, ignore_errors=True)
    except Exception:
        pass

    return key, len(events)


# --------------------- MAIN ---------------------

def main():
    # destinos
    bucket = os.getenv("TARGET_BUCKET", "raw").lower()
    slot = os.getenv("CDC_SLOT", "data_sync_slot")

    # 1) Ler mensagens via PEEK
    conn = pg_connect()
    rows = peek_changes(conn, slot)
    total_msgs = len(rows)
    if total_msgs == 0:
        print("[CDC] Nenhuma mudança pendente no slot. Nada a fazer.")
        return

    print(f"[CDC] Mensagens no slot (PEEK): {total_msgs}")

    # 2) Normalizar eventos e filtrar apenas db_loja.cliente
    events = normalize_messages(rows)
    events = [e for e in events if e]  # remove vazios

    if not events:
        print("[CDC] Havia mensagens, mas nenhuma da tabela db_loja.cliente. Nada a subir.")
        # ainda assim avançamos o slot, pois são mensagens consumíveis
        consume_changes(conn, slot, total_msgs)
        print(f"[CDC] Slot avançado (descartando {total_msgs} mensagens não relevantes).")
        return

    # 3) CSV + Upload (SDK MinIO)
    mc = get_minio()
    key, n = write_csv_and_upload(events, bucket, mc)
    print(f"[UPLOAD] s3://{bucket}/{key} ({n} eventos)")

    # 4) Avançar o slot — consome exatamente as mensagens que vimos no PEEK
    consume_changes(conn, slot, total_msgs)
    print(f"[CDC] Slot '{slot}' avançado em {total_msgs} mensagens.")

if __name__ == "__main__":
    import os
    main()