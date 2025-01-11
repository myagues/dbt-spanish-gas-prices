from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery as bq, bigquery_storage as bq_storage

project_id = ""
dataset_id = "_a002d67a8f92515d51c02fe3572af784e52d49d0"
table_id = "anon6da37c1ee3c5bab28a711451d9bac0e69d58b5ba31d198c0153f8eb670cc78f3"

table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
parent = f"projects/{project_id}"
data_export_path = Path("data_export")


def parquet_write(session: bq_storage.ReadSession, reader: bq_storage.ReadRowsStream):
    pq_schema = pa.ipc.read_schema(pa.py_buffer(session.arrow_schema.serialized_schema))

    # still fills memory bc of Parquet metadata, see: https://github.com/apache/arrow/issues/26073
    with pq.ParquetWriter(
        data_export_path / f"{table_id}.parquet", schema=pq_schema, compression="zstd"
    ) as writer:
        for page in reader.rows(session).pages:
            writer.write(page.to_arrow())


def table_export():
    client = bq_storage.BigQueryReadClient()

    read_options = bq_storage.ReadSession.TableReadOptions(
        selected_fields=[],
        # row_restriction="",  # where clause filer
    )
    requested_session = bq_storage.ReadSession(
        table=table,
        data_format=bq_storage.DataFormat.ARROW,
        read_options=read_options,
    )
    session = client.create_read_session(
        parent=parent, read_session=requested_session, max_stream_count=1
    )

    reader = client.read_rows(session.streams[0].name)
    parquet_write(session, reader)


def query_export():
    bq_client = bq.Client()
    job = bq_client.query((data_export_path / "prices__export.sql").read_text())
    job.result()
    # logging.info(f"Destination table")

    storage_client = bq_storage.BigQueryReadClient()
    assert isinstance(job.destination, bq.TableReference)
    requested_session = bq_storage.ReadSession(
        table=f"projects/{job.destination.project}/datasets/{job.destination.dataset_id}/tables/{job.destination.table_id}",
        data_format=bq_storage.DataFormat.ARROW,
    )
    session = storage_client.create_read_session(
        parent=f"projects/{project_id}",
        read_session=requested_session,
        max_stream_count=1,
    )
    reader = storage_client.read_rows(session.streams[0].name)
    parquet_write(session, reader)


if __name__ == "__main__":
    table_export()
