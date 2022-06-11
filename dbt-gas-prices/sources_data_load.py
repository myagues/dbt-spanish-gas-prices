import argparse
import time
from datetime import date, timedelta
from typing import Mapping, Sequence, Tuple, TypedDict, Union

import pandas as pd  # type: ignore
import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm  # type: ignore

_PRICES_COLUMN_RENAME = {
    "Municipio": "municipality",
    "IDMunicipio": "municipality_id",
    "Provincia": "province",
    "IDProvincia": "province_id",
    "IDCCAA": "region_id",
    "Localidad": "town",
    "Dirección": "address",
    "Longitud (WGS84)": "longitude",
    "Latitud": "latitude",
    "Margen": "road_side",
    "Rótulo": "name",
    "Tipo Venta": "restriction",
    "Remisión": "sender",
    "Horario": "schedule",
    "C.P.": "zip_code",
    "IDEESS": "station_id",
    "Precio Gasolina 95 E5": "petrol_95E5",
    "Precio Gasolina 95 E10": "petrol_95E10",
    "Precio Gasolina 95 E5 Premium": "petrol_95E5_premium",
    "Precio Gasolina 98 E5": "petrol_98E5",
    "Precio Gasolina 98 E10": "petrol_98E10",
    "Precio Gasoleo A": "diesel_A",
    "Precio Gasoleo B": "diesel_B",
    "Precio Gasoleo Premium": "diesel_premium",
    "Precio Bioetanol": "bioetanol",
    "Precio Biodiesel": "biodiesel",
    "Precio Gases licuados del petróleo": "lpg",
    "Precio Gas Natural Comprimido": "cng",
    "Precio Gas Natural Licuado": "lng",
    "Precio Hidrogeno": "hydrogen",
}


class TableMetaData(TypedDict):
    url: str
    columns: Mapping[str, str]


_TABLE_CONFIG: Mapping[str, TableMetaData] = {
    "prices": {
        "url": "EstacionesTerrestresHist",
        "columns": _PRICES_COLUMN_RENAME,
    },
    "region": {
        "url": "ComunidadesAutonomas",
        "columns": {"IDCCAA": "id", "CCAA": "name"},
    },
    "province": {
        "url": "Provincias",
        "columns": {
            "IDPovincia": "id",
            "IDCCAA": "region_id",
            "Provincia": "name",
        },
    },
    "municipality": {
        "url": "Municipios",
        "columns": {
            "IDMunicipio": "id",
            "IDProvincia": "province_id",
            "Municipio": "name",
        },
    },
}


def create_source_table(
    client: bigquery.Client,
    dataset_ref: bigquery.DatasetReference,
    table_schema: Tuple[str, Sequence[str]],
):
    """Creates a table with non-nullable columns of format string."""
    table_name, table_columns = table_schema
    source_column = lambda x: bigquery.SchemaField(x, "STRING", mode="REQUIRED")

    table_ref = dataset_ref.table(table_name)
    schema = list(map(source_column, table_columns))
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)


def create_tables(client: bigquery.Client, dataset_ref: bigquery.DatasetReference):
    """Creates tables given client configuration."""

    # create empty time-partitioned source table for prices and stations
    table_ref = dataset_ref.table("prices")
    schema = [
        bigquery.SchemaField("municipality", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("municipality_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("province", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("province_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("region_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("town", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("longitude", "STRING"),
        bigquery.SchemaField("latitude", "STRING"),
        bigquery.SchemaField("road_side", "STRING", max_length=1, mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("restriction", "STRING", max_length=1, mode="REQUIRED"),
        bigquery.SchemaField("sender", "STRING", max_length=2, mode="REQUIRED"),
        bigquery.SchemaField("schedule", "STRING"),
        bigquery.SchemaField("zip_code", "STRING", max_length=5, mode="REQUIRED"),
        bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("petrol_95E5", "STRING"),
        bigquery.SchemaField("petrol_95E10", "STRING"),
        bigquery.SchemaField("petrol_95E5_premium", "STRING"),
        bigquery.SchemaField("petrol_98E5", "STRING"),
        bigquery.SchemaField("petrol_98E10", "STRING"),
        bigquery.SchemaField("diesel_A", "STRING"),
        bigquery.SchemaField("diesel_B", "STRING"),
        bigquery.SchemaField("diesel_premium", "STRING"),
        bigquery.SchemaField("bioetanol", "STRING"),
        bigquery.SchemaField("biodiesel", "STRING"),
        bigquery.SchemaField("lpg", "STRING"),
        bigquery.SchemaField("cng", "STRING"),
        bigquery.SchemaField("lng", "STRING"),
        bigquery.SchemaField("hydrogen", "STRING"),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, expiration_ms=7776000000
    )  # 90 days expiration
    client.create_table(table, exists_ok=True)

    # create empty source table for regions
    create_source_table(client, dataset_ref, ("region", ["id", "name"]))

    # create empty source table for provinces
    create_source_table(client, dataset_ref, ("province", ["id", "region_id", "name"]))

    # create empty source table for municipalities
    create_source_table(
        client, dataset_ref, ("municipality", ["id", "province_id", "name"])
    )


def data_upload(
    client: bigquery.Client,
    dataset_ref: bigquery.DatasetReference,
    table: str,
    start_date: Union[str, None],
    batch_size=15,
    sleep=2,
    max_retries=5,
):
    """Uploads data from API to BigQuery.
    Args:
        client: BigQuery client
        dataset_ref: dataset where tables and views are saved
        table: table name where data will be uploaded
        start_date: if given, starting date to upload data
        batch_size: days of data to accumulate before uploading
        sleep: sleep timer between iterations, to not overburden the API
        max_retries: number of retries in case the API returns an error
    """
    table_id = dataset_ref.table(table)
    # set up request properties
    base_API_url = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes"
    http = requests.Session()
    retries = Retry(
        total=max_retries, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retries)
    http.mount("https://", adapter)

    if table in ("region", "province", "municipality"):
        r = http.get(f"{base_API_url}/Listados/{_TABLE_CONFIG[table]['url']}/")
        # response JSON to DataFrame
        df = pd.DataFrame.from_dict(r.json())
        # select subset of columns
        df = df[_TABLE_CONFIG[table]["columns"].keys()]
        # rename columns to match our table schema
        df = df.rename(columns=_TABLE_CONFIG[table]["columns"])
        # config upload to overwrite content
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

    elif table == "prices":
        if start_date is None:
            # get the last day for which we have data
            query = f"SELECT max(date) AS last_date FROM {dataset_ref.project}.{dataset_ref.dataset_id}.fct_prices"
            results = client.query(query)
            last_date: Union[date, None] = None
            for row in results:
                last_date = row["last_date"]
            # upload the next day
            if last_date is not None:
                last_date += timedelta(days=1)
            # get data from last date or from yesterday if no value in prices
            query_date = last_date or date.today() - timedelta(days=1)
        else:
            query_date = date.fromisoformat(start_date)

        end_date = date.today()
        step = timedelta(days=1)

        df_batch = None
        day_count = 0

        with tqdm(
            total=(end_date - query_date).days,
            bar_format="{postfix[0][query]} ({n_fmt}/{total_fmt}): {percentage:3.0f}% {bar} [{elapsed}<{remaining}, {rate_fmt}]",
            postfix=[dict(query=query_date.strftime("%Y-%m-%d"))],
        ) as t:
            while query_date < end_date:

                day_count += 1
                t.postfix[0]["query"] = query_date.strftime("%Y-%m-%d")
                t.update()

                str_date = query_date.strftime("%d-%m-%Y")
                time.sleep(sleep)
                r = http.get(f"{base_API_url}/{_TABLE_CONFIG[table]['url']}/{str_date}")
                data = r.json()["ListaEESSPrecio"]

                # some days might be missing, but response is still 200 with and empty JSON
                # skip the day if content is empty
                if data:
                    df = pd.DataFrame.from_dict(data)
                    # add date column
                    df["date"] = query_date
                    df = df.drop(
                        columns=[
                            "% BioEtanol",
                            "% Éster metílico",
                        ]
                    )
                    # rename columns to match our table schema
                    df = df.rename(columns=_TABLE_CONFIG[table]["columns"])
                    # concatenate multiple days before loading the batch in BigQuery
                    df_batch = (
                        df
                        if df_batch is None
                        else pd.concat([df_batch, df], ignore_index=True)
                    )

                query_date += step
                # upload data if last iter or batch_size
                if (
                    query_date == end_date or day_count == batch_size
                ) and df_batch is not None:
                    job = client.load_table_from_dataframe(df_batch, table_id)
                    job.result()

                    day_count = 0
                    df_batch = None
    else:
        raise ValueError(f"Table {table} is not valid.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load data from PreciosCarburantes API (https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/help) to BigQuery."
    )
    parser.add_argument(
        "--dataset",
        help="Dataset reference.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--service_acc_path",
        help="Path to JSON credentials. See https://cloud.google.com/bigquery/docs/authentication/service-account-file#python for more information",
        default=None,
        type=str,
    )
    parser.add_argument(
        "--table",
        help="Source table to target.",
        choices=[
            "prices",
            "region",
            "province",
            "municipality",
        ],
        default=None,
        type=str,
    )
    parser.add_argument(
        "--create_tables",
        help="Whether to create new source tables for: 'prices', 'region', 'province' and 'municipality'.",
        action="store_true",
    )
    parser.add_argument(
        "--batch_size",
        help="Days to add in a batch before uploading to BigQuery.",
        default=15,
        type=int,
    )
    parser.add_argument(
        "--sleep_timer",
        help="Seconds to wait between API calls.",
        default=2,
        type=int,
    )
    parser.add_argument(
        "--start_date",
        help="Starting date for querying prices (requires ISO format, 'yyyy-mm-dd').",
        default=None,
        type=str,
    )
    args = parser.parse_args()

    client = (
        bigquery.Client()
        if args.service_acc_path is None
        else bigquery.Client.from_service_account_json(args.service_acc_path)
    )
    dataset_ref = bigquery.DatasetReference(client.project, args.dataset)

    if args.create_tables:
        create_tables(client, dataset_ref)

    if args.table is not None:
        data_upload(
            client,
            dataset_ref,
            args.table,
            start_date=args.start_date,
            batch_size=args.batch_size,
            sleep=args.sleep_timer,
        )
