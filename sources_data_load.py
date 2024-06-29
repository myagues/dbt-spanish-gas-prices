import argparse
import asyncio
import json
import logging
from datetime import date
from typing import Mapping, TypedDict, Union

import aiohttp
import numpy as np
import pandas as pd
from google.cloud import bigquery
from tqdm.asyncio import tqdm

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
    "Precio Gasolina 95 E5": "gasoline_95E5",
    "Precio Gasolina 95 E10": "gasoline_95E10",
    "Precio Gasolina 95 E5 Premium": "gasoline_95E5_premium",
    "Precio Gasolina 98 E5": "gasoline_98E5",
    "Precio Gasolina 98 E10": "gasoline_98E10",
    "Precio Gasoleo A": "diesel_A",
    "Precio Gasoleo B": "diesel_B",
    "Precio Gasoleo Premium": "diesel_premium",
    "Precio Bioetanol": "bioetanol",
    "Precio Biodiesel": "biodiesel",
    "Precio Gases licuados del petróleo": "lpg",
    "Precio Gas Natural Comprimido": "cng",
    "Precio Gas Natural Licuado": "lng",
    "Precio Hidrogeno": "hydrogen",
    "% BioEtanol": "perc_bioetanol",
    "% Éster metílico": "perc_methyl_ester",
}
_BASE_API_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes"
logger = logging.getLogger(__name__)


class TableMetaData(TypedDict):
    url: str
    columns: Mapping[str, str]


_TABLE_CONFIG: Mapping[str, TableMetaData] = {
    "gas_prices": {
        "url": "EstacionesTerrestresHist",
        "columns": _PRICES_COLUMN_RENAME,
    },
    "regions": {
        "url": "ComunidadesAutonomas",
        "columns": {"IDCCAA": "id", "CCAA": "name"},
    },
    "provinces": {
        "url": "Provincias",
        "columns": {
            "IDPovincia": "id",
            "IDCCAA": "region_id",
            "Provincia": "name",
        },
    },
    "municipalities": {
        "url": "Municipios",
        "columns": {
            "IDMunicipio": "id",
            "IDProvincia": "province_id",
            "Municipio": "name",
        },
    },
}


async def worker(day, session, table_id):
    if day == date.today():
        url = f"{_BASE_API_URL}/EstacionesTerrestres"
    else:
        str_date = day.strftime("%d-%m-%Y")
        url = f"{_BASE_API_URL}/{_TABLE_CONFIG['gas_prices']['url']}/{str_date}"

    async with session.get(url) as resp:
        data = await resp.json()
        # some days might be missing, but response is still 200 with an empty JSON
        # skip the day if content is empty
        if data:
            df = pd.DataFrame.from_dict(data["ListaEESSPrecio"])
            # add date column
            df["date"] = day
            # rename columns to match our table schema
            df = df.rename(columns=_TABLE_CONFIG["gas_prices"]["columns"])

            # non-awaitable :(, see https://github.com/googleapis/python-bigquery/issues/18
            # job = client.load_table_from_dataframe(df, table_id)
            # job.result()
            return df


async def data_upload(
    client: bigquery.Client,
    dataset_ref: bigquery.DatasetReference,
    table: str,
    start_date: Union[str, None],
    end_date: Union[str, None],
    max_connections=5,
):
    """Uploads data from API to BigQuery.
    Args:
        client: BigQuery client
        dataset_ref: dataset where tables and views are saved
        table: table name where data will be uploaded
        start_date: if given, starting date to upload data
        end_date: if given, final date to upload data
        max_connections: maximum parallel connections to the host.
    """
    table_id = dataset_ref.table(f"raw_{table}")
    # set up request properties
    headers = {"content-type": "application/json"}

    if table in ("regions", "provinces", "municipalities"):
        logger.info(f"Querying {table}.")
        request_url = f"{_BASE_API_URL}/Listados/{_TABLE_CONFIG[table]['url']}/"
        logger.info(f"URL: {request_url}")
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(request_url) as resp:
                # response JSON to DataFrame
                df = pd.DataFrame.from_dict(json.loads(await resp.text()))
        # select subset of columns
        df = df[_TABLE_CONFIG[table]["columns"].keys()]
        # rename columns to match our table schema
        df = df.rename(columns=_TABLE_CONFIG[table]["columns"])
        # config upload to overwrite content
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info("Upload done.")

    elif table == "gas_prices":
        logger.info("Querying gas prices.")
        connector = aiohttp.TCPConnector(limit_per_host=max_connections)
        async with aiohttp.ClientSession(
            connector=connector, headers=headers
        ) as session:

            if start_date is None:
                df = await worker(date.today(), session, table_id)
                job = client.load_table_from_dataframe(df, table_id)
                job.result()

            else:
                if end_date is not None:
                    end_date_ = date.fromisoformat(end_date)
                else:
                    end_date_ = date.today()

                dates_range = pd.date_range(
                    start=date.fromisoformat(start_date),
                    end=end_date_,
                )
                # extra loop for BQ upload, ugly workaround as a batch
                splits = (
                    [dates_range]
                    if len(dates_range) < 200
                    else np.array_split(dates_range, 100)
                )
                logger.info(f"Split amount: {len(splits)}.")
                for date_range in tqdm(splits):
                    tasks = [worker(day.date(), session, table_id) for day in date_range]
                    df_list = await tqdm.gather(*tasks, leave=False)
                    df = pd.concat(df_list)
                    job = client.load_table_from_dataframe(df, table_id)
                    job.result()
                    logger.info(f"Upload done for {date_range.date}")

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
            "gas_prices",
            "regions",
            "provinces",
            "municipalities",
        ],
        default=None,
        type=str,
    )
    parser.add_argument(
        "--max_connections",
        help="Maximum parallel connections to the host.",
        default=5,
        type=int,
    )
    parser.add_argument(
        "--start_date",
        help="Starting date for querying prices (requires ISO format, 'yyyy-mm-dd').",
        default=None,
        type=str,
    )
    parser.add_argument(
        "--end_date",
        help="End date for querying prices (requires ISO format, 'yyyy-mm-dd').",
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

    if args.table is not None:
        asyncio.run(
            data_upload(
                client,
                dataset_ref,
                args.table,
                start_date=args.start_date,
                end_date=args.end_date,
                max_connections=args.max_connections,
            )
        )
