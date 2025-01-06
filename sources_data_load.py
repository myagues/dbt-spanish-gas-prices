import argparse
import asyncio
import logging
from datetime import date
from itertools import batched
from typing import Mapping, Optional, TypedDict

import aiohttp
import numpy as np
import numpy.typing as npt
import pandas as pd
import pyarrow as pa
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


async def fetch_json(url: str, session: aiohttp.ClientSession) -> dict:
    """Fetch JSON data from the given URL using the provided session.
    Args:
        url: URL to fetch
        session: Session to use for the connection
    """
    try:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.json()

    except aiohttp.ClientError as e:
        logger.error(f"HTTP error occurred: {e}")
        return {}

    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        return {}


async def upload_to_bigquery(
    client: bigquery.Client,
    data: pd.DataFrame,
    table_id: bigquery.TableReference,
    write_disposition: str = "WRITE_APPEND",
):
    """Uploads DataFrame to BigQuery.
    Args:
        client: BigQuery client
        data: Content to upload
        table_id: Table where content has to be inserted
        write_disposition: Specifies the action if the destination table already exists
    """
    try:
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
        # non-awaitable :(, see https://github.com/googleapis/python-bigquery/issues/18
        job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
        job.result()
        logger.info(f"Upload completed with write disposition: {write_disposition}.")
    except Exception as e:
        logger.error(f"Error uploading data to BigQuery: {e}")


async def process_gas_prices_data(
    day: date, session: aiohttp.ClientSession, table_id: bigquery.TableReference
):
    if day == date.today():
        url = f"{_BASE_API_URL}/EstacionesTerrestres"
    else:
        url = f"{_BASE_API_URL}/{_TABLE_CONFIG['gas_prices']['url']}/{day.strftime('%d-%m-%Y')}"

    data = await fetch_json(url, session)
    # some days might be missing, but response is still 200 with an empty JSON
    # skip the day if content is empty
    if data:
        df = pd.DataFrame.from_dict(
            data["ListaEESSPrecio"], dtype=pd.ArrowDtype(pa.string())
        )
        # add date column
        df["date"] = pa.scalar(day, type=pa.date32())
        # rename columns to match our table schema
        df = df.rename(columns=_TABLE_CONFIG["gas_prices"]["columns"])
        df = df.replace(r"^\s*$", None, regex=True)
        return df
    return pd.DataFrame()


async def data_upload(
    client: bigquery.Client,
    dataset_ref: bigquery.DatasetReference,
    table: str,
    start_date: Optional[str],
    end_date: Optional[str],
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
            data = await fetch_json(request_url, session)

        df = pd.DataFrame.from_dict(data, dtype=pd.ArrowDtype(pa.string()))
        # select subset of columns
        df = df[_TABLE_CONFIG[table]["columns"].keys()]
        # rename columns to match our table schema
        df = df.rename(columns=_TABLE_CONFIG[table]["columns"])
        df = df.replace(r"^\s*$", None, regex=True)
        # config upload to overwrite content
        await upload_to_bigquery(client, df, table_id, "WRITE_TRUNCATE")
        logger.info(f"Data upload of {df.shape[0]} rows.")

    elif table == "gas_prices":
        logger.info("Querying gas prices.")

        dates_range: npt.NDArray[np.datetime64] = pd.date_range(
            start=date.fromisoformat(start_date) if start_date else date.today(),
            end=date.fromisoformat(end_date) if end_date else date.today(),
        ).date
        logger.info(
            f"Fetching interval between '{dates_range[0]}' and '{dates_range[-1]}'."
        )

        connector = aiohttp.TCPConnector(limit_per_host=max_connections)
        async with aiohttp.ClientSession(
            connector=connector, headers=headers
        ) as session:
            # extra loop for BQ upload, ugly workaround as a batch
            splits = list(batched(dates_range, 100))
            logger.info(f"Split amount: {len(splits)}.")

            for date_range in tqdm(splits):
                tasks = [
                    process_gas_prices_data(day, session, table_id)
                    for day in date_range
                ]
                df_list = await tqdm.gather(*tasks, leave=False)
                df = pd.concat(df_list)
                await upload_to_bigquery(client, df, table_id)
                logger.info(
                    f"Data upload of {df.shape[0]} rows, for interval between '{date_range[0]}' and '{date_range[-1]}'."
                )
    else:
        raise ValueError(f"Table {table} is not valid.")


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
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
        required=True,
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
    return parser.parse_args()


async def main():
    args = parse_arguments()
    client = (
        bigquery.Client()
        if not args.service_acc_path
        else bigquery.Client.from_service_account_json(args.service_acc_path)
    )
    dataset_ref = bigquery.DatasetReference(client.project, args.dataset)

    await data_upload(
        client,
        dataset_ref,
        args.table,
        start_date=args.start_date,
        end_date=args.end_date,
        max_connections=args.max_connections,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
