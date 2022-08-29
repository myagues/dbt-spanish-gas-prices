# Spanish Daily Gas Prices

Since 2007, Spanish gas stations must send their daily prices to the Spanish regulatory authorities, as described in [ITC/2308/2007](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2007-14592). This data is published daily in the [Spanish Open Data Catalog](https://datos.gob.es/es/catalogo/e05068001-precio-de-carburantes-en-las-gasolineras-espanolas) and offered in different formats. However, there is no way to retrieve historical data from that endpoint, nor a bulk download per year or month, although there exists an [API](https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/help) you can query to obtain specific days. All endpoints containing `EstacionesTerrestresHist` can be retrieved using a specific date between "2007-01-01" and the present day.

Therefore, the only way to build the historical time series is to query every single day and store the results. Using this method, you end up with daily prices for all available stations, but take into consideration that a lot of these stations do not change prices on a daily basis, so many of the rows are repeated values. Getting rid of those repeated rows will net you a much smaller dataset, but having the exact value for a day will involve forward filling values and some complicated mechanics.

## Steps to reproduce

This repository uses [dbt](https://www.getdbt.com) with [BigQuery](https://cloud.google.com/bigquery), and you probably need basic familiarity with both tools and Python, but it is just a toy project that involves basic procedures and SQL primitives. If you are familiar with a different flavor of data warehouse, just set up the connection with the dbt profile and change the data upload script.

Refer to the following [tutorial](https://cloud.google.com/resource-manager/docs/creating-managing-projects) to set up a project in Google Cloud.

The project requires Python v3.9+, and my recommendation is to set up a virtual environment. For using Python with BigQuery you can follow the steps in the following [tutorial](https://codelabs.developers.google.com/codelabs/cloud-bigquery-python).

### Create source tables

Change location and names according to your needs and preferences:

```bash
$ bq --location=EU mk -d --description "Daily prices for Spanish gas stations." gas_prices_esp

$ bq mk -t --description "List of Spanish regions." gas_prices_esp.raw_regions ./table_schemas/regions.json
$ bq mk -t --description "List of Spanish provinces." gas_prices_esp.raw_provinces ./table_schemas/provinces.json
$ bq mk -t --description "List of Spanish municipalities." gas_prices_esp.raw_municipalities ./table_schemas/municipalities.json
$ bq mk -t --description "Data of daily gas prices in Spanish gas stations from API." \
    --time_partitioning_field date \
    --time_partitioning_type MONTH \
    gas_prices_esp.raw_gas_prices ./table_schemas/gas_prices.json
```

### Upload source data

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS=/path_to_key.json
$ python sources_data_load.py --dataset=gas_prices_esp --table=regions
$ python sources_data_load.py --dataset=gas_prices_esp --table=provinces
$ python sources_data_load.py --dataset=gas_prices_esp --table=municipalities
$ python sources_data_load.py --dataset=gas_prices_esp --table=gas_prices --start_date=2007-01-01
```

To automate the process follow the steps in [@myagues/dbt-eu-oil-bulletin](https://github.com/myagues/dbt-eu-oil-bulletin).
