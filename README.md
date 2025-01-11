# Spanish Daily Gas Prices

Since 2007, Spanish gas stations must send their daily prices to the Spanish regulatory authorities, as described in [ITC/2308/2007](https://www.boe.es/diario_boe/txt.php?id=BOE-A-2007-14592). This data is published daily in the [Spanish Open Data Catalog](https://datos.gob.es/es/catalogo/e05068001-precio-de-carburantes-en-las-gasolineras-espanolas) and offered in different formats. However, there is no way to retrieve historical data from that endpoint, nor a bulk download per year or month, although there exists an [API](https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/help) you can query to obtain specific days. All endpoints containing `EstacionesTerrestresHist` can be retrieved using a specific date between "2007-01-01" and the present day.

Therefore, the only way to build the historical time series is to query every single day and store the results. Using this method, you end up with daily prices for all available stations, but take into consideration that a lot of these stations do not change prices on a daily basis, so many of the rows are repeated values. Getting rid of those repeated rows will net you a much smaller dataset, but having the exact value for a day will involve forward filling values and some complicated mechanics.

## Steps to reproduce

This repository uses [dbt](https://www.getdbt.com) with [BigQuery](https://cloud.google.com/bigquery), and you probably need basic familiarity with both tools and Python, but it is just a toy project that involves basic procedures and SQL primitives. If you are familiar with a different flavor of data warehouse, just set up the connection with the dbt profile and change the data upload script.

Refer to the following [tutorial](https://cloud.google.com/resource-manager/docs/creating-managing-projects) to set up a project in Google Cloud.

The project requires Python v3.12+, and my recommendation is to set up a virtual environment. For using Python with BigQuery you can follow the steps in the following [tutorial](https://codelabs.developers.google.com/codelabs/cloud-bigquery-python).

### Create source tables

Change location and names according to your needs and preferences:

```bash
$ bq --location=EU mk -d --description "Daily prices for Spanish gas stations." gas_prices_spa
$ bq mk -t --description "List of Spanish regions." gas_prices_spa.raw_regions ./table_schemas/raw_regions.json
$ bq mk -t --description "List of Spanish provinces." gas_prices_spa.raw_provinces ./table_schemas/raw_provinces.json
$ bq mk -t --description "List of Spanish municipalities." gas_prices_spa.raw_municipalities ./table_schemas/raw_municipalities.json
$ bq mk -t --description "Data of daily prices in Spanish gas stations." \
    --time_partitioning_field date \
    --time_partitioning_type MONTH \
    --require_partition_filter \
    gas_prices_spa.raw_gas_prices ./table_schemas/raw_gas_prices.json
```

### Upload source data

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS=/path_to_key.json
$ python sources_data_load.py --dataset=gas_prices_spa --table=regions
$ python sources_data_load.py --dataset=gas_prices_spa --table=provinces
$ python sources_data_load.py --dataset=gas_prices_spa --table=municipalities
$ python sources_data_load.py --dataset=gas_prices_spa --table=gas_prices --start_date=2007-01-01
```

## Automate with Cloud Run jobs

[Michael Whitaker](https://www.michaelwhitaker.com/posts/2022-06-01-cloud-run-jobs)'s blog post shows a convenient way to automate runs with Cloud Run jobs. We can adapt the process to:

1. Build a Docker image and store it in [Artifact Registry](https://cloud.google.com/artifact-registry)
2. Create a [Cloud Run job](https://cloud.google.com/run/docs/create-jobs)

### Custom Docker image in Artifact Registry

From a [Cloud Shell](https://cloud.google.com/shell/docs), create and configure an Artifact Registry repository ([official documentation](https://cloud.google.com/artifact-registry/docs/repositories/create-repos#create)):

```bash
$ gcloud artifacts repositories create data-ingestion \
    --repository-format=docker \
    --description="Raw data ingestion" \
    --location=$CLOUD_RUN_REGION
```

Then, we build the basic files for creating an image, first the `Dockerfile`:

```bash
$ cat <<EOF > Dockerfile
FROM python:3.12-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY sources_data_load.py ./

CMD [ "python", "./sources_data_load.py", "--dataset=gas_prices_spa", "--table=gas_prices" ]
EOF
```

We build a repository to store the cloud artifacts:

```bash
$ gcloud artifacts repositories create data-ingestion \
    --repository-format=docker \
    --location=$CLOUD_RUN_REGION \
    --description="Cloud artifacts for data ingestion tasks." \
    --disable-vulnerability-scanning
```

Now we register the image with:

```bash
$ gcloud builds submit \
    --tag $CLOUD_RUN_REGION-docker.pkg.dev/$PROJECT_ID/data-ingestion/gas-prices-spa
```

### Cloud Run job

See [official documentation](https://cloud.google.com/run/docs/create-jobs) for more details:

```bash
$ gcloud run jobs create gas-prices-spa \
    --image $CLOUD_RUN_REGION-docker.pkg.dev/$PROJECT_ID/data-ingestion/gas-prices-spa \
    --max-retries=1 \
    --parallelism=1 \
    --region=$CLOUD_RUN_REGION
```
### Execute the Cloud Run job on a schedule

See [official documentation](https://cloud.google.com/run/docs/execute/jobs-on-schedule) for more details:

```bash
$ gcloud projects describe $PROJECT_ID --format="value(projectNumber)"
```

```bash
$ gcloud scheduler jobs create http gas-prices-spa \
  --location $SCHEDULER_REGION \
  --schedule="0 18 * * *" \
  --uri="https://$CLOUD_RUN_REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/gas-prices-spa:run" \
  --http-method POST \
  --oauth-service-account-email $PROJECT_NUMBER-compute@developer.gserviceaccount.com
```
