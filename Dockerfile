FROM python:3.10-slim

WORKDIR /dbt
COPY script.sh ./
COPY requirements.txt ./
# the directory itself is not copied, just its contents
COPY dbt-gas-prices ./

RUN pip install --no-cache-dir -r requirements.txt

CMD ["./script.sh"]
