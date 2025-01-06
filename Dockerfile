FROM python:3.12-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY sources_data_load.py ./

CMD [ "python", "./sources_data_load.py", "--dataset=gas_prices_spa", "--table=gas_prices" ]
