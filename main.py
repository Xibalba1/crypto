from api import request_data
from db import make_data_array, stream_data_to_bigquery


def request_and_write_gateio_currencies(data, context):
    api_data = request_data("list_currencies")
    prep_data = make_data_array(api_data, "gateio")
    errors = stream_data_to_bigquery(
        prep_data, table_id="augur-black.gateio.currency_landing"
    )
    print(errors)


def request_and_write_gateio_tickers(data, context):
    api_data = request_data("list_tickers")
    prep_data = make_data_array(api_data, "gateio")
    errors = stream_data_to_bigquery(
        prep_data, table_id="augur-black.gateio.ticker_landing"
    )
    print(errors)
