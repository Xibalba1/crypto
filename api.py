import gate_api
from gate_api.exceptions import ApiException, GateApiException
from db import make_data_array, stream_data_to_bigquery

configuration = gate_api.Configuration(host="https://api.gateio.ws/api/v4")
api_client = gate_api.ApiClient(configuration)

api_instance = gate_api.SpotApi(api_client)


def request_data(request_type):
    try:
        if request_type == "list_tickers":
            # retrieve ticker information
            return api_instance.list_tickers()
        elif request_type == "list_currencies":
            return api_instance.list_currencies()
    except Exception as e:
        if isinstance(e, GateApiException):
            print(f"Gate api exception: {e}")
        elif isinstance(e, ApiException):
            print(f"Exception when calling SpotApi->list_tickers: {e}\n")
        else:
            print(f"Exception occurred: {e}\n")
        return None


if __name__ == "__main__":
    api_data = request_data("list_currencies")
    prep_data = make_data_array(api_data, "gateio")
    errors = stream_data_to_bigquery(
        prep_data, table_id="augur-black.gateio.currency_landing"
    )
    print(errors)
