from __future__ import print_function
import gate_api
from gate_api.exceptions import ApiException, GateApiException
import pandas as pd

# Defining the host is optional and defaults to https://api.gateio.ws/api/v4
# See configuration.py for a list of all supported configuration parameters.
configuration = gate_api.Configuration(host="https://api.gateio.ws/api/v4")
api_client = gate_api.ApiClient(configuration)

# Create an instance of the API class
api_instance = gate_api.SpotApi(api_client)

try:
    # Retrieve ticker information
    api_response = api_instance.list_tickers()

    col_names = []
    rows = []
    for i, m in enumerate(api_response):
        if i == 0:
            col_names = [k for k in m.attribute_map.keys()]
        row = [getattr(m, cn) for cn in col_names]
        rows.append(row)

    df = pd.DataFrame(rows)
    df.columns = col_names
    df.to_csv("~/Desktop/tickers.csv", sep="\t", index=False)

except GateApiException as ex:
    print(f"Gate api exception, label: {ex.label}, message: {ex.message}")
except ApiException as e:
    print(f"Exception when calling SpotApi->list_tickers: {e}\n")
