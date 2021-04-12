# general
import datetime
import os

# data
from google.cloud.bigquery import Client
from dataclasses import dataclass, fields, field
import pandas as pd
import gate_api.models.ticker as gticker
import gate_api.models.currency as gcurrency


class BQClient:
    def __init__(self):
        # check for bq creds json file in environment vars first,
        # or then look in local directory (for deployed cloud functions)
        try:
            self.bq_cred_path = os.environ["AUGUR_BLACK_CREDS"]
        except:
            self.bq_cred_path = "./secrets/db_creds.json"

        self.bq_client = Client.from_service_account_json(self.bq_cred_path)


@dataclass
class Ticker:
    retrieved_at: datetime.datetime
    currency_pair: str = ""
    last: float = -1.0
    lowest_ask: float = -1.0
    highest_bid: float = -1.0
    change_percentage: float = -1.0
    base_volume: float = -1.0
    quote_volume: float = -1.0
    high_24h: float = -1.0
    low_24h: float = -1.0
    etf_net_value: float = -1.0
    etf_pre_net_value: float = -1.0
    etf_pre_timestamp: float = -1.0
    etf_leverage: float = -1.0


@dataclass
class Currency:
    retrieved_at: datetime.datetime = datetime.datetime.utcnow()
    delisted: bool = False
    withdraw_disabled: bool = False
    withdraw_delayed: bool = False
    deposit_disabled: bool = False
    trade_disabled: bool = False
    retrieved_at: bool = False
    currency: str = ""


def prepare_to_float(value):
    if value is None:
        return -1.0
    if isinstance(value, str):
        if not value:
            return 0.0
        else:
            return float(value)
    elif isinstance(value, int):
        return float(value)
    else:
        return -1.0


def prepare_to_datetime(value):
    """Rounds and stringifies a datetime object to the nearest second"""
    x = value - datetime.timedelta(microseconds=value.microsecond)
    return str(x)


def prepare_to_string(value):
    return str(value)


def prepare_to_bool(value):
    return bool(value)


prep_funcs = {
    float: prepare_to_float,
    datetime.datetime: prepare_to_datetime,
    str: prepare_to_string,
    bool: prepare_to_bool,
}


def set_vals_from_gateio_instance(ginstance, crypto_instance):

    # loop over attributes in the source instance
    for k in ginstance.attribute_map.keys():

        # get the value of the attr from the source instance
        val = getattr(ginstance, k)
        # get the dtype of the target attr in the *destination* instance
        destination_dtype = type(getattr(crypto_instance, k))

        # transform the attr value of the source instance to the type of
        # of the target attr in the destination instance
        xform_val = prep_funcs[destination_dtype](val)

        # set the attr in the destination instance
        setattr(crypto_instance, k, xform_val)

    crypto_instance.retrieved_at = prep_funcs[datetime.datetime](
        datetime.datetime.utcnow()
    )
    return crypto_instance


def make_data_array(data, source):
    """
    Makes an array of instances from an API source

    Args:
      data: (list) List of data objects
      source: (str) identifies the API/provider the data comes from (and thus its structure)
    """
    if data is None or len(data) == 0:
        print("No data provided")

    # return data goes here
    out_rows = []

    if source == "gateio":

        # get the type of the first instance
        first_instance = data[0]

        if isinstance(first_instance, gticker.Ticker):

            # for all rows, convert them to one of
            # the instances in this project
            for row in data:
                crypto_obj = set_vals_from_gateio_instance(row, Ticker())
                out_rows.append(crypto_obj)

        elif isinstance(first_instance, gcurrency.Currency):
            # for all rows, convert them to one of
            # the instances in this project
            for row in data:
                crypto_obj = set_vals_from_gateio_instance(row, Currency())
                out_rows.append(crypto_obj)

    return out_rows


def stream_data_to_bigquery(
    data_li=[], chunk_max_rows=1000, table_id="",
):
    """
    Writes review data to bigquery table via streaming.

    Args:
        data_li: (list) One or more dataclass instances.
        chunk_max_rows: (int) maximum numbers of rows for a single streaming request.
        table_id: (str) the qualified table name to which rows will be written.
    Returns:
        errors: (list) list of streaming errors.
    """

    all_rows = []
    row_chunk = []

    # iterate over dataclass instances in the list
    for d in data_li:

        # if size limit reached, create a new `row_chunk`
        if len(row_chunk) == chunk_max_rows:
            all_rows.append(row_chunk)
            row_chunk = []

        # by 'schema' we just mean the field names
        dc_schema = [f.name for f in fields(d)]

        # make a single row dict
        row = {f: getattr(d, f) for f in dc_schema}
        row_chunk.append(row)

    all_rows.append(row_chunk)

    # instantinate bigquery client instance
    client = BQClient()

    # iterate over chunks, writing each to the destination table
    errors = []
    for chunk in all_rows:
        errors += client.bq_client.insert_rows_json(
            table_id, chunk, row_ids=[None] * len(chunk)
        )
    return errors
