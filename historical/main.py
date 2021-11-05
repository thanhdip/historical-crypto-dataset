import pandas as pd
import os.path
import pyarrow
from binance import AsyncClient
from binance.client import Client
from binance import AsyncClient, BinanceSocketManager

PARQUETFOLDERNAME = "data_archive"

LABELS = [
    'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'close_time',
    'quote_asset_volume',
    'number_of_trades',
    'taker_buy_base_asset_volume',
    'taker_buy_quote_asset_volume'
]


def all_symbol_pairs(client):
    "Gets exchange info from Binance and returns a list of symbol pairs as tuples"
    SYMBOLS = "symbols"
    BASEASSET = "baseAsset"
    QUOTEASSET = "quoteAsset"
    info = client.get_exchange_info()
    symbol_pairs = [(sym[BASEASSET], sym[QUOTEASSET]) for sym in info[SYMBOLS]]
    return symbol_pairs


def saved_parquet(sym_pair):
    "Gets saved historical parquet data or create new file."
    filename = f"{sym_pair[0]}-{sym_pair[1]}.parquet"
    filepath = f"./{PARQUETFOLDERNAME}/{filename}"
    if os.path.exists(filepath):
        return pd.read_parquet(filepath)
    else:
        df = pd.DataFrame(columns=LABELS)
        df.to_parquet(filepath, index=False)
        return df


def save_parquet(sym_pair, df, append=""):
    filename = f"{sym_pair[0]}-{sym_pair[1]}{append}.parquet"
    print("Saving as: " + filename)
    filepath = f"./{PARQUETFOLDERNAME}/{filename}"
    df.to_parquet(filepath, index=False)


def main():
    # initialise the binance api client
    binance_client = Client()
    all_pairs = all_symbol_pairs(binance_client)

    for pair in all_pairs:
        sym = f"{pair[0]}{pair[1]}"
        saved_df = saved_parquet(pair)
        print(sym)
        if saved_df.empty:
            start = "1 Jan, 1990"
        else:
            # Seconds to milliseconds
            start = str(saved_df["open_time"].max().timestamp() * 1000)
        for kline in binance_client.get_historical_klines_generator(
                sym, Client.KLINE_INTERVAL_1MINUTE, start):
            # Ignore last data from kline
            saved_df.append(
                pd.Series(kline[:11], index=saved_df.columns), ignore_index=True)
        save_parquet(pair, saved_df, "test")


if __name__ == "__main__":
    main()
