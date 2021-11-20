import pandas as pd
import os.path
import logging
import json
import time
from os import listdir
from binance.client import Client
from binance.exceptions import BinanceRequestException, BinanceAPIException


class CryptoDatasetManager():
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
        'taker_buy_quote_asset_volume',
        'ignore'
    ]

    DATATYPES = {
        'open_time': 'datetime64[ns]',
        'open': 'float32',
        'high': 'float32',
        'low': 'float32',
        'close': 'float32',
        'volume': 'float32',
        'close_time': 'datetime64[ns]',
        'quote_asset_volume': 'float32',
        'number_of_trades': 'uint16',
        'taker_buy_base_asset_volume': 'float32',
        'taker_buy_quote_asset_volume': 'float32',
        'ignore': 'float64'
    }

    def __init__(self, foldername=PARQUETFOLDERNAME):
        self.binance_client = Client()
        self.PARQUETFOLDERNAME = foldername
        self.get_existing_symbol_pairs()

    def save_metadata(self):
        filename = "metadata.json"
        filepath = f"./{self.PARQUETFOLDERNAME}/{filename}"
        with open(filepath) as metadatafile:
            data = {}
            data["symbol_pair_count"] = len(self.symbol_pairs)
            json.dump(data, metadatafile)

    def get_existing_symbol_pairs(self):
        "Gets existing symbol pairs in archived folder."
        filepath = f"./{self.PARQUETFOLDERNAME}"
        all_symbol_pairs = [(os.path.splitext(f)[0].split("-")[0], os.path.splitext(f)[0].split("-")[1])
                            for f in listdir(filepath) if os.path.isfile(os.path.join(filepath, f))]
        self.symbol_pairs = all_symbol_pairs

    def update_symbol_pairs(self):
        "Gets exchange info from Binance and sets a list of symbol pairs as tuples. Accessed by self.symbol_pairs"
        SYMBOLS = "symbols"
        BASEASSET = "baseAsset"
        QUOTEASSET = "quoteAsset"
        info = self.binance_client.get_exchange_info()
        symbol_pairs = [(sym[BASEASSET], sym[QUOTEASSET])
                        for sym in info[SYMBOLS]]
        self.symbol_pairs = symbol_pairs

    def _get_saved_parquet(self, sym_pair):
        "Gets saved historical parquet data or create new file."
        filename = f"{sym_pair[0]}-{sym_pair[1]}.parquet"
        filepath = f"./{self.PARQUETFOLDERNAME}/{filename}"
        logging.debug(f"Reading in parquet file: {filepath}")
        if os.path.exists(filepath):
            return pd.read_parquet(filepath)
        else:
            df = pd.DataFrame(columns=self.LABELS[:-1])
            df.to_parquet(filepath, index=False)
            return df

    def _write_parquet(self, sym_pair, df, append=""):
        "Save a parquet file to disk from a pandas dataframe."
        filename = f"{sym_pair[0]}-{sym_pair[1]}{append}.parquet"
        logging.debug("Saving as: " + filename)
        filepath = f"./{self.PARQUETFOLDERNAME}/{filename}"
        df.to_parquet(filepath, index=False)

    def update_data(self, pairs=None):
        "Update parquet files with new data. Defaults to updating all symbol pairs unless given list of specified pairs."
        if pairs == None:
            pairs = self.symbol_pairs
        for pair in pairs:
            sym = f"{pair[0]}{pair[1]}"
            symbol_pair_df = self._get_saved_parquet(pair)
            last_timestamp = self._grab_max_time(symbol_pair_df)
            # Get current time in seconds. Convert to milli and subtract 5 minutes
            current_timestamp = time.time() * 1000 - 300000

            counter = 0
            while int(last_timestamp) < current_timestamp:
                logging.debug(
                    f"Updating: {sym} | Latest timestamp: {last_timestamp}")

                # Request from binance
                try:
                    new_data = self._get_klines(sym, last_timestamp)
                except BinanceAPIException as e:
                    print(e)
                except BinanceRequestException as e:
                    print(e)
                finally:
                    symbol_pair_df = symbol_pair_df.append(new_data)
                    last_timestamp = self._grab_max_time(symbol_pair_df)
                    # Write to file every five cycles
                    if counter == 5:
                        counter = 0
                        self._write_parquet(pair, symbol_pair_df)
                    counter += 1

    def _grab_max_time(self, df):
        "Return max open timestamp of or a default start date of 01/01/1990."
        if df.empty:
            # Monday, January 1, 1990 12:00:00 AM GMT in UTC milliseconds
            return str(631152000000)
        # Seconds to milliseconds
        return str(int(df["open_time"].max().timestamp() * 1000 + 1))

    def _get_klines(self, sym, start, end=None):
        "Update given symbol pair and returns a DataFrame of the klines."
        # Request
        response = self.binance_client.get_klines(
            symbol=sym, interval=Client.KLINE_INTERVAL_1MINUTE, limit=1000, startTime=start)
        updatedPart = pd.DataFrame(response, columns=self.LABELS)
        # Milliseconds to nanoseconds
        updatedPart["open_time"] = 1000000 * updatedPart["open_time"]
        updatedPart = updatedPart.astype(self.DATATYPES)
        # Drop useless columns
        updatedPart.drop(['close_time', 'ignore'], axis=1, inplace=True)
        return updatedPart


def main():
    logging.basicConfig(level=logging.DEBUG)
    dataset_manager = CryptoDatasetManager()
    dataset_manager.update_data([("BTC", "USDT")])


if __name__ == "__main__":
    main()
