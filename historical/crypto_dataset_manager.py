import pandas as pd
from pyarrow import compute
from pyarrow.ipc import new_file
import pyarrow.parquet as pq
import pyarrow as pa
import os.path
import logging
import json
import time
import math
from os import listdir
from os import remove
from binance.client import Client
from binance.exceptions import BinanceRequestException, BinanceAPIException
import concurrent.futures


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
        'open_time': 'datetime64[ms]',
        'open': 'float32',
        'high': 'float32',
        'low': 'float32',
        'close': 'float32',
        'volume': 'float32',
        'close_time': 'datetime64[ms]',
        'quote_asset_volume': 'float32',
        'number_of_trades': 'int64',
        'taker_buy_base_asset_volume': 'float32',
        'taker_buy_quote_asset_volume': 'float32',
        'ignore': 'float64'
    }

    def __init__(self, foldername=PARQUETFOLDERNAME, con_max_size=20):
        # Set conneciton pool max size
        # self._http_connection_pool_inject(maxsize=con_max_size)
        # Start binance clietn
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
        "Gets saved historical parquet data or create new file and return it."
        filename = f"{sym_pair[0]}-{sym_pair[1]}.parquet"
        filepath = f"./{self.PARQUETFOLDERNAME}/{filename}"
        logging.info(f"Reading in parquet file: {filepath}")
        if os.path.exists(filepath):
            try:
                ret = pq.read_table(filepath)
                return ret
            except:
                # Unable to read file, probably corrupted. Delete file.
                remove(filepath)
        # Create empty and return if can't read file.
        df = pd.DataFrame(columns=self.LABELS)
        df = df.astype(self.DATATYPES)
        df.drop(['close_time', 'ignore'], axis=1, inplace=True)
        tb = pa.Table.from_pandas(df,  preserve_index=False)
        self._write_parquet(sym_pair, tb, timestamp="ms")
        return tb

    def _write_parquet(self, sym_pair, pa_tb, append="", timestamp=None):
        "Save a parquet file to disk using Pyarrow."
        filename = f"{sym_pair[0]}-{sym_pair[1]}{append}.parquet"
        logging.info("Saving as: " + filename)
        filepath = f"./{self.PARQUETFOLDERNAME}/{filename}"
        pq.write_table(pa_tb, filepath, coerce_timestamps=timestamp)

    def update_data(self, pairs=None, threads=6, counter_limit=50):
        """Update parquet files with new data. Defaults to updating all symbol pairs unless given list of specified pairs.
        Splits data into parts to update seperately on different threads. Default 6 threads.
        """
        if pairs == None:
            pairs = self.symbol_pairs
        batches = self.split_batches(pairs, threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as exec:
            for batch in batches:
                exec.submit(self._batch_update, batch, counter_limit)

    def split_batches(self, list, batch_size):
        "Splits list into list of list and returns it."
        llen = len(list)
        pairs_batch_size = int(math.ceil(llen/batch_size))
        pair_batches = [list[n:n+pairs_batch_size]
                        for n in range(0, llen, pairs_batch_size)]
        return pair_batches

    def _batch_update(self, pairs, counter_limit):
        "Updates a batch of symbol pairs. Returns True when whole batch is done."
        for pair in pairs:
            sym = f"{pair[0]}{pair[1]}"
            symbol_pair_pa_tb = self._get_saved_parquet(pair)
            last_timestamp = self._grab_max_time(symbol_pair_pa_tb)
            # Get current time in seconds. Convert to milli and subtract 10 minutes
            current_timestamp = time.time() * 1000 - 600000

            counter = 1
            while int(last_timestamp) < current_timestamp:
                logging.info(
                    f"Updating: {sym} | Latest T: {last_timestamp} | Current T: {current_timestamp}")
                # Request from binance
                try:
                    new_data = self._get_klines(sym, last_timestamp)
                    logging.info(f"Got kline data...")
                    if new_data.num_rows == 0:
                        logging.info(
                            f"Breaking out of loop. Done getting data for {sym}.")
                        break
                except BinanceAPIException as e:
                    logging.debug(e)
                except BinanceRequestException as e:
                    logging.debug(e)
                finally:
                    symbol_pair_pa_tb = pa.concat_tables(
                        [symbol_pair_pa_tb, new_data])
                    last_timestamp = self._grab_max_time(new_data)
                    # Write to file every counter cycle
                    if counter == counter_limit:
                        counter = 0
                        self._write_parquet(pair, symbol_pair_pa_tb)
                    counter += 1
            # Last write if loop breaks before counter_limit
            self._write_parquet(pair, symbol_pair_pa_tb)
        return True

    def _grab_max_time(self, pa_tb):
        "Return max open timestamp of or a default start date of 01/01/1990."
        if pa_tb.num_rows == 0:
            # Monday, January 1, 1990 12:00:00 AM GMT in UTC milliseconds
            return str(631152000000)
        # milliseconds
        max_time = pa.compute.max(pa_tb["open_time"]).value
        return str(max_time + 1)

    def _get_klines(self, sym, start, end=None):
        "Update given symbol pair and returns a pyarrow table of the klines."
        # Request
        response = self.binance_client.get_klines(
            symbol=sym, interval=Client.KLINE_INTERVAL_1MINUTE, limit=1000, startTime=start)
        updatedPart = pd.DataFrame(response, columns=self.LABELS)
        # convert to pyarrow table and return.
        return self._clean_dataframe(updatedPart)

    def _clean_dataframe(self, df):
        "Clean dataframe and return a pyarrow table."
        pa_schema = pa.schema([
            pa.field("open_time", pa.timestamp("ms")),
            pa.field("open", pa.float32()),
            pa.field("high", pa.float32()),
            pa.field("low", pa.float32()),
            pa.field("close", pa.float32()),
            pa.field("volume", pa.float32()),
            pa.field("quote_asset_volume", pa.float32()),
            pa.field("number_of_trades", pa.uint16()),
            pa.field("taker_buy_base_asset_volume", pa.float32()),
            pa.field("taker_buy_quote_asset_volume", pa.float32()),
        ])
        df = df.astype(self.DATATYPES)
        df.drop(['close_time', 'ignore'], axis=1, inplace=True)
        tb = pa.Table.from_pandas(df, preserve_index=False)
        return tb.cast(pa_schema)

    def delete_sym_pair(self, sym_pair):
        "Delete sym pair file."
        filename = f"{sym_pair[0]}-{sym_pair[1]}.parquet"
        filepath = f"./{self.PARQUETFOLDERNAME}/{filename}"
        logging.info(f"Deleting file for {sym_pair}")
        remove(filepath)

    def _get_klines_rf(self, symbol, interval, limit, startTime):
        "Sends a kline request directly using requests instead of binance package."
        pass

    def _http_connection_pool_inject(self, **constructor_kwargs):
        """
        Changed urllib3's connectionpool default params for when the connection cannot be changed.
        Arguments are same as would be for class urllib3.HTTPConnectionPool

        RUN BEFORE ANY CONNECTION IS MADE.
        Usage:
        patch_http_connection_pool(maxsize=16)
        """
        from urllib3 import connectionpool, poolmanager

        class HTTPConnectionPoolInject(connectionpool.HTTPConnectionPool):
            def __init__(self, *args, **kwargs):
                kwargs.update(constructor_kwargs)
                super(HTTPConnectionPoolInject, self).__init__(*args, **kwargs)

        poolmanager.pool_classes_by_scheme['http'] = HTTPConnectionPoolInject
        poolmanager.pool_classes_by_scheme['https'] = HTTPConnectionPoolInject


def main():
    logging.basicConfig(level=logging.DEBUG)
    dataset_manager = CryptoDatasetManager()
    dataset_manager.update_data(threads=10)


if __name__ == "__main__":
    main()
