import pyarrow.parquet as pq
import pyarrow as pa
import os.path
import logging
import json
import time
import math
import concurrent.futures
from os import listdir
from os import remove
from binance.client import Client
from binance.exceptions import BinanceRequestException, BinanceAPIException


class CryptoDatasetManager():

    PA_TYPES = [pa.timestamp("ms"), pa.float32(), pa.float32(), pa.float32(), pa.float32(), pa.float32(),
                pa.timestamp("ms"), pa.float32(), pa.uint16(), pa.float32(), pa.float32(), pa.float32()]

    PA_NAMES = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume",
                "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"]

    def __init__(self, foldername="data_archive", con_max_size=20):
        # Prep pyarrow schema
        pa_field = [pa.field(g[0], g[1])
                    for g in zip(self.PA_NAMES, self.PA_TYPES)]
        pa_schema = pa.schema(pa_field)
        self.PA_SCHEMA = pa_schema
        # Start binance client
        try:
            self.binance_client = Client()
        except BinanceAPIException(e):
            logging.debug(f"Unable to connect to Binance servers.")
            logging.debug(e)

        self.PARQUETFOLDERNAME = foldername
        self.symbol_pairs = self.get_existing_symbol_pairs()

    def save_metadata(self):
        "Save metadata as a json."
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
        return all_symbol_pairs

    def update_symbol_pairs(self):
        "Gets exchange info from Binance and sets a list of symbol pairs as tuples. Accessed by self.symbol_pairs."
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
        # Create empty pa table.
        tb = self._to_pa_table([])
        self._write_parquet(sym_pair, tb, timestamp="ms")
        return tb

    def _write_parquet(self, sym_pair, pa_tb, append="", timestamp=None):
        "Save a parquet file to disk using Pyarrow."
        filename = f"{sym_pair[0]}-{sym_pair[1]}{append}.parquet"
        logging.info("Saving as: " + filename)
        filepath = f"./{self.PARQUETFOLDERNAME}/{filename}"
        pq.write_table(pa_tb, filepath, coerce_timestamps=timestamp)

    def update_data(self, pairs=None, threads=6, counter_limit=50, sleep_time=120):
        """Update parquet files with new data. Defaults to updating all symbol pairs unless given list of specified pairs.
        Splits data into parts to update seperately on different threads. Default 6 threads.
        """
        if pairs == None:
            pairs = self.symbol_pairs
        batches = self.split_batches(pairs, threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as exec:
            for batch in batches:
                exec.submit(self._batch_update, batch,
                            counter_limit, sleep_time)

    def split_batches(self, list, batch_size):
        "Splits list into list of list and returns it."
        llen = len(list)
        pairs_batch_size = int(math.ceil(llen/batch_size))
        pair_batches = [list[n:n+pairs_batch_size]
                        for n in range(0, llen, pairs_batch_size)]
        return pair_batches

    def _batch_update(self, pairs, counter_limit, sleep_time):
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
                # Wait if exception is raised. Prob due to request speed.
                except BinanceRequestException as e:
                    logging.debug(e)
                    time.sleep(sleep_time)
                except BinanceAPIException as e:
                    logging.debug(e)
                    time.sleep(sleep_time)
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
        # convert to pyarrow table and return.
        return self._to_pa_table(response)

    def _to_pa_table(self, response_list):
        "Clean response data and return a pyarrow table."
        if response_list == []:
            ret = pa.table(
                [[] for _ in range(0, len(self.PA_SCHEMA))], schema=self.PA_SCHEMA)
            return self._clean_table(ret)

        # Create pyarrow array of columns from rows
        list_of_cols = self._transpose_list(response_list)
        pa_cols = [pa.array([float(data) for data in list_of_cols[n]], self.PA_TYPES[n])
                   for n in range(0, len(self.PA_TYPES))]
        # Create pyarrow table
        pa_table = pa.table(pa_cols, schema=self.PA_SCHEMA)
        # Drop unneccesary columns.
        return self._clean_table(pa_table)

    def _clean_table(self, table):
        "Clean table of uneccesary columns and return new table."
        try:
            table = table.drop(["close_time", "ignore"])
        except:
            pass
        return table

    def _transpose_list(self, ll):
        "Given a list of list, transpose the list. I.e list of rows turns to a list of columns."
        return [list(row) for row in zip(*ll)]

    def delete_sym_pair(self, sym_pair):
        "Delete sym pair file."
        filename = f"{sym_pair[0]}-{sym_pair[1]}.parquet"
        filepath = f"./{self.PARQUETFOLDERNAME}/{filename}"
        logging.info(f"Deleting file for {sym_pair}")
        remove(filepath)


def main():
    logging.basicConfig(level=logging.DEBUG)
    dataset_manager = CryptoDatasetManager()
    dataset_manager.update_symbol_pairs()
    dataset_manager.update_data(threads=3)


if __name__ == "__main__":
    main()
