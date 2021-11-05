import asyncio
import pandas as pd
from binance import AsyncClient
from binance.client import Client


async def main():
    # initialise the client
    client = await AsyncClient.create()
    res = await client.get_exchange_info()
    print(client.response.headers)

    await client.close_connection()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
