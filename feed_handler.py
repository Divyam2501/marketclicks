import os
import warnings

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

from dotenv import load_dotenv
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from typing import List

load_dotenv()

api_key = os.environ["POLYGON_API_KEY"]

client = WebSocketClient(
	api_key=api_key,
	feed=Feed.Delayed,
	market=Market.Stocks
	)

# trades
client.subscribe("T.AAPL")        # stocks trades
# client.subscribe("XT.BTC-USD")  # crypto trades use XT prefix
# client.subscribe("T.*")  # all trades
# client.subscribe("T.TSLA") # single tickers
# client.subscribe("T.TSLA", "T.UBER") # multiple tickers

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

# print messages
client.run(handle_msg)
