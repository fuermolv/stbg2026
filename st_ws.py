import json
import threading
import time
import websocket
from nacl.signing import SigningKey



class StandXWSBase:
    def __init__(self, name, ws_url="wss://perps.standx.com/ws-stream/v1", reconnect_sleep=1):
        self.name = name
        self.ws_url = ws_url
        self.reconnect_sleep = reconnect_sleep
        self._ws = None
        self._stop = False


    def start_in_thread(self, daemon=True):
        t = threading.Thread(target=self.start, daemon=daemon)
        t.start()
        return t

    def start(self):
        self._stop = False
        while not self._stop:
            self._ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._ws.run_forever()
            if not self._stop:
                time.sleep(self.reconnect_sleep)

    def stop(self):
        self._stop = True
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
    
    def _on_error(self, ws, error):
        print(f"{self.name} ws error:", error)

    def _on_close(self, ws, close_status_code, close_msg):
        print(f"{self.name} ws closed: code={close_status_code} msg={close_msg}")

    def _on_open(self, ws):
        raise NotImplementedError()

    def _on_message(self, ws, message):
        raise NotImplementedError()


class StandXPriceWS(StandXWSBase):
    def __init__(
        self,
        setter,
        symbol="BTC-USD",
        ws_url="wss://perps.standx.com/ws-stream/v1",
        reconnect_sleep=1,
    ):
        super().__init__("price", ws_url, reconnect_sleep)
        self.symbol = symbol
        self.setter = setter

   
    def _on_open(self, ws):
        ws.send(
            json.dumps(
                {
                    "subscribe": {
                        "channel": "price",
                        "symbol": self.symbol,
                    }
                }
            )
        )

    def _on_message(self, ws, message):
        msg = json.loads(message)
        if msg.get("channel") == "price":
            data = msg.get("data")
            self.setter(data)
        else:
            print("price ws other message:", msg)


class StandXPositionWS(StandXWSBase):
    def __init__(
        self,
        setter,
        access_token,
        symbol="BTC-USD",
        ws_url="wss://perps.standx.com/ws-stream/v1",
        reconnect_sleep=1,
    ):
        super().__init__("position", ws_url, reconnect_sleep)
        self.symbol = symbol
        self.setter = setter
        self.access_token = access_token

   
    def _on_open(self, ws):
        auth_msg = {
            "auth": {
                "token": self.access_token,
                "streams": [{"channel": "position"}]
            }
        }
        ws.send(json.dumps(auth_msg))


    def _on_message(self, ws, message):
        msg = json.loads(message)
        ch = msg.get("channel")
        if ch == "position":
            p = msg.get("data", {})
            self.setter(p)
            return
        else:
            print("position ws other message:", msg)





class BinancePriceWS(StandXWSBase):
    """
    Binance spot bookTicker via raw stream URL.

    URL format:
      wss://stream.binance.com:9443/ws/<streamName>

    Example:
      wss://stream.binance.com:9443/ws/btcusdt@bookTicker
    """

    def __init__(self, setter, symbol="btcusdt", reconnect_sleep=1):
        self.symbol = symbol.lower()
        self.setter = setter
        ws_url = "wss://data-stream.binance.vision/ws/btcusdt@bookTicker"
        super().__init__("binance_book_ticker", ws_url, reconnect_sleep)

    def _on_open(self, ws):
        print("binance ws opened")

    def _on_message(self, ws, message):
        # 直接打印，验证是否能收到
        msg = json.loads(message)
        self.setter(msg)




bn_price = None

if __name__ == "__main__":
    with open("standx_beggar_auth.json", "r") as f:
        auth_json = json.load(f)
        auth = {
            'access_token': auth_json['access_token'],
            'signing_key': SigningKey(bytes.fromhex(auth_json['signing_key'])),
        }

    def ser_bn_price(msg):
        global bn_price
        mid_price = (float(msg['a']) + float(msg['b'])) / 2
        bn_price = format(mid_price, '.4f')


    # ws = StandXPriceWS(set_price)
    # ws.start_in_thread()

    # pos_ws = StandXPositionWS(
    #     set_position,
    #     access_token=auth['access_token'],
    # )
    # pos_ws.start_in_thread()
    ws = BinancePriceWS(setter=ser_bn_price)
    ws.start_in_thread()

    while True:
        time.sleep(1)
        print('----------------------------------')
        print(bn_price)
     
