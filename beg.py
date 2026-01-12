
import json
import time
from nacl.signing import SigningKey
from backoff import CancelBackoff
import signal
import argparse
from st_ws import StandXPriceWS, StandXPositionWS
from st_http import cancel_orders
from zoneinfo import ZoneInfo
from datetime import datetime
from config import SKIP_HOUR_START, SKIP_HOUR_END
from common import create_orders, clean_positions, clean_orders


def _on_term(signum, frame):
    global _should_exit
    _should_exit = True

signal.signal(signal.SIGTERM, _on_term)
signal.signal(signal.SIGINT, _on_term)


BPS = 20
MIN_BPS = 10
MAX_BPS = 30
SIDE = "sell"

_should_exit = False
st_price_dict = None
st_position = None


def main(position, auth):
    backoff = CancelBackoff()
    print(f"Starting beggar with position size: {position}")
    def set_price(p):
        global st_price_dict
        st_price_dict = p

    def set_position(p):
        global st_position
        st_position = p
    
    ws = StandXPriceWS(set_price)
    ws.start_in_thread()

    pos_ws = StandXPositionWS(set_position, access_token=auth['access_token'])
    pos_ws.start_in_thread()
    
    order_dict = None
    last_price = 0

    while True:
        if not st_price_dict:
            print("waiting for price data...")
            time.sleep(1)
            continue
        mark_price = float(st_price_dict.get("mid_price", 0))
        if not mark_price:
            raise Exception("invalid mark price from ws")
        if order_dict:
            bps = abs(mark_price - order_dict['price']) / mark_price * 10000
            if last_price != mark_price:
                last_price = mark_price
                print(f'pos:{position}, mark_price: {mark_price}, bps: {bps}')
            if st_position:
                if st_position['qty'] and float(st_position['qty']) != 0:
                    print("existing position detected, canceling orders and cleaning position")
                    clean_orders(auth)
                    print("position filled, cleaning position")
                    clean_positions(auth)
                    order_dict = None
                    print("position cleaned, placing new orders after 900 seconds")
                    for i in range(900):
                        if _should_exit:
                            break
                        time.sleep(1)
            if bps < MIN_BPS or bps > MAX_BPS:
                cancel_orders(auth, [order_dict['cl_ord_id']] if order_dict['cl_ord_id'] else [])
                order_dict = None
                next_sleep = backoff.next_sleep()
                print(f"bps out of range, canceling orders, sleeping for {next_sleep} seconds")
                time.sleep(next_sleep)
        else:   
            current_time = datetime.now(ZoneInfo("Asia/Shanghai"))
            current_hour = current_time.hour
            if SKIP_HOUR_START <= current_hour < SKIP_HOUR_END:
                print(f'now is between {SKIP_HOUR_START} and {SKIP_HOUR_END}, skipping order creation')
                time.sleep(10)
            if SIDE == "buy":
                order = {
                    'price': format(mark_price * (1 - BPS / 10000), ".2f"),
                    'qty': format(position / (mark_price * (1 - BPS / 10000)), ".4f"),
                    'side': 'buy',
                }
            else:
                order = {
                    'price': format(mark_price * (1 + BPS / 10000), ".2f"),
                    'qty': format(position / (mark_price * (1 + BPS / 10000)), ".4f"),
                    'side': 'sell',
                }
            orders = [order]
            cl_ord_ids = create_orders(auth, orders)
            order_dict = {
                'cl_ord_id': cl_ord_ids[0],
                'price': float(order['price']),
            }
        if _should_exit:
            break
        time.sleep(0.05)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--position", default=500, type=int, help="Position size")
    args = parser.parse_args()

    with open("standx_beggar_auth.json", "r") as f:
        auth_json = json.load(f)
        auth = {
            'access_token': auth_json['access_token'],
            'signing_key': SigningKey(bytes.fromhex(auth_json['signing_key'])),
        }
    try:
        main(args.position, auth)
    finally:
        clean_orders(auth)
        clean_positions(auth)
        print("Exiting beggar")

