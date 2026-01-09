
import json
import time
from nacl.signing import SigningKey
from common import query_order, cancel_order, taker_clean_position, get_price, create_order, maker_clean_position, query_positions
from backoff import CancelBackoff
from config import POSITION
import signal
from zoneinfo import ZoneInfo
from datetime import datetime
from config import SKIP_HOUR_START, SKIP_HOUR_END

BPS = 20
MIN_BPS = 10
MAX_BPS = 30
SIDE = "sell"



_should_exit = False

def _on_term(signum, frame):
    global _should_exit
    _should_exit = True

signal.signal(signal.SIGTERM, _on_term)
signal.signal(signal.SIGINT, _on_term)

def clean_position(auth):
    positions = query_positions(auth)
    for position in positions:
        if not position['qty'] or float(position['qty']) == 0:
            continue
        side = 'sell' if float(position['qty']) < 0 else 'buy'
        qty = abs(float(position['qty']))
        clean_side = 'buy' if side == 'sell' else 'sell'
        position_vaule = abs(float(position['position_value']))
        entry_price = float(position['entry_price'])
        if clean_side == 'buy':
            price = entry_price
        else:
            price = entry_price
        print(f'Cleaning position: side={side}, qty={qty}, entry_price={entry_price}, maker price {price}, position_value={position_vaule}')
        cl_ord_id = maker_clean_position(auth, price, qty, clean_side)
        try:
            for index in range(15):
                order = query_order(auth, cl_ord_id)
                print(f'{index} waiting maker cleaning position order status: {order["status"]} qty: {order["qty"]} price: {entry_price}, order price: {order["price"]}')
                if order["status"] == "filled":
                    return
                time.sleep(1)
        except Exception as e:
            print("maker clean position exception, using taker to clean position")
            taker_clean_position(auth, qty, clean_side)
            raise e
        print("maker clean position timeout, canceling order")
        cancel_order(auth, cl_ord_id)
        print("using taker to clean position")
        taker_clean_position(auth, qty, clean_side)
        print("position cleaned")


def main():
    backoff = CancelBackoff()
    with open("standx_beggar_auth.json", "r") as f:
        auth_json = json.load(f)
        auth = {
            'access_token': auth_json['access_token'],
            'signing_key': SigningKey(bytes.fromhex(auth_json['signing_key'])),
        }
    cl_ord_id = None
    order = None
    try:
        while True:
            try:
                mark_price = float(get_price(auth)["mark_price"])
                if cl_ord_id:
                    if not order:
                        order = query_order(auth, cl_ord_id)
                    diff_bps = abs(mark_price - float(order["price"])) / mark_price * 10000
                    print(f'pos:{POSITION} order pos: {order["qty"]} status: {order["status"]}, mark_price: {mark_price}, order price: {order["price"]},  diff_bps: {diff_bps}')
                    positions = query_positions(auth)
                    if [p for p in positions if p['qty'] and float(p['qty']) != 0]:
                        print("existing position detected, canceling order and cleaning position")
                        cancel_order(auth, cl_ord_id)
                        clean_position(auth)
                        cl_ord_id = None
                        order = None
                        print("position cleaned, placing new orders after 900 seconds")
                        time.sleep(900)
                    if diff_bps <= MIN_BPS or diff_bps >= MAX_BPS:
                        cancel_order(auth, cl_ord_id)
                        cl_ord_id = None
                        order = None
                        next_sleep = backoff.next_sleep()
                        print(f"bps out of range, canceling order, sleeping for {next_sleep} seconds")
                        time.sleep(next_sleep)
                else:
                    current_time = datetime.now(ZoneInfo("Asia/Shanghai"))
                    current_hour = current_time.hour
                    if SKIP_HOUR_START <= current_hour < SKIP_HOUR_END:
                        print(f'now is between {SKIP_HOUR_START} and {SKIP_HOUR_END}, skipping order creation')
                        time.sleep(10)
                        continue
                    sign = 1 if SIDE == "sell" else -1
                    order_price = mark_price * (1 + sign * BPS / 10000)
                    order_price = format(order_price, ".2f")
                    qty = POSITION / float(order_price)
                    qty = format(qty, ".4f")
                    cl_ord_id = create_order(auth, order_price, qty, SIDE)
                if _should_exit:
                    break
                time.sleep(0.05)
            except Exception as e:
                    print(f"Exception in main loop: {e}, cleaning up")
                    if cl_ord_id:
                        print("cleaning up open order")
                        cancel_order(auth, cl_ord_id)
                    clean_position(auth)
                    print("sleeping for 60 seconds before restarting main loop")
                    time.sleep(60)
    finally:
        if cl_ord_id:
            print("cleaning up open order")
            cancel_order(auth, cl_ord_id)
        clean_position(auth)



if __name__ == "__main__":
    main()