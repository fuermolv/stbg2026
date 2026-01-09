
import json
import time
from nacl.signing import SigningKey
from common import query_order, cancel_order, taker_clean_position, get_price, create_order, maker_clean_position, query_positions
from concurrent.futures import ThreadPoolExecutor, as_completed
from backoff import CancelBackoff
from config import POSITION
import signal
from zoneinfo import ZoneInfo
from datetime import datetime
from config import SKIP_HOUR_START, SKIP_HOUR_END

BPS = 8.5
MIN_BPS = 7
MAX_BPS = 10



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


def query_orders(auth, cl_ord_ids, max_workers=5):
    order_dict = {}

    def _fetch_one(cl_ord_id):
        return cl_ord_id, query_order(auth, cl_ord_id)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_fetch_one, cid) for cid in cl_ord_ids]
        for fut in as_completed(futures):
            cid, order = fut.result()
            order_dict[cid] = order

    return order_dict

def cancel_orders(auth, cl_ord_ids, max_workers=5):
    def _cancel_one(cl_ord_id):
        cancel_order(auth, cl_ord_id)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_cancel_one, cid) for cid in cl_ord_ids]
        for fut in as_completed(futures):
            fut.result()


def main():
    backoff = CancelBackoff()
    with open("standx_beggar_auth.json", "r") as f:
        auth_json = json.load(f)
        auth = {
            'access_token': auth_json['access_token'],
            'signing_key': SigningKey(bytes.fromhex(auth_json['signing_key'])),
        }
        long_cl_ord_id = None
        short_cl_ord_id = None
        order_dict = None
    try:
        while True:
            try:
                mark_price = float(get_price(auth)["mark_price"])
                if long_cl_ord_id and short_cl_ord_id:
                    if not order_dict:
                        order_dict = query_orders(auth, [cid for cid in [long_cl_ord_id, short_cl_ord_id] if cid])
                    long_diff_bps = abs(mark_price - float(order_dict[long_cl_ord_id]["price"])) / mark_price * 10000 if long_cl_ord_id else None
                    short_diff_bps = abs(mark_price - float(order_dict[short_cl_ord_id]["price"])) / mark_price * 10000 if short_cl_ord_id else None
                    print(f'pos:{POSITION}, mark_price: {mark_price}, long order bps: {long_diff_bps}, short order bps: {short_diff_bps}')
                    positions = query_positions(auth)
                    if [p for p in positions if p['qty'] and float(p['qty']) != 0]:
                        print("existing position detected, canceling orders and cleaning position")
                        cancel_orders(auth, [cid for cid in [long_cl_ord_id, short_cl_ord_id] if cid])
                        print("position filled, cleaning position")
                        clean_position(auth)
                        long_cl_ord_id = None
                        short_cl_ord_id = None
                        order_dict = None
                        print("position cleaned, placing new orders after 900 seconds")
                        time.sleep(900)
                    if long_diff_bps <= MIN_BPS or long_diff_bps >= MAX_BPS or short_diff_bps <= MIN_BPS or short_diff_bps >= MAX_BPS:
                        cancel_orders(auth, [cid for cid in [long_cl_ord_id, short_cl_ord_id] if cid])
                        long_cl_ord_id = None
                        short_cl_ord_id = None
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
                    else:
                        long_order_price = mark_price * (1 - BPS / 10000)
                        long_order_price = format(long_order_price, ".2f")
                        long_qty = POSITION / float(long_order_price)
                        long_qty = format(long_qty, ".4f")
                        long_cl_ord_id = create_order(auth, long_order_price, long_qty, "buy")
                        short_order_price = mark_price * (1 + BPS / 10000)
                        short_order_price = format(short_order_price, ".2f")
                        short_qty = POSITION / float(short_order_price)
                        short_qty = format(short_qty, ".4f")
                        short_cl_ord_id = create_order(auth, short_order_price, short_qty, "sell")
                if _should_exit:
                    break
                time.sleep(0.05)
            except Exception as e:
                print(f"Exception in main loop: {e}, cleaning up")
                cl_ord_ids = [cid for cid in [long_cl_ord_id, short_cl_ord_id] if cid]
                if cl_ord_ids:
                    print("cleaning up open orders")
                    cancel_orders(auth, cl_ord_ids)
                clean_position(auth)
                print("cleanup done, sleeping for 60 seconds before next iteration")
                time.sleep(60)
    finally:
        cl_ord_ids = [cid for cid in [long_cl_ord_id, short_cl_ord_id] if cid]
        if cl_ord_ids:
            print("cleaning up open orders")
            cancel_orders(auth, cl_ord_ids)
        clean_position(auth)
    



if __name__ == "__main__":
    main()
