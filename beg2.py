
import json
import logging
import time
from nacl.signing import SigningKey
from backoff import CancelBackoff
import signal
import argparse
from st_ws import StandXBookWS, StandXPositionWS
from st_http import cancel_orders
from zoneinfo import ZoneInfo
from datetime import datetime
from config import SKIP_HOUR_START, SKIP_HOUR_END
from common import create_orders, clean_positions, clean_orders


from logconf import setup_logging

setup_logging()
logger = logging.getLogger(__name__)




def _on_term(signum, frame):
    global _should_exit
    _should_exit = True

signal.signal(signal.SIGTERM, _on_term)
signal.signal(signal.SIGINT, _on_term)


BPS = 8.5
MIN_BPS = 7
MAX_BPS = 10
THROTTLE_BPS = 12

_should_exit = False
st_book = None
st_position = None


def main(position, auth):
    backoff = CancelBackoff()
    logger.info(f"Starting beggar with position size: {position}")
    def set_book(b):
        global st_book
        st_book = b

    def set_position(p):
        global st_position
        # if p :
        #     if p['qty'] and float(p['qty']) != 0:
        #         logger.info(f"position update: {p}")
        st_position = p
    
    book_ws = StandXBookWS(set_book)
    book_ws.start_in_thread()

    pos_ws = StandXPositionWS(set_position, access_token=auth['access_token'])
    pos_ws.start_in_thread()
    
    order_dict = None
    last_price = 0

    while True:
        if not st_book:
            logger.info("waiting for price data...")
            time.sleep(1)
            continue
        mark_price = book_ws.get_mid_price(st_book)
        if not mark_price:
            raise Exception("invalid mark price from ws")
        if order_dict:
            long_diff_bps = abs(mark_price - order_dict['long_price']) / mark_price * 10000 if order_dict['long_cl_ord_id'] else None
            short_diff_bps = abs(mark_price - order_dict['short_price']) / mark_price * 10000 if order_dict['short_cl_ord_id'] else None
            if last_price != mark_price:
                last_price = mark_price
                logger.info(f'pos:{position}, mark_price: {mark_price}, long order bps: {long_diff_bps}, short order bps: {short_diff_bps}')
            if st_position:
                if st_position['qty'] and float(st_position['qty']) != 0:
                    logger.info("existing position detected, canceling orders and cleaning position")
                    clean_orders(auth)
                    logger.info("position filled, cleaning position")
                    clean_positions(auth)
                    order_dict = None
                    logger.info("position cleaned, placing new orders after 900 seconds")
                    for i in range(900):
                        if _should_exit:
                            break
                        time.sleep(1)
            if long_diff_bps <= MIN_BPS or long_diff_bps >= MAX_BPS or short_diff_bps <= MIN_BPS or short_diff_bps >= MAX_BPS:
                cancel_orders(auth, [cid for cid in [order_dict['long_cl_ord_id'], order_dict['short_cl_ord_id']] if cid])
                order_dict = None
                if abs(long_diff_bps) > THROTTLE_BPS or abs(short_diff_bps) > THROTTLE_BPS:
                    logger.info(f"bps out of throttle range {THROTTLE_BPS}, canceling orders, sleeping for 300 seconds")
                    time.sleep(300)
                    backoff.penalty(3)
                else:
                    next_sleep = backoff.next_sleep()
                    logger.info(f"bps out of range, canceling orders, sleeping for {next_sleep} seconds")
                    time.sleep(next_sleep)
        else:   
            current_time = datetime.now(ZoneInfo("Asia/Shanghai"))
            current_hour = current_time.hour
            if SKIP_HOUR_START <= current_hour < SKIP_HOUR_END:
                if order_dict:
                    cancel_orders(auth, [cid for cid in [order_dict['long_cl_ord_id'], order_dict['short_cl_ord_id']] if cid])
                logger.info(f'now is between {SKIP_HOUR_START} and {SKIP_HOUR_END}, skipping order creation')
                time.sleep(10)
                continue
            long_order = {
                'price': format(mark_price * (1 - BPS / 10000), ".2f"),
                'qty': format(position / (mark_price * (1 - BPS / 10000)), ".4f"),
                'side': 'buy',
            }
            short_order = {
                'price': format(mark_price * (1 + BPS / 10000), ".2f"),
                'qty': format(position / (mark_price * (1 + BPS / 10000)), ".4f"),
                'side': 'sell',
            }
            orders = [long_order, short_order]
            cl_ord_ids = create_orders(auth, orders)
            order_dict = {
                'long_cl_ord_id': cl_ord_ids[0],
                'short_cl_ord_id': cl_ord_ids[1],
                'long_price': float(long_order['price']),
                'short_price': float(short_order['price']),
            }
        if _should_exit:
            break
        time.sleep(0.05)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--position", default=500, type=int, help="Position size")
    parser.add_argument("--auth", default="standx_beggar_auth.json", type=str, help="Path to auth json file")
    args = parser.parse_args()

    with open(args.auth, "r") as f:
        auth_json = json.load(f)
        auth = {
            'access_token': auth_json['access_token'],
            'signing_key': SigningKey(bytes.fromhex(auth_json['signing_key'])),
        }
    while True:
        try:
            main(args.position, auth)
        except Exception as e:
            logger.info(f"Exception in beggar: {e} traceback: {e.__traceback__}")
        finally:
            clean_orders(auth)
            clean_positions(auth)
            logger.info("Exiting beggar")
        if _should_exit:
            break
        logger.info("Restarting beggar after 120 seconds")
        time.sleep(120)


