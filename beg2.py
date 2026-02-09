
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



_should_exit = False
st_book = None
st_book_ts = 0
st_position = None



def main(position, auth):
    backoff = CancelBackoff()
    logger.info(f"Starting beggar with position size: {position}")
    def set_book(b):
        global st_book
        global st_book_ts
        st_book = b
        st_book_ts = time.time()

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
    last_log_timestamp = 0

    while True:
        if not st_book:
            logger.info("waiting for price data...")
            time.sleep(1)
            continue
        mark_price = book_ws.get_mid_price(st_book)
        best_ask_price, best_bid_price = book_ws.get_best_ask_bid(st_book)
        if not mark_price:
            raise Exception("invalid mark price from ws")
        
        if order_dict:
            # long_diff_bps = (best_bid_price - order_dict['long_price']) / best_bid_price * 10000 if order_dict['long_cl_ord_id'] else None
            # short_diff_bps = (order_dict['short_price'] - best_ask_price) / best_ask_price * 10000 if order_dict['short_cl_ord_id'] else None
            
            # mark_price
            long_diff_bps = (mark_price - order_dict['long_price']) / mark_price * 10000 if order_dict['long_cl_ord_id'] else None 
            short_diff_bps = (order_dict['short_price'] - mark_price) / mark_price * 10000 if order_dict['short_cl_ord_id'] else None

            short_depeth = book_ws.depth_below_price(st_book, order_dict['short_price'])
            long_depeth = book_ws.depth_above_price(st_book, order_dict['long_price'])
            if last_price != mark_price:
                last_price = mark_price
                now_timestmp = time.time()
                if now_timestmp - last_log_timestamp > 1:
                    logger.info(f'pos:{position}, mark_price: {mark_price}, best_ask: {best_ask_price}, best_bid: {best_bid_price}, long order bps: {long_diff_bps}, short order bps: {short_diff_bps}, long_depth:{format(long_depeth, ".3f")}, short_depth:{format(short_depeth, ".3f")}')
                    last_log_timestamp = now_timestmp
            if st_position:
                logger.info(f'pos:{position}, mark_price: {mark_price}, best_ask: {best_ask_price}, best_bid: {best_bid_price}, long order bps: {long_diff_bps}, short order bps: {short_diff_bps}, long_depth:{format(long_depeth, ".3f")}, short_depth:{format(short_depeth, ".3f")}')
                if st_position['qty'] and float(st_position['qty']) != 0:
                    logger.info("existing position detected, canceling orders and cleaning position")
                    cancel_orders(auth, [cid for cid in [order_dict['long_cl_ord_id'], order_dict['short_cl_ord_id']] if cid])
                    clean_positions(auth)
                    order_dict = None
                    logger.info("position cleaned, placing new orders after 900 seconds")
                    for i in range(900):
                        if _should_exit:
                            break
                        time.sleep(1)
                continue
            time_diff = time.time() - st_book_ts
            if (long_diff_bps <= MIN_BPS or long_diff_bps >= MAX_BPS or short_diff_bps <= MIN_BPS or short_diff_bps >= MAX_BPS) \
            or time_diff > 0.6 \
            or (short_depeth < MIN_DEP or long_depeth < MIN_DEP):

                logger.info(f'pos:{position}, mark_price: {mark_price}, best_ask: {best_ask_price}, best_bid: {best_bid_price}, long order bps: {long_diff_bps}, short order bps: {short_diff_bps}, long_depth:{format(long_depeth, ".3f")}, short_depth:{format(short_depeth, ".3f")}, time_diff: {format(time_diff, ".3f")}')
                cancel_orders(auth, [cid for cid in [order_dict['long_cl_ord_id'], order_dict['short_cl_ord_id']] if cid])
                clean_orders(auth)
                order_dict = None
                if abs(long_diff_bps) > THROTTLE_BPS or abs(short_diff_bps) > THROTTLE_BPS:
                    logger.info(f"bps out of throttle range {THROTTLE_BPS}, canceling orders, sleeping for 300 seconds")
                    time.sleep(300)
                    backoff.penalty(3)
                else:
                    next_sleep = backoff.next_sleep()
                    logger.info(f"bps out of range, canceling orders, sleeping for {next_sleep} seconds")
                    for _ in range(int(next_sleep)):
                        if st_position:
                            break
                        time.sleep(1)
                
        else:   
            current_time = datetime.now(ZoneInfo("Asia/Shanghai"))
            current_hour = current_time.hour
            current_weekday = current_time.weekday()
            if current_weekday < 5:  # Skip on weekends
                if SKIP_HOUR_START <= current_hour < SKIP_HOUR_END:
                    if order_dict:
                        clean_orders(auth)
                        order_dict = None
                    logger.info(f'now is between {SKIP_HOUR_START} and {SKIP_HOUR_END}, skipping order creation')
                    time.sleep(10)
                    continue
            clean_orders(auth)
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
            time_diff = time.time() - st_book_ts
            if  time_diff > 0.3:
                logger.info(f"book data too old, skipping order creation, { time_diff }")
                time.sleep(1)
                continue
            short_depeth = book_ws.depth_below_price(st_book, short_order['price'])
            long_depeth = book_ws.depth_above_price(st_book, long_order['price'])

            if short_depeth < MIN_DEP or long_depeth < MIN_DEP:
                next_sleep = backoff.next_sleep()
                logger.info(f"not enough depth to place orders, long_depth:{format(long_depeth, '.3f')}, short_depth:{format(short_depeth, '.3f')}, skipping order creation for {next_sleep} seconds")
                time.sleep(next_sleep)
                continue

            cl_ord_ids = create_orders(auth, [long_order, short_order])
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
    parser.add_argument("--bps", default=8.5, type=float, help="BPS for order placement")
    parser.add_argument("--max_bps", default=10, type=float, help="Max BPS for order placement")
    parser.add_argument("--min_bps", default=7, type=float, help="Min BPS for order placement")
    parser.add_argument("--throttle_bps", default=12, type=float, help="BPS for throttling order placement when market is unfavorable")
    parser.add_argument("--min_dep", default=4, type=float, help="Minimum depth required to place orders")
    parser.add_argument("--auth", default="standx_beggar_auth.json", type=str, help="Path to auth json file")
    args = parser.parse_args()


    global BPS, MAX_BPS, MIN_BPS, THROTTLE_BPS, MIN_DEP
    BPS = args.bps
    MAX_BPS = args.max_bps
    MIN_BPS = args.min_bps
    THROTTLE_BPS = args.throttle_bps
    MIN_DEP = args.min_dep



    with open(args.auth, "r") as f:
        auth_json = json.load(f)
        auth = {
            'access_token': auth_json['access_token'],
            'signing_key': SigningKey(bytes.fromhex(auth_json['signing_key'])),
        }
    print(f"Starting beggar with position: {args.position}, bps: {BPS}, max_bps: {MAX_BPS}, min_bps: {MIN_BPS}, throttle_bps: {THROTTLE_BPS}, min_dep: {MIN_DEP}")
    while True:
        try:
            clean_orders(auth)
            clean_positions(auth)
            main(args.position, auth)
        except Exception as e:
            logger.info(f"Exception in beggar: {e} traceback: {e.__traceback__}")
        finally:
            clean_orders(auth)
            clean_positions(auth)
        if _should_exit:
            break
        for i in range(120):
            time.sleep(1)
            clean_orders(auth)
            clean_positions(auth)
            print(f"Restarting beggar in {120 - i} seconds...")


