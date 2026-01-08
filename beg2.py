
import json
import time
from nacl.signing import SigningKey
from common import query_order, cancel_order, taker_clean_position, get_price, create_order, maker_clean_position, query_positions
from concurrent.futures import ThreadPoolExecutor, as_completed

POSITION = 5000
BPS = 9
MIN_BPS = 8
MAX_BPS = 10



def clean_position(auth):
    positions = query_positions(auth)
    for position in positions:
        side = 'sell' if float(position['qty']) < 0 else 'buy'
        qty = abs(float(position['qty']))
        clean_side = 'buy' if side == 'sell' else 'sell'
        position_vaule = abs(float(position['position_value']))
        entry_price = float(position['entry_price'])

        if clean_side == 'buy':
            price = entry_price - 20
        else:
            price = entry_price + 20
        print(f'Cleaning position: side={side}, qty={qty}, entry_price={entry_price}, maker price {price}, position_value={position_vaule}')
        cl_ord_id = maker_clean_position(auth, price, qty, clean_side)
        try:
            for index in range(60):
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
    with open("standx_beggar_auth.json", "r") as f:
        auth_json = json.load(f)
        auth = {
            'access_token': auth_json['access_token'],
            'signing_key': SigningKey(bytes.fromhex(auth_json['signing_key'])),
        }
        long_cl_ord_id = None
        short_cl_ord_id = None
        try:
            while True:
                index_price = float(get_price(auth)["mid_price"])
                if long_cl_ord_id and short_cl_ord_id:
                    order_dict = query_orders(auth, [cid for cid in [long_cl_ord_id, short_cl_ord_id] if cid])
                    long_diff_bps = abs(index_price - float(order_dict[long_cl_ord_id]["price"])) / index_price * 10000 if long_cl_ord_id else None
                    short_diff_bps = abs(index_price - float(order_dict[short_cl_ord_id]["price"])) / index_price * 10000 if short_cl_ord_id else None
                    print(f'pos:{POSITION}, index price: {index_price}, long order bps: {long_diff_bps}, short order bps: {short_diff_bps}')
                    for order in order_dict.values():
                        if order["status"] == "filled":
                            cancel_orders(auth, [cid for cid in [long_cl_ord_id, short_cl_ord_id] if cid and cid != order["cl_ord_id"]])
                            print("position filled, cleaning position")
                            clean_position(auth)
                            long_cl_ord_id = None
                            short_cl_ord_id = None
                            print("position cleaned, placing new orders after 10 seconds")
                            time.sleep(10)
                    if long_diff_bps <= MIN_BPS or long_diff_bps >= MAX_BPS:
                        cancel_orders(auth, [cid for cid in [long_cl_ord_id, short_cl_ord_id] if cid])
                        long_cl_ord_id = None
                        short_cl_ord_id = None
                        time.sleep(1)
                    if short_diff_bps <= MIN_BPS or short_diff_bps >= MAX_BPS:
                        cancel_orders(auth, [cid for cid in [long_cl_ord_id, short_cl_ord_id] if cid])
                        long_cl_ord_id = None
                        short_cl_ord_id = None
                        time.sleep(1)
                else:   
                    long_order_price = index_price * (1 - BPS / 10000)
                    long_order_price = format(long_order_price, ".2f")
                    long_qty = POSITION / float(long_order_price)
                    long_qty = format(long_qty, ".4f")
                    long_cl_ord_id = create_order(auth, long_order_price, long_qty, "buy")

                    short_order_price = index_price * (1 + BPS / 10000)
                    short_order_price = format(short_order_price, ".2f")
                    short_qty = POSITION / float(short_order_price)
                    short_qty = format(short_qty, ".4f")
                    short_cl_ord_id = create_order(auth, short_order_price, short_qty, "sell")
        finally:
            cl_ord_ids = [cid for cid in [long_cl_ord_id, short_cl_ord_id] if cid]
            if cl_ord_ids:
                print("cleaning up open orders")
                cancel_orders(auth, cl_ord_ids)
            clean_position(auth)
  
    



if __name__ == "__main__":
    main()
