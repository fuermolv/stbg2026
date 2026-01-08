
import json
import time
from nacl.signing import SigningKey
from common import query_order, cancel_order, taker_clean_position, get_price, create_order, maker_clean_position


POSITION = 50000
BPS = 20
MIN_BPS = 10
MAX_BPS = 30
SIDE = "sell"


def clean_position(auth, qty, price):
    clean_side = "buy" if SIDE == "sell" else "sell"
    cl_ord_id = maker_clean_position(auth, price, qty, clean_side)
    try:
        for index in range(120):
            order = query_order(auth, cl_ord_id)
            print(f'{index} waiting maker cleaning position order status: {order["status"]} qty: {order["qty"]} price: {price}, order price: {order["price"]}')
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


def main():
    with open("standx_beggar_auth.json", "r") as f:
        auth_json = json.load(f)
        auth = {
            'access_token': auth_json['access_token'],
            'signing_key': SigningKey(bytes.fromhex(auth_json['signing_key'])),
        }
    cl_ord_id = None
    try:
        while True:
            mark_price = float(get_price(auth)["mark_price"])
            if cl_ord_id:
                order = query_order(auth, cl_ord_id)
                diff_bps = abs(mark_price - float(order["price"])) / mark_price * 10000
                print(f'pos:{POSITION} order pos: {order["qty"]} status: {order["status"]}, mark_price: {mark_price}, order price: {order["price"]},  diff_bps: {diff_bps}')
                if order["status"] == "filled":
                    clean_position(auth, float(order["qty"]), float(order["price"]))
                    cl_ord_id = None
                    print("position cleaned, placing new order after 10 minutes")
                    time.sleep(1)
                    time.sleep(600)
                if diff_bps <= MIN_BPS or diff_bps >= MAX_BPS:
                    cancel_order(auth, cl_ord_id)
                    cl_ord_id = None
            else:
                sign = 1 if SIDE == "sell" else -1
                order_price = mark_price * (1 + sign * BPS / 10000)
                order_price = format(order_price, ".2f")
                qty = POSITION / float(order_price)
                qty = format(qty, ".4f")
                cl_ord_id = create_order(auth, order_price, qty, SIDE)
            time.sleep(0.05)
    finally:
        if cl_ord_id:
            print("cleaning up open order")
            cancel_order(auth, cl_ord_id)



if __name__ == "__main__":
    main()
