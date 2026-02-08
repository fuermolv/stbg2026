import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor

from st_http import query_orders, query_positions, maker_clean_position, taker_clean_position, cancel_orders, create_order
import logging

logger = logging.getLogger(__name__)

LARK_URL = os.getenv("LARK_URL", "")


def send_lark_message(message: str):
    if not LARK_URL:
        return
    prfix = 'lyu'
    full_message = f"{prfix}\n{message}"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "msg_type": "text", 
        "content": {
            "text": full_message
        }
    }
    try:
        response = requests.post(LARK_URL, json=data, headers=headers)
        if response.status_code != 200:
            logger.error(f"Failed to send message to Lark: {response.status_code}, {response.text}")
        else:
            logger.info("Message sent to Lark successfully")
    except Exception as e:
        logger.error(f"Exception occurred while sending message to Lark: {e}")



def clean_positions(auth):
    for _ in range(5):
        clean_orders(auth)
        time.sleep(1)
    positions = query_positions(auth)
    if not [position for position in positions if position['qty'] and float(position['qty']) != 0]:
        logger.info("no positions to clean")
        return
    for position in positions:
        if not position['qty'] or float(position['qty']) == 0:
            continue
        side = 'sell' if float(position['qty']) < 0 else 'buy'
        qty = abs(float(position['qty']))
        clean_side = 'buy' if side == 'sell' else 'sell'
        entry_price = float(position['entry_price'])
        price = entry_price
        send_lark_message(f'Cleaning position: side={side}, qty={qty}, entry_price={entry_price}, maker price {price}, position_value={abs(float(position["position_value"]))}')
        logger.info(f'Cleaning position: side={side}, qty={qty}, entry_price={entry_price}, maker price {price}, position_value={abs(float(position["position_value"]))}')
        cl_ord_id = maker_clean_position(auth, price, qty, clean_side)
        maker_time = 60*30 if qty > 0.5 else 180
        for index in range(maker_time):
            logger.info(f'{index} waiting maker cleaning position order  qty: {qty}  order price: {price}')
            if not [position for position in query_positions(auth) if position['qty'] and float(position['qty']) != 0]:
                logger.info("maker clean position filled")
                return
            time.sleep(1)
        logger.info("maker clean position timeout, canceling order")
        cancel_orders(auth, [cl_ord_id])
        
    send_lark_message("using taker to clean position")
    STEP_QTY = 0.1
    positions = query_positions(auth)
    while [position for position in positions if position['qty'] and float(position['qty']) != 0]:
        logger.info("using taker to clean position")
        for position in positions:
            if not position['qty'] or float(position['qty']) == 0:
                continue
            side = 'sell' if float(position['qty']) < 0 else 'buy'
            qty = abs(float(position['qty']))
            clean_side = 'buy' if side == 'sell' else 'sell'
            clean_qty = qty if qty < STEP_QTY else STEP_QTY
            logger.info(f"taker cleaning position: side={side}, qty={qty}, cleaning qty={clean_qty}")
            taker_clean_position(auth, clean_qty, clean_side)
            time.sleep(5)
        positions = query_positions(auth)
    logger.info("taker clean position done")
    


def create_orders(auth, orders):
    def _create_one(order):
        return create_order(
            auth,
            order['price'],
            order['qty'],
            order['side'],
        )
    with ThreadPoolExecutor(max_workers=5) as ex:
        return list(ex.map(_create_one, orders))


def clean_orders(auth):
    while True:
        orders = query_orders(auth).get("result", [])
        cl_order_ids = [order["cl_ord_id"] for order in orders]
        if cl_order_ids:
            logger.info(f"try canceled all open orders: {cl_order_ids}")
            cancel_orders(auth, cl_order_ids)
            time.sleep(0.1)
        else:
            logger.info("no open orders to cancel")
            break