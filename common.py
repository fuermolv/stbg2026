import json
import uuid
import time
import base64
import random
import requests
from nacl.signing import SigningKey


BASE_URL = "https://perps.standx.com"
PAIR = "BTC-USD"


# --------- NEW: a shared session + retry wrapper (minimal intrusion) ---------
session = requests.Session()

def request_with_retry(
    session,
    method,
    url,
    *,
    headers=None,
    params=None,
    data=None,
    timeout=(3.0, 10.0),      # (connect_timeout, read_timeout)
    max_retries=3,
    backoff_base=0.4,         # seconds
):
    """
    Retry only on connection-level failures (ConnectionError/Timeout/etc).
    Does NOT change your existing HTTP status handling logic.
    """
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            return session.request(
                method,
                url,
                headers=headers,
                params=params,
                data=data,
                timeout=timeout,
            )
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            last_exc = e
            if attempt >= max_retries:
                raise
            # exponential backoff + small jitter
            sleep_s = backoff_base * (2 ** attempt) + random.uniform(0, 0.2)
            time.sleep(sleep_s)
    # theoretically unreachable
    raise last_exc
# ---------------------------------------------------------------------------


def get_headers(auth, payload_str=None):
    x_request_version = "v1"
    x_request_id = str(uuid.uuid4())
    x_request_timestamp = str(int(time.time() * 1000))
    access_token = auth['access_token']
    signing_key = auth['signing_key']
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-request-sign-version": x_request_version,
        "x-request-id": x_request_id,
        "x-request-timestamp": x_request_timestamp,
    }
    if payload_str:
        msg = f"{x_request_version},{x_request_id},{x_request_timestamp},{payload_str}"
        msg_bytes = msg.encode("utf-8")
        signed = signing_key.sign(msg_bytes)
        signature = base64.b64encode(signed.signature).decode("ascii")
        headers["x-request-signature"] = signature
        headers["Content-Type"] = "application/json"
    return headers


# https://docs.standx.com/standx-api/perps-http#query-symbol-price
def get_price(auth):
    url = f"{BASE_URL}/api/query_symbol_price"
    params = {"symbol": PAIR}
    resp = request_with_retry(session, "GET", url, headers=get_headers(auth), params=params)
    if resp.status_code != 200:
        raise Exception(f"get_price failed: {resp.status_code} {resp.text}")
    return resp.json()


# https://docs.standx.com/standx-api/perps-http#create-new-order
def create_order(auth, price, qty, side):
    url = f"{BASE_URL}/api/new_order"
    cl_ord_id = str(uuid.uuid4())
    data = {
        "symbol": PAIR,
        "side": side,
        "order_type": "limit",
        "qty": qty,
        "price": str(price),
        "margin_mode": "cross",
        "time_in_force": "gtc",
        "reduce_only": False,
        "cl_ord_id": cl_ord_id,
    }

    payload_str = json.dumps(data, separators=(",", ":"))
    resp = request_with_retry(session, "POST", url, headers=get_headers(auth, payload_str), data=payload_str)
    if resp.status_code != 200:
        raise Exception(f"create_order failed: {resp.status_code} {resp.text} data: {data}")
    print(f"creating order: side={side}, price={price}, qty={qty}, cl_ord_id={cl_ord_id}")
    return cl_ord_id


def maker_clean_position(auth, price, qty, side):
    url = f"{BASE_URL}/api/new_order"
    cl_ord_id = str(uuid.uuid4())
    data = {
        "symbol": PAIR,
        "side": side,
        "order_type": "limit",
        "qty": qty,
        "price": str(price),
        "margin_mode": "cross",
        "time_in_force": "gtc",
        "reduce_only": True,
        "cl_ord_id": cl_ord_id,
    }

    payload_str = json.dumps(data, separators=(",", ":"))
    resp = request_with_retry(session, "POST", url, headers=get_headers(auth, payload_str), data=payload_str)
    if resp.status_code != 200:
        raise Exception(f"create_order failed: {resp.status_code} {resp.text}")
    print(f"maker cleaning position with limit order: side={side}, price={price}, qty={qty}")
    return cl_ord_id


def taker_clean_position(auth, qty, side):
    url = f"{BASE_URL}/api/new_order"
    data = {
        "symbol": PAIR,
        "side": side,
        "order_type": "market",
        "qty": qty,
        "margin_mode": "cross",
        "time_in_force": "gtc",
        "reduce_only": True,
    }
    payload_str = json.dumps(data, separators=(",", ":"))
    resp = request_with_retry(session, "POST", url, headers=get_headers(auth, payload_str), data=payload_str)
    if resp.status_code != 200:
        raise Exception(f"create_order failed: {resp.status_code} {resp.text}")
    print(f"cleaning position with taker: side={side}, qty={qty}")
    return resp.json()


# https://docs.standx.com/standx-api/perps-http#cancel-multiple-orders
def cancel_order(auth, cl_order_id):
    url = f"{BASE_URL}/api/cancel_orders"
    data = {
        "cl_ord_id_list": [cl_order_id],
    }
    payload_str = json.dumps(data, separators=(",", ":"))
    resp = request_with_retry(session, "POST", url, headers=get_headers(auth, payload_str), data=payload_str)
    if resp.status_code != 200:
        raise Exception(f"cancel_orders failed: {resp.status_code} {resp.text}")
    print(f"cancel order: {cl_order_id}")
    return resp.json()


def query_order(auth, cl_ord_id):
    url = f"{BASE_URL}/api/query_order"
    params = {"cl_ord_id": cl_ord_id}
    resp = request_with_retry(session, "GET", url, headers=get_headers(auth), params=params)
    if resp.status_code != 200:
        raise Exception(f"query_open_orders failed: {resp.status_code} {resp.text}")
    return resp.json()


def query_positions(auth):
    url = f"{BASE_URL}/api/query_positions"
    params = {"symbol": PAIR}
    resp = request_with_retry(session, "GET", url, headers=get_headers(auth), params=params)
    if resp.status_code != 200:
        raise Exception(f"query_position failed: {resp.status_code} {resp.text}")
    return resp.json()
