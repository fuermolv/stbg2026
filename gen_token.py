import os
import json
import requests
import base58
import base64
from nacl.signing import SigningKey

from eth_account import Account
from eth_account.messages import encode_defunct



def gen_token():
    addr = os.getenv("STANDX_BEGGAR_ADDR")
    if not addr:
        raise ValueError("STANDX_BEGGAR_ADDR environment variable is not set.")

    pk = os.getenv("STANDX_BEGGAR_PK")
    if not pk:
        raise ValueError("STANDX_BEGGAR_PK environment variable is not set.")

    print("Generating gen_token...")

    # ------------------------------------------------------------
    # Step 1: requestId
    # ------------------------------------------------------------
    signing_key = SigningKey.generate()
    public_key_bytes = signing_key.verify_key.encode()
    request_id = base58.b58encode(public_key_bytes).decode()
    print(f"Generated Request ID: {request_id}")

    # ------------------------------------------------------------
    # Step 2: prepare-signin
    # ------------------------------------------------------------
    resp = requests.post(
        "https://api.standx.com/v1/offchain/prepare-signin?chain=bsc",
        headers={"Content-Type": "application/json"},
        json={
            "address": addr,
            "requestId": request_id,
        },
    )

    if resp.status_code != 200:
        raise Exception(f"prepare-signin failed: {resp.status_code} {resp.text}")

    signed_data = resp.json().get("signedData")
    if not signed_data:
        raise Exception("No signedData in response")

    print(f"Received signedData: {signed_data[:10]}...")

    # ------------------------------------------------------------
    # Step 4: sign message with wallet private key
    # （直接 parse payload，不做任何校验）
    # ------------------------------------------------------------
    payload_b64 = signed_data.split(".")[1]
    payload_b64 += "=" * (-len(payload_b64) % 4)
    payload_json = json.loads(base64.urlsafe_b64decode(payload_b64).decode("utf-8"))
    message = payload_json["message"]
    print('-----------------Message to sign-----------------------')
    print(message)
    print('-------------------------------------------------------')

    acct = Account.from_key(pk)
    signed_msg = acct.sign_message(encode_defunct(text=message))
    wallet_signature = "0x" + signed_msg.signature.hex()

    print(f"Wallet signature: {wallet_signature[:10]}...")

    # ------------------------------------------------------------
    # Step 5: login -> access token
    # ------------------------------------------------------------
    login_resp = requests.post(
        "https://api.standx.com/v1/offchain/login?chain=bsc",
        headers={"Content-Type": "application/json"},
        json={
            "signature": wallet_signature,
            "signedData": signed_data,
            "expiresSeconds": 604800,
        },
    )

    if login_resp.status_code != 200:
        raise Exception(f"login failed: {login_resp.status_code} {login_resp.text}")

    login_data = login_resp.json()
    token = login_data.get("token")
    if not token:
        raise Exception("No token in login response", login_data)

    print("Access token received")
    print(f"Address: {login_data.get('address')}")
    print(f"Chain: {login_data.get('chain')}")
    return token


def main():
    token = gen_token()
    print(f"access token: {token}")


if __name__ == "__main__":
    main()
