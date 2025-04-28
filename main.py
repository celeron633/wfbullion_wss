#!/usr/bin/env python3

from websocket import create_connection, WebSocketConnectionClosedException
import ssl
import re
import json
import typing
import redis
import time

# 配置
wss_url = "wss://quote.wfgroup.com.hk:8083/socket.io/?token=applepieapplepieapplepieapplepie&EIO=3&transport=websocket"

wfgroup_ws_cmd = {
    "SERVER_FIRST_MSG": "40",
    "GET_PRICE_START": "40/bquote",
    "KEEPALIVE_REQ": "2",
    "KEEPALIVE_ACK": "3"
}

redis_host = "xxx.xxx.xxx.xxx"
redis_port = 6379
redis_pass = "xxx"

# 连接Redis
def connect_redis():
    r = redis.Redis(host=redis_host, port=redis_port, password=redis_pass)
    try:
        r.ping()
        print("Connected to Redis successfully.")
        return r
    except Exception as e:
        print("Connect to Redis server failed:", e)
        exit(-1)

# 建立WebSocket连接
def connect_ws():
    while True:
        try:
            ws = create_connection(
                wss_url,
                sslopt={"cert_reqs": ssl.CERT_NONE},
                header=["User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"]
            )
            print("Connected to WebSocket server successfully.")
            return ws
        except Exception as e:
            print("Failed to connect to WebSocket server, retrying in 5 seconds...", e)
            time.sleep(5)

if __name__ == "__main__":
    r_inst = connect_redis()

    while True:
        ws = connect_ws()
        items_got = 0
        try:
            while True:
                result = ws.recv()

                if re.match(r'^\d.*', result):
                    # 40: 连接后服务器返回40, 需要发回40/bquote
                    if result == wfgroup_ws_cmd["SERVER_FIRST_MSG"]:
                        print("Got handshake request, sending start command...")
                        ws.send(wfgroup_ws_cmd["GET_PRICE_START"])

                    # KEEPALIVE响应
                    elif result == wfgroup_ws_cmd["KEEPALIVE_ACK"]:
                        print("Got server keepalive ACK.")

                    # 价格数据
                    elif result.startswith("42/bquote"):
                        items_got += 1

                        # 每收25条, 向服务器发2, 服务器回3, 继续走
                        if items_got % 25 == 0:
                            print("Sending keepalive to WebSocket server...")
                            ws.send(wfgroup_ws_cmd["KEEPALIVE_REQ"])

                        try:
                            # 取返回消息中, json的部分
                            json_msg = result[28:-1]
                            price_data = json.loads(json_msg)
                            gold_price = price_data["products"]["XAU="]
                            print(gold_price)

                            r_inst.set("gold_price_rt", json.dumps(gold_price))
                        except (KeyError, json.JSONDecodeError) as e:
                            print("Failed to parse price data:", e)

        except (WebSocketConnectionClosedException, ConnectionResetError) as e:
            print("WebSocket connection closed, reconnecting...", e)
            ws.close()
            time.sleep(2)
            continue
        except KeyboardInterrupt:
            print("User interrupted, exiting.")
            ws.close()
            break
        except Exception as e:
            print("Unexpected error, reconnecting...", e)
            ws.close()
            time.sleep(2)
            continue
