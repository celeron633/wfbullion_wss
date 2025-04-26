#!/usr/bin/python

from websocket import create_connection
import ssl
import re
import json
import typing
import redis

wss_url = "wss://quote.wfgroup.com.hk:8083/socket.io/?token=applepieapplepieapplepieapplepie&EIO=3&transport=websocket"

wfgroup_ws_cmd = {
    "SERVER_FIRST_MSG": "40",
    "GET_PRICE_START": "40/bquote",
    "KEEPALIVE_REQ": "2",
    "KEEPALIVE_ACK": "3"
}

# redis
redis_host = "xxx.xxx.xxx.xxx"
redis_port = 6379
redis_pass = "xxx"


if __name__ == "__main__":
    # 连接redis
    r_inst = redis.Redis(host=redis_host, port=redis_port, password=redis_pass)
    try:
        r_inst.ping()
    except Exception as e:
        print("connect to redis server failed!")
        exit(-1)

    # 连接websocket
    ws = create_connection(wss_url, \
            sslopt={"cert_reqs": ssl.CERT_NONE},
            header=["User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0"])
    
    items_got = 0
    while True:
        # print("recving...")
        result = ws.recv()
        # print(result)

        if re.match(r'^\d.*', result):
            # 40: 连接后服务器返回40, 需要发回40/bquote
            if result == wfgroup_ws_cmd["SERVER_FIRST_MSG"]:
                print("got handshake req, send back start cmd..")
                ws.send(wfgroup_ws_cmd["GET_PRICE_START"])

            # KEEPALIVE响应
            if result == wfgroup_ws_cmd["KEEPALIVE_ACK"]:
                print("got server keepalive ACK!")

        # 价格数据
        if result.startswith("42/bquote"):
            items_got += 1
            # print(items_got)
            # print("result:", result)

            # 每收25条, 向服务器发2, 服务器回3, 继续走
            if items_got > 0 and items_got % 25 == 0:
                print("sending keepalive to wss server!")
                ws.send(wfgroup_ws_cmd["KEEPALIVE_REQ"])
            
            # 取返回消息中, json的部分
            json_msg = result[28:-1]
            # print(json_msg)

            # 反序列化
            price_dict = json.loads(json_msg)
            print(price_dict["products"]["XAU="])

            # 发送到redis
            r_inst.lpush("gold_price", json.dumps(price_dict["products"]["XAU="]))
