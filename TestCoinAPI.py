from websocket import create_connection
import datetime
import json

test_key = '22168C17-6BE1-4182-B4F6-DD23D8BFB986'

class CoinAPIv1_subscribe(object):
  def __init__(self, apikey):
    self.type = "hello"
    self.apikey = apikey
    self.heartbeat = True
    self.subscribe_data_type = ["quote"]

ws = create_connection("wss://ws.coinapi.io/v1")
sub = CoinAPIv1_subscribe(test_key);
ws.send(json.dumps(sub.__dict__))
while True:
  msg =  ws.recv()
  #print(msg);
ws.close()