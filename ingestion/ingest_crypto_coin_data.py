import requests
from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
from datetime import datetime

# connect to kafka as a producer
producer = KafkaProducer(bootstrap_servers=['10.128.0.16:19092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# list of crypto currencies we are interested in
crypto_coins = ['BTC', 'ETH', 'XRP', 'BCH', 'LTC', 'EOS', 'XLM', 'LINK', 'DASH', 'XTZ']

# go on a never-ending loop
while True:
    # loop over every coin we are interested in
    for coin in crypto_coins:
        # format the url for the api to get a OK response
        url = 'https://api.coinbase.com/v2/prices/{}-USD/sell'.format(coin)
        usd_value = loads(requests.get(url).text)['data']['amount']
        time_now = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        # prepare the value and timestamp for sending to kafka
        data = {'timestamp': time_now, 'usd_value': usd_value}
        # send data to kafka
        producer.send(coin, value=data)
        # a small sleep to not overwhelm the API
        sleep(1)
    # wait 30 seconds till we send the next update
    sleep(30)