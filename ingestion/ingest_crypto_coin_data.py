import requests
from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(bootstrap_servers=['10.128.0.16:19092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

crypto_coins = ['BTC', 'ETH', 'XRP', 'BCH', 'LTC', 'EOS', 'XLM', 'LINK', 'DASH', 'XTZ']

while True:

    for coin in crypto_coins:
        url = 'https://api.coinbase.com/v2/prices/{}-USD/sell'.format(coin)
        usd_value = loads(requests.get(url).text)['data']['amount']
        time_now = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
        data = dumps({'timestamp': time_now, 'usd_value': usd_value})
        producer.send(coin, value=data)
        print('sent data to kafka ' + data)
        sleep(1)

    sleep(30)