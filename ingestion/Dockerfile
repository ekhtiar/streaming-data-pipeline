FROM mrkcse/docker-python-librdkafka

ADD ingest_crypto_coin_data.py /

RUN pip install kafka-python \
  && pip install requests

CMD [ "python","-u","./ingest_crypto_coin_data.py" ]