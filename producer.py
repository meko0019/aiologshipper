import time
import sys 
import random
from pathlib import Path

import urllib3



def connect_to_es():
    http = urllib3.PoolManager()
    counter = 0
    while True:
        try:
             http.request("GET", "http://es01:9200/_cat/health?v")
        except Exception as e:
            if counter > 20:
                raise
            time.sleep(counter)
            counter += 1
        else:
            print(f"Connection established after {counter} tries.")
            break


path = "logstash-tutorial.log"
text = Path(path).read_text().split("\n")

def main():
    connect_to_es()
    while True:
        for line in text:
            print(line)
            time.sleep(random.random()*0.01)


if __name__ == "__main__":
    main()
