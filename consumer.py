import sys
import re
import json
import os
import time
import asyncio
import threading
import queue
from  queue import Empty
import logging 

import aiohttp
import urllib3
import docker
import uvloop


INT = '(?:[+-]?(?:[0-9]+))'
IP = '(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)'
USER = '[a-zA-Z0-9._-]+'
MONTHDAY =  '(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])'
MONTH = '(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
YEAR = '(?:\d\d){1,2}'
HOUR = '(?:2[0123]|[01]?[0-9])'
MINUTE = '(?:[0-5][0-9])'
SECOND = '(?:(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?)'
TIME = f'(?!<[0-9]){HOUR}:{MINUTE}(?::{SECOND})(?![0-9])'
HTTPDATE = f'{MONTHDAY}/{MONTH}/{YEAR}:{TIME} {INT}'
VERSION = '[0-9]+(?:(?:\.[0-9])+)?'
REQUEST = f'\\"(?:\w+ \S+(?: HTTP/{VERSION})?|.*?)\\"'
TIMESTAMP = f"\[{HTTPDATE}\]"
QUOTE = f'(?:\\".+\\")'
log = '83.149.9.216 - - [04/Jan/2015:05:13:42 +0000] "GET /presentations/logstash-monitorama-2013/images/kibana-search.png HTTP/1.1" 200 203023 "http://semicomplete.com/presentations/logstash-monitorama-2013/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36"'
COMMONLOG = r" ".join([f'(?P<ip>{IP})',f'(?P<ident>{USER})',f'(?P<auth>{USER})',f'(?P<timestamp>{TIMESTAMP})',f'(?P<request>{REQUEST})',"(?P<status>\d+)", "(?P<bytes>\d+|-)", f'(?P<referrer>{QUOTE})', f'(?P<agent>{QUOTE})'])

http = urllib3.PoolManager()

def connect_to_es():
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

def get_container(id_or_name):
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    counter = 1
    while True:
        try:
            container =  client.containers.get(id_or_name)
        except:
            if counter > 1:
                raise
            time.sleep(counter/100)
            counter += 1
        else:
            print(f"Connection established after {counter} tries.")
            return container

pattern = re.compile(COMMONLOG)
queue = queue.Queue()

async def worker(name, client):
    log = logging.getLogger(name)
    while True:
        try:
            line = queue.get_nowait()
        except Empty as e:
            log.info('Queue is empty.')
            await asyncio.sleep(1)
        else:
            line = line.decode('utf-8')
            match = pattern.match(line)
            if match is None:
                log.info(f"No match found for {line.strip()}")
                await asyncio.sleep(0.001)
            else:
                async with client.post("http://es01:9200/logs/_doc/",
                               data=json.dumps(match.groupdict()).encode('utf-8'),
                              headers={'Content-Type': 'application/json'}
                          ) as resp:
                    if resp.status != 201:
                         err = await resp.text()
                         log.info(f"{resp.status}: {err}")
                    else:
                        log.info("Upload succesful.")
       

def reader(container):
    log = logging.getLogger('reader')
    stream = container.logs(stream=True)
    while True:
        try:
            queue.put_nowait(next(stream))
        except StopIteration as e:
            log.debug("No more logs")
            time.sleep(0.001)

async def controller(max_size=100):
    log = logging.getLogger('controller')
    async with aiohttp.ClientSession() as client:
        # start with 5 workers
        [asyncio.create_task(worker(f"worker{i}", client)) for i in range(5)]
        num = 5
        delay = 1
        curr_size = 0
        prev_size = 0
        while True:
            curr_size = queue.qsize() 
            if curr_size > max_size and curr_size > prev_size:
                asyncio.create_task(worker(f"worker{num}", client))
                await asyncio.sleep(delay/100)
                delay += 1
                num += 1
            else:
                await asyncio.sleep(1)
                delay = 1
            prev_size = curr_size
            log.debug(f"Curently running {len(asyncio.all_tasks()) -2} workers. Queue size: {queue.qsize()}")

async def main():
    c_name = os.getenv('SOURCE_CONTAINER')
    if not c_name:
        print("You must specify a source container name.")
        sys.exit(1)
    container = get_container(c_name)
    connect_to_es()
    threading.Thread(target=reader, args = (container,), daemon=True).start()
    done, pending = await asyncio.wait([asyncio.create_task(controller())])


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(threadName)s %(name)s: %(message)s',
        stream=sys.stderr,
    )
    uvloop.install()
    asyncio.run(main())
