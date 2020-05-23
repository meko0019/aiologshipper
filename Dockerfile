FROM python:3.8-slim

WORKDIR /code

RUN pip install uvloop aiohttp urllib3 docker

COPY . . 


