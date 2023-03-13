FROM python:3.11-slim-bullseye

RUN apt update

RUN mkdir proj
WORKDIR /proj

COPY ./ ./

RUN pip install -r requirements.txt
