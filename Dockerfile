FROM python:3.11-slim-bullseye

RUN apt-get update

RUN mkdir proj
WORKDIR /proj

COPY blobfish/ ./
COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
