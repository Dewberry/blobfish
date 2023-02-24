FROM python:3.11-slim-bullseye

RUN apt update && apt install -y git

RUN mkdir proj
WORKDIR /proj

COPY ./ ./

RUN pip install -r requirements.txt

RUN update-ca-certificates --fresh
RUN export SSL_CERT_DIR=/etc/ssl/certs
