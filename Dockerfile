FROM python:3.11-slim-bullseye

RUN mkdir proj
WORKDIR /proj

COPY ./ ./

RUN pip install -r requirements.txt

RUN update-ca-certificates --fresh
RUN export SSL_CERT_DIR=/etc/ssl/certs
