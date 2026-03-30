FROM python:3.10-slim

WORKDIR /app

RUN pip3 install --upgrade \
    flask \
    redis \
    jsonpickle \
    requests \
    minio

COPY rest-server.py .

EXPOSE 5000

CMD ["python3", "rest-server.py"]
