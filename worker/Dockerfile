FROM xserrat/facebook-demucs:latest

RUN pip3 install --upgrade minio redis requests

WORKDIR /app

COPY worker.py .

ENTRYPOINT ["python3", "worker.py"]
