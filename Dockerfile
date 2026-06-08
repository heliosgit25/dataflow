FROM python:3.14.5

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

CMD ["python", "pipelines/producer/event_producer.py"]
