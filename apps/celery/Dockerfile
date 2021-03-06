FROM python:3.8
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
WORKDIR /app
COPY . /app
CMD ["celery", "-A", "app.app", "worker", "-l", "info", "-B"]