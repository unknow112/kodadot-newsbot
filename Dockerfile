FROM python:3.9.5-slim

WORKDIR /app

COPY requirements.txt src/*.py ./

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python" ,"/app/main.py"]

