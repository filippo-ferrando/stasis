FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY blockchain-service.py .
COPY static ./static
COPY templates ./templates

EXPOSE 5000

CMD ["python3", "blockchain-service.py"]
