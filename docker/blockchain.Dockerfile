FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY stasis-api.py .
COPY static ./static
COPY templates ./templates

EXPOSE 5000

CMD ["python3", "stasis-api.py"]
