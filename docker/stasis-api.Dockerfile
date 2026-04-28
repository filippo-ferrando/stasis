FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY stasis_api.py .
COPY stasis_discovery.py .
COPY static ./static
COPY templates ./templates

EXPOSE 5000
EXPOSE 7000

CMD ["python3", "stasis_api.py"]
