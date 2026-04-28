FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY stasis_watchdog.py .

VOLUME ["/images"]

CMD ["python3", "stasis_watchdog.py"]
