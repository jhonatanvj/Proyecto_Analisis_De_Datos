# Image for both Dash apps (services governance-dashboard and storytelling-dashboard).
# The code is mounted as a volume, not copied, so dashboard changes need no rebuild.
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# listen on every interface so localhost:8050/8051 on the host reaches the container;
# Gold is mounted at /app/datalake_gold
ENV DASH_HOST=0.0.0.0 \
    DATALAKE_ROOT=/app \
    PYTHONUNBUFFERED=1
