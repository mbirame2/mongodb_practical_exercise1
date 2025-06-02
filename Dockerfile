FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN apt-get update && \
    apt-get install -y cron && \
    pip install pymongo pandas python-dotenv && \
    rm -rf /var/lib/apt/lists/*

COPY . .

# Set up cron
RUN chmod +x /app/etl/pipeline.py && \
    touch /app/cron.log && \
    echo "*/5 * * * * /usr/local/bin/python /app/pipeline.py >> /app/cron.log 2>&1" > /etc/cron.d/pipeline-cron && \
    chmod 0644 /etc/cron.d/pipeline-cron && \
    crontab /etc/cron.d/pipeline-cron

# Start services
CMD service cron start && tail -f /app/cron.log