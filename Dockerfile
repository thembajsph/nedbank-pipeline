# Use your locally built base image
FROM nedbank-base:latest

WORKDIR /app

COPY requirements.txt .
RUN if [ -s requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

COPY pipeline/ ./pipeline/
COPY config/ ./config/

ENV PYTHONPATH=/app

# Run all pipeline layers
CMD ["python", "-m", "pipeline.run_all"]
