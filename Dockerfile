FROM nedbank-base:latest

WORKDIR /app

COPY requirements.txt .
RUN if [ -s requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

COPY pipeline/ ./pipeline/
COPY config/ ./config/

ENV PYTHONPATH=/app

CMD ["python", "-m", "pipeline.run_all"]
