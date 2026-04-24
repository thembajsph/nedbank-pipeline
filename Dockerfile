FROM nedbank-base:latest

WORKDIR /app

COPY pipeline/ ./pipeline/
COPY config/ ./config/

# Ensure duckdb is available (it's already in base image)
RUN python3 -c "import duckdb; print(f'DuckDB version: {duckdb.__version__}')"

CMD ["python3", "-c", "print('Nedbank Pipeline Ready')"]
