FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir .

COPY app ./app

RUN useradd -m -u 10001 service && chown -R service:service /app
USER service

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import httpx,sys;sys.exit(0 if httpx.get('http://127.0.0.1:8080/healthz',timeout=5).status_code==200 else 1)"

CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8080"]
