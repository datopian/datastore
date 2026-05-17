FROM python:3.13-slim AS base
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

FROM base AS deps
WORKDIR /build
COPY pyproject.toml README.md ./
COPY datastore ./datastore
RUN pip install --prefix=/install .

FROM base AS runtime
WORKDIR /srv
COPY --from=deps /install /usr/local
COPY datastore ./datastore
RUN useradd -m -u 1000 datastore && chown -R datastore:datastore /srv
USER datastore
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).status == 200 else 1)"
CMD ["uvicorn", "datastore.main:app", "--host", "0.0.0.0", "--port", "8000"]
