FROM python:3.13-slim AS base
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

FROM base AS deps
WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --prefix=/install .

FROM base AS runtime
WORKDIR /app
COPY --from=deps /install /usr/local
COPY src ./src
RUN useradd -m -u 1000 datastore && chown -R datastore:datastore /app
USER datastore
EXPOSE 8000
CMD ["uvicorn", "datastore.main:app", "--host", "0.0.0.0", "--port", "8000"]
