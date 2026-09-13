# The relay server. It holds long-lived websockets and a SQLite file, so it
# runs as one ordinary always-on process, not as a serverless function.
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY relay ./relay
RUN pip install --no-cache-dir ".[server]"

# The database is the whole state of the server, so it lives on the mounted
# volume, not in the container's own filesystem.
ENV RELAY_DB=/data/relay.db
EXPOSE 8700

CMD ["sh", "-c", "exec python -m relay serve --host 0.0.0.0 --port ${PORT:-8700}"]
