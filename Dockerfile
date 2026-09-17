# The relay: many workspaces in one deployment, with its console served from
# the same address. State lives in Postgres, so the container keeps nothing
# and can be restarted or replaced without losing anything.
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml requirements.txt ./
COPY relay ./relay
COPY app.py ./
# The console is part of the server now: it is served from the same origin as
# the API, which is what makes /acme both a page and a relay.
COPY ui ./ui
# The workers too, so the same image can be deployed again with a different
# start command: one service serves the relay, another watches a channel and
# sends what arrives somewhere. They share nothing but the image.
COPY examples ./examples
RUN pip install --no-cache-dir . "uvicorn[standard]>=0.30"

# The host says which port to listen on; 8000 is only a fallback for running
# the image by hand.
ENV PORT=8000
EXPOSE 8000

CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]
