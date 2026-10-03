# Build context is the repository root (see docker-compose.yml).
FROM python:3.12-slim
WORKDIR /app
COPY replayer/requirements.txt replayer/requirements.txt
RUN pip install --no-cache-dir -r replayer/requirements.txt
COPY schemas schemas
COPY replayer/replayer replayer/replayer
WORKDIR /app/replayer
ENTRYPOINT ["python", "-m", "replayer.replay"]
