# Data Sentinel publish worker (V2) - to WORKER-STANDARD 2026-46-10, architect 2026-47-46 with note 2026-47-47
FROM python:3.13-slim-trixie

ARG DS_UID=10001
ARG DS_GID=10001

# Who it is. On DEV the services are ds_publish_01 to ds_publish_04 on 5701 to 5704, set in the compose file.
ENV DS_SERVICE=ds_publish \
    DS_SERVICE_EXTERNAL_HOST=ds_publish \
    DS_SERVICE_EXTERNAL_PORT=5700 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.txt

COPY main.py ds_publish.py ./

RUN groupadd --gid ${DS_GID} sentinel \
 && useradd --uid ${DS_UID} --gid ${DS_GID} --no-create-home --shell /usr/sbin/nologin sentinel \
 && mkdir -p /mnt/logs && chown ${DS_UID}:${DS_GID} /mnt/logs

USER ${DS_UID}:${DS_GID}

EXPOSE 5700
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.environ.get('DS_SERVICE_EXTERNAL_PORT', '5700') + '/health', timeout=4)" || exit 1
STOPSIGNAL SIGTERM

CMD ["python", "main.py"]
