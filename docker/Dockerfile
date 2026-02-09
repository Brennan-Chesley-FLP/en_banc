FROM prefecthq/prefect:3-python3.13

RUN apt-get update && \
    apt-get install -y curl && \
    curl -fsSL https://tailscale.com/install.sh | sh && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install uv

COPY juriscraper /juriscraper
COPY en_banc/pyproject.toml en_banc/uv.lock /app/
COPY en_banc/infrastructure/sdks /app/infrastructure/sdks
WORKDIR /app
RUN uv pip install --system --override /dev/null \
    /juriscraper .

COPY en_banc /app
COPY en_banc/docker/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
