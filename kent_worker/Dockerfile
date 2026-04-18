FROM prefecthq/prefect:3-python3.13

RUN apt-get update && \
    apt-get install -y curl iptables && \
    curl -fsSL -o /tmp/litestream.deb \
        https://github.com/benbjohnson/litestream/releases/download/v0.3.13/litestream-v0.3.13-linux-$(dpkg --print-architecture).deb && \
    dpkg -i /tmp/litestream.deb && \
    rm /tmp/litestream.deb && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://tailscale.com/install.sh | sh

RUN pip install uv

# Install kent and juriscraper from named build contexts
COPY --from=kent . /build/kent
COPY --from=juriscraper . /build/juriscraper
RUN uv pip install --system \
    /build/juriscraper \
    "/build/kent[persistent-driver,web]" \
    prefect-aws \
    pyyaml \
    uvicorn \
    boto3 \
    aiosqlite \
    zstandard

# Copy application code
COPY flows/ /app/flows/
COPY workers/ /app/workers/
COPY worker-entrypoint.sh /worker-entrypoint.sh

WORKDIR /app
ENTRYPOINT ["/worker-entrypoint.sh"]
