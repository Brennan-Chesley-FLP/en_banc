FROM prefecthq/prefect:3-python3.13

RUN apt-get update && \
    apt-get install -y curl libpq-dev iptables && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://tailscale.com/install.sh | sh

RUN pip install uv

# Copy only package metadata from sibling repos for install
COPY --from=juriscraper pyproject.toml README* LICENSE* /build/juriscraper/
COPY --from=juriscraper juriscraper /build/juriscraper/juriscraper
COPY --from=kent pyproject.toml README* LICENSE* /build/kent/
COPY --from=kent kent /build/kent/kent
COPY --from=steno pyproject.toml README* LICENSE* /build/steno/
COPY --from=steno steno /build/steno/steno

WORKDIR /app
RUN uv pip install --system \
    /build/juriscraper \
    "/build/kent[persistent-driver,web]" \
    /build/steno \
    prefect-aws \
    prefect-sqlalchemy \
    sqlmodel \
    psycopg2-binary \
    boto3 \
    pyyaml \
    eyecite \
    "sqlmesh[postgres]"

COPY en_banc/flows /app/flows
COPY en_banc/warehouse /app/warehouse
COPY en_banc/sql_processing /app/sql_processing
COPY en_banc/docker/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
