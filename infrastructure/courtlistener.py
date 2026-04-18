"""CourtListener development stack containers."""

from pathlib import Path

import pulumi
import pulumi_docker as docker

CL_BASE_DIR = "/Users/bc/code/freelaw/courtlistener"


def _read_env_file(path: str) -> list[str]:
    """Read a .env file and return KEY=VALUE strings.

    Strips quotes, skips comments and empty values.
    """
    envs = []
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            value = value.strip().strip('"').strip("'")
            if not value:
                continue
            envs.append(f"{key.strip()}={value}")
        else:
            envs.append(line)
    return envs


def create_courtlistener(network: docker.Network):
    config = pulumi.Config()
    cl_base = config.get("cl-base-dir") or CL_BASE_DIR

    # Read .env.dev and layer on explicit overrides for service hostnames
    cl_envs = _read_env_file(f"{cl_base}/.env.dev")
    cl_envs += [
        # Database — use the en-banc courtlistener container
        "DB_HOST=cl-postgres",
        "DB_NAME=courtlistener",
        "DB_USER=postgres",
        "DB_PASSWORD=postgres",
        "DB_SSL_MODE=disable",
        # Redis
        "REDIS_HOST=eb-redis",
        # Elasticsearch
        "ELASTICSEARCH_DSL_HOST=https://eb-es:9200",
        "ELASTICSEARCH_USER=elastic",
        "ELASTICSEARCH_PASSWORD=password",
        "ELASTICSEARCH_CA_CERT=/opt/courtlistener/docker/elastic/ca.crt",
        # Django
        "SECRET_KEY=dev-secret-key-not-for-production",
        "DEBUG=on",
        "DEVELOPMENT=on",
        "ALLOWED_HOSTS=*",
        # Prefect
        "PREFECT_API_URL=http://prefect:4200/api",
        # Doctor / Disclosures
        "DOCTOR_HOST=http://eb-doctor:5050",
        "DISCLOSURE_HOST=http://eb-disclosures:5050",
    ]

    # -----------------------------------------------------------------------
    # Redis
    # -----------------------------------------------------------------------
    redis_image = docker.RemoteImage(
        "cl-redis-image", name="redis", keep_locally=True,
    )
    eb_redis = docker.Container(
        "eb-redis",
        image=redis_image.image_id,
        name="eb-redis",
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name, aliases=["eb-redis"],
            )
        ],
        opts=pulumi.ResourceOptions(ignore_changes=["image"]),
    )

    # -----------------------------------------------------------------------
    # Selenium
    # -----------------------------------------------------------------------
    selenium_image = docker.RemoteImage(
        "cl-selenium-image",
        name="seleniarm/standalone-chromium:124.0",
        keep_locally=True,
    )
    eb_selenium = docker.Container(
        "eb-selenium",
        image=selenium_image.image_id,
        name="eb-selenium",
        ports=[
            docker.ContainerPortArgs(internal=5900, external=7130),
        ],
        envs=["JAVA_OPTS=-Dwebdriver.chrome.whitelistedIps="],
        volumes=[
            docker.ContainerVolumeArgs(
                host_path="/dev/shm", container_path="/dev/shm",
            ),
        ],
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name, aliases=["eb-selenium"],
            )
        ],
        opts=pulumi.ResourceOptions(ignore_changes=["image"]),
    )

    # -----------------------------------------------------------------------
    # Doctor
    # -----------------------------------------------------------------------
    doctor_image = docker.RemoteImage(
        "cl-doctor-image",
        name="freelawproject/doctor:latest",
        keep_locally=True,
    )
    eb_doctor = docker.Container(
        "eb-doctor",
        image=doctor_image.image_id,
        name="eb-doctor",
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name, aliases=["eb-doctor"],
            )
        ],
        opts=pulumi.ResourceOptions(ignore_changes=["image"]),
    )

    # -----------------------------------------------------------------------
    # Disclosures
    # -----------------------------------------------------------------------
    disclosures_image = docker.RemoteImage(
        "cl-disclosures-image",
        name="freelawproject/disclosure-extractor:latest",
        keep_locally=True,
    )
    eb_disclosures = docker.Container(
        "eb-disclosures",
        image=disclosures_image.image_id,
        name="eb-disclosures",
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name, aliases=["eb-disclosures"],
            )
        ],
        opts=pulumi.ResourceOptions(ignore_changes=["image"]),
    )

    # -----------------------------------------------------------------------
    # Webhook Sentry
    # -----------------------------------------------------------------------
    webhook_image = docker.RemoteImage(
        "cl-webhook-sentry-image",
        name="juggernaut/webhook-sentry:latest",
        keep_locally=True,
    )
    docker.Container(
        "eb-webhook-sentry",
        image=webhook_image.image_id,
        name="eb-webhook-sentry",
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name, aliases=["eb-webhook-sentry"],
            )
        ],
        opts=pulumi.ResourceOptions(ignore_changes=["image"]),
    )

    # -----------------------------------------------------------------------
    # Elasticsearch
    # -----------------------------------------------------------------------
    es_image = docker.RemoteImage(
        "cl-es-image",
        name="elastic/elasticsearch:9.0.1",
        keep_locally=True,
    )
    eb_es = docker.Container(
        "eb-es",
        image=es_image.image_id,
        name="eb-es",
        envs=[
            "discovery.type=single-node",
            "cluster.name=courtlistener-cluster",
            "cluster.routing.allocation.disk.threshold_enabled=false",
            "xpack.security.enabled=true",
            "xpack.security.http.ssl.enabled=true",
            "xpack.security.http.ssl.key=certs/cl-es.key",
            "xpack.security.http.ssl.certificate=certs/cl-es.crt",
            "xpack.security.http.ssl.certificate_authorities=certs/ca.crt",
            "xpack.security.transport.ssl.enabled=true",
            "xpack.security.transport.ssl.key=certs/cl-es.key",
            "xpack.security.transport.ssl.certificate=certs/cl-es.crt",
            "xpack.security.transport.ssl.certificate_authorities=certs/ca.crt",
            "xpack.security.transport.ssl.verification_mode=certificate",
            "ELASTIC_PASSWORD=password",
            "ES_JAVA_OPTS=-Xms512m -Xmx512m",
        ],
        ports=[
            docker.ContainerPortArgs(internal=9200, external=7140),
        ],
        volumes=[
            docker.ContainerVolumeArgs(
                host_path=f"{cl_base}/docker/elastic/cl-es.crt",
                container_path="/usr/share/elasticsearch/config/certs/cl-es.crt",
            ),
            docker.ContainerVolumeArgs(
                host_path=f"{cl_base}/docker/elastic/cl-es.key",
                container_path="/usr/share/elasticsearch/config/certs/cl-es.key",
            ),
            docker.ContainerVolumeArgs(
                host_path=f"{cl_base}/docker/elastic/ca.crt",
                container_path="/usr/share/elasticsearch/config/certs/ca.crt",
            ),
            docker.ContainerVolumeArgs(
                host_path=f"{cl_base}/cl/search/elasticsearch_files/synonyms_en.txt",
                container_path="/usr/share/elasticsearch/config/dictionaries/synonyms_en.txt",
            ),
            docker.ContainerVolumeArgs(
                host_path=f"{cl_base}/cl/search/elasticsearch_files/stopwords_en.txt",
                container_path="/usr/share/elasticsearch/config/dictionaries/stopwords_en.txt",
            ),
        ],
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name, aliases=["eb-es"],
            )
        ],
        opts=pulumi.ResourceOptions(ignore_changes=["image"]),
    )

    # -----------------------------------------------------------------------
    # Webpack (Node)
    # -----------------------------------------------------------------------
    node_image = docker.RemoteImage(
        "cl-node-image", name="node:16", keep_locally=True,
    )
    eb_webpack = docker.Container(
        "eb-webpack",
        image=node_image.image_id,
        name="eb-webpack",
        working_dir="/opt/courtlistener/cl",
        command=["sh", "-c", "npm install && exec npx webpack --progress --watch --mode=development"],
        volumes=[
            docker.ContainerVolumeArgs(
                host_path=cl_base, container_path="/opt/courtlistener",
            ),
        ],
        healthcheck=docker.ContainerHealthcheckArgs(
            tests=["CMD-SHELL", "ps aux | grep -v grep | grep webpack >/dev/null 2>&1 && echo 0 || echo 1"],
            interval="1s",
            timeout="1s",
            retries=45,
            start_period="1s",
        ),
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name, aliases=["eb-webpack"],
            )
        ],
        opts=pulumi.ResourceOptions(ignore_changes=["image"]),
    )

    # -----------------------------------------------------------------------
    # Tailwind reload (Node)
    # -----------------------------------------------------------------------
    docker.Container(
        "eb-tailwind-reload",
        image=node_image.image_id,
        name="eb-tailwind-reload",
        working_dir="/opt/courtlistener/cl",
        command=["sh", "-c", "npm run dev"],
        tty=True,
        volumes=[
            docker.ContainerVolumeArgs(
                host_path=cl_base, container_path="/opt/courtlistener",
            ),
        ],
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name, aliases=["eb-tailwind-reload"],
            )
        ],
        opts=pulumi.ResourceOptions(
            depends_on=[eb_webpack],
            ignore_changes=["image"],
        ),
    )

    # -----------------------------------------------------------------------
    # Django app server + Celery worker
    # -----------------------------------------------------------------------
    # The CL Django image must be pre-built:
    #   cd ../courtlistener
    #   podman build --no-cache -t eb-django:dev --build-arg BUILD_ENV=dev \
    #       -f docker/django/Dockerfile .
    #
    # If elasticsearch-dsl is missing, patch it:
    #   podman run --name fix --user root --entrypoint uv eb-django:dev \
    #       pip install --python /opt/venv/bin/python elasticsearch-dsl
    #   podman commit fix eb-django:dev && podman rm fix

    eb_celery = docker.Container(
        "eb-celery",
        image="eb-django:dev",
        name="eb-celery",

        command=["celery"],
        envs=cl_envs,
        volumes=[
            docker.ContainerVolumeArgs(
                host_path=cl_base, container_path="/opt/courtlistener",
            ),
        ],
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name, aliases=["eb-celery"],
            )
        ],
        opts=pulumi.ResourceOptions(
            depends_on=[eb_redis, eb_doctor, eb_disclosures],
            ignore_changes=["image"],
        ),
    )

    docker.Container(
        "eb-django",
        image="eb-django:dev",
        name="eb-django",

        command=["web-dev"],
        user="root",
        envs=cl_envs,
        ports=[
            docker.ContainerPortArgs(internal=8000, external=7150),
        ],
        volumes=[
            docker.ContainerVolumeArgs(
                host_path=cl_base, container_path="/opt/courtlistener",
            ),
            docker.ContainerVolumeArgs(
                host_path=f"{cl_base}/.postgresql",
                container_path="/root/.postgresql",
            ),
        ],
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name, aliases=["eb-django"],
            )
        ],
        opts=pulumi.ResourceOptions(
            depends_on=[eb_redis, eb_celery, eb_selenium, eb_doctor, eb_disclosures, eb_es],
            ignore_changes=["image"],
        ),
    )

    # -----------------------------------------------------------------------
    # Sync worker — runs `manage.py sync_worker` inside the CL Django image
    # -----------------------------------------------------------------------
    docker.Container(
        "eb-sync-worker",
        image="eb-django:dev",
        name="eb-sync-worker",
        entrypoints=["python"],
        command=["manage.py", "sync_worker"],
        working_dir="/opt/courtlistener",
        envs=cl_envs,
        restart="unless-stopped",
        volumes=[
            docker.ContainerVolumeArgs(
                host_path=cl_base, container_path="/opt/courtlistener",
            ),
        ],
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name, aliases=["eb-sync-worker"],
            )
        ],
        opts=pulumi.ResourceOptions(
            depends_on=[eb_redis, eb_es, eb_doctor, eb_disclosures],
            ignore_changes=["image"],
        ),
    )

    pulumi.export("courtlistener_url", "http://localhost:7150")
