"""Prefect server container."""

import pulumi
import pulumi_docker as docker


def create_prefect_server(network: docker.Network, prefect_db_container=None):
    config = pulumi.Config()
    pg_password = config.get("pg-password") or "postgres"
    db_url = f"postgresql+asyncpg://postgres:{pg_password}@prefect-db:5432/prefect"

    depends = []
    if prefect_db_container:
        depends.append(prefect_db_container)

    image = docker.RemoteImage(
        "prefect-server-image",
        name="prefecthq/prefect:3-latest",
        keep_locally=True,
    )

    container = docker.Container(
        "prefect-server",
        image=image.image_id,
        name="en-banc-prefect-server",
        command=["prefect", "server", "start", "--host", "0.0.0.0"],
        envs=[
            "PREFECT_SERVER_API_HOST=0.0.0.0",
            "PREFECT_UI_API_URL=http://localhost:7100/api",
            f"PREFECT_API_DATABASE_CONNECTION_URL={db_url}",
        ],
        opts=pulumi.ResourceOptions(depends_on=depends, ignore_changes=["image"]),
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name,
                aliases=["prefect"],
            )
        ],
        ports=[
            docker.ContainerPortArgs(
                internal=4200,
                external=7100,
            )
        ],
        healthcheck=docker.ContainerHealthcheckArgs(
            tests=["CMD-SHELL", "python -c 'import urllib.request; urllib.request.urlopen(\"http://localhost:4200/api/health\")'"],
            interval="10s",
            timeout="5s",
            retries=6,
        ),
        wait=True,
        wait_timeout=60,
    )

    pulumi.export("prefect_url", "http://localhost:7100")
    pulumi.export("prefect_container_name", container.name)
