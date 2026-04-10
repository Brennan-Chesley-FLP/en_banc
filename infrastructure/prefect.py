"""Prefect server container."""

import pulumi
import pulumi_docker as docker


def create_prefect_server(network: docker.Network):
    container = docker.Container(
        "prefect-server",
        image="prefecthq/prefect:3-latest",
        name="en-banc-prefect-server",
        command=["prefect", "server", "start", "--host", "0.0.0.0"],
        envs=[
            "PREFECT_SERVER_API_HOST=0.0.0.0",
        ],
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
