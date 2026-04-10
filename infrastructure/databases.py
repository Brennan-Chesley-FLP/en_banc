"""PostgreSQL database containers."""

import pulumi
import pulumi_docker as docker


def create_databases(network: docker.Network, pg_password: str):
    databases = {
        "warehouse": 7101,
        "courtlistener": 7102,
        "replica_client_a": 7103,
    }

    for db_name, port in databases.items():
        container = docker.Container(
            f"pg-{db_name}",
            image="postgres:16",
            name=f"en-banc-pg-{db_name}",
            envs=[
                f"POSTGRES_DB={db_name}",
                f"POSTGRES_PASSWORD={pg_password}",
            ],
            networks_advanced=[
                docker.ContainerNetworksAdvancedArgs(
                    name=network.name,
                    aliases=[db_name],
                )
            ],
            ports=[
                docker.ContainerPortArgs(
                    internal=5432,
                    external=port,
                )
            ],
            healthcheck=docker.ContainerHealthcheckArgs(
                tests=["CMD-SHELL", "pg_isready -U postgres"],
                interval="5s",
                timeout="5s",
                retries=5,
            ),
            wait=True,
            wait_timeout=30,
        )

        pulumi.export(f"{db_name}_container_name", container.name)
        pulumi.export(
            f"{db_name}_port",
            container.ports.apply(
                lambda ports: ports[0].external if ports else None
            ),
        )
