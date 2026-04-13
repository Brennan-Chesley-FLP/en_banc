"""PostgreSQL database containers."""

import pulumi
import pulumi_docker as docker


DATABASES = {
    "prefect": {"port": 7104, "alias": "prefect-db"},
    "warehouse": {"port": 7101},
    "courtlistener": {"port": 7102, "aliases": ["courtlistener", "cl-postgres"]},
    "replica_client_a": {"port": 7103},
}


def create_databases(network: docker.Network, pg_password: str):
    databases = DATABASES
    containers = {}

    for db_name, db_config in databases.items():
        port = db_config["port"]
        aliases = db_config.get("aliases", [db_config.get("alias", db_name)])

        image = docker.RemoteImage(
            f"pg-{db_name}-image",
            name="postgres:18.3",
            keep_locally=True,
        )

        volume = docker.Volume(f"pg-{db_name}-data", name=f"en-banc-pg-{db_name}-data")

        container = docker.Container(
            f"pg-{db_name}",
            image=image.image_id,
            name=f"en-banc-pg-{db_name}",
            opts=pulumi.ResourceOptions(ignore_changes=["image"]),
            envs=[
                f"POSTGRES_DB={db_name}",
                f"POSTGRES_PASSWORD={pg_password}",
            ],
            networks_advanced=[
                docker.ContainerNetworksAdvancedArgs(
                    name=network.name,
                    aliases=aliases,
                )
            ],
            ports=[
                docker.ContainerPortArgs(
                    internal=5432,
                    external=port,
                )
            ],
            volumes=[
                docker.ContainerVolumeArgs(
                    volume_name=volume.name,
                    container_path="/var/lib/postgresql",
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

        containers[db_name] = container
        pulumi.export(f"{db_name}_container_name", container.name)
        pulumi.export(
            f"{db_name}_port",
            container.ports.apply(
                lambda ports: ports[0].external if ports else None
            ),
        )

    return containers
