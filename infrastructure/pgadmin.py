"""pgAdmin container."""

import json

import pulumi
import pulumi_docker as docker


def create_pgadmin(network: docker.Network, pg_password: str):
    image = docker.RemoteImage(
        "pgadmin-image",
        name="dpage/pgadmin4:9.14",
        keep_locally=True,
    )

    servers_json = json.dumps({
        "Servers": {
            "1": {
                "Name": "warehouse",
                "Group": "en-banc",
                "Host": "warehouse",
                "Port": 5432,
                "MaintenanceDB": "warehouse",
                "Username": "postgres",
                "SSLMode": "prefer",
            }
        }
    })

    container = docker.Container(
        "pgadmin",
        image=image.image_id,
        name="en-banc-pgadmin",
        opts=pulumi.ResourceOptions(ignore_changes=["image"]),
        envs=[
            "PGADMIN_DEFAULT_EMAIL=admin@freelaw.org",
            f"PGADMIN_DEFAULT_PASSWORD={pg_password}",
            f"PGADMIN_SERVER_JSON_FILE=/pgadmin4/servers.json",
        ],
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name,
                aliases=["pgadmin"],
            )
        ],
        ports=[
            docker.ContainerPortArgs(
                internal=80,
                external=7120,
            )
        ],
        uploads=[
            docker.ContainerUploadArgs(
                file="/pgadmin4/servers.json",
                content=servers_json,
            )
        ],
        healthcheck=docker.ContainerHealthcheckArgs(
            tests=["CMD-SHELL", "wget -q --spider http://localhost/misc/ping || exit 1"],
            interval="10s",
            timeout="5s",
            retries=6,
        ),
        wait=True,
        wait_timeout=60,
    )

    pulumi.export("pgadmin_url", "http://localhost:7120")
    pulumi.export("pgadmin_container_name", container.name)
