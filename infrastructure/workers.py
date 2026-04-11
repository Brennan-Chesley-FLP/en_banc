"""Prefect worker containers."""

import pulumi
import pulumi_docker as docker


def create_kent_worker(remote_provider: docker.Provider):
    """Kent scraper worker on the remote host."""
    config = pulumi.Config()
    opts = pulumi.ResourceOptions(
        provider=remote_provider,
        ignore_changes=["image"],
    )

    image = docker.RemoteImage(
        "kent-worker-image",
        name="prefecthq/prefect:3-python3.13",
        keep_locally=True,
        opts=pulumi.ResourceOptions(provider=remote_provider),
    )

    container = docker.Container(
        "kent-worker",
        image=image.image_id,
        name="en-banc-kent-worker",
        restart="unless-stopped",
        command=["python", "-m", "workers.in_process"],
        envs=[
            f"PREFECT_API_URL={config.get('prefect-api-url') or 'http://localhost:7100/api'}",
            "SCRAPER_RUNS_DIR=/app/runs",
        ],
        volumes=[
            docker.ContainerVolumeArgs(
                host_path="/Volumes/Public/freelaw/kent_worker/runs",
                container_path="/app/runs",
            ),
        ],
        opts=opts,
    )

    pulumi.export("kent_worker_container_name", container.name)


def create_warehouse_worker(network: docker.Network):
    """Warehouse worker on the local host."""
    config = pulumi.Config()
    pg_password = config.get("pg-password") or "postgres"

    image = docker.RemoteImage(
        "warehouse-worker-image",
        name="prefecthq/prefect:3-python3.13",
        keep_locally=True,
    )

    container = docker.Container(
        "warehouse-worker",
        image=image.image_id,
        name="en-banc-warehouse-worker",
        restart="unless-stopped",
        command=[
            "prefect", "worker", "start",
            "--pool", "docker-pool",
            "--type", "docker",
        ],
        envs=[
            f"PREFECT_API_URL={config.get('prefect-api-url') or 'http://prefect:4200/api'}",
        ],
        networks_advanced=[
            docker.ContainerNetworksAdvancedArgs(
                name=network.name,
                aliases=["warehouse-worker"],
            )
        ],
        opts=pulumi.ResourceOptions(ignore_changes=["image"]),
    )

    pulumi.export("warehouse_worker_container_name", container.name)
