"""Prefect worker containers."""

import pulumi
import pulumi_docker as docker


def create_kent_worker(remote_provider: docker.Provider):
    """Kent scraper worker on the remote host.

    The kent-worker image must be pre-built and available on the remote
    host. Build and transfer with:

        podman build -t en-banc-kent-worker kent_worker/
        podman save en-banc-kent-worker | ssh bc@mini.bopp-justice.ts.net podman load
    """
    config = pulumi.Config()

    container = docker.Container(
        "kent-worker",
        image="en-banc-kent-worker:latest",
        name="en-banc-kent-worker",
        restart="unless-stopped",
        envs=[
            f"PREFECT_API_URL={config.get('prefect-api-url') or 'http://brennans-macbook-pro.bopp-justice.ts.net:7100/api'}",
            "SCRAPER_RUNS_DIR=/app/runs",
            "PREFECT_LOGGING_EXTRA_LOGGERS=kent,kent.driver,kent.driver.persistent_driver,juriscraper",
        ],
        volumes=[
            docker.ContainerVolumeArgs(
                host_path=config.get("kent-runs-dir") or "/Users/bc/kent_worker/runs",
                container_path="/app/runs",
            ),
        ],
        opts=pulumi.ResourceOptions(
            provider=remote_provider,
            ignore_changes=["image"],
        ),
    )

    pulumi.export("kent_worker_container_name", container.name)


