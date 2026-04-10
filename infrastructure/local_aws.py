"""MinIO on remote server for S3-compatible object storage."""

import pulumi
import pulumi_docker as docker


def create_minio():
    config = pulumi.Config()
    remote_host = (
        config.get("remote-docker-host")
        or "ssh://bc@mini.bopp-justice.ts.net"
    )

    remote_provider = docker.Provider(
        "remote-docker",
        host=remote_host,
    )

    opts = pulumi.ResourceOptions(provider=remote_provider)

    container = docker.Container(
        "minio",
        image="minio/minio",
        name="eb-minio",
        restart="unless-stopped",
        command=["server", "/data", "--console-address", ":9001"],
        ports=[
            docker.ContainerPortArgs(
                internal=9000,
                external=7110,
            ),
            docker.ContainerPortArgs(
                internal=9001,
                external=7111,
            ),
        ],
        envs=[
            "MINIO_ROOT_USER=minioadmin",
            "MINIO_ROOT_PASSWORD=minioadmin",
        ],
        volumes=[
            docker.ContainerVolumeArgs(
                host_path="/Volumes/Public/freelaw/localstack",
                container_path="/data",
            ),
        ],
        opts=opts,
    )

    pulumi.export("minio_api_url", "http://mini.bopp-justice.ts.net:7110")
    pulumi.export("minio_console_url", "http://mini.bopp-justice.ts.net:7111")
    pulumi.export("minio_container_name", container.name)
