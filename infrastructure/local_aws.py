"""MinIO on remote server for S3-compatible object storage."""

import pulumi
import pulumi_docker as docker


def create_remote_provider() -> docker.Provider:
    config = pulumi.Config()
    remote_host = (
        config.get("remote-docker-host")
        or "ssh://bc@mini.bopp-justice.ts.net"
    )
    return docker.Provider("remote-docker", host=remote_host)


def create_minio(remote_provider: docker.Provider):
    opts = pulumi.ResourceOptions(provider=remote_provider)

    image = docker.RemoteImage(
        "minio-image",
        name="minio/minio",
        keep_locally=True,
        opts=opts,
    )

    container = docker.Container(
        "minio",
        image=image.image_id,
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
                host_path="/Volumes/Public/freelaw/minio",
                container_path="/data",
            ),
        ],
        opts=pulumi.ResourceOptions(provider=remote_provider, ignore_changes=["image"]),
    )

    pulumi.export("minio_api_url", "http://mini.bopp-justice.ts.net:7110")
    pulumi.export("minio_console_url", "http://mini.bopp-justice.ts.net:7111")
    pulumi.export("minio_container_name", container.name)
