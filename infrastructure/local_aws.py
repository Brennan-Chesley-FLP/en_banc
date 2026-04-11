"""LocalStack on remote server for local AWS services."""

import pulumi
import pulumi_docker as docker


def create_localstack():
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

    image = docker.RemoteImage(
        "localstack-image",
        name="localstack/localstack:community-archive",
        keep_locally=True,
        opts=opts,
    )

    container = docker.Container(
        "localstack",
        image=image.image_id,
        name="eb-localstack",
        restart="unless-stopped",
        ports=[
            docker.ContainerPortArgs(
                internal=4566,
                external=7110,
            ),
        ],
        envs=[
            "SERVICES=s3,sns,sqs,secretsmanager,lambda",
            "PERSISTENCE=1",
        ],
        volumes=[
            docker.ContainerVolumeArgs(
                host_path="/Volumes/Public/freelaw/localstack",
                container_path="/var/lib/localstack",
            ),
            docker.ContainerVolumeArgs(
                host_path="/run/user/501/podman/podman.sock",
                container_path="/var/run/docker.sock",
            ),
        ],
        opts=pulumi.ResourceOptions(provider=remote_provider, ignore_changes=["image"]),
    )

    pulumi.export("localstack_url", "http://mini.bopp-justice.ts.net:7110")
    pulumi.export("localstack_container_name", container.name)
