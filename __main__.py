"""A Python Pulumi program"""

import pulumi
import pulumi_docker as docker

from infrastructure.courtlistener import create_courtlistener
from infrastructure.databases import create_databases
from infrastructure.local_aws import create_minio, create_remote_provider
from infrastructure.pgadmin import create_pgadmin
from infrastructure.prefect.prefect import create_prefect_server
from infrastructure.workers import create_kent_worker

config = pulumi.Config()
pg_password = config.get("pg-password") or "postgres"

network = docker.Network("en-banc-network")

db_containers = create_databases(network, pg_password)
create_prefect_server(network, prefect_db_container=db_containers["prefect"])
create_pgadmin(network, pg_password)

remote_provider = create_remote_provider()
create_minio(remote_provider)
create_kent_worker(remote_provider)
create_courtlistener(network)
