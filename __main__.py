"""A Python Pulumi program"""

import pulumi
import pulumi_docker as docker

from infrastructure.databases import create_databases
from infrastructure.local_aws import create_localstack
from infrastructure.pgadmin import create_pgadmin
from infrastructure.prefect.prefect import create_prefect_server

config = pulumi.Config()
pg_password = config.get("pg-password") or "postgres"

network = docker.Network("en-banc-network")

db_containers = create_databases(network, pg_password)
create_prefect_server(network, prefect_db_container=db_containers["prefect"])
create_pgadmin(network, pg_password)
create_localstack()
