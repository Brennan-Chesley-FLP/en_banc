Deployment
==========

en-banc uses `Pulumi <https://www.pulumi.com/>`_ to define all
infrastructure as Python code. A single Pulumi program
(``infrastructure/__main__.py``) provisions both AWS resources and Prefect
objects (blocks, deployments, automations), keeping everything in sync.

.. contents:: On this page
   :local:
   :depth: 2

Local Development
-----------------

The local stack runs entirely on your machine using Podman, LocalStack,
and a self-hosted Prefect server.

Prerequisites
~~~~~~~~~~~~~

Install the following before you begin:

- `Podman <https://podman.io/>`_ with ``podman compose``
- `mise <https://mise.jdx.dev/>`_ (task runner and env manager)
- `uv <https://docs.astral.sh/uv/>`_ (Python package manager)
- `Pulumi CLI <https://www.pulumi.com/docs/install/>`_

Start the Podman machine::

    podman machine start

Install Python dependencies::

    uv sync

Starting services
~~~~~~~~~~~~~~~~~

Bring up Postgres, the Prefect server, a Docker worker, and LocalStack::

    mise run up

The Prefect UI will be available at http://localhost:4200.

Provisioning infrastructure
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pulumi provisions LocalStack AWS resources and creates the corresponding
Prefect blocks and deployments in a single pass::

    mise run infra

On the first run this also generates the Pulumi Prefect provider SDK
locally (via ``pulumi package add``). Subsequent runs skip that step.

The program creates:

**AWS resources (via LocalStack)**

- S3 buckets: ``scrapers``, ``emails``
- SNS topics: ``email-notices``
- SQS queues: ``outbox``

**Prefect blocks**

- ``aws-credentials`` -- ``localstack-creds`` with test credentials
- ``s3-bucket`` -- one block per S3 bucket
- ``secret`` -- one block per SNS topic and SQS queue (storing ARN/URL)

**Prefect deployments**

- ``hello-flow`` -- example flow for verifying the stack
- ``alabama-publicportal-backfill`` -- Alabama court scraper backfill
- ``sqs-listener`` -- listens for messages on the outbox queue
- ``follow-up`` -- runs after the SQS listener completes

**Prefect automations**

- ``sqs-listener-trigger`` -- fires the SQS listener on a custom event
  (4-minute debounce)
- ``follow-up-trigger`` -- fires the follow-up flow when the SQS
  listener completes

Running a flow
~~~~~~~~~~~~~~

Trigger a deployed flow manually::

    prefect deployment run 'hello-flow/hello-flow'

Configuration
~~~~~~~~~~~~~

Environment variables are managed in ``mise.toml``:

.. list-table::
   :header-rows: 1
   :widths: 30 50 20

   * - Variable
     - Description
     - Default
   * - ``PREFECT_API_URL``
     - Prefect server API endpoint
     - ``http://localhost:4200/api``
   * - ``LOCALSTACK_DATA_DIR``
     - Persistent storage for LocalStack
     - ``.localstack``
   * - ``DOCKER_HOST_SOCKET``
     - Path to Podman socket
     - (machine-dependent)
   * - ``TS_AUTHKEY``
     - Tailscale ephemeral auth key (optional)
     - unset

Find your Podman socket path::

    podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}'

Deploying on AWS
----------------

TBD
