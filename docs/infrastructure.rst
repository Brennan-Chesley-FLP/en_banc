Infrastructure (Pulumi)
======================

All infrastructure is defined as Python code using `Pulumi <https://www.pulumi.com/>`_.
The setup is split into two independent Pulumi projects so that Docker
infrastructure can be managed without the Prefect server running.

.. contents:: On this page
   :local:
   :depth: 2


Two-Stack Architecture
----------------------

**Why two stacks?**
The Prefect Pulumi provider requires the Prefect server API to be
reachable.  When the server container is stopped, ``pulumi refresh`` and
``pulumi up`` fail for every Prefect resource, blocking management of
unrelated Docker containers.  Splitting into two projects lets the
foundation layer be managed independently.

Foundation (``en-banc-foundation``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run from the **repository root**.  Manages Docker containers, networks,
volumes, and images.  Uses only the ``pulumi-docker`` provider.

.. code-block:: bash

   pulumi up          # from repo root

Resources:

- Docker network (``en-banc-network``)
- PostgreSQL containers: ``prefect`` (7104), ``warehouse`` (7101),
  ``courtlistener`` (7102), ``replica_client_a`` (7103)
- Prefect server container (7100)
- LocalStack container on remote host (7110)

App (``en-banc-app``)
~~~~~~~~~~~~~~~~~~~~~

Run from the **app/** subdirectory.  Manages Prefect API objects and
LocalStack AWS resources.  Uses ``pulumi-aws`` and ``pulumi-prefect``
providers.

.. code-block:: bash

   cd app && pulumi up

Resources:

- S3 buckets (via LocalStack): ``scrapers``, ``emails``
- Prefect blocks: AWS credentials, S3 bucket blocks, SQLAlchemy
  database connectors
- Prefect work pools: ``docker-pool``, ``scraper-pool``, ``sync-pool``
- Prefect flows and deployments

Both projects share the same Python virtual environment and import
shared modules from ``infrastructure/``.


Services and Ports
------------------

All local services are exposed in the **7100** range:

.. list-table::
   :header-rows: 1
   :widths: 30 10 20

   * - Service
     - Port
     - Host
   * - Prefect server
     - 7100
     - localhost
   * - PostgreSQL (warehouse)
     - 7101
     - localhost
   * - PostgreSQL (courtlistener)
     - 7102
     - localhost
   * - PostgreSQL (replica_client_a)
     - 7103
     - localhost
   * - PostgreSQL (prefect)
     - 7104
     - localhost
   * - MinIO API (S3)
     - 7110
     - mini.bopp-justice.ts.net
   * - MinIO Console
     - 7111
     - mini.bopp-justice.ts.net
   * - pgAdmin
     - 7120
     - localhost
   * - CourtListener Selenium VNC
     - 7130
     - localhost
   * - CourtListener Elasticsearch
     - 7140
     - localhost
   * - CourtListener Django
     - 7150
     - localhost


Remote Docker Host
------------------

The LocalStack container runs on a remote Mac mini
(``mini.bopp-justice.ts.net``) via Pulumi's SSH Docker transport.
The remote host runs Podman with Colima's Docker CLI.

Requirements on the remote host:

- ``docker`` binary on the non-interactive SSH ``PATH``
  (symlink or ``~/.zshenv`` PATH entry for ``/usr/local/bin`` and
  ``/opt/homebrew/bin``)
- Podman socket running (``systemctl --user enable --now podman.socket``)
- ``DOCKER_HOST`` pointing at the Podman socket in ``~/.zshenv``

The SSH host defaults to ``ssh://bc@mini.bopp-justice.ts.net`` and can
be overridden:

.. code-block:: bash

   pulumi config set remote-docker-host ssh://user@other-host


Adding a Database
-----------------

To add a new PostgreSQL database, edit the ``DATABASES`` dict in
``infrastructure/databases.py``:

.. code-block:: python

   DATABASES = {
       "prefect": {"port": 7104, "alias": "prefect-db"},
       "warehouse": {"port": 7101},
       "courtlistener": {"port": 7102},
       "replica_client_a": {"port": 7103},
       "my_new_db": {"port": 7105},  # add here
   }

This automatically creates:

1. A Docker container with a named volume (foundation stack)
2. A Prefect SQLAlchemy connector block (app stack)

Use the optional ``alias`` key if the Docker network hostname should
differ from the database name (e.g., ``prefect-db`` to avoid colliding
with the Prefect server's network alias).


Provisioning
------------

Use ``mise`` to bring everything up in order:

.. code-block:: bash

   mise run infra            # both stacks
   mise run infra-foundation # Docker only
   mise run infra-app        # Prefect API only

Or run Pulumi directly:

.. code-block:: bash

   # Foundation
   pulumi up

   # App (requires Prefect server to be running)
   cd app && pulumi up
