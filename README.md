# en-banc

Prefect flow orchestration for Free Law Project, running locally with Podman, LocalStack, and Pulumi.

## Prerequisites

- [Podman](https://podman.io/) with `podman compose`
- [mise](https://mise.jdx.dev/)
- [uv](https://docs.astral.sh/uv/)
- [Pulumi](https://www.pulumi.com/docs/install/)

Ensure the Podman machine is running:

```bash
podman machine start
```

## Setup

Install Python dependencies:

```bash
uv sync
```

## Configuration

Environment variables are set in `mise.toml`. Uncomment and edit as needed:

| Variable | Description | Default |
|---|---|---|
| `PREFECT_API_URL` | Prefect server API endpoint | `http://localhost:4200/api` |
| `LOCALSTACK_DATA_DIR` | Persistent storage for LocalStack | `.localstack` in project root |
| `DOCKER_HOST_SOCKET` | Path to Podman socket | `/run/podman/podman.sock` |
| `TS_AUTHKEY` | Tailscale ephemeral auth key (optional) | unset |

Find your Podman socket path with:

```bash
podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}'
```

## Tasks

### Start services

Starts Postgres, Prefect server, Docker worker, and LocalStack:

```bash
mise run up
```

The Prefect UI is available at http://localhost:4200.

### Deploy flows

Creates the Docker work pool (if needed) and deploys all flows:

```bash
mise run deploy
```

Trigger a flow run:

```bash
prefect deployment run 'hello-flow/hello-flow'
```

### Provision infrastructure

Provisions AWS resources in LocalStack, creates corresponding Prefect blocks, and registers the deployment — all via Pulumi:

```bash
mise run infra
```

On first run this generates the Pulumi Prefect provider SDK locally (via `pulumi package add`). Subsequent runs skip this step.

This provisions:

**AWS resources (LocalStack):**
- **S3 buckets:** `scrapers`, `emails`
- **SNS topics:** `email-notices`
- **SQS queues:** `outbox`

**Prefect blocks:**
- `aws-credentials` — `localstack-creds` block with test credentials
- `s3-bucket` — one block per S3 bucket (`scrapers`, `emails`)
- `json` — one block per SNS topic and SQS queue (storing ARN/URL and endpoint)

**Prefect deployment:**
- `hello-flow` deployment targeting the `docker-pool` work pool

## Project structure

```
.
├── docker/
│   ├── docker-compose.yml   # Prefect server, worker, Postgres, LocalStack
│   ├── Dockerfile           # Flow runner image (includes Tailscale)
│   └── entrypoint.sh        # Starts Tailscale before running flows
├── infrastructure/
│   ├── __main__.py          # Pulumi program for AWS + Prefect resources
│   └── Pulumi.yaml          # Pulumi config (LocalStack + Prefect server)
├── hello.py                 # Example 3-step flow
├── prefect.yaml             # Flow deployment definitions
├── mise.toml                # Environment variables and tasks
└── pyproject.toml           # Python dependencies
```

## Tailscale (optional)

Flow-run containers can join your tailnet automatically. Generate a reusable ephemeral auth key from the [Tailscale admin console](https://login.tailscale.com/admin/settings/keys) and set `TS_AUTHKEY` in `mise.toml`. Each container will appear on your tailnet with the Prefect flow-run name as its hostname.
