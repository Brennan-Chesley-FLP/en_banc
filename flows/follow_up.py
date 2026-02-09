"""Follow-up flow — placeholder triggered after SQS listener completes."""

from prefect import flow


@flow(log_prints=True)
def follow_up() -> None:
    print("Insert follow up flow here")


if __name__ == "__main__":
    follow_up()
