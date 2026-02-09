from prefect import flow, task


@task
def fetch_data() -> dict:
    """Step 1: Fetch some data."""
    print("Fetching data...")
    return {"items": ["apple", "banana", "cherry"], "count": 3}


@task
def process_data(data: dict) -> list[str]:
    """Step 2: Process the data."""
    print(f"Processing {data['count']} items...")
    return [item.upper() for item in data["items"]]


@task
def save_results(results: list[str]) -> str:
    """Step 3: Save the results."""
    output = ", ".join(results)
    print(f"Saving results: {output}")
    return output


@flow(log_prints=True)
def hello_flow() -> str:
    """A 3-step example flow."""
    data = fetch_data()
    processed = process_data(data)
    result = save_results(processed)
    return result


if __name__ == "__main__":
    hello_flow()
