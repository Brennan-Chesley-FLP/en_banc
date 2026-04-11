import pulumi
import pulumi_aws as aws

# ---------------------------------------------------------------------------
# AWS resources (LocalStack)
# ---------------------------------------------------------------------------

s3_buckets = ["scrapers", "emails"]

buckets = {}

for name in s3_buckets:
    buckets[name] = aws.s3.Bucket(name, bucket=name)


