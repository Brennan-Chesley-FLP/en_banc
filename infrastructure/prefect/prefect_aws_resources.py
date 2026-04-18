import pulumi
import pulumi_aws as aws

# ---------------------------------------------------------------------------
# S3 buckets (via MinIO, S3-compatible)
# ---------------------------------------------------------------------------

s3_buckets = ["scrapers", "emails"]

buckets = {}

for name in s3_buckets:
    buckets[name] = aws.s3.Bucket(name, bucket=name)


