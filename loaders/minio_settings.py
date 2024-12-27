import os

minio_username = os.getenv("MINIO_USERNAME")
minio_password = os.getenv("MINIO_PASSWORD")
minio_host = os.getenv("MINIO_HOST")
minio_bucket = os.getenv("MINIO_BUCKET", "fhi-dev")
