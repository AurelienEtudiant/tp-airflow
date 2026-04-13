from airflow.hooks.base import BaseHook
from minio import Minio


class MinioHook(BaseHook):
    def __init__(self, minio_conn_id="minio_default"):
        self.minio_conn_id = minio_conn_id
        self.conn = BaseHook.get_connection(minio_conn_id)

    def get_client(self):
        extras = self.conn.extra_dejson if self.conn.extra else {}
        endpoint_url = extras.get("endpoint_url", f"{self.conn.host}:9000")

        if endpoint_url.startswith("http://"):
            endpoint_url = endpoint_url.replace("http://", "")
        elif endpoint_url.startswith("https://"):
            endpoint_url = endpoint_url.replace("https://", "")

        return Minio(
            endpoint=endpoint_url,
            access_key=self.conn.login,
            secret_key=self.conn.password,
            secure=False,
        )
