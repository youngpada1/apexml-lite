import os
from pathlib import Path
from cryptography.hazmat.primitives import serialization


def get_session():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        from snowflake.snowpark import Session
        with open(Path(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]), "rb") as f:
            private_key = serialization.load_pem_private_key(f.read(), password=None)
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return Session.builder.configs({
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "private_key": private_key_bytes,
            "role": os.environ.get("SNOWFLAKE_ROLE", "APEXML_READER"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "APEXML_WH"),
            "database": os.environ.get("SNOWFLAKE_DATABASE", "APEXML_DB"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PROD"),
        }).create()
