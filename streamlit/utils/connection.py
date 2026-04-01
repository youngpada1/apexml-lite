import os


def get_session():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        from snowflake.snowpark import Session
        return Session.builder.configs({
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "private_key_path": os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
            "authenticator": "SNOWFLAKE_JWT",
            "role": os.environ.get("SNOWFLAKE_ROLE", "APEXML_READER"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "APEXML_WH"),
            "database": os.environ.get("SNOWFLAKE_DATABASE", "APEXML_DB"),
            "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PROD"),
        }).create()
