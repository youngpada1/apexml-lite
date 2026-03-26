import os


SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]        # e.g. QJCHIDB-BU78481
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]              # e.g. FLAVS
SNOWFLAKE_PRIVATE_KEY_PATH = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]  # path to rsa_key.p8
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "APEXML_DB")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "RAW")
SNOWFLAKE_PROD_SCHEMA = os.environ.get("SNOWFLAKE_PROD_SCHEMA", "PROD")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "APEXML_WH")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE", "APEXML_WRITER")

SNOWFLAKE_SQL_API_URL = (
    f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com/api/v2/statements"
)

# Account locator format needed for SQL API (e.g. LP64516.eu-central-2)
SNOWFLAKE_ACCOUNT_LOCATOR = os.environ.get("SNOWFLAKE_ACCOUNT_LOCATOR", SNOWFLAKE_ACCOUNT)
SNOWFLAKE_SQL_API_URL = (
    f"https://{SNOWFLAKE_ACCOUNT_LOCATOR}.snowflakecomputing.com/api/v2/statements"
)

OPENF1_BASE_URL = "https://api.openf1.org/v1"
