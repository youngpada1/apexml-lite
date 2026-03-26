"""Generate a JWT token for Snowflake SQL API key pair authentication."""

import base64
import hashlib
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from ingestion.src import config


def _load_private_key():
    key_path = Path(config.SNOWFLAKE_PRIVATE_KEY_PATH)
    with open(key_path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


def _get_public_key_fingerprint(private_key) -> str:
    public_key = private_key.public_key()
    public_key_der = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    digest = hashlib.sha256(public_key_der).digest()
    return "SHA256:" + base64.b64encode(digest).decode("utf-8")


def generate_jwt() -> str:
    private_key = _load_private_key()
    fingerprint = _get_public_key_fingerprint(private_key)

    account = config.SNOWFLAKE_ACCOUNT.upper()
    user = config.SNOWFLAKE_USER.upper()
    qualified_username = f"{account}.{user}"

    now = int(time.time())
    expiry = now + 3600  # 1 hour

    import json

    header = {"alg": "RS256", "typ": "JWT"}
    payload = {
        "iss": f"{qualified_username}.{fingerprint}",
        "sub": qualified_username,
        "iat": now,
        "exp": expiry,
    }

    def b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")

    header_enc = b64url(json.dumps(header).encode())
    payload_enc = b64url(json.dumps(payload).encode())
    signing_input = f"{header_enc}.{payload_enc}".encode()

    signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    signature_enc = b64url(signature)

    return f"{header_enc}.{payload_enc}.{signature_enc}"
