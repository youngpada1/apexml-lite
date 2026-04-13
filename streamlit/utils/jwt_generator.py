import base64
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
    load_pem_private_key,
)


class JWTGenerator:
    LIFETIME = timedelta(minutes=59)
    RENEWAL_DELTA = timedelta(minutes=54)
    ALGORITHM = "RS256"

    def __init__(self, account: str, user: str, private_key_path: str):
        self.account = self._prepare_account(account).upper()
        self.user = user.upper()
        self.qualified_username = f"{self.account}.{self.user}"
        self.renew_time = datetime.now(timezone.utc)
        self.token = None

        with open(Path(private_key_path), "rb") as f:
            self.private_key = load_pem_private_key(f.read(), password=None, backend=default_backend())

    def _prepare_account(self, account: str) -> str:
        if ".global" not in account:
            idx = account.find(".")
            if idx > 0:
                return account[:idx]
        else:
            idx = account.find("-")
            if idx > 0:
                return account[:idx]
        return account

    def _fingerprint(self) -> str:
        pub_der = self.private_key.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
        digest = hashlib.sha256(pub_der).digest()
        return "SHA256:" + base64.b64encode(digest).decode("utf-8")

    def get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self.token is None or self.renew_time <= now:
            self.renew_time = now + self.RENEWAL_DELTA
            payload = {
                "iss": f"{self.qualified_username}.{self._fingerprint()}",
                "sub": self.qualified_username,
                "iat": now,
                "exp": now + self.LIFETIME,
            }
            token = jwt.encode(payload, key=self.private_key, algorithm=self.ALGORITHM)
            self.token = token.decode("utf-8") if isinstance(token, bytes) else token
        return self.token
