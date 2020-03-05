import os
import hmac
import json
import secrets
import asks
from contextlib import contextmanager
import pendulum

# Partial duplicate of gidgethub.sansio.validate_event
def sign_webhook(body: bytes, secret: str):
    hmaccer = hmac.new(secret.encode("ascii"), msg=body, digestmod="sha1")
    sig = "sha1=" + hmaccer.hexdigest()
    return sig


def fake_webhook(event_type, payload, secret):
    body = json.dumps(payload).encode("ascii")
    headers = {
        "x-github-event": event_type,
        "content-type": "application/json",
        "x-github-delivery": secrets.token_hex(16),
    }
    if secret is not None:
        headers["x-hub-signature"] = sign_webhook(body, secret)
    return headers, body


@contextmanager
def save_environ():
    saved_env = dict(os.environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved_env)


# Workaround until https://github.com/sdispater/pendulum/pull/445 is fixed
@contextmanager
def mock_time(t):
    pendulum.set_test_now(t)
    try:
        yield
    finally:
        pendulum.set_test_now()
