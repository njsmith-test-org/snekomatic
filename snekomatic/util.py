from base64 import urlsafe_b64encode
from canonicaljson import encode_canonical_json
import hashlib
import attr
import trio

# Gives a deterministic, globally unique, relatively human-friendly hash of an
# arbitrary JSON object. Currently we use SHA-256 truncated to 128 bits,
# base64-encoded, with base64's useless padding removed.
def hash_json(args):
    blob = encode_canonical_json(args)
    h = hashlib.sha256()
    h.update(blob)
    return urlsafe_b64encode(h.digest()[:16]).strip(b"=").decode("ascii")


@attr.s
class Pulse:
    _count = attr.ib(default=0)
    _wakeup = attr.ib(factory=trio.Event)

    def pulse(self):
        self._count += 1
        self._wakeup.set()
        self._wakeup = trio.Event()

    # yields immediately at startup, and then again after pulse() is called.
    # Pulses may be coalesced, but will never be lost.
    async def subscribe(self):
        max_seen = -1
        while True:
            if self._count > max_seen:
                max_seen = self._count
                yield
            else:
                await self._wakeup.wait()
