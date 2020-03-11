import attr
from weakref import WeakValueDictionary
from glom import glom

from .util import Pulse
from .db import open_session, PDictDBEntry

_UPDATE_PULSES = WeakValueDictionary()


def _pulse_for(domain, item):
    return _UPDATE_PULSES.setdefault((domain, item), Pulse())


def unify(v1, v2):
    if v1 == v2:
        return v1

    if isinstance(v1, dict) and isinstance(v2, dict):
        unified = {}
        for k in v1.keys() - v2.keys():
            unified[k] = v1[k]
        for k in v2.keys() - v1.keys():
            unified[k] = v2[k]
        for k in v1.keys() & v2.keys():
            unified[k] = unify(v1[k], v2[k])
        return unified

    raise ValueError(f"can't unify {v1!r} with {v2!r}")


@attr.s(frozen=True)
class PDict:
    """A persistent dict. The dict value is mutable, but "monotonic" -- new
    keys can be added to the dict or sub-dicts, but once a key is present its
    value will never change.

    The idea is that multiple independent parts of the program can write
    updates into the dict as new information is accumulated, and then other
    parts of the program can subscribe to updates and wait until the
    information they want is available.

    It's safe to write the same settings multiple times; identical updates
    will be silently coalesced. Attempts to write inconsistent values will
    raise an exception and have no effect.

    Values can be arbitrary JSON-serializable data.

    """

    domain = attr.ib()
    item = attr.ib()

    def update(self, new_value):
        if not isinstance(new_value, dict):
            raise TypeError(
                f"PDict value should be a dict, not {new_value!r}"
            )
        with open_session() as session:
            existing = (
                session.query(PDictDBEntry)
                .filter_by(domain=self.domain, item=self.item)
                .one_or_none()
            )
            if existing is None:
                session.add(
                    PDictDBEntry(
                        domain=self.domain, item=self.item, value=new_value
                    )
                )
            else:
                try:
                    existing.value = unify(existing.value, new_value)
                except ValueError as exc:
                    raise ValueError(
                        f"inconsistent values for PDict({self.domain}, {self.item}): "
                        f"current={existing.value!r}, new={new_value!r}"
                    ) from exc
            session.commit()
        _pulse_for(self.domain, self.item).pulse()

    async def subscribe(self):
        """Yields a sequence of dict snapshots."""
        last_yield = None
        async for _ in _pulse_for(self.domain, self.item).subscribe():
            with open_session() as session:
                existing = (
                    session.query(PDictDBEntry)
                    .filter_by(domain=self.domain, item=self.item)
                    .one_or_none()
                )
                if existing is None:
                    value = {}
                else:
                    value = existing.value
                    session.commit()
            if value != last_yield:
                yield value
                last_yield = value

    async def glom(self, key):
        async for state in self.subscribe():
            try:
                return glom(state, key)
            except LookupError:
                continue
