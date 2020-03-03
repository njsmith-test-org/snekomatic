# Persistent channels.
#
# These let us pass messages between different parts of the app, with the nice
# property that they're persistent and write-once, so if the app restarts,
# then the next call to messages(...) will automatically start over from the
# beginning. This makes it easier to implement idempotent operations.

import pendulum
from weakref import WeakValueDictionary

from .util import Pulse
from .db import open_session, ChannelMessage

_MESSAGE_DELIVERED_PULSES = WeakValueDictionary()


def _pulse_for(domain, channel):
    return _MESSAGE_DELIVERED_PULSES.setdefault((domain, channel), Pulse())


async def messages(domain, channel):
    max_seen = -1
    async for pulse in _pulse_for(domain, channel).subscribe():
        print(f"pulsed! ({max_seen})")
        with open_session() as session:
            try:
                print(session.query(ChannelMessage).one().__dict__)
            except:
                pass
            messages = (
                session.query(ChannelMessage)
                .filter_by(domain=domain, channel=channel)
                .filter(ChannelMessage.order > max_seen)
                .order_by(ChannelMessage.order)
                .all()
            )
            print(messages)
            for message in messages:
                yield message.message
                if message.final:
                    return
                max_seen = message.order


def send_message(domain, channel, message_id, message, *, final):
    with open_session() as session:
        existing = (
            session.query(ChannelMessage)
            .filter_by(domain=domain, channel=channel, message_id=message_id)
            .one_or_none()
        )
        if existing is not None:
            if message != existing.message or final != existing.final:
                raise ValueError(
                    f"conflicting payloads for {domain}:{channel}:{message_id}: "
                    f"{existing.message} (final={existing.final}) "
                    f"vs {message} (final=final)"
                )
            return

        already_finished = session.query(
            session.query(ChannelMessage)
            .filter_by(domain=domain, channel=channel, final=True)
            .exists()
        ).scalar()
        if already_finished:
            raise ValueError(
                f"received new message for {domain}:{channel}, "
                f"but it was already marked complete (new message: {message})"
            )

        new = ChannelMessage(
            domain=domain,
            channel=channel,
            message_id=message_id,
            message=message,
            final=final,
            created=pendulum.now(tz="UTC"),
        )
        session.add(new)

    print("pulsing!")
    _pulse_for(domain, channel).pulse()
