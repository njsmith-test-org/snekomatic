import pytest
from collections import defaultdict
import trio
from snekomatic.channels import messages, send_message


async def test_channels(heroku_style_pg, nursery, autojump_clock):
    records = {}

    async def collect_messages(domain, channel, key):
        records[key] = []
        async for message in messages(domain, channel):
            records[key].append(message)
        records[key].append("-done-")

    nursery.start_soon(collect_messages, "d1", "c1", "k1")
    nursery.start_soon(collect_messages, "d1", "c1", "k2")
    nursery.start_soon(collect_messages, "d1", "c2", "d1-c2")
    nursery.start_soon(collect_messages, "d2", "c1", "d2-c1")

    await trio.sleep(1)
    assert records == {"k1": [], "k2": [], "d1-c2": [], "d2-c1": []}

    send_message("d1", "c1", "m1", {"hi": "there"}, final=False)

    await trio.sleep(1)
    assert records == {
        "k1": [{"hi": "there"}],
        "k2": [{"hi": "there"}],
        "d1-c2": [],
        "d2-c1": [],
    }

    # Duplicate messages with the same message-id are ignored
    send_message("d1", "c1", "m1", {"hi": "there"}, final=False)

    await trio.sleep(1)
    assert records == {
        "k1": [{"hi": "there"}],
        "k2": [{"hi": "there"}],
        "d1-c2": [],
        "d2-c1": [],
    }

    # Varying messages with the same message-id raise an exception
    with pytest.raises(ValueError):
        send_message("d1", "c1", "m1", {"hi": "oops"}, final=False)

    with pytest.raises(ValueError):
        send_message("d1", "c1", "m1", {"hi": "there"}, final=True)

    await trio.sleep(1)
    assert records == {
        "k1": [{"hi": "there"}],
        "k2": [{"hi": "there"}],
        "d1-c2": [],
        "d2-c1": [],
    }

    # If another task joins us, it gets the old messages + future messages
    nursery.start_soon(collect_messages, "d1", "c1", "k3")

    await trio.sleep(1)
    assert records == {
        "k1": [{"hi": "there"}],
        "k2": [{"hi": "there"}],
        "k3": [{"hi": "there"}],
        "d1-c2": [],
        "d2-c1": [],
    }

    # Final messages terminate iteration
    send_message("d1", "c1", "m2", "the end!", final=True)

    await trio.sleep(1)
    assert records == {
        "k1": [{"hi": "there"}, "the end!", "-done-"],
        "k2": [{"hi": "there"}, "the end!", "-done-"],
        "k3": [{"hi": "there"}, "the end!", "-done-"],
        "d1-c2": [],
        "d2-c1": [],
    }

    # Trying to send another message on a finished channel is an error
    with pytest.raises(ValueError):
        send_message("d1", "c1", "m3", "MOAR", final=False)

    # We can add another task, and it gets the messages from the completed
    # channel, in the correct order
    await collect_messages("d1", "c1", "k4")
    assert records == {
        "k1": [{"hi": "there"}, "the end!", "-done-"],
        "k2": [{"hi": "there"}, "the end!", "-done-"],
        "k3": [{"hi": "there"}, "the end!", "-done-"],
        "k4": [{"hi": "there"}, "the end!", "-done-"],
        "d1-c2": [],
        "d2-c1": [],
    }

    # If we send on another channel, those messages are kept distinct
    send_message("d2", "c1", "m1", {"another": "channel"}, final=True)

    await trio.sleep(1)
    assert records == {
        "k1": [{"hi": "there"}, "the end!", "-done-"],
        "k2": [{"hi": "there"}, "the end!", "-done-"],
        "k3": [{"hi": "there"}, "the end!", "-done-"],
        "k4": [{"hi": "there"}, "the end!", "-done-"],
        "d1-c2": [],
        "d2-c1": [{"another": "channel"}, "-done-"],
    }
