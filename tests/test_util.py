import trio
from trio.testing import wait_all_tasks_blocked
from snekomatic.util import hash_json, Pulse


def test_hash_json():
    assert hash_json({"a": 1, "b": 2}) == hash_json({"b": 2, "a": 1})
    assert hash_json({"a": 1}) != hash_json({"a": 2})


async def test_Pulse_basics(nursery):
    p = Pulse()

    seen_pulses = 0

    async def background():
        nonlocal seen_pulses
        async for _ in p.subscribe():
            seen_pulses += 1

    nursery.start_soon(background)

    # The loop always runs once initially, whether or not pulse() has been
    # called:
    await wait_all_tasks_blocked()
    assert seen_pulses == 1

    # Calling pulse() lets it run again:
    p.pulse()
    await wait_all_tasks_blocked()
    assert seen_pulses == 2

    # Multiple pulses are coalesced into one:
    p.pulse()
    p.pulse()
    p.pulse()
    await wait_all_tasks_blocked()
    assert seen_pulses == 3


async def test_Pulse_while_task_is_elsewhere(autojump_clock, nursery):
    p = Pulse()

    seen_pulses = 0

    async def background():
        nonlocal seen_pulses
        async for _ in p.subscribe():
            seen_pulses += 1
            await trio.sleep(10)

    nursery.start_soon(background)

    # Double-check that it's all idle and settled waiting for a pulse
    await trio.sleep(5)
    assert seen_pulses == 1
    await trio.sleep(10)
    assert seen_pulses == 1

    # Wake it up
    p.pulse()

    # Now it's sitting in trio.sleep()...
    await trio.sleep(5)
    assert seen_pulses == 2

    # ...when another pulse arrives.
    p.pulse()

    # It still wakes up though
    await trio.sleep(10)
    assert seen_pulses == 3


async def test_Pulse_subscribe_independence(autojump_clock, nursery):
    p = Pulse()

    seen_pulses = [0, 0]

    async def background(i, sleep_time):
        nonlocal seen_pulses
        async for _ in p.subscribe():
            seen_pulses[i] += 1
            await trio.sleep(sleep_time)

    nursery.start_soon(background, 0, 10)
    nursery.start_soon(background, 1, 100)

    await trio.sleep(5)
    assert seen_pulses == [1, 1]

    p.pulse()
    await trio.sleep(10)

    assert seen_pulses == [2, 1]

    p.pulse()
    p.pulse()
    await trio.sleep(10)

    assert seen_pulses == [3, 1]

    await trio.sleep(100)
    assert seen_pulses == [3, 2]

    p.pulse()
    await trio.sleep(100)
    assert seen_pulses == [4, 3]
