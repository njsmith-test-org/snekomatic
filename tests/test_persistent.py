import pytest
import trio
from snekomatic.persistent import unify, PDict


def test_unify():
    assert unify(1, 1) == 1
    assert unify("hi", "hi") == "hi"
    assert unify([1, "hi"], [1, "hi"]) == [1, "hi"]

    with pytest.raises(ValueError):
        unify(1, 2)
    with pytest.raises(ValueError):
        unify(1, "hi")
    with pytest.raises(ValueError):
        unify(["hi"], "hi")

    assert unify({}, {}) == {}
    assert unify({"a": 1}, {"b": ["hi"]}) == {"a": 1, "b": ["hi"]}

    assert unify({"a": 1, "b": 2}, {"b": 2, "c": 3}) == {
        "a": 1,
        "b": 2,
        "c": 3,
    }

    assert unify(
        {"a": 1, "subdict": {"s1": 2}}, {"b": 3, "subdict": {"s2": 4}}
    ) == {"a": 1, "b": 3, "subdict": {"s1": 2, "s2": 4}}


async def test_PDict(heroku_style_pg, nursery, autojump_clock):
    snapshots = {}

    async def collect_snapshots(domain, item, key):
        pd = PDict(domain, item)
        snapshots[key] = []
        async for snapshot in pd.subscribe():
            snapshots[key].append(snapshot)

    nursery.start_soon(collect_snapshots, "d1", "i1", "k1")
    nursery.start_soon(collect_snapshots, "d1", "i1", "k2")
    nursery.start_soon(collect_snapshots, "d1", "i2", "d1-i2")
    nursery.start_soon(collect_snapshots, "d2", "i1", "d2-i1")

    await trio.sleep(1)
    assert snapshots == {"k1": [{}], "k2": [{}], "d1-i2": [{}], "d2-i1": [{}]}

    PDict("d1", "i1").update({"hi": "there"})

    await trio.sleep(1)
    assert snapshots == {
        "k1": [{}, {"hi": "there"}],
        "k2": [{}, {"hi": "there"}],
        "d1-i2": [{}],
        "d2-i1": [{}],
    }

    # Redundant updates are collapsed out
    PDict("d1", "i1").update({"hi": "there"})

    await trio.sleep(1)
    assert snapshots == {
        "k1": [{}, {"hi": "there"}],
        "k2": [{}, {"hi": "there"}],
        "d1-i2": [{}],
        "d2-i1": [{}],
    }

    # Inconsistent updates raise an exception and do nothing
    with pytest.raises(ValueError):
        PDict("d1", "i1").update({"hi": "oops"})

    await trio.sleep(1)
    assert snapshots == {
        "k1": [{}, {"hi": "there"}],
        "k2": [{}, {"hi": "there"}],
        "d1-i2": [{}],
        "d2-i1": [{}],
    }

    # If another task joins us, it gets the latest snapshot + future snapshots
    nursery.start_soon(collect_snapshots, "d1", "i1", "k3")

    await trio.sleep(1)
    assert snapshots == {
        "k1": [{}, {"hi": "there"}],
        "k2": [{}, {"hi": "there"}],
        "k3": [{"hi": "there"}],
        "d1-i2": [{}],
        "d2-i1": [{}],
    }

    PDict("d1", "i1").update({"new": "data"})

    await trio.sleep(1)
    assert snapshots == {
        "k1": [{}, {"hi": "there"}, {"hi": "there", "new": "data"}],
        "k2": [{}, {"hi": "there"}, {"hi": "there", "new": "data"}],
        "k3": [{"hi": "there"}, {"hi": "there", "new": "data"}],
        "d1-i2": [{}],
        "d2-i1": [{}],
    }

    # Trying to add inconsistent data raises an exception
    with pytest.raises(ValueError):
        PDict("d1", "i1").update({"hi": "somewhere else"})

    # If we write to another PDict, it's kept distinct
    PDict("d2", "i1").update({"another": "PDict"})

    await trio.sleep(1)
    assert snapshots == {
        "k1": [{}, {"hi": "there"}, {"hi": "there", "new": "data"}],
        "k2": [{}, {"hi": "there"}, {"hi": "there", "new": "data"}],
        "k3": [{"hi": "there"}, {"hi": "there", "new": "data"}],
        "d1-i2": [{}],
        "d2-i1": [{}, {"another": "PDict"}],
    }
